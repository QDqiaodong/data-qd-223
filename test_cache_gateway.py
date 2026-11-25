"""Offline regression tests for shareable, coalesced gateway GETs.

All HTTP traffic goes to real temporary servers on loopback.  Cache age is
controlled with a manually advanced monotonic clock; blocked origins are
released with events rather than timing guesses.
"""
import math
import threading
import time
import unittest

from deadline_fixture import FixtureServer, respond
from outage_router import OutageRouter
from router import DeadlineExceeded, Router, Unavailable


class FakeClock:
    def __init__(self, start=0.0):
        self.now = start
        self.lock = threading.Lock()

    def __call__(self):
        with self.lock:
            return self.now

    def advance(self, seconds):
        with self.lock:
            self.now += seconds
            return self.now


def start_call(gateway, *args, **kwargs):
    outcome = {}

    def run():
        try:
            outcome["value"] = gateway.get_cached(*args, **kwargs)
        except BaseException as exc:  # reported by join_outcome
            outcome["error"] = exc

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread, outcome


def join_outcome(test, thread, outcome, timeout=5.0):
    thread.join(timeout)
    test.assertFalse(thread.is_alive(), "cached gateway call did not finish")
    return outcome


def wait_paths(server, count, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with server.lock:
            if len(server.paths) >= count:
                return True
        time.sleep(0.005)
    return False


def wait_for_waiters(gateway, count, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with gateway._cache_condition:
            if gateway._cache_waiter_count >= count:
                return True
        time.sleep(0.002)
    return False


def cache_extra(etag, max_age=60, *, vary=True, set_cookie=False,
                cache_control=None):
    extra = [
        "Cache-Control: "
        + (cache_control if cache_control is not None
           else "max-age=%d" % max_age),
        'ETag: "%s"' % etag,
    ]
    if vary:
        extra.append("Vary: Accept-Language")
    if set_cookie:
        extra.append("Set-Cookie: session=do-not-cache")
    return extra


def not_modified_extra(etag, max_age=60, *, content_length=None,
                       vary=True):
    extra = cache_extra(etag, max_age, vary=vary)
    if content_length is not None:
        extra.append("Content-Length: %d" % content_length)
    return extra


class CacheGatewayTestBase(unittest.TestCase):
    def setUp(self):
        self.servers = []

    def tearDown(self):
        for server in reversed(self.servers):
            server.__exit__(None, None, None)

    def make_server(self):
        server = FixtureServer().__enter__()
        self.servers.append(server)
        return server

    def build_gateway(self, *, clock=None, timeout=5.0, cooldown=5.0):
        self.main = self.make_server()
        self.backup = self.make_server()
        self.clock = clock if clock is not None else FakeClock()
        self.router = Router(
            (self.main.address, self.backup.address),
            clock=self.clock, timeout=timeout, cooldown=cooldown,
            deadline_mode=True)
        self.gateway = OutageRouter(self.router, {
            "main": ("east", self.main.address),
            "backup": ("north", self.backup.address),
        })
        self.gateway.install(
            {0: ("main", ["backup"])},
            {"main": 0, "backup": 1})
        return self.gateway

    def header_values(self, server, connection_index, name):
        headers = server.connections[connection_index].request_headers
        return headers.get(name.lower(), [])


class LanguageSiteAndRevalidationTest(CacheGatewayTestBase):
    def test_language_site_identity_fresh_hits_and_304_with_length(self):
        gateway = self.build_gateway()
        chinese = "中文正文".encode("utf-8")
        english = b"english-body"
        backup_body = b"backup-body"

        def main_behavior(connection):
            language = connection.request_headers.get(
                "accept-language", ["zh"])[0]
            if language == "en":
                body, etag = english, "en-v1"
            else:
                body, etag = chinese, "zh-v1"
            connection.send_head(
                200, body, extra=cache_extra(etag, 100))

        self.main.behavior = main_behavior
        self.backup.behavior = respond(
            200, backup_body, extra=cache_extra("backup-v1", 100))

        self.assertEqual(
            gateway.get_cached(0, "/i18n", budget=5.0), (200, chinese))
        self.assertEqual(
            gateway.get_cached(0, "/i18n", budget=5.0, language="en"),
            (200, english))
        self.assertEqual(
            gateway.get_cached(0, "/i18n", budget=5.0,
                               outage_zone="east"),
            (200, backup_body))

        self.assertEqual(self.main.dials, 2)
        self.assertEqual(self.backup.dials, 1)
        self.assertEqual(self.header_values(self.main, 0, "Accept-Language"),
                         ["zh"])
        self.assertEqual(self.header_values(self.main, 1, "Accept-Language"),
                         ["en"])
        self.assertEqual(
            self.header_values(self.backup, 0, "Accept-Language"), ["zh"])
        self.assertEqual(self.main.paths, ["/i18n", "/i18n"])
        self.assertEqual(self.backup.paths, ["/i18n"])

        # Fresh repetitions create no new origin requests.
        self.assertEqual(
            gateway.get_cached(0, "/i18n", budget=5.0), (200, chinese))
        self.assertEqual(
            gateway.get_cached(0, "/i18n", budget=5.0, language="en"),
            (200, english))
        self.assertEqual(
            gateway.get_cached(0, "/i18n", budget=5.0,
                               outage_zone="east"),
            (200, backup_body))
        self.assertEqual((self.main.dials, self.backup.dials), (2, 1))

        # Exactly at max-age the Chinese entry is stale and revalidates with
        # its original strong validator.
        self.clock.advance(100.0)

        def revalidate_chinese(connection):
            self.assertEqual(
                connection.request_headers.get("if-none-match"),
                ['"zh-v1"'])
            connection.send_head(
                304, b"",
                content_length=len(chinese),
                extra=not_modified_extra("zh-v1", 200))

        self.main.behavior = revalidate_chinese
        self.assertEqual(
            gateway.get_cached(0, "/i18n", budget=5.0), (200, chinese))
        self.assertTrue(wait_paths(self.main, 3))
        self.assertEqual(
            self.header_values(self.main, 2, "If-None-Match"), ['"zh-v1"'])

        # The 304's new max-age=200 starts at fake t=100, so t=299 is fresh.
        self.clock.advance(199.0)
        self.assertEqual(
            gateway.get_cached(0, "/i18n", budget=5.0), (200, chinese))
        self.assertEqual(self.main.dials, 3)

    def test_mismatched_304_validator_does_not_deliver_body(self):
        gateway = self.build_gateway()
        body = b"tagged-body"
        self.main.behavior = respond(
            200, body, extra=cache_extra("good-tag", 60))
        path = "/bad-304-tag"
        self.assertEqual(gateway.get_cached(0, path, budget=5.0),
                         (200, body))

        self.clock.advance(60.0)
        self.main.behavior = lambda conn: conn.send_head(
            304, b"", extra=not_modified_extra("wrong-tag", 60))

        with self.assertRaises(Unavailable):
            gateway.get_cached(0, path, budget=5.0)
        self.assertEqual(
            self.header_values(self.main, 1, "If-None-Match"),
            ['"good-tag"'])
        self.assertEqual(self.main.dials, 2)

    def test_304_without_content_length_is_supported(self):
        gateway = self.build_gateway()
        body = b"no-content-length-304"
        self.main.behavior = respond(
            200, body, extra=cache_extra("no-cl-tag", 60))
        path = "/304-no-cl"
        self.assertEqual(
            gateway.get_cached(0, path, budget=5.0), (200, body))
        self.clock.advance(60.0)

        def raw_304_without_length(connection):
            connection.write(
                b"HTTP/1.1 304 Not Modified\r\n"
                b'ETag: "no-cl-tag"\r\n'
                b"Cache-Control: max-age=120\r\n"
                b"Vary: Accept-Language\r\n"
                b"Connection: close\r\n\r\n")

        self.main.behavior = raw_304_without_length
        self.assertEqual(
            gateway.get_cached(0, path, budget=5.0), (200, body))
        self.assertEqual(self.main.dials, 2)
        self.clock.advance(119.0)
        self.assertEqual(
            gateway.get_cached(0, path, budget=5.0), (200, body))
        self.assertEqual(self.main.dials, 2)

    def test_cold_request_receiving_304_is_protocol_failure(self):
        gateway = self.build_gateway()
        self.main.behavior = lambda conn: conn.send_head(
            304, b"", extra=not_modified_extra("cold-tag", 60))

        with self.assertRaises(Unavailable):
            gateway.get_cached(0, "/cold-304", budget=5.0)
        self.assertEqual(self.main.dials, 1)
        self.assertNotIn(
            "if-none-match",
            self.main.connections[0].request_headers)

    def test_non_shareable_200_header_variations_are_delivered_not_cached(self):
        gateway = self.build_gateway()
        invalid_extras = [
            ["Cache-Control: no-store", 'ETag: "x"'],
            ["Cache-Control: max-age=60"],
            ["Cache-Control: max-age=60", "ETag: W/\"weak\""],
            ["Cache-Control: max-age=60", "ETag: \"bad tag\""],
            ["Cache-Control: max-age=0", 'ETag: "x"'],
            ["Cache-Control: max-age=3601", 'ETag: "x"'],
            ["Cache-Control: max-age=60", 'ETag: "x"',
             "Set-Cookie: a=b"],
            ["Cache-Control: max-age=60", 'ETag: "x"', "Vary: *"],
            ["Cache-Control: max-age=60, public", 'ETag: "x"'],
        ]
        for index, extras in enumerate(invalid_extras):
            path = "/uncacheable-%d" % index
            body = ("private-%d" % index).encode()
            self.main.behavior = respond(200, body, extra=extras)
            with self.subTest(path=path):
                self.assertEqual(
                    gateway.get_cached(0, path, budget=5.0), (200, body))
                dials = self.main.dials
                self.assertEqual(
                    gateway.get_cached(0, path, budget=5.0), (200, body))
                self.assertEqual(self.main.dials, dials + 1)

    def test_404_removes_old_entry_and_is_not_cached(self):
        gateway = self.build_gateway()
        path = "/maybe-gone"
        self.main.behavior = respond(
            200, b"here", extra=cache_extra("gone-tag", 60))
        self.assertEqual(
            gateway.get_cached(0, path, budget=5.0), (200, b"here"))
        self.clock.advance(61.0)

        self.main.behavior = respond(404, b"missing")
        self.assertEqual(
            gateway.get_cached(0, path, budget=5.0), (404, b"missing"))
        self.assertNotIn(
            (0, self.main.address, path, "zh"), gateway._cache)

        self.main.behavior = respond(404, b"missing-again")
        self.assertEqual(
            gateway.get_cached(0, path, budget=5.0),
            (404, b"missing-again"))
        self.assertEqual(self.main.dials, 3)


class CoalescingTest(CacheGatewayTestBase):
    def test_short_follower_timeout_keeps_revalidation_and_other_hit(self):
        gateway = self.build_gateway()
        hot = b"hot-body"
        fresh = b"fresh-body"
        self.main.behavior = respond(
            200, hot, extra=cache_extra("hot-tag", 10))
        self.assertEqual(
            gateway.get_cached(0, "/hot", budget=5.0), (200, hot))
        self.main.behavior = respond(
            200, fresh, extra=cache_extra("fresh-tag", 1000))
        self.assertEqual(
            gateway.get_cached(0, "/fresh", budget=5.0), (200, fresh))
        self.clock.advance(10.0)

        release = threading.Event()

        def blocked_304(connection):
            release.wait(10.0)
            connection.send_head(
                304, b"",
                content_length=len(hot),
                extra=not_modified_extra("hot-tag", 100))

        self.main.behavior = blocked_304
        leader, leader_out = start_call(
            gateway, 0, "/hot", budget=5.0)
        self.assertTrue(wait_paths(self.main, 3))

        long_worker, long_out = start_call(
            gateway, 0, "/hot", budget=5.0)
        short_worker, short_out = start_call(
            gateway, 0, "/hot", budget=0.2)
        self.assertTrue(wait_for_waiters(gateway, 2))

        self.clock.advance(0.2)
        short_worker.join(2.0)
        self.assertFalse(short_worker.is_alive())
        self.assertIsInstance(short_out.get("error"), DeadlineExceeded)
        self.assertEqual(self.main.dials, 3)

        # An unrelated fresh identity remains serviceable while /hot is held.
        self.assertEqual(
            gateway.get_cached(0, "/fresh", budget=5.0), (200, fresh))
        self.assertEqual(self.main.dials, 3)

        release.set()
        join_outcome(self, leader, leader_out)
        long_worker.join(5.0)
        self.assertFalse(long_worker.is_alive())
        self.assertEqual(leader_out.get("value"), (200, hot))
        self.assertEqual(long_out.get("value"), (200, hot))
        self.assertNotIn("error", long_out)
        self.assertEqual(self.main.dials, 3)

        self.assertEqual(
            gateway.get_cached(0, "/hot", budget=5.0), (200, hot))
        self.assertEqual(self.main.dials, 3)

    def test_initiator_deadline_allows_follower_continuation(self):
        gateway = self.build_gateway(timeout=5.0, cooldown=5.0)
        body = b"continue-body"
        self.main.behavior = respond(
            200, body, extra=cache_extra("continue-tag", 10))
        path = "/continue"
        self.assertEqual(
            gateway.get_cached(0, path, budget=5.0), (200, body))
        self.clock.advance(10.0)

        first_release = threading.Event()
        second_release = threading.Event()
        state = {"requests": 0}
        state_lock = threading.Lock()

        def behavior(connection):
            with state_lock:
                state["requests"] += 1
                number = state["requests"]
            if number == 1:
                first_release.wait(5.0)
                try:
                    connection.send_head(
                        304, b"",
                        content_length=len(body),
                        extra=not_modified_extra("continue-tag", 100))
                except OSError:
                    pass
            else:
                second_release.wait(5.0)
                connection.send_head(
                    304, b"",
                    content_length=len(body),
                    extra=not_modified_extra("continue-tag", 100))

        self.main.behavior = behavior
        first_worker, first_out = start_call(
            gateway, 0, path, budget=0.3)
        self.assertTrue(wait_paths(self.main, 2))
        with self.router._state_lock:
            old_token = self.router._states[self.main.address]["busy"]
        self.assertIsNotNone(old_token)

        follower, follower_out = start_call(
            gateway, 0, path, budget=5.0)
        self.assertTrue(wait_for_waiters(gateway, 1))

        # The initiator's own budget expires first.  Router ties the clamped
        # attempt deadline to this request deadline and must not cool down.
        self.clock.advance(0.3)
        join_outcome(self, first_worker, first_out)
        self.assertIsInstance(first_out.get("error"), DeadlineExceeded)
        with self.router._state_lock:
            current_token = self.router._states[self.main.address]["busy"]
        self.assertIsNot(current_token, old_token)
        self.assertEqual(
            self.router._states[self.main.address]["excluded_until"],
            float("-inf"))

        # The follower wins the continuation only after the old attempt has
        # released its endpoint token; then release that second exchange.
        self.assertTrue(wait_paths(self.main, 3))
        with self.router._state_lock:
            self.assertIsNot(
                self.router._states[self.main.address]["busy"], old_token)
        first_release.set()
        second_release.set()

        follower.join(5.0)
        self.assertFalse(follower.is_alive())
        self.assertEqual(follower_out.get("value"), (200, body))
        self.assertEqual(self.main.dials, 3)
        for index in (1, 2):
            self.assertEqual(
                self.header_values(self.main, index, "If-None-Match"),
                ['"continue-tag"'])

    def test_several_waiters_coalesce_the_retry_after_private_response(self):
        gateway = self.build_gateway()
        first_release = threading.Event()
        state = {"requests": 0}
        lock = threading.Lock()

        def behavior(connection):
            with lock:
                state["requests"] += 1
                number = state["requests"]
            if number == 1:
                first_release.wait(5.0)
                connection.send_head(
                    200, b"only-leader",
                    extra=["Cache-Control: no-store"])
            else:
                connection.send_head(
                    200, b"shared-retry",
                    extra=cache_extra("retry-tag", 60))

        self.main.behavior = behavior
        leader, leader_out = start_call(
            gateway, 0, "/private-batch", budget=5.0)
        self.assertTrue(wait_paths(self.main, 1))
        followers = [
            start_call(gateway, 0, "/private-batch", budget=5.0)
            for _ in range(3)
        ]
        self.assertTrue(wait_for_waiters(gateway, 3))

        first_release.set()
        join_outcome(self, leader, leader_out)
        self.assertEqual(
            leader_out.get("value"), (200, b"only-leader"))
        self.assertTrue(wait_paths(self.main, 2))

        for worker, outcome in followers:
            worker.join(5.0)
            self.assertFalse(worker.is_alive())
            self.assertEqual(
                outcome.get("value"), (200, b"shared-retry"))
        self.assertEqual(self.main.dials, 2)

    def test_non_shareable_response_is_private_and_never_cached(self):
        gateway = self.build_gateway()
        first_release = threading.Event()
        state = {"requests": 0}
        lock = threading.Lock()

        def behavior(connection):
            with lock:
                state["requests"] += 1
                number = state["requests"]
            if number == 1:
                first_release.wait(5.0)
                connection.send_head(
                    200, b"private-one",
                    extra=["Cache-Control: no-store"])
            elif number == 2:
                connection.send_head(
                    200, b"private-two",
                    extra=["Cache-Control: no-store"])
            else:
                connection.send_head(
                    200, b"public-three",
                    extra=cache_extra("three-tag", 60))

        self.main.behavior = behavior
        first_worker, first_out = start_call(
            gateway, 0, "/private", budget=5.0)
        self.assertTrue(wait_paths(self.main, 1))

        second_worker, second_out = start_call(
            gateway, 0, "/private", budget=5.0)
        self.assertTrue(wait_for_waiters(gateway, 1))
        first_release.set()

        join_outcome(self, first_worker, first_out)
        second_worker.join(5.0)
        self.assertFalse(second_worker.is_alive())
        self.assertEqual(first_out.get("value"), (200, b"private-one"))
        self.assertEqual(second_out.get("value"), (200, b"private-two"))
        self.assertNotEqual(
            first_out.get("value"), second_out.get("value"))

        self.assertEqual(
            gateway.get_cached(0, "/private", budget=5.0),
            (200, b"public-three"))
        self.assertEqual(self.main.dials, 3)
        self.assertEqual(
            gateway.get_cached(0, "/private", budget=5.0),
            (200, b"public-three"))
        self.assertEqual(self.main.dials, 3)
        cached_bodies = [entry.body for entry in gateway._cache.values()]
        self.assertIn(b"public-three", cached_bodies)
        self.assertNotIn(b"private-one", cached_bodies)
        self.assertNotIn(b"private-two", cached_bodies)


class FailureAndEvictionTest(CacheGatewayTestBase):
    def test_upstream_failure_clears_flight_and_does_not_serve_stale(self):
        gateway = self.build_gateway(cooldown=5.0)
        stale_body = b"stale-body"
        self.main.behavior = respond(
            200, stale_body, extra=cache_extra("stale-tag", 60))
        path = "/fail"
        self.assertEqual(
            gateway.get_cached(0, path, budget=5.0), (200, stale_body))

        self.clock.advance(61.0)
        self.main.behavior = respond(503, b"try-later")
        with self.assertRaises(Unavailable):
            gateway.get_cached(0, path, budget=5.0)
        self.assertEqual(gateway._in_flight, {})

        # After cooldown, the retained stale validator may revalidate/fetch,
        # but the 503 itself did not deliver stale bytes.
        self.clock.advance(10.0)
        new_body = b"new-body"
        self.main.behavior = respond(
            200, new_body, extra=cache_extra("new-tag", 60))
        self.assertEqual(
            gateway.get_cached(0, path, budget=5.0), (200, new_body))
        self.assertEqual(self.main.dials, 3)
        self.assertEqual(
            self.header_values(self.main, 2, "If-None-Match"),
            ['"stale-tag"'])

    def test_disconnect_does_not_serve_expired_body(self):
        gateway = self.build_gateway(cooldown=5.0)
        body = b"disconnect-stale"
        self.main.behavior = respond(
            200, body, extra=cache_extra("dc-tag", 10))
        path = "/disconnect"
        self.assertEqual(
            gateway.get_cached(0, path, budget=5.0), (200, body))
        self.clock.advance(11.0)

        def close_without_response(connection):
            connection.sock.close()

        self.main.behavior = close_without_response
        with self.assertRaises(Unavailable):
            gateway.get_cached(0, path, budget=5.0)
        self.assertEqual(gateway._in_flight, {})

    def test_confirmed_failure_is_broadcast_without_retry_storm(self):
        gateway = self.build_gateway(cooldown=30.0)
        release = threading.Event()

        def blocked_503(connection):
            release.wait(5.0)
            connection.send_head(503, b"down")

        self.main.behavior = blocked_503
        workers = []
        outcomes = []
        for _ in range(4):
            worker, outcome = start_call(
                gateway, 0, "/batch-fail", budget=5.0)
            workers.append(worker)
            outcomes.append(outcome)
        self.assertTrue(wait_paths(self.main, 1))
        self.assertTrue(wait_for_waiters(gateway, 3))

        release.set()
        for worker, outcome in zip(workers, outcomes):
            worker.join(5.0)
            self.assertFalse(worker.is_alive())
            self.assertIsInstance(outcome.get("error"), Unavailable)
        self.assertEqual(self.main.dials, 1)
        self.assertEqual(gateway._in_flight, {})

    def test_cache_respects_128_entry_limit(self):
        gateway = self.build_gateway()

        def dynamic(connection):
            number = int(connection.path.rsplit("-", 1)[1])
            body = ("body-%03d" % number).encode("ascii")
            connection.send_head(
                200, body,
                extra=cache_extra("tag-%03d" % number, 3600))

        self.main.behavior = dynamic
        for number in range(129):
            path = "/item-%03d" % number
            self.assertEqual(
                gateway.get_cached(0, path, budget=5.0),
                (200, ("body-%03d" % number).encode()))

        self.assertEqual(len(gateway._cache), 128)
        dials = self.main.dials
        self.assertEqual(
            gateway.get_cached(0, "/item-000", budget=5.0),
            (200, b"body-000"))
        self.assertEqual(self.main.dials, dials + 1)
        self.assertEqual(
            gateway.get_cached(0, "/item-000", budget=5.0),
            (200, b"body-000"))
        self.assertEqual(self.main.dials, dials + 1)
        self.assertEqual(len(gateway._cache), 128)


class ValidationAndConcurrencyTest(CacheGatewayTestBase):
    def test_invalid_arguments_and_no_plan_send_nothing(self):
        server = self.make_server()
        second = self.make_server()
        router = Router(
            (server.address, second.address), clock=FakeClock(),
            timeout=1.0, cooldown=1.0, deadline_mode=True)
        unplanned = OutageRouter(router, {
            "a": ("zone-a", server.address),
            "b": ("zone-b", second.address),
        })
        with self.assertRaises(Unavailable):
            unplanned.get_cached(0, "/x", budget=1.0)

        gateway = self.build_gateway()
        bad_cases = [
            dict(bucket=9, path="/x", budget=1.0),
            dict(bucket=0, path="/x", budget=1.0,
                 outage_zone="nowhere"),
            dict(bucket=0, path="/x", budget=1.0, language="fr"),
            dict(bucket=0, path="/x", budget=1.0, language=None),
            dict(bucket=0, path="/x", budget=0),
            dict(bucket=0, path="/x", budget=-1.0),
            dict(bucket=0, path="/x", budget=math.inf),
            dict(bucket=0, path="/x", budget=math.nan),
            dict(bucket=0, path="//x", budget=1.0),
            dict(bucket=0, path="/bad path", budget=1.0),
            dict(bucket=0, path="/café", budget=1.0),
            dict(bucket=0, path="/" + "a" * 2048, budget=1.0),
        ]
        for kwargs in bad_cases:
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(ValueError):
                    gateway.get_cached(**kwargs)
        self.assertEqual(self.main.dials, 0)
        self.assertEqual(self.backup.dials, 0)

        long_path = "/" + "a" * 2047
        self.main.behavior = respond(
            200, b"long-ok", extra=cache_extra("long-tag", 60))
        self.assertEqual(
            gateway.get_cached(0, long_path, budget=5.0),
            (200, b"long-ok"))
        self.assertEqual(self.main.paths[-1], long_path)

    def test_at_most_32_call_slots_while_batch_uses_one_origin(self):
        gateway = self.build_gateway(timeout=5.0)
        release = threading.Event()
        gateway_identity_path = "/slot-batch"
        self.main.behavior = lambda conn: (
            release.wait(5.0),
            conn.send_head(
                200, b"batch", extra=cache_extra("batch-tag", 60)))

        workers = []
        outcomes = []
        for _ in range(32):
            worker, outcome = start_call(
                gateway, 0, gateway_identity_path, budget=5.0)
            workers.append(worker)
            outcomes.append(outcome)

        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            with self.router._call_lock:
                active = self.router._active_calls
            if active == 32:
                break
            time.sleep(0.005)
        self.assertEqual(active, 32)

        with self.assertRaises(Unavailable):
            gateway.get_cached(0, gateway_identity_path, budget=5.0)

        release.set()
        for worker, outcome in zip(workers, outcomes):
            worker.join(5.0)
            self.assertFalse(worker.is_alive())
            self.assertEqual(outcome.get("value"), (200, b"batch"))
        self.assertEqual(self.main.dials, 1)


if __name__ == "__main__":
    unittest.main()
