"""Acceptance tests for the passive OutageRouter observation callback."""

import threading
import time
import unittest

from deadline_fixture import FixtureServer, respond, stall_headers
from outage_router import OutageRouter
from router import DeadlineExceeded, Router, Unavailable


class FakeClock:
    def __init__(self, start=0.0):
        self.now = start
        self.lock = threading.Lock()

    def __call__(self):
        with self.lock:
            return self.now

    def set(self, value):
        with self.lock:
            self.now = value


class Sink:
    def __init__(self):
        self.records = []
        self.lock = threading.Lock()

    def __call__(self, record):
        with self.lock:
            self.records.append(record)

    def wait_count(self, count, timeout=5.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self.lock:
                if len(self.records) >= count:
                    return True
            time.sleep(0.005)
        return False

    def snapshot(self):
        with self.lock:
            return list(self.records)


class BoomSink(Sink):
    def __call__(self, record):
        super().__call__(record)
        raise RuntimeError("observer failure must be passive")


def start_call(gateway, *args, **kwargs):
    outcome = {}

    def run():
        try:
            outcome["value"] = gateway.get_cached(*args, **kwargs)
        except BaseException as exc:  # reported after join
            outcome["error"] = exc

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread, outcome


def join_call(test, thread, outcome):
    thread.join(5.0)
    test.assertFalse(thread.is_alive(), "call did not finish")
    return outcome


def wait_for_waiters(gateway, count, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with gateway._cache_condition:
            if gateway._cache_waiter_count >= count:
                return True
        time.sleep(0.002)
    return False


def cache_headers():
    return [
        "Cache-Control: max-age=60",
        'ETag: "acceptance-etag"',
    ]


class ObservationHookTest(unittest.TestCase):
    def setUp(self):
        self.servers = []

    def tearDown(self):
        for server in reversed(self.servers):
            server.__exit__(None, None, None)

    def make_server(self):
        server = FixtureServer().__enter__()
        self.servers.append(server)
        return server

    def build_gateway(self, *, observer=None, timeout=2.0, clock=None):
        self.main = self.make_server()
        self.backup = self.make_server()
        self.clock = clock if clock is not None else time.monotonic
        self.router = Router(
            (self.main.address, self.backup.address),
            clock=self.clock,
            cooldown=30.0,
            timeout=timeout,
            deadline_mode=True,
        )
        self.sink = observer if observer is not None else Sink()
        self.gateway = OutageRouter(
            self.router,
            {
                "main": ("east", self.main.address),
                "backup": ("north", self.backup.address),
            },
            observe=self.sink,
        )
        return self.gateway

    def install_old_plan(self):
        return self.gateway.install(
            {0: ("main", ["backup"])},
            {"main": 0, "backup": 1},
        )

    def test_late_old_call_keeps_old_revision_and_site(self):
        gateway = self.build_gateway()
        self.install_old_plan()

        release_old = threading.Event()
        self.main.behavior = lambda conn: (
            release_old.wait(5.0),
            conn.send_head(200, b"old-late"))
        # The tuple lambda above evaluates send_head after the wait.

        outcome = {}

        def old_call():
            try:
                outcome["old"] = gateway.get(
                    0, "/old-late", budget=5.0)
            except BaseException as exc:
                outcome["old_error"] = exc

        old_thread = threading.Thread(target=old_call, daemon=True)
        old_thread.start()
        self.assertTrue(self.main.wait_dials(1))

        # Revision 2 makes the new foreground call use the other real loopback
        # site; the old call remains blocked at revision 1.
        revision, _ = gateway.install(
            {0: ("backup", ["main"])},
            {"main": 1, "backup": 0},
        )
        self.assertEqual(revision, 2)
        self.backup.behavior = respond(200, b"new-fast")
        self.assertEqual(
            gateway.get(0, "/new-fast", budget=5.0),
            (200, b"new-fast"))

        release_old.set()
        old_thread.join(5.0)
        self.assertFalse(old_thread.is_alive())
        self.assertEqual(outcome.get("old"), (200, b"old-late"))

        self.assertTrue(self.sink.wait_count(2))
        records = self.sink.snapshot()
        new_record = records[0]
        old_record = records[1]

        self.assertEqual(
            set(new_record),
            {"revision", "site", "completed_ms", "latency_ms", "outcome"})
        self.assertEqual(new_record["revision"], 2)
        self.assertEqual(new_record["site"], "backup")
        self.assertEqual(new_record["outcome"], "delivered")
        self.assertEqual(old_record["revision"], 1)
        self.assertEqual(old_record["site"], "main")
        self.assertEqual(old_record["outcome"], "delivered")
        self.assertIsNot(new_record, old_record)

        # Both exchanges were genuine loopback HTTP requests.
        self.assertEqual(self.main.paths, ["/old-late"])
        self.assertEqual(self.backup.paths, ["/new-fast"])

        # A later plan can select the old site again; attribution remains
        # per call/revision rather than being merged by site identity.
        gateway.install(
            {0: ("main", ["backup"])},
            {"main": 0, "backup": 1},
        )
        self.main.behavior = respond(200, b"again")
        self.assertEqual(gateway.get(0, "/again", budget=5.0), (200, b"again"))
        self.assertTrue(self.sink.wait_count(3))
        third = self.sink.snapshot()[2]
        self.assertEqual((third["revision"], third["site"]), (3, "main"))

    def test_callback_runs_after_call_and_endpoint_slots_are_released(self):
        states = []

        def observe(record):
            with self.router._state_lock:
                busy = any(
                    state["busy"] is not None
                    for state in self.router._states.values())
            states.append((self.router._active_calls, busy, dict(record)))

        gateway = self.build_gateway(observer=observe)
        self.install_old_plan()
        self.main.behavior = respond(200, b"slot")

        self.assertEqual(gateway.get(0, "/slot", budget=5.0), (200, b"slot"))
        self.assertEqual(len(states), 1)
        active_calls, busy, record = states[0]
        self.assertEqual(active_calls, 0)
        self.assertFalse(busy)
        self.assertEqual(record["outcome"], "delivered")

    def test_monotonic_clock_values_are_floored_and_latency_is_capped(self):
        for elapsed, expected_completed, expected_latency in (
                (0.1239, 123, 123),
                (1.5, 1500, 1000)):
            clock = FakeClock()
            gateway = self.build_gateway(clock=clock)
            self.install_old_plan()

            def behavior(connection, elapsed=elapsed):
                # Deterministically fix the result time immediately before
                # the complete loopback response is written.
                clock.set(elapsed)
                connection.send_head(200, b"clock")

            self.main.behavior = behavior
            self.assertEqual(
                gateway.get(0, "/clock", budget=5.0), (200, b"clock"))
            record = self.sink.snapshot()[-1]
            self.assertEqual(record["completed_ms"], expected_completed)
            self.assertEqual(record["latency_ms"], expected_latency)

    def test_no_plan_and_validation_rejections_emit_nothing(self):
        gateway = self.build_gateway()

        with self.assertRaises(Unavailable):
            gateway.get(0, "/no-plan", budget=1.0)
        with self.assertRaises(Unavailable):
            gateway.get_cached(0, "/no-plan", budget=1.0)
        self.assertEqual(self.sink.snapshot(), [])

        self.install_old_plan()
        bad_calls = [
            lambda: gateway.get(99, "/bad-bucket", budget=1.0),
            lambda: gateway.get(0, "/bad-zone", budget=1.0,
                                outage_zone="atlantis"),
            lambda: gateway.get(0, "/bad-budget", budget=0),
            lambda: gateway.get(0, "bad-path", budget=1.0),
            lambda: gateway.get_cached(99, "/bad-bucket", budget=1.0),
            lambda: gateway.get_cached(0, "/bad-language", budget=1.0,
                                      language="fr"),
            lambda: gateway.get_cached(0, "/bad-budget", budget=-1.0),
            lambda: gateway.get_cached(0, "bad-path", budget=1.0),
        ]
        for bad_call in bad_calls:
            with self.subTest(call=bad_call):
                with self.assertRaises(ValueError):
                    bad_call()

        self.assertEqual(self.sink.snapshot(), [])
        self.assertEqual(self.main.dials, 0)
        self.assertEqual(self.backup.dials, 0)

    def test_delivered_404_is_observed_as_delivered(self):
        gateway = self.build_gateway()
        self.install_old_plan()
        self.main.behavior = respond(404, b"missing")

        self.assertEqual(
            gateway.get(0, "/missing", budget=5.0), (404, b"missing"))
        self.assertEqual(
            gateway.get_cached(0, "/missing-cached", budget=5.0),
            (404, b"missing"))
        records = self.sink.snapshot()
        self.assertEqual(len(records), 2)
        self.assertTrue(all(record["outcome"] == "delivered"
                            for record in records))
        self.assertTrue(all(0 <= record["latency_ms"] <= 1000
                            for record in records))

    def test_deadline_is_observed(self):
        release_stall = threading.Event()
        gateway = self.build_gateway(timeout=1.0)
        self.install_old_plan()
        self.main.behavior = stall_headers(release_stall)
        try:
            with self.assertRaises(DeadlineExceeded):
                gateway.get(0, "/slow", budget=0.15)
        finally:
            release_stall.set()

        records = self.sink.snapshot()
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record["outcome"], "deadline")
        self.assertEqual((record["revision"], record["site"]), (1, "main"))
        self.assertIsInstance(record["completed_ms"], int)
        self.assertFalse(isinstance(record["completed_ms"], bool))
        self.assertTrue(0 <= record["latency_ms"] <= 1000)

    def test_unavailable_is_observed_for_using_an_approved_plan(self):
        gateway = self.build_gateway()
        self.install_old_plan()
        # Close the real loopback server, making the single selected site
        # fail and leaving no candidate to try.
        self.main.__exit__(None, None, None)

        with self.assertRaises(Unavailable):
            gateway.get(0, "/down", budget=2.0)
        records = self.sink.snapshot()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["outcome"], "unavailable")
        self.assertEqual(records[0]["site"], "main")

    def test_observer_exception_does_not_change_successful_result(self):
        gateway = self.build_gateway(observer=BoomSink())
        self.install_old_plan()
        self.main.behavior = respond(200, b"boom-visible")

        # The RuntimeError raised by the sink must not escape or retry.
        self.assertEqual(
            gateway.get(0, "/boom", budget=5.0), (200, b"boom-visible"))
        self.assertEqual(self.main.dials, 1)

    def test_observer_exception_does_not_replace_request_exception(self):
        gateway = self.build_gateway(observer=BoomSink())
        self.install_old_plan()
        self.main.__exit__(None, None, None)

        with self.assertRaises(Unavailable):
            gateway.get(0, "/down-boom", budget=2.0)

    def test_fresh_cache_hit_sends_no_packet_but_adds_a_record(self):
        gateway = self.build_gateway()
        self.install_old_plan()
        body = b"cacheable-body"
        self.main.behavior = respond(200, body, extra=cache_headers())

        for _ in range(2):
            self.assertEqual(
                gateway.get_cached(0, "/hot", budget=5.0), (200, body))

        self.assertEqual(self.main.dials, 1)
        records = self.sink.snapshot()
        self.assertEqual(len(records), 2)
        self.assertEqual([r["outcome"] for r in records],
                         ["delivered", "delivered"])
        self.assertIsNot(records[0], records[1])
        self.assertEqual(records[0]["revision"], records[1]["revision"])

    def test_coalesced_calls_each_receive_their_own_observation(self):
        gateway = self.build_gateway()
        self.install_old_plan()
        release_origin = threading.Event()

        def blocked_cacheable(connection):
            release_origin.wait(5.0)
            connection.send_head(
                200, b"shared-body", extra=cache_headers())

        self.main.behavior = blocked_cacheable
        calls = [
            start_call(gateway, 0, "/shared", budget=5.0)
            for _ in range(4)
        ]
        self.assertTrue(self.main.wait_dials(1))
        self.assertTrue(wait_for_waiters(gateway, 3))

        release_origin.set()
        for thread, outcome in calls:
            result = join_call(self, thread, outcome)
            self.assertNotIn("error", result)
            self.assertEqual(result["value"], (200, b"shared-body"))

        self.assertEqual(self.main.dials, 1)
        self.assertTrue(self.sink.wait_count(4))
        records = self.sink.snapshot()
        self.assertEqual(len(records), 4)
        self.assertTrue(all(r["outcome"] == "delivered" for r in records))
        self.assertEqual(gateway.router._active_calls, 0)


if __name__ == "__main__":
    unittest.main()
