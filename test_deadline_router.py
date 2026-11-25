import math
import threading
import time
import unittest

from deadline_fixture import (
    FixtureServer,
    headers_5xx_then_block,
    respond,
    slow_drip,
    stall_headers,
    wait_then_respond,
)
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


def call_async(router, path, budget):
    outcome = {}

    def run():
        try:
            outcome["result"] = router.get(path, budget=budget)
        except BaseException as error:  # noqa: BLE001 - reported to the test
            outcome["error"] = error

    worker = threading.Thread(target=run, daemon=True)
    worker.start()
    return worker, outcome


def join_outcome(test, worker, outcome, timeout=5.0):
    worker.join(timeout)
    test.assertFalse(worker.is_alive(), "router call did not finish")
    return outcome


class DeadlineModeValidationTest(unittest.TestCase):
    def test_budget_is_required_and_validated_before_dialing(self):
        with FixtureServer() as server:
            router = Router((server.address,), clock=time.monotonic,
                            deadline_mode=True)
            for bad in (None, 0, -1, math.inf, math.nan, True, "0.5"):
                with self.subTest(budget=bad):
                    with self.assertRaises(ValueError):
                        if bad is None:
                            router.get("/resource")
                        else:
                            router.get("/resource", budget=bad)
            self.assertEqual(server.dials, 0)

    def test_invalid_paths_rejected_before_dialing(self):
        with FixtureServer() as server:
            router = Router((server.address,), clock=time.monotonic,
                            deadline_mode=True)
            for path in ("resource", "//resource", "", "/bad\r\nX: y",
                         "/bad path", "/café"):
                with self.subTest(path=path):
                    with self.assertRaises(ValueError):
                        router.get(path, budget=1.0)
            self.assertEqual(server.dials, 0)

    def test_constructor_validation(self):
        good = ("127.0.0.1", 1)
        cases = [
            dict(endpoints=()),
            dict(endpoints=(good, good)),
            dict(endpoints=(("localhost", 80),)),
            dict(endpoints=(("::1", 80),)),
            dict(endpoints=(("127.0.0.1", 0),)),
            dict(endpoints=(("127.0.0.1", 70000),)),
            dict(endpoints=(("127.0.0.1", 80),) * 17),
        ]
        for case in cases:
            with self.subTest(case=case):
                with self.assertRaises(ValueError):
                    Router(clock=time.monotonic, deadline_mode=True, **case)
        for bad_timeout in (0, -1, math.inf, math.nan):
            with self.subTest(timeout=bad_timeout):
                with self.assertRaises(ValueError):
                    Router((good,), clock=time.monotonic, timeout=bad_timeout,
                           deadline_mode=True)
        for bad_cooldown in (0, -1, math.inf, math.nan):
            with self.subTest(cooldown=bad_cooldown):
                with self.assertRaises(ValueError):
                    Router((good,), clock=time.monotonic,
                           cooldown=bad_cooldown, deadline_mode=True)
        # 16 distinct endpoints are the supported maximum.
        Router(tuple(("127.0.0.%d" % host, 80) for host in range(1, 17)),
               clock=time.monotonic, deadline_mode=True)

    def test_exact_size_limits_are_accepted(self):
        body = b"x" * (64 * 1024)
        with FixtureServer() as server:
            server.behavior = respond(200, body)
            router = Router((server.address,), clock=time.monotonic,
                            deadline_mode=True)
            self.assertEqual(router.get("/max", budget=2.0), (200, body))

    def test_budget_is_rejected_in_legacy_mode(self):
        router = Router((("127.0.0.1", 9),), clock=time.monotonic)
        with self.assertRaises(ValueError):
            router.get("/resource", budget=1.0)


class DeadlineBudgetTest(unittest.TestCase):
    def test_dripping_upstream_hits_request_budget_and_connection_closes(self):
        """Acceptance 1: budget ends a still-dripping response, no cooldown."""
        with FixtureServer() as server:
            server.behavior = slow_drip(server, interval=0.025,
                                        chunk=b"0123456789", length=60000)
            router = Router((server.address,), clock=time.monotonic,
                            timeout=1.5, cooldown=30, deadline_mode=True)
            started = time.monotonic()
            with self.assertRaises(DeadlineExceeded):
                router.get("/slow", budget=0.45)
            elapsed = time.monotonic() - started

            self.assertGreaterEqual(elapsed, 0.35)
            self.assertLess(elapsed, 1.0)
            connection = server.connection(0)
            self.assertTrue(
                connection.client_closed.wait(1.0),
                "router left the upstream connection open after its budget")
            self.assertLess(connection.closed_at - started, 1.0)
            self.assertEqual(server.dials, 1)

            # The budget timeout must not place the endpoint in cooldown.
            server.behavior = respond(200, b"fresh")
            self.assertEqual(router.get("/slow", budget=1.0), (200, b"fresh"))
            self.assertEqual(server.dials, 2)

            # Budget failures are also absent from the shared state.
            self.assertEqual(
                router._states[server.address]["excluded_until"],
                float("-inf"))

    def test_5xx_headers_fail_over_without_waiting_for_body(self):
        """Acceptance 3: conclusive 5xx headers are enough to switch."""
        release = threading.Event()
        try:
            with FixtureServer() as first, FixtureServer() as second:
                first.behavior = headers_5xx_then_block(
                    release, status=503, content_length=60000)
                second.behavior = respond(200, b"secondary")
                now = FakeClock(0.0)
                router = Router((first.address, second.address), clock=now,
                                timeout=2.0, cooldown=5.0, deadline_mode=True)
                started = time.monotonic()
                self.assertEqual(router.get("/resource", budget=2.0),
                                 (200, b"secondary"))
                self.assertLess(time.monotonic() - started, 0.5)
                self.assertEqual(first.dials, 1)
                self.assertEqual(second.dials, 1)
                self.assertGreater(
                    router._states[first.address]["excluded_until"], 0.0)
        finally:
            release.set()

    def test_attempt_timeout_then_failover_then_cooldown_then_404(self):
        """Acceptance 3: attempt deadline is absolute and failover continues."""
        stop = threading.Event()
        try:
            with FixtureServer() as first, FixtureServer() as second:
                first.behavior = stall_headers(stop)
                second.behavior = respond(200, b"complete-response")
                now = FakeClock(0.0)
                router = Router((first.address, second.address), clock=now,
                                timeout=0.25, cooldown=5.0,
                                deadline_mode=True)

                worker, outcome = call_async(router, "/resource", 2.0)
                self.assertTrue(first.first_connection.wait(5.0))
                now.set(0.30)  # past the attempt deadline, inside the budget
                outcome = join_outcome(self, worker, outcome)
                self.assertEqual(outcome.get("result"),
                                 (200, b"complete-response"))
                self.assertNotIn("error", outcome)
                self.assertEqual(first.dials, 1)
                self.assertEqual(second.dials, 1)

                # Before cooldown expires, starting at the first endpoint must
                # skip it and use the second one.
                with router._state_lock:
                    router.next_index = 0
                self.assertEqual(router.get("/resource", budget=2.0),
                                 (200, b"complete-response"))
                self.assertEqual(first.dials, 1)
                self.assertEqual(second.dials, 2)

                # Once cooldown expires the first endpoint serves a complete
                # 404 unchanged.
                first.behavior = respond(404, b"missing-body")
                now.set(5.31)  # failure recorded at 0.30, cooldown 5.0
                with router._state_lock:
                    router.next_index = 0
                self.assertEqual(router.get("/resource", budget=2.0),
                                 (404, b"missing-body"))
                self.assertEqual(first.dials, 2)
        finally:
            stop.set()

    def test_4xx_is_returned_without_trying_next_endpoint(self):
        with FixtureServer() as first, FixtureServer() as second:
            first.behavior = respond(404, b"not-here")
            second.behavior = respond(200, b"other")
            router = Router((first.address, second.address),
                            clock=time.monotonic, deadline_mode=True)
            self.assertEqual(router.get("/missing", budget=1.0),
                             (404, b"not-here"))
            self.assertEqual(second.dials, 0)

    def test_all_candidates_unavailable_without_deadline(self):
        stop = threading.Event()
        try:
            with FixtureServer() as first, FixtureServer() as second:
                first.behavior = stall_headers(stop)
                second.behavior = respond(503, b"down")
                clock = FakeClock(0.0)
                router = Router((first.address, second.address), clock=clock,
                                timeout=0.1, cooldown=5.0,
                                deadline_mode=True)
                worker, outcome = call_async(router, "/resource", 5.0)
                self.assertTrue(first.first_connection.wait(5.0))
                clock.set(0.15)  # first attempt fails at 0.1
                # Let the second endpoint answer its 503 right away; its own
                # attempt deadline (0.25) is far from the request deadline.
                outcome = join_outcome(self, worker, outcome)
                self.assertIsInstance(outcome.get("error"), Unavailable)
                self.assertEqual(first.dials, 1)
                self.assertEqual(second.dials, 1)
        finally:
            stop.set()

    def test_invalid_responses_are_upstream_failures(self):
        cases = {
            "short-body": lambda c: (c.send_head(200, content_length=100),
                                     c.write(b"tiny")),
            "no-length": lambda c: c.write(
                b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nbody"),
            "oversize": lambda c: c.send_head(200, content_length=65537),
            "premature": lambda c: c.write(
                b"HTTP/1.1 200 OK\r\nContent-Le"),
            "malformed-length": lambda c: c.write(
                b"HTTP/1.1 200 OK\r\nContent-Length: nope\r\n\r\n"),
            "conflicting-lengths": lambda c: c.write(
                b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n"
                b"Content-Length: 2\r\n\r\nab"),
            "chunked": lambda c: c.write(
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n"),
            "oversized-headers": lambda c: (
                c.write(b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nX-Pad: "),
                c.write(b"a" * (16 * 1024 + 1)),
                c.write(b"\r\n\r\nx")),
        }
        for name, behavior in cases.items():
            with self.subTest(case=name):
                with FixtureServer() as first, FixtureServer() as second:
                    first.behavior = behavior
                    second.behavior = respond(200, b"fallback")
                    router = Router((first.address, second.address),
                                    clock=time.monotonic, cooldown=30,
                                    deadline_mode=True)
                    self.assertEqual(router.get("/resource", budget=2.0),
                                     (200, b"fallback"))
                    self.assertEqual(first.dials, 1)
                    self.assertEqual(second.dials, 1)
                    self.assertGreater(
                        router._states[first.address]["excluded_until"], 0.0)


class DeadlineConcurrencyTest(unittest.TestCase):
    def test_busy_endpoint_is_not_redialed_and_other_endpoint_serves(self):
        """Acceptance 2: synchronize on the first connection being accepted."""
        release = threading.Event()
        try:
            with FixtureServer() as first, FixtureServer() as second:
                first.behavior = wait_then_respond(release, b"held")
                second.behavior = respond(200, b"secondary")
                router = Router((first.address, second.address),
                                clock=time.monotonic, timeout=2.0,
                                cooldown=30, deadline_mode=True)

                worker, outcome = call_async(router, "/held", 5.0)
                self.assertTrue(first.first_connection.wait(5.0),
                                "first request never reached the upstream")

                # The occupied endpoint must not be dialed a second time;
                # with another endpoint available the request succeeds there.
                self.assertEqual(router.get("/resource", budget=2.0),
                                 (200, b"secondary"))
                self.assertEqual(first.dials, 1)
                self.assertEqual(second.dials, 1)

                release.set()
                outcome = join_outcome(self, worker, outcome)
                self.assertEqual(outcome.get("result"), (200, b"held"))
        finally:
            release.set()

    def test_busy_endpoint_is_skipped_for_a_later_candidate(self):
        release = threading.Event()
        try:
            with FixtureServer() as first, FixtureServer() as second:
                first.behavior = wait_then_respond(release, b"held")
                second.behavior = respond(200, b"secondary")
                router = Router((first.address, second.address),
                                clock=time.monotonic, timeout=2.0,
                                cooldown=30, deadline_mode=True)
                worker, outcome = call_async(router, "/held", 5.0)
                self.assertTrue(first.first_connection.wait(5.0))

                # Force the second call to consider the busy endpoint first.
                with router._state_lock:
                    router.next_index = 0
                self.assertEqual(router.get("/resource", budget=2.0),
                                 (200, b"secondary"))
                self.assertEqual(first.dials, 1)
                self.assertEqual(second.dials, 1)

                release.set()
                join_outcome(self, worker, outcome)
        finally:
            release.set()

    def test_only_busy_endpoint_yields_unavailable_while_budget_remains(self):
        release = threading.Event()
        try:
            with FixtureServer() as only:
                only.behavior = wait_then_respond(release, b"held")
                router = Router((only.address,), clock=time.monotonic,
                                timeout=5.0, cooldown=30, deadline_mode=True)
                worker, outcome = call_async(router, "/held", 5.0)
                self.assertTrue(only.first_connection.wait(5.0))

                started = time.monotonic()
                with self.assertRaises(Unavailable):
                    router.get("/other", budget=2.0)
                self.assertLess(time.monotonic() - started, 0.5)
                self.assertEqual(only.dials, 1)

                release.set()
                outcome = join_outcome(self, worker, outcome)
                self.assertEqual(outcome.get("result"), (200, b"held"))
        finally:
            release.set()

    def test_at_most_32_concurrent_calls(self):
        class BlockingRouter(Router):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.entered = 0
                self.entered_lock = threading.Lock()
                self.release = threading.Event()

            def _get_with_deadline(self, path, budget):
                with self.entered_lock:
                    self.entered += 1
                self.release.wait(10.0)
                return (200, b"ok")

        router = BlockingRouter((("127.0.0.1", 9),), clock=time.monotonic,
                                deadline_mode=True)
        workers = [threading.Thread(
            target=lambda: router.get("/resource", budget=5.0), daemon=True)
            for _ in range(32)]
        for worker in workers:
            worker.start()
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            with router.entered_lock:
                if router.entered == 32:
                    break
            time.sleep(0.005)
        with router.entered_lock:
            self.assertEqual(router.entered, 32)
        with self.assertRaises(Unavailable):
            router.get("/resource", budget=5.0)
        router.release.set()
        for worker in workers:
            worker.join(5.0)
            self.assertFalse(worker.is_alive())


class ControlledClockTest(unittest.TestCase):
    def test_request_deadline_wins_a_tie_and_does_not_cool_down(self):
        release = threading.Event()
        try:
            with FixtureServer() as server:
                server.behavior = wait_then_respond(release, b"too-late")
                clock = FakeClock(0.0)
                router = Router((server.address,), clock=clock, timeout=1.0,
                                cooldown=5.0, deadline_mode=True)
                worker, outcome = call_async(router, "/resource", 0.5)
                self.assertTrue(server.first_connection.wait(5.0))

                # timeout == budget, so both deadlines coincide at 0.5.
                clock.set(0.5)
                release.set()
                outcome = join_outcome(self, worker, outcome)
                self.assertIsInstance(outcome.get("error"),
                                      DeadlineExceeded)
                self.assertNotIn("result", outcome)
                self.assertEqual(
                    router._states[server.address]["excluded_until"],
                    float("-inf"))

                # Same endpoint works immediately afterwards.
                server.behavior = respond(200, b"after-timeout")
                self.assertEqual(router.get("/again", budget=0.5),
                                 (200, b"after-timeout"))
                self.assertEqual(server.dials, 2)
        finally:
            release.set()

    def test_late_failure_and_late_cleanup_cannot_touch_a_newer_attempt(self):
        router = Router((("127.0.0.1", 9),), clock=FakeClock(0.0),
                        deadline_mode=True)
        endpoint = router.endpoints[0]
        old_token = router._claim(endpoint)
        self.assertIsNotNone(old_token)
        router._release(endpoint, old_token)

        current_token = router._claim(endpoint)
        self.assertIsNotNone(current_token)
        self.assertIsNot(router._states[endpoint]["busy"], old_token)

        # A late cleanup/failure from the finished attempt must not free the
        # newer attempt's slot or introduce/refresh cooldown.
        router._release(endpoint, old_token)
        self.assertIs(router._states[endpoint]["busy"], current_token)
        router._finish_failed(endpoint, old_token, failed_at=100.0)
        self.assertIs(router._states[endpoint]["busy"], current_token)
        self.assertEqual(router._states[endpoint]["excluded_until"],
                         float("-inf"))

        router._release(endpoint, current_token)
        self.assertIsNone(router._states[endpoint]["busy"])


if __name__ == "__main__":
    unittest.main()
