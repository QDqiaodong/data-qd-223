import os
import random
import subprocess
import sys
import threading
import time
import unittest
from pathlib import Path

from deadline_fixture import FixtureServer, respond, wait_then_respond
from outage_router import NoFeasiblePlan, OutageRouter
from router import Router, Unavailable

PROJECT_DIR = Path(__file__).resolve().parent


def get_async(outage_router, bucket, path, budget, outage_zone=None):
    outcome = {}

    def run():
        try:
            outcome["result"] = outage_router.get(
                bucket, path, budget=budget, outage_zone=outage_zone)
        except BaseException as error:  # noqa: BLE001 - reported to the test
            outcome["error"] = error

    worker = threading.Thread(target=run, daemon=True)
    worker.start()
    return worker, outcome


def join_outcome(test, worker, outcome, timeout=5.0):
    worker.join(timeout)
    test.assertFalse(worker.is_alive(), "gateway call did not finish")
    return outcome


class DeploymentTestBase(unittest.TestCase):
    """Sites: east/south are primary zones, north/west are healthy zones."""

    def setUp(self):
        self.servers = []

    def tearDown(self):
        for server in reversed(self.servers):
            server.__exit__(None, None, None)

    def server(self, body=b"ok"):
        server = FixtureServer().__enter__()
        server.behavior = respond(200, body)
        self.servers.append(server)
        return server

    def build(self, *, cooldown=30.0, timeout=1.0):
        east = self.server(b"east")
        south = self.server(b"south")
        x = self.server(b"x")
        y = self.server(b"y")
        self.endpoints = {
            "east1": east.address, "south1": south.address,
            "x": x.address, "y": y.address,
        }
        self.router = Router(
            (east.address, south.address, x.address, y.address),
            clock=time.monotonic, cooldown=cooldown, timeout=timeout,
            deadline_mode=True)
        self.router_under_test = OutageRouter(
            self.router,
            {
                "east1": ("east", east.address),
                "south1": ("south", south.address),
                "x": ("north", x.address),
                "y": ("west", y.address),
            },
        )
        return east, south, x, y


class PlanApprovalTest(DeploymentTestBase):
    BUCKETS = {0: ("east1", ["x", "y"]),
               1: ("east1", ["x"]),
               2: ("south1", ["x"])}
    SPARE = {"east1": 0, "south1": 0, "x": 1, "y": 1}

    def test_forced_assignment_and_revision(self):
        self.build()
        revision, assignments = self.router_under_test.install(
            dict(self.BUCKETS), dict(self.SPARE))
        self.assertEqual(revision, 1)
        self.assertEqual(assignments, {0: "y", 1: "x", 2: "x"})

        # The returned mapping is a copy: mutating it cannot rewrite the plan.
        assignments.clear()
        self.assertEqual(self.router_under_test._plan.by_bucket[0][2], "y")

        # The logged plan is a fresh revision even for the same configuration.
        revision, assignments = self.router_under_test.install(
            dict(self.BUCKETS), dict(self.SPARE))
        self.assertEqual(revision, 2)
        self.assertEqual(assignments, {0: "y", 1: "x", 2: "x"})

    def test_candidate_subset_overflow_fails_without_state_change(self):
        self.build()
        revision, good = self.router_under_test.install(
            dict(self.BUCKETS), dict(self.SPARE))
        self.assertEqual((revision, good), (1, {0: "y", 1: "x", 2: "x"}))

        # Total out-of-zone capacity (X+Y = 1+2 = 3) would fit, but buckets 1
        # and 3 may only use X, which has one slot.  Structural validity is
        # fine, so this must be NoFeasiblePlan, not ValueError.
        buckets = dict(self.BUCKETS)
        buckets[3] = ("east1", ["x"])
        spare = {"east1": 0, "south1": 0, "x": 1, "y": 2}
        with self.assertRaises(NoFeasiblePlan) as caught:
            self.router_under_test.install(buckets, spare)
        self.assertIn("east", caught.exception.zones)

        # Failure consumed no revision and left the previous plan serving.
        self.assertEqual(self.router_under_test._revision, 1)
        self.assertEqual(self.router_under_test._plan.by_bucket[0][2], "y")
        self.assertEqual(self.router_under_test.get(
            0, "/still-old", budget=2.0, outage_zone="east"),
            (200, b"y"))

        # The next successful install is revision 2, with no skipped number.
        buckets.pop(3)
        revision, assignments = self.router_under_test.install(buckets, spare)
        self.assertEqual(revision, 2)
        self.assertEqual(assignments, {0: "y", 1: "x", 2: "x"})

    def test_assignment_is_independent_of_input_order(self):
        self.build()
        first = self.router_under_test.install(
            dict(self.BUCKETS), dict(self.SPARE))

        items = list(self.BUCKETS.items())
        random.Random(7).shuffle(items)
        shuffled_buckets = {}
        for bucket, (primary, candidates) in items:
            candidates = list(candidates)
            random.Random(bucket).shuffle(candidates)
            shuffled_buckets[bucket] = (primary, candidates)
        spare_items = list(self.SPARE.items())
        random.Random(11).shuffle(spare_items)
        second = self.router_under_test.install(
            shuffled_buckets, dict(spare_items))
        self.assertEqual(first[1], second[1])
        self.assertEqual(second[1], {0: "y", 1: "x", 2: "x"})

    def test_same_zone_and_self_candidates_are_legal_but_not_takeover(self):
        self.build()
        spare = {"east1": 0, "south1": 0, "x": 1, "y": 0}

        # Listing a site from the primary's own zone (even the primary itself)
        # is structurally valid, but such candidates cannot take over that
        # zone.  Only X can cover east, so the second install works.
        revision, assignments = self.router_under_test.install(
            {0: ("east1", ["east1", "x"]),
             1: ("south1", ["east1", "x"])}, spare)
        self.assertEqual(revision, 1)
        # East buckets: X is the only usable backup.  South buckets: X's one
        # slot is reusable because the two outage scenarios are exclusive.
        self.assertEqual(assignments, {0: "x", 1: "x"})

        # A bucket whose only candidate is in its own zone has no takeover
        # option; that is a capacity verdict, not a structural error.
        with self.assertRaises(NoFeasiblePlan) as caught:
            self.router_under_test.install({0: ("east1", ["east1"])}, spare)
        self.assertEqual(caught.exception.zones, ("east",))
        self.assertEqual(self.router_under_test._revision, 1)

    def test_structural_validation(self):
        self.build()
        router = self.router_under_test
        base_buckets = dict(self.BUCKETS)
        base_spare = dict(self.SPARE)

        bad_cases = [
            (base_buckets, {**base_spare, "x": True}),
            (base_buckets, {**base_spare, "x": 4097}),
            (base_buckets, {**base_spare, "x": -1}),
            (base_buckets, {**base_spare, "x": 1.0}),
            (base_buckets, {"east1": 0, "south1": 0, "x": 1}),
            ({**base_buckets, 4096: ("east1", ["x"])}, base_spare),
            ({1: ("east1", ["x"]), 2: ("east1", ["x"]),
              False: ("east1", ["x"])}, base_spare),
            ({**base_buckets, 0: ("east1", ["x"] * 16)}, base_spare),
            ({**base_buckets, 0: ("east1", ["x", "x"])}, base_spare),
            ({**base_buckets, 0: ("nowhere", ["x"])}, base_spare),
            ({**base_buckets, 0: ("east1", ["nowhere"])}, base_spare),
            ({**base_buckets, 0: ("east1", "x")}, base_spare),
            ({}, base_spare),
        ]
        for buckets, spare in bad_cases:
            with self.subTest(buckets=buckets, spare=spare):
                with self.assertRaises(ValueError):
                    router.install(buckets, spare)
        self.assertEqual(router._revision, 0)

    def test_site_directory_validation(self):
        endpoints = tuple(("127.0.0.%d" % host, 80) for host in range(1, 5))
        router = Router(endpoints, clock=time.monotonic, deadline_mode=True)

        good = {
            "a": ("one", endpoints[0]),
            "b": ("two", endpoints[1]),
            "c": ("two", endpoints[2]),
            "d": ("three", endpoints[3]),
        }
        OutageRouter(router, good)

        bad_directories = [
            {"a": ("one", endpoints[0])},  # too few
            {**good, "a": ("one", endpoints[0]),
             "e": ("four", endpoints[0])},  # duplicate endpoint
            {**good, "e": ("four", ("127.0.0.9", 80))},  # unknown endpoint
            {"a": ("one", endpoints[0]), "b": ("one", endpoints[1])},
            {**good, "bad id": ("four", endpoints[0])},
            {**good, "e": ("x" * 33, endpoints[0])},
            {**good, "e": ("café", endpoints[0])},
            {**good, "e": ("four",)} ,
        ]
        for sites in bad_directories:
            with self.subTest(sites=sites):
                with self.assertRaises(ValueError):
                    OutageRouter(router, sites)

        legacy = Router(endpoints, clock=time.monotonic)
        with self.assertRaises(ValueError):
            OutageRouter(legacy, good)


class GatewayRoutingTest(DeploymentTestBase):
    BUCKETS = PlanApprovalTest.BUCKETS
    SPARE = PlanApprovalTest.SPARE

    def test_normal_and_outage_traffic_reaches_only_the_chosen_site(self):
        east, south, x, y = self.build()
        self.router_under_test.install(dict(self.BUCKETS), dict(self.SPARE))

        self.assertEqual(
            self.router_under_test.get(0, "/normal", budget=2.0),
            (200, b"east"))
        self.assertEqual(east.paths, ["/normal"])

        # Declaring a different zone leaves the primary in charge.
        self.assertEqual(
            self.router_under_test.get(0, "/south-down", budget=2.0,
                                       outage_zone="south"),
            (200, b"east"))
        self.assertEqual(east.paths, ["/normal", "/south-down"])

        # East is down: bucket 0 must use its backup Y, bucket 1 uses X.
        self.assertEqual(
            self.router_under_test.get(0, "/east-down", budget=2.0,
                                       outage_zone="east"),
            (200, b"y"))
        self.assertEqual(
            self.router_under_test.get(1, "/east-down-1", budget=2.0,
                                       outage_zone="east"),
            (200, b"x"))
        self.assertEqual(y.paths, ["/east-down"])
        self.assertEqual(x.paths, ["/east-down-1"])

        # South is down: bucket 2 uses X, and Y is not eligible for it.
        self.assertEqual(
            self.router_under_test.get(2, "/south-down-2", budget=2.0,
                                       outage_zone="south"),
            (200, b"x"))
        self.assertEqual(x.paths, ["/east-down-1", "/south-down-2"])
        self.assertEqual(y.paths, ["/east-down"])
        self.assertEqual(south.paths, [])

    def test_failing_chosen_site_never_dials_another_site(self):
        east, south, x, y = self.build()
        self.router_under_test.install(dict(self.BUCKETS), dict(self.SPARE))

        # A legitimate 404 is delivered unchanged, without trying Y.
        x.behavior = respond(404, b"gone")
        self.assertEqual(
            self.router_under_test.get(1, "/missing", budget=2.0,
                                       outage_zone="east"),
            (404, b"gone"))
        self.assertEqual(x.dials, 1)
        self.assertEqual(y.dials, 0)
        self.assertEqual(east.dials, 0)
        self.assertEqual(south.dials, 0)

        # A failing chosen site follows single-candidate semantics: no
        # rotation to Y, the same-zone primary or any unassigned site.
        x.behavior = respond(503, b"down")
        with self.assertRaises(Unavailable):
            self.router_under_test.get(1, "/east-down", budget=2.0,
                                       outage_zone="east")
        self.assertEqual(x.dials, 2)
        self.assertEqual(y.dials, 0)
        self.assertEqual(east.dials, 0)
        self.assertEqual(south.dials, 0)

    def test_install_does_not_wait_for_an_in_flight_old_request(self):
        east, south, x, y = self.build(timeout=5.0)
        self.router_under_test.install(dict(self.BUCKETS), dict(self.SPARE))

        release = threading.Event()
        try:
            y.behavior = wait_then_respond(release, b"old-y")
            worker, outcome = get_async(
                self.router_under_test, 0, "/blocked-old", 10.0,
                outage_zone="east")
            self.assertTrue(y.first_connection.wait(5.0))
            self.assertEqual(y.dials, 1)

            # New revision sends bucket 0 to X, which is idle.
            new_buckets = {0: ("east1", ["x"]),
                           1: ("east1", ["x"]),
                           2: ("south1", ["x"])}
            new_spare = {"east1": 0, "south1": 0, "x": 2, "y": 0}
            started = time.monotonic()
            revision, assignments = self.router_under_test.install(
                new_buckets, new_spare)
            self.assertLess(time.monotonic() - started, 1.0)
            self.assertEqual(assignments, {0: "x", 1: "x", 2: "x"})
            self.assertEqual(revision, 2)
            self.assertTrue(worker.is_alive(),
                            "old request should still be waiting on Y")

            self.assertEqual(
                self.router_under_test.get(0, "/new-x", budget=2.0,
                                           outage_zone="east"),
                (200, b"x"))
            self.assertEqual(x.paths, ["/new-x"])

            # The old request is still pinned to Y and completes there.
            release.set()
            outcome = join_outcome(self, worker, outcome)
            self.assertEqual(outcome.get("result"), (200, b"old-y"))
            self.assertEqual(y.paths, ["/blocked-old"])
            self.assertEqual(east.paths, [])
        finally:
            release.set()

    def test_rejections_happen_before_any_dial(self):
        east, south, x, y = self.build()
        with self.assertRaises(Unavailable):
            self.router_under_test.get(0, "/no-plan", budget=2.0)

        self.router_under_test.install(dict(self.BUCKETS), dict(self.SPARE))
        for bucket, zone in ((9, None), (0, "nowhere")):
            with self.subTest(bucket=bucket, zone=zone):
                with self.assertRaises(ValueError):
                    self.router_under_test.get(
                        bucket, "/bad", budget=2.0, outage_zone=zone)
        for server in (east, south, x, y):
            self.assertEqual(server.dials, 0)

    def test_install_is_not_blocked_by_many_parallel_gets(self):
        east, south, x, y = self.build(timeout=5.0)
        self.router_under_test.install(dict(self.BUCKETS), dict(self.SPARE))

        release = threading.Event()
        barrier = threading.Barrier(32)
        outcomes = [None] * 32

        def fetch(index):
            barrier.wait(5.0)
            try:
                outcomes[index] = self.router_under_test.get(
                    0, "/held", budget=10.0, outage_zone="east")
            except BaseException as error:  # noqa: BLE001 - reported below
                outcomes[index] = error

        workers = [threading.Thread(target=fetch, args=(index,), daemon=True)
                   for index in range(32)]
        try:
            y.behavior = wait_then_respond(release, b"held")
            for worker in workers:
                worker.start()
            self.assertTrue(y.first_connection.wait(5.0))

            begin = time.monotonic()
            revision, _ = self.router_under_test.install(
                {0: ("east1", ["x"]), 1: ("east1", ["x"]),
                 2: ("south1", ["x"])},
                {"east1": 0, "south1": 0, "x": 2, "y": 0})
            # `release` is still unset, so Y is still holding its response:
            # a completed install proves it did not wait for the GET.
            self.assertLess(time.monotonic() - begin, 1.0)
            self.assertEqual(revision, 2)
        finally:
            release.set()
            for worker in workers:
                worker.join(5.0)

        for worker in workers:
            self.assertFalse(worker.is_alive())
        # Exactly one request got Y's slot before the install; requests that
        # raced (busy slot) failed fast, and those beginning after the install
        # correctly used the new plan's X.  Nothing else was dialed.
        self.assertEqual(y.paths, ["/held"],
                         "the busy old site must not be redialed")
        delivered = [outcome for outcome in outcomes
                     if not isinstance(outcome, BaseException)]
        self.assertIn((200, b"held"), delivered)
        self.assertEqual(
            [result for result in delivered if result != (200, b"held")],
            [(200, b"x")] * (len(delivered) - 1))
        for outcome in outcomes:
            if isinstance(outcome, BaseException):
                self.assertIsInstance(outcome, Unavailable)
        self.assertEqual(east.dials, 0)
        self.assertEqual(south.dials, 0)


class LargeControlPlaneTest(unittest.TestCase):
    def test_4096_buckets_under_192_mib_address_space(self):
        if not sys.platform.startswith("linux"):
            self.skipTest("address-space limit check requires Linux")

        script = """
import json
from router import Router
from outage_router import OutageRouter

endpoints = tuple(("127.0.0.1", 7000 + index) for index in range(5))
router = Router(endpoints, clock=lambda: 0.0, deadline_mode=True)
sites = {
    "east": ("east-zone", endpoints[0]),
    "a": ("zone-a", endpoints[1]),
    "b": ("zone-b", endpoints[2]),
    "c": ("zone-c", endpoints[3]),
    "d": ("zone-d", endpoints[4]),
}
outage = OutageRouter(router, sites)
buckets = {bucket: ("east", ("a", "b", "c", "d")) for bucket in range(4096)}
spare = {"east": 0, "a": 4096, "b": 4096, "c": 4096, "d": 4096}
revision, assignments = outage.install(buckets, spare)
assert revision == 1, revision
assert set(assignments) == set(range(4096)), len(assignments)
counts = {site: 0 for site in ("a", "b", "c", "d")}
for bucket, site in assignments.items():
    assert site in ("a", "b", "c", "d"), (bucket, site)
    assert outage.router.endpoints.index(sites[site][1]) != 0
    counts[site] += 1
for site, count in counts.items():
    assert count <= 4096, (site, count)
print("SCALE_OK", len(assignments), json.dumps(counts, sort_keys=True))
"""

        def limit_address_space():
            import resource
            limit = 192 * 1024 * 1024
            resource.setrlimit(resource.RLIMIT_AS, (limit, limit))

        environment = dict(os.environ)
        environment.pop("PYTHONPATH", None)
        completed = subprocess.run(
            [sys.executable, "-B", "-c", script],
            cwd=str(PROJECT_DIR),
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=limit_address_space,
            timeout=60,
        )
        self.assertEqual(
            completed.returncode, 0,
            completed.stderr.decode("utf-8", "replace"))
        self.assertIn(b"SCALE_OK 4096", completed.stdout)


if __name__ == "__main__":
    unittest.main()
