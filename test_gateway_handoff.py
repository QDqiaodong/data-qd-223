"""Regression tests for the durable gateway incident handoff store."""

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import gateway_handoff
from gateway_handoff import HandoffStore

PROJECT_DIR = Path(__file__).resolve().parent


def obs(t, *, revision=1, site="s", latency=1, outcome="delivered"):
    return {
        "revision": revision,
        "site": site,
        "completed_ms": t,
        "latency_ms": latency,
        "outcome": outcome,
    }


class StoreCase(unittest.TestCase):
    CONFIG = dict(origin_ms=0, duration_s=70,
                  scopes={(1, "s"): (0, 70_000)})

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.directory = self._tmp.name

    def open(self, **overrides):
        config = dict(self.CONFIG)
        config.update(overrides)
        store = HandoffStore(self.directory, **config)
        self.addCleanup(store.close)
        return store


class CollectionTest(StoreCase):
    def test_seal_first_out_of_order_duplicates_and_resend(self):
        store = self.open()
        self.assertIsNone(store.missing(1, "s", 0))

        store.seal(1, "s", 0, 3)
        self.assertEqual(list(store.missing(1, "s", 0)), [(0, 3)])

        same = obs(5_000, latency=10)
        other = obs(5_000, latency=99)
        store.add([(2, other), (0, same)])
        self.assertEqual(list(store.missing(1, "s", 0)), [(1, 2)])

        # Identical content re-sent under an existing identity is a no-op.
        store.add([(0, same), (0, same)])
        self.assertEqual(list(store.missing(1, "s", 0)), [(1, 2)])

        store.add([(1, obs(6_000, latency=11))])
        self.assertEqual(list(store.missing(1, "s", 0)), [])

        # Sealing again with the same credential is a no-op.
        store.seal(1, "s", 0, 3)
        self.assertEqual(list(store.missing(1, "s", 0)), [])

    def test_missing_merges_adjacent_gaps(self):
        store = self.open()
        store.seal(1, "s", 0, 10)
        store.add([(0, obs(1_000)), (1, obs(1_001)), (5, obs(1_002)),
                   (6, obs(1_003))])
        self.assertEqual(
            list(store.missing(1, "s", 0)), [(2, 5), (7, 10)])

    def test_unsealed_slice_is_unknown_even_when_empty(self):
        store = self.open()
        self.assertIsNone(store.missing(1, "s", 10_000))
        store.seal(1, "s", 10_000, 0)
        self.assertEqual(list(store.missing(1, "s", 10_000)), [])

    def test_whole_batch_is_rejected_on_any_invalid_later_item(self):
        store = self.open()
        store.seal(1, "s", 0, 4)
        bad_batches = [
            [(0, obs(1_000)), (1, obs(1_001)), (2, {"revision": 1})],
            [(0, obs(1_000)), (1, obs(1_001)), (2, obs(1_002)),
             (2, obs(1_003))],
            [(0, obs(1_000)), (1, obs(1_001)), (True, obs(1_002))],
            [(0, obs(1_000)), (1, obs(1_001)), (2, obs(70_000))],
        ]
        for batch in bad_batches:
            with self.subTest(batch=batch):
                with self.assertRaises(ValueError):
                    store.add(iter(batch))
                # No prefix from the rejected batch was stored.
                self.assertEqual(list(store.missing(1, "s", 0)), [(0, 4)])

    def test_conflicting_identity_and_post_seal_ordinal_do_not_change_state(self):
        store = self.open()
        store.seal(1, "s", 0, 2)
        store.add([(0, obs(1_000, latency=5))])
        with self.assertRaises(ValueError):
            store.add([(0, obs(1_000, latency=6))])
        with self.assertRaises(ValueError):
            store.add([(2, obs(2_000))])
        with self.assertRaises(ValueError):
            store.add([(1_999_999, obs(2_000))])
        self.assertEqual(list(store.missing(1, "s", 0)), [(1, 2)])

    def test_conflicting_seal_and_contradicting_seal(self):
        store = self.open()
        store.add([(0, obs(1_000)), (1, obs(1_001))])
        with self.assertRaises(ValueError):
            store.seal(1, "s", 0, 1)  # contradicts received ordinal 1
        self.assertIsNone(store.missing(1, "s", 0))
        store.seal(1, "s", 0, 2)
        with self.assertRaises(ValueError):
            store.seal(1, "s", 0, 3)
        self.assertEqual(list(store.missing(1, "s", 0)), [])

    def test_ordinal_and_expected_validation(self):
        store = self.open()
        for ordinal in (True, -1, 2_000_000, "0", 1.0):
            with self.subTest(ordinal=ordinal):
                with self.assertRaises(ValueError):
                    store.add([(ordinal, obs(1_000))])
        for expected in (True, -1, 2_000_001, "1", 1.5):
            with self.subTest(expected=expected):
                with self.assertRaises(ValueError):
                    store.seal(1, "s", 0, expected)

    def test_batch_of_at_most_4096(self):
        store = self.open()
        store.seal(1, "s", 0, 4_096)
        with self.assertRaises(ValueError):
            store.add((ordinal, obs(1_000)) for ordinal in range(4_097))
        self.assertEqual(list(store.missing(1, "s", 0)), [(0, 4_096)])
        store.add((ordinal, obs(1_000)) for ordinal in range(4_096))
        self.assertEqual(list(store.missing(1, "s", 0)), [])

    def test_observation_cap_is_atomic(self):
        store = self.open()
        store.seal(1, "s", 0, 5)
        with mock.patch.object(gateway_handoff, "_MAX_OBSERVATIONS", 3):
            with self.assertRaises(ValueError):
                store.add([(0, obs(1_000)), (1, obs(1_001)),
                           (2, obs(1_002)), (3, obs(1_003))])
        self.assertEqual(list(store.missing(1, "s", 0)), [(0, 5)])
        store.add([(0, obs(1_000)), (1, obs(1_001)), (2, obs(1_002))])
        self.assertEqual(list(store.missing(1, "s", 0)), [(3, 5)])

    def test_invalid_slice_fields(self):
        store = self.open()
        invalid = [
            (True, "s", 0),
            (0, "s", 0),
            (2, "s", 0),
            (1, "bad site", 0),
            (1, "s", 5_000),
            (1, "s", 70_000),
            (1, "s", -10_000),
            (1, "s", True),
        ]
        for revision, site, start in invalid:
            with self.subTest(args=(revision, site, start)):
                with self.assertRaises(ValueError):
                    store.missing(revision, site, start)
                with self.assertRaises(ValueError):
                    store.seal(revision, site, start, 0)


class CheckpointTest(StoreCase):
    def test_names_and_limits(self):
        store = self.open()
        for name in ("", "bad name", "a" * 33, "a.b", "a/b", 5, None):
            with self.subTest(name=name):
                with self.assertRaises(ValueError):
                    store.checkpoint(name)
        with self.assertRaises(ValueError):
            store.evaluate_checkpoint("unknown")
        for index in range(8):
            store.checkpoint("cp%d" % index)
        with self.assertRaises(ValueError):
            store.checkpoint("cp8")
        # Re-sending an existing name is fine and does not move it.
        store.checkpoint("cp0")

    def test_same_name_never_moves_and_later_writes_are_excluded(self):
        store = self.open()
        store.seal(1, "s", 0, 2)
        store.add([(0, obs(1_000, latency=10))])
        store.checkpoint("handoff")

        windows, actions = store.evaluate_checkpoint("handoff")
        self.assertEqual((windows[0][3], windows[0][5]), (1, 10))
        self.assertFalse(windows[0][6])  # seal incomplete, so no coverage

        store.add([(1, obs(2_000, latency=10))])
        store.checkpoint("handoff")  # same name: must not move
        again, again_actions = store.evaluate_checkpoint("handoff")
        self.assertEqual(again, windows)
        self.assertEqual(again_actions, actions)


class ReopenTest(StoreCase):
    def test_reopen_uses_directory_only_and_normalized_config(self):
        store = self.open()
        store.seal(1, "s", 0, 1)
        store.add([(0, obs(1_000))])
        store.checkpoint("cp")
        store.close()

        reopened = HandoffStore(self.directory)
        self.addCleanup(reopened.close)
        self.assertEqual(list(reopened.missing(1, "s", 0)), [])
        self.assertEqual(len(reopened.evaluate_checkpoint("cp")[0]), 2)

        reopened.close()
        # Supplying the identical configuration is allowed.
        same = HandoffStore(
            self.directory, origin_ms=0, duration_s=70,
            scopes={(1, "s"): (0, 70_000)})
        same.close()

    def test_reopen_rejects_different_configuration(self):
        self.open().close()
        for overrides in (
                {"origin_ms": 1},
                {"duration_s": 80},
                {"scopes": {(1, "t"): (0, 70_000)}},
        ):
            with self.subTest(overrides=overrides):
                with self.assertRaises(ValueError):
                    HandoffStore(self.directory, **{**self.CONFIG, **overrides})

    def test_new_store_requires_empty_directory_and_full_config(self):
        with self.assertRaises(ValueError):
            HandoffStore(self.directory, origin_ms=0, duration_s=70)
        with self.assertRaises(ValueError):
            HandoffStore(self.directory)
        os.rmdir(self.directory)
        os.makedirs(self.directory)
        Path(self.directory, "stray.txt").write_text("x")
        with self.assertRaises(ValueError):
            HandoffStore(self.directory, **self.CONFIG)


class _Scenario2Base(StoreCase):
    CONFIG = dict(origin_ms=1_000_000, duration_s=70,
                  scopes={(1, "s"): (0, 70_000)})

    def time(self, relative):
        return self.CONFIG["origin_ms"] + relative

    def build_initial_evidence(self, store):
        # Empty slices with an explicit zero credential are covered.
        for start in (20_000, 30_000, 40_000, 50_000):
            store.seal(1, "s", start, 0)

        # Slice 0 declared two records, but only ordinal 0 was delivered.
        store.seal(1, "s", 0, 2)
        store.add([(0, obs(self.time(5_000), latency=10))])

        # Twenty high-latency delivered samples in [10000, 20000): this
        # slice is an input to both complete windows.
        store.seal(1, "s", 10_000, 20)
        store.add([
            (ordinal, obs(self.time(15_000), latency=500))
            for ordinal in range(20)
        ])

    def expected_windows(self, *, count_at_60, complete_60, verdict_60,
                         count_at_70, complete_70, verdict_70):
        return [
            (1, "s", 60_000, count_at_60, 0, 500, complete_60, verdict_60),
            (1, "s", 70_000, count_at_70, 0, 500, complete_70, verdict_70),
        ]


class IncidentScenarioTest(_Scenario2Base):
    def test_gap_first_then_backfill_then_zero_seal(self):
        store = self.open()
        self.build_initial_evidence(store)
        store.checkpoint("first")

        windows, actions = store.evaluate_checkpoint("first")
        self.assertEqual(windows, self.expected_windows(
            count_at_60=21, complete_60=False, verdict_60="gap",
            count_at_70=20, complete_70=False, verdict_70="gap"))
        self.assertEqual(actions, [(1, "s", 60_000, "open_gap")])

        # Backfill the one declared-but-missing ordinal of slice 0.  The
        # second handoff still lacks a credential for the empty [60000,
        # 70000) slice, so the second window remains a gap.
        store.add([(1, obs(self.time(6_000), latency=10))])
        store.checkpoint("second")

        windows, actions = store.evaluate_checkpoint("second")
        self.assertEqual(windows, self.expected_windows(
            count_at_60=22, complete_60=True, verdict_60="bad",
            count_at_70=20, complete_70=False, verdict_70="gap"))
        self.assertEqual(actions, [(1, "s", 70_000, "open_gap")])

        # Only a zero-record credential for the previously empty slice is
        # added.  The same twenty high-latency samples now form two
        # consecutive bad windows and open the service incident.
        store.seal(1, "s", 60_000, 0)
        store.checkpoint("third")

        windows, actions = store.evaluate_checkpoint("third")
        self.assertEqual(windows, self.expected_windows(
            count_at_60=22, complete_60=True, verdict_60="bad",
            count_at_70=20, complete_70=True, verdict_70="bad"))
        self.assertEqual(actions, [(1, "s", 70_000, "open_incident")])

        # The frozen first handoff still reflects its own instant.
        self.assertEqual(
            store.evaluate_checkpoint("first"),
            (self.expected_windows(
                count_at_60=21, complete_60=False, verdict_60="gap",
                count_at_70=20, complete_70=False, verdict_70="gap"),
             [(1, "s", 60_000, "open_gap")]))
        store.checkpoint("first")
        self.assertEqual(
            store.evaluate_checkpoint("first")[0][0][3], 21)

        store.close()

        reopened = HandoffStore(self.directory)
        self.addCleanup(reopened.close)
        self.assertEqual(
            reopened.evaluate_checkpoint("second"),
            (self.expected_windows(
                count_at_60=22, complete_60=True, verdict_60="bad",
                count_at_70=20, complete_70=False, verdict_70="gap"),
             [(1, "s", 70_000, "open_gap")]))
        reopened.checkpoint("second")  # cannot upgrade an old handoff
        self.assertFalse(reopened.evaluate_checkpoint("second")[0][1][6])
        self.assertEqual(
            reopened.evaluate_checkpoint("third")[1],
            [(1, "s", 70_000, "open_incident")])


_SCENARIO1_CHILD = r"""
import os
import sys

from gateway_handoff import HandoffStore

store = HandoffStore(sys.argv[1])


def record(latency):
    return {
        "revision": 1,
        "site": "s",
        "completed_ms": 15_000,
        "latency_ms": latency,
        "outcome": "delivered",
    }


def batch():
    # These identities are already durable; the following ones are new.
    yield (0, record(30))
    yield (1, record(40))
    yield (2, record(50))
    yield (3, record(60))
    # Simulate an abrupt process exit in the middle of the batch iterator.
    os._exit(17)


store.add(batch())
os._exit(0)
"""


class CrashRecoveryTest(StoreCase):
    CONFIG = dict(origin_ms=0, duration_s=120,
                  scopes={(1, "s"): (0, 120_000)})

    def test_unfinished_batch_leaves_no_partial_rows(self):
        store = self.open()
        # Credential arrives before its records, which then arrive shuffled;
        # ordinals 0 and 1 carry exactly the same content.
        store.seal(1, "s", 0, 3)
        identical = obs(5_000, latency=10)
        store.add([(2, obs(5_000, latency=20)), (1, identical), (0, identical)])
        store.add([(0, identical)])  # permitted resend, no new row

        store.add([(0, obs(15_000, latency=30)),
                   (1, obs(15_000, latency=40))])
        store.checkpoint("before-crash")
        self.assertEqual(list(store.missing(1, "s", 0)), [])
        store.close()

        env = dict(os.environ)
        env["PYTHONPATH"] = str(PROJECT_DIR)
        completed = subprocess.run(
            [sys.executable, "-B", "-c", _SCENARIO1_CHILD, self.directory],
            cwd=str(PROJECT_DIR), env=env,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
        self.assertEqual(completed.returncode, 17, completed.stderr)

        reopened = HandoffStore(self.directory)
        self.addCleanup(reopened.close)

        # The confirmed records, seal and named handoff survived.
        self.assertEqual(list(reopened.missing(1, "s", 0)), [])
        windows, actions = reopened.evaluate_checkpoint("before-crash")
        self.assertEqual(windows[0][3], 5)
        self.assertEqual(windows[0][4], 0)
        self.assertFalse(windows[0][6])  # [10000,20000) slice is unsealed
        self.assertEqual(actions, [(1, "s", 60_000, "open_gap")])

        # The interrupted batch left no prefix: ordinals 2 and 3 of the
        # [10000, 20000) slice were never stored, so a four-record credential
        # is accepted without contradiction and the handoff person sees
        # exactly those two ordinals to backfill.
        reopened.seal(1, "s", 10_000, 4)
        self.assertEqual(list(reopened.missing(1, "s", 10_000)), [(2, 4)])

        # Ordinal 0 was part of the pre-crash durable batch, so it cannot be
        # re-collected with different content; ordinal 2 is still new.
        with self.assertRaises(ValueError):
            reopened.add([(0, obs(15_000, latency=99))])
        reopened.add([(2, obs(15_000, latency=70))])
        self.assertEqual(list(reopened.missing(1, "s", 10_000)), [(3, 4)])
        reopened.add([(3, obs(15_000, latency=80))])
        self.assertEqual(list(reopened.missing(1, "s", 10_000)), [])


_MEMORY_CHILD = r"""
import json
import os
import resource
import shutil
import sys
import tempfile

_LIMIT = 96 * 1024 * 1024
resource.setrlimit(resource.RLIMIT_AS, (_LIMIT, _LIMIT))

from gateway_handoff import HandoffStore

N = 2_000_000
directory = tempfile.mkdtemp(prefix="handoff-memory-")
try:
    store = HandoffStore(
        directory, origin_ms=0, duration_s=70,
        scopes={(1, "m"): (0, 70_000)})
    store.seal(1, "m", 0, N)
    record = {
        "revision": 1,
        "site": "m",
        "completed_ms": 5_000,
        "latency_ms": 7,
        "outcome": "delivered",
    }

    def batches():
        for start in range(0, N, 4096):
            stop = min(start + 4096, N)
            yield [(ordinal, record) for ordinal in range(start, stop)]

    for batch in batches():
        store.add(batch)

    store.checkpoint("full")
    windows, actions = store.evaluate_checkpoint("full")
    store.close()

    store = HandoffStore(directory)
    reopened_windows, reopened_actions = store.evaluate_checkpoint("full")
    store.close()
    print(json.dumps({
        "live": [windows[0], actions],
        "reopened": [reopened_windows[0], reopened_actions],
    }))
finally:
    shutil.rmtree(directory, ignore_errors=True)
"""


@unittest.skipUnless(sys.platform.startswith("linux"), "Linux RLIMIT_AS test")
class MemoryAcceptanceTest(unittest.TestCase):
    def test_two_million_distinct_ordinals_under_96_mib(self):
        before = set(Path(tempfile.gettempdir()).glob("handoff-memory-*"))
        env = dict(os.environ)
        env["PYTHONPATH"] = str(PROJECT_DIR)
        completed = subprocess.run(
            [sys.executable, "-B", "-c", _MEMORY_CHILD],
            cwd=str(PROJECT_DIR), env=env,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            timeout=90)
        self.assertEqual(
            completed.returncode, 0,
            "memory child failed:\n%s" % completed.stderr)
        # The child used its own temporary directory and removed it.
        after = set(Path(tempfile.gettempdir()).glob("handoff-memory-*"))
        self.assertEqual(after, before)

        data = json.loads(completed.stdout)
        window = data["live"][0]
        self.assertEqual(window[:5], [1, "m", 60_000, 2_000_000, 0])
        self.assertEqual(window[5], 7)
        self.assertFalse(window[6])  # [60000,70000) slice has no credential
        self.assertEqual(data["live"][1], [[1, "m", 60_000, "open_gap"]])
        self.assertEqual(data["reopened"], data["live"])


if __name__ == "__main__":
    unittest.main()
