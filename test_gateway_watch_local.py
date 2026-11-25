"""Hand-calculated semantics tests for gateway_watch.evaluate."""

import random
import unittest

from gateway_watch import evaluate


def obs(revision=1, site="s", t=0, outcome="delivered", latency=1):
    return {
        "revision": revision,
        "site": site,
        "completed_ms": t,
        "latency_ms": latency,
        "outcome": outcome,
    }


def good_points(start, end, *, revision=1, site="s", step=10_000,
                copies=4, latency=10):
    """Four samples per 10s slab: 24 good samples per 60s window."""
    result = []
    t = start
    while t < end:
        for _ in range(copies):
            result.append(
                obs(revision=revision, site=site, t=t, latency=latency))
        t += step
    return result


class WindowMetricTest(unittest.TestCase):
    def test_duplicate_times_endpoint_boundary_and_exact_p95(self):
        records = [
            # Twenty samples at the same millisecond.  Latencies 1..20 make
            # nearest-rank p95 for n=20 equal the 19th sorted value.
            obs(t=1_000, latency=latency) for latency in range(1, 21)
        ]
        records.extend([
            # Exact duplicate content counts twice.
            obs(t=0, latency=7),
            obs(t=0, latency=7),
            # t=60000 is excluded from the end=60000 window but included in
            # the following [10000, 70000) window.
            obs(t=60_000, latency=999),
        ])
        windows, _ = evaluate(
            records,
            origin_ms=0,
            duration_s=120,
            scopes={(1, "s"): (0, 120_000)},
            coverage={(1, "s"): [(0, 120_000)]},
        )

        first = windows[0]
        self.assertEqual(first[2], 60_000)
        self.assertEqual(first[3], 22)  # 20 same-ms + two exact duplicates
        self.assertEqual(first[4], 0)
        self.assertEqual(first[5], 19)
        self.assertTrue(first[6])
        self.assertEqual(first[7], "good")  # 22 >= 20 and p95 19 < 200

        second = windows[1]
        self.assertEqual(second[2], 70_000)
        self.assertEqual(second[3], 1)  # only the exact endpoint sample
        self.assertEqual(second[5], 999)
        self.assertEqual(second[7], "low")

    def test_failure_rate_five_percent_is_an_inclusive_threshold(self):
        records = [obs(site="f", t=1_000, latency=10) for _ in range(19)]
        records.append(
            obs(site="f", t=2_000, outcome="unavailable", latency=1000))
        windows, _ = evaluate(
            records,
            origin_ms=0,
            duration_s=60,
            scopes={(1, "f"): (0, 60_000)},
            coverage={(1, "f"): [(0, 60_000)]},
        )
        self.assertEqual(len(windows), 1)
        window = windows[0]
        self.assertEqual((window[3], window[4]), (20, 1))
        self.assertEqual(window[5], 10)  # ceil(0.95*19)=19, the maximum
        self.assertEqual(window[7], "bad")

    def test_p95_200_is_an_inclusive_bad_threshold(self):
        records = [obs(site="p", t=1_000, latency=199) for _ in range(18)]
        records.extend(obs(site="p", t=2_000, latency=200) for _ in range(2))
        windows, _ = evaluate(
            records,
            origin_ms=0,
            duration_s=60,
            scopes={(1, "p"): (0, 60_000)},
            coverage={(1, "p"): [(0, 60_000)]},
        )
        self.assertEqual(windows[0][5], 200)
        self.assertEqual(windows[0][7], "bad")

    def test_coverage_adjacent_pieces_cover_and_one_millisecond_gap_fails(self):
        records = [obs(site="c", t=1_000 + i, latency=10) for i in range(20)]
        scopes = {(1, "c"): (0, 60_000)}

        complete, _ = evaluate(
            records, origin_ms=0, duration_s=60, scopes=scopes,
            coverage={(1, "c"): [(0, 30_000), (30_000, 60_000)]})
        self.assertTrue(complete[0][6])
        self.assertEqual(complete[0][7], "good")

        incomplete, actions = evaluate(
            records, origin_ms=0, duration_s=60, scopes=scopes,
            coverage={(1, "c"): [(0, 30_000), (30_001, 60_000)]})
        self.assertFalse(incomplete[0][6])
        self.assertEqual(incomplete[0][7], "gap")
        self.assertEqual(actions, [(1, "c", 60_000, "open_gap")])


class ActionStateTest(unittest.TestCase):
    def test_identical_samples_service_alert_is_blocked_without_coverage(self):
        # Last two windows (ends 110000 and 120000) each contain 20 failures
        # placed in [100000, 110000), which belongs to exactly those two
        # fully-in-scope windows for a scope ending at 120000.
        records = [
            obs(site="a", t=100_000, outcome="deadline") for _ in range(20)
        ]
        scopes = {(1, "a"): (0, 120_000)}

        covered_windows, covered_actions = evaluate(
            records, origin_ms=0, duration_s=120, scopes=scopes,
            coverage={(1, "a"): [(0, 120_000)]})
        self.assertEqual(covered_windows[-2][7], "bad")
        self.assertEqual(covered_windows[-1][7], "bad")
        self.assertIn((1, "a", 120_000, "open_incident"), covered_actions)

        gapped_windows, gapped_actions = evaluate(
            records, origin_ms=0, duration_s=120, scopes=scopes,
            coverage={})
        self.assertTrue(all(window[7] == "gap" for window in gapped_windows))
        self.assertNotIn("open_incident",
                         {action[3] for action in gapped_actions})
        self.assertEqual(
            gapped_actions, [(1, "a", 60_000, "open_gap")])

    def test_gap_delays_open_incident_for_identical_observations(self):
        records = [
            obs(site="d", t=100_000, outcome="deadline") for _ in range(20)
        ]
        scopes = {(1, "d"): (0, 180_000)}
        full_coverage = {(1, "d"): [(0, 180_000)]}
        delayed_coverage = {
            # Missing [50000, 60000) makes the end=110000 window incomplete;
            # ends 120000..160000 remain bad, so the alert opens at 130000.
            (1, "d"): [(0, 50_000), (60_000, 180_000)]
        }

        _, full_actions = evaluate(
            records, origin_ms=0, duration_s=180, scopes=scopes,
            coverage=full_coverage)
        _, delayed_actions = evaluate(
            records, origin_ms=0, duration_s=180, scopes=scopes,
            coverage=delayed_coverage)

        self.assertIn((1, "d", 120_000, "open_incident"), full_actions)
        self.assertIn((1, "d", 120_000, "resolve_gap"), delayed_actions)
        self.assertIn((1, "d", 130_000, "open_incident"), delayed_actions)
        self.assertNotIn((1, "d", 120_000, "open_incident"), delayed_actions)

    def test_open_incident_crosses_gap_and_needs_two_good_windows_afterward(self):
        records = good_points(0, 600_000, revision=9, site="x")
        records.extend(
            obs(revision=9, site="x", t=100_000, outcome="unavailable",
                latency=1000)
            for _ in range(20))
        scopes = {(9, "x"): (0, 600_000)}

        # Missing [160000, 170000) makes ends 170000..220000 gap windows.
        coverage = {(9, "x"): [(0, 160_000), (170_000, 600_000)]}
        windows, actions = evaluate(
            records, origin_ms=0, duration_s=600, scopes=scopes,
            coverage=coverage)
        verdict_by_end = {window[2]: window[7] for window in windows}

        self.assertEqual(verdict_by_end[110_000], "bad")
        self.assertEqual(verdict_by_end[120_000], "bad")
        self.assertIn((9, "x", 120_000, "open_incident"), actions)

        gap_ends = [end for end, verdict in verdict_by_end.items()
                    if verdict == "gap"]
        self.assertEqual(gap_ends, [170_000, 180_000, 190_000,
                                    200_000, 210_000, 220_000])
        self.assertIn((9, "x", 170_000, "open_gap"), actions)
        self.assertIn((9, "x", 230_000, "resolve_gap"), actions)

        # End 230000 is the first non-gap and is good, but only the first of
        # a new consecutive pair.  End 240000 is the second good window.
        self.assertEqual(verdict_by_end[230_000], "good")
        self.assertEqual(verdict_by_end[240_000], "good")
        self.assertIn((9, "x", 240_000, "resolve_incident"), actions)
        resolve_index = actions.index((9, "x", 240_000, "resolve_incident"))
        self.assertFalse(
            any(action[3] == "resolve_incident"
                and action[2] < 240_000
                for action in actions[:resolve_index]))

    def test_first_non_gap_window_resolves_gap_even_if_low(self):
        records = [obs(site="l", t=20_000, latency=10) for _ in range(3)]
        windows, actions = evaluate(
            records, origin_ms=0, duration_s=70,
            scopes={(1, "l"): (0, 70_000)},
            # First window [0,60000) misses [0,10000); second [10000,70000)
            # is fully covered but has only three samples.
            coverage={(1, "l"): [(10_000, 70_000)]})
        self.assertEqual(windows[0][7], "gap")
        self.assertEqual(windows[1][7], "low")
        self.assertEqual(
            actions,
            [(1, "l", 60_000, "open_gap"),
             (1, "l", 70_000, "resolve_gap")])

    def test_no_resolution_is_invented_at_interval_end(self):
        # One bad window is not enough to open; then the interval ends while
        # collection gap is open.  Neither a second service action nor a gap
        # resolution may be synthesized.
        records = [obs(site="e", t=1_000, outcome="deadline") for _ in range(20)]
        windows, actions = evaluate(
            records, origin_ms=0, duration_s=60,
            scopes={(1, "e"): (0, 60_000)}, coverage={})
        self.assertEqual(windows[0][7], "gap")
        self.assertEqual(actions, [(1, "e", 60_000, "open_gap")])

    def test_same_site_is_not_merged_across_revisions(self):
        records = []
        for revision in (1, 2):
            records.extend(
                obs(revision=revision, site="same", t=1_000,
                    latency=10, outcome="deadline")
                for _ in range(20))
        windows, _ = evaluate(
            records, origin_ms=0, duration_s=60,
            scopes={(1, "same"): (0, 60_000),
                    (2, "same"): (0, 60_000)},
            coverage={(1, "same"): [(0, 60_000)],
                      (2, "same"): [(0, 60_000)]})
        self.assertEqual([window[0] for window in windows], [1, 2])
        self.assertTrue(all(window[3] == 20 and window[4] == 20
                            and window[7] == "bad" for window in windows))

    def test_record_order_does_not_change_result(self):
        origin = 10**12
        records = []
        rng = random.Random(20261125)
        for i in range(500):
            records.append(obs(
                revision=3, site="ord",
                t=origin + rng.randrange(0, 240_000),
                latency=rng.randrange(0, 300),
                outcome=rng.choice(
                    ["delivered", "delivered", "deadline", "unavailable"])))
        scopes = {(3, "ord"): (0, 240_000)}
        coverage = {(3, "ord"): [(0, 240_000)]}

        first_windows, first_actions = evaluate(
            iter(list(records)), origin_ms=origin, duration_s=240,
            scopes=scopes, coverage=coverage)
        shuffled = list(records)
        rng.shuffle(shuffled)
        second_windows, second_actions = evaluate(
            iter(shuffled), origin_ms=origin, duration_s=240,
            scopes=scopes, coverage=coverage)
        self.assertEqual(first_windows[0][2], 60_000)
        self.assertEqual(first_windows, second_windows)
        self.assertEqual(first_actions, second_actions)


class ValidationTest(unittest.TestCase):
    BASE_KWARGS = dict(
        origin_ms=0,
        duration_s=60,
        scopes={(1, "s"): (0, 60_000)},
        coverage={},
    )

    def assert_invalid(self, **changes):
        kwargs = dict(self.BASE_KWARGS)
        kwargs.update(changes)
        with self.assertRaises(ValueError):
            evaluate(iter(()), **kwargs)

    def test_basic_arguments_must_have_specified_structure(self):
        self.assert_invalid(origin_ms=True)
        self.assert_invalid(origin_ms=-1)
        self.assert_invalid(duration_s=50)
        self.assert_invalid(duration_s=65)
        self.assert_invalid(duration_s=3610)
        self.assert_invalid(scopes={})
        self.assert_invalid(scopes={(True, "s"): (0, 60_000)})
        self.assert_invalid(scopes={(1, "bad site"): (0, 60_000)})
        self.assert_invalid(scopes={(1, "s"): (0, 50_000)})
        self.assert_invalid(scopes={(1, "s"): (1, 60_001)})
        self.assert_invalid(coverage=[])
        self.assert_invalid(coverage={(2, "s"): [(0, 60_000)]})
        self.assert_invalid(
            coverage={(1, "s"): [(0, 30_000), (30_001, 60_001)]})

    def test_observation_validation(self):
        bad_records = [
            {},
            {**obs(), "extra": 1},
            {**obs(revision=True)},
            {**obs(revision=0)},
            {**obs(t=True)},
            {**obs(t=-1)},
            {**obs(latency=True)},
            {**obs(latency=-1)},
            {**obs(latency=1001)},
            {**obs(outcome="unknown")},
            # Valid fields but no matching scope.
            obs(revision=2),
            # Outside the half-open scope.
            obs(t=60_000),
        ]
        for record in bad_records:
            with self.subTest(record=record):
                with self.assertRaises(ValueError):
                    evaluate(iter([record]), **self.BASE_KWARGS)

    def test_too_many_observations_are_rejected(self):
        def generated():
            for i in range(2_000_001):
                yield obs(t=(i % 60) * 1000)

        with self.assertRaises(ValueError):
            evaluate(generated(), **self.BASE_KWARGS)


if __name__ == "__main__":
    unittest.main()
