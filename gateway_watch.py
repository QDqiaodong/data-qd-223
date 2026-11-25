"""Offline reconstruction of gateway service and collection-gap actions.

The public entry point is :func:`evaluate`.  It consumes passive request
attribution records produced by :class:`outage_router.OutageRouter`, groups
them by the exact ``(revision, site)`` that scheduled each call, and computes
60-second rolling windows plus two independent kinds of on-call actions.

Only the Python standard library is used.  To stay bounded under a small
address-space limit, delivered latencies are accumulated in compact 32-bit
histograms; raw observations are never retained as a list.
"""

from collections.abc import Mapping
import re
from array import array

__all__ = ["evaluate"]

_REVISION_MIN = 1
_REVISION_MAX = (1 << 63) - 1
_TIME_MAX = (1 << 63) - 1
_MAX_SCOPES = 24
_MAX_COVERAGE_INTERVALS = 720
_MAX_OBSERVATIONS = 2_000_000

_STEP_MS = 10_000
_WINDOW_MS = 60_000
_MIN_SCOPE_MS = 60_000
_LATENCY_BINS = 1001

_SITE_RE = re.compile(r"[A-Za-z0-9_-]{1,32}\Z")
_OBSERVATION_KEYS = frozenset((
    "revision", "site", "completed_ms", "latency_ms", "outcome"))
_DELIVERED = "delivered"
_DEADLINE = "deadline"
_UNAVAILABLE = "unavailable"
_OUTCOMES = frozenset((_DELIVERED, _DEADLINE, _UNAVAILABLE))


def _is_integer(value):
    return isinstance(value, int) and not isinstance(value, bool)


def _valid_site(value):
    return isinstance(value, str) and _SITE_RE.fullmatch(value) is not None


def _integer_pair(value, *, name):
    if not isinstance(value, (tuple, list)) or len(value) != 2:
        raise ValueError("%s must be an (lo, hi) pair" % name)
    lo, hi = value
    if not _is_integer(lo) or not _is_integer(hi):
        raise ValueError("%s endpoints must be integers" % name)
    return lo, hi


def _validate_scopes(scopes, duration_ms):
    if not isinstance(scopes, Mapping):
        raise ValueError("scopes must be a mapping")
    if not 1 <= len(scopes) <= _MAX_SCOPES:
        raise ValueError("scopes must contain 1-24 sequences")

    normalized = {}
    for key, interval in scopes.items():
        if not isinstance(key, tuple) or len(key) != 2:
            raise ValueError("scope keys must be (revision, site) tuples")
        revision, site = key
        if (not _is_integer(revision)
                or not _REVISION_MIN <= revision <= _REVISION_MAX):
            raise ValueError("scope revision must be an integer 1..2**63-1")
        if not _valid_site(site):
            raise ValueError("scope site must be a valid gateway site id")

        lo, hi = _integer_pair(interval, name="scope interval")
        if not 0 <= lo < hi <= duration_ms:
            raise ValueError("scope interval must lie within the analysis range")
        if lo % _STEP_MS != 0 or hi % _STEP_MS != 0:
            raise ValueError("scope endpoints must align to 10000 ms")
        if hi - lo < _MIN_SCOPE_MS:
            raise ValueError("scope interval must be at least 60000 ms")

        normalized[(revision, site)] = (lo, hi)
    return normalized


def _validate_coverage(coverage, scopes):
    if not isinstance(coverage, Mapping):
        raise ValueError("coverage must be a mapping")

    normalized = {key: () for key in scopes}
    for raw_key, intervals in coverage.items():
        if not isinstance(raw_key, tuple) or len(raw_key) != 2:
            raise ValueError("coverage keys must be (revision, site) tuples")
        revision, site = raw_key
        if (not _is_integer(revision)
                or not _REVISION_MIN <= revision <= _REVISION_MAX
                or not _valid_site(site)):
            raise ValueError("coverage key is not a valid sequence")
        key = (revision, site)
        if key not in scopes:
            raise ValueError("coverage key must be one of scopes")

        scope_lo, scope_hi = scopes[key]
        merged_parts = []
        count = 0
        try:
            interval_iter = iter(intervals)
        except TypeError as exc:
            raise ValueError("coverage intervals must be iterable") from exc

        for interval in interval_iter:
            if count >= _MAX_COVERAGE_INTERVALS:
                raise ValueError(
                    "a sequence may have at most 720 coverage intervals")
            lo, hi = _integer_pair(interval, name="coverage interval")
            if not scope_lo <= lo < hi <= scope_hi:
                raise ValueError(
                    "coverage interval must be a nonempty integer interval "
                    "inside its sequence range")
            merged_parts.append((lo, hi))
            count += 1

        normalized[key] = _merge_intervals(merged_parts)
    return normalized


def _merge_intervals(intervals):
    if not intervals:
        return ()
    ordered = sorted(intervals)
    merged = [[ordered[0][0], ordered[0][1]]]
    for lo, hi in ordered[1:]:
        current_hi = merged[-1][1]
        if lo > current_hi:
            merged.append([lo, hi])
        elif hi > current_hi:
            merged[-1][1] = hi
    return tuple((lo, hi) for lo, hi in merged)


def _covered(start, end, merged_intervals):
    """Whether merged half-open intervals contain all of [start, end)."""
    position = start
    for lo, hi in merged_intervals:
        if hi <= position:
            continue
        if lo > position:
            return False
        position = hi
        if position >= end:
            return True
    return position >= end


def _percentile95(window_index, histogram, delivered):
    if delivered <= 0:
        return None
    # Exact nearest rank: ceil(0.95 * n) = ceil(19n/20).
    rank = (19 * delivered + 19) // 20
    offset = window_index * _LATENCY_BINS
    cumulative = 0
    for latency_ms in range(_LATENCY_BINS):
        cumulative += histogram[offset + latency_ms]
        if cumulative >= rank:
            return latency_ms
    return None  # pragma: no cover - rank is guaranteed to be found


def evaluate(observations, *, origin_ms, duration_s, scopes, coverage):
    """Compute rolling windows and service/collection actions.

    The returned pair is ``(windows, actions)``.  See the project README for
    the complete semantics.  Invalid input raises :class:`ValueError` before a
    result is returned.
    """
    if not _is_integer(origin_ms) or not 0 <= origin_ms <= _TIME_MAX:
        raise ValueError("origin_ms must be an integer 0..2**63-1")
    if (not _is_integer(duration_s)
            or not 60 <= duration_s <= 3600
            or duration_s % 10 != 0):
        raise ValueError(
            "duration_s must be an integer multiple of 10 from 60 to 3600")
    duration_ms = duration_s * 1000

    scope_ranges = _validate_scopes(scopes, duration_ms)
    coverage_by_key = _validate_coverage(coverage, scope_ranges)

    # One contiguous histogram segment per possible complete 60-second window.
    # Window ends are relative to origin_ms and lie on the 10-second grid.
    sequence_by_key = {}
    window_meta = []
    for key, (lo, hi) in scope_ranges.items():
        revision, site = key
        first_end = lo + _WINDOW_MS
        window_count = (hi - first_end) // _STEP_MS + 1
        base = len(window_meta)
        sequence_by_key[key] = (lo, hi, first_end, window_count, base)
        for index in range(window_count):
            end_ms = first_end + index * _STEP_MS
            window_meta.append((revision, site, end_ms))

    window_count_total = len(window_meta)
    counts = [0] * window_count_total
    failures = [0] * window_count_total
    delivered_counts = [0] * window_count_total
    histogram = array("I", [0]) * (window_count_total * _LATENCY_BINS)

    try:
        observation_iter = iter(observations)
    except TypeError as exc:
        raise ValueError("observations must be a one-time iterable") from exc

    seen = 0
    for record in observation_iter:
        if seen >= _MAX_OBSERVATIONS:
            raise ValueError("at most 2000000 observations are supported")
        seen += 1

        if not isinstance(record, dict) or record.keys() != _OBSERVATION_KEYS:
            raise ValueError(
                "each observation must be a dict with exactly the five "
                "required attribution keys")

        revision = record["revision"]
        site = record["site"]
        completed_ms = record["completed_ms"]
        latency_ms = record["latency_ms"]
        outcome = record["outcome"]

        if (not _is_integer(revision)
                or not _REVISION_MIN <= revision <= _REVISION_MAX):
            raise ValueError("observation revision is invalid")
        if not _valid_site(site):
            raise ValueError("observation site is invalid")
        if (not _is_integer(completed_ms)
                or not 0 <= completed_ms <= _TIME_MAX):
            raise ValueError("completed_ms must be an integer 0..2**63-1")
        if not _is_integer(latency_ms) or not 0 <= latency_ms <= 1000:
            raise ValueError("latency_ms must be an integer 0..1000")
        if outcome not in _OUTCOMES:
            raise ValueError("observation outcome is invalid")

        key = (revision, site)
        sequence = sequence_by_key.get(key)
        if sequence is None:
            raise ValueError("observation does not belong to a scope")

        relative_ms = completed_ms - origin_ms
        lo, hi, first_end, _sequence_windows, base = sequence
        if not lo <= relative_ms < hi:
            raise ValueError(
                "observation completed_ms is outside its sequence interval")

        # A sample at t belongs to ends e with e-WINDOW <= t < e.  The strict
        # lower bound on e is why a sample exactly at an old window end starts
        # appearing in the following window rather than the ending one.
        first_grid_end = (relative_ms // _STEP_MS + 1) * _STEP_MS
        if first_grid_end < first_end:
            first_grid_end = first_end
        last_grid_end = min(hi, relative_ms + _WINDOW_MS)
        last_grid_end = (last_grid_end // _STEP_MS) * _STEP_MS

        if first_grid_end > last_grid_end:
            continue

        local_index = (first_grid_end - first_end) // _STEP_MS
        end_value = first_grid_end
        is_failure = outcome != _DELIVERED
        while end_value <= last_grid_end:
            window_index = base + local_index
            counts[window_index] += 1
            if is_failure:
                failures[window_index] += 1
            else:
                delivered_counts[window_index] += 1
                histogram[window_index * _LATENCY_BINS + latency_ms] += 1

            end_value += _STEP_MS
            local_index += 1

    order = sorted(
        range(window_count_total),
        key=lambda index: (
            window_meta[index][2],
            window_meta[index][0],
            window_meta[index][1],
        ),
    )

    states = {
        key: {
            "incident_open": False,
            "bad_streak": 0,
            "good_streak": 0,
            "gap_open": False,
        }
        for key in scope_ranges
    }
    windows = []
    actions = []

    for window_index in order:
        revision, site, end_ms = window_meta[window_index]
        key = (revision, site)
        start_ms = end_ms - _WINDOW_MS
        complete = _covered(
            start_ms, end_ms, coverage_by_key.get(key, ()))

        count = counts[window_index]
        failure_count = failures[window_index]
        delivered = delivered_counts[window_index]
        p95_ms = _percentile95(window_index, histogram, delivered)

        if not complete:
            verdict = "gap"
        elif count < 20:
            verdict = "low"
        elif failure_count * 20 >= count or (
                p95_ms is not None and p95_ms >= 200):
            verdict = "bad"
        else:
            verdict = "good"

        windows.append((
            revision, site, end_ms, count, failure_count, p95_ms,
            complete, verdict,
        ))

        state = states[key]
        if verdict == "bad":
            state["bad_streak"] += 1
            state["good_streak"] = 0
            if state["bad_streak"] >= 2 and not state["incident_open"]:
                state["incident_open"] = True
                actions.append((revision, site, end_ms, "open_incident"))
        elif verdict == "good":
            state["good_streak"] += 1
            state["bad_streak"] = 0
            if state["good_streak"] >= 2 and state["incident_open"]:
                state["incident_open"] = False
                actions.append((revision, site, end_ms, "resolve_incident"))
        else:
            # Both low and gap interrupt service-verdict continuity.  Neither
            # resolves an already open service incident.
            state["bad_streak"] = 0
            state["good_streak"] = 0

        if verdict == "gap":
            if not state["gap_open"]:
                state["gap_open"] = True
                actions.append((revision, site, end_ms, "open_gap"))
        elif state["gap_open"]:
            state["gap_open"] = False
            actions.append((revision, site, end_ms, "resolve_gap"))

    actions.sort(key=lambda action: (action[2], action[0], action[1]))
    return windows, actions
