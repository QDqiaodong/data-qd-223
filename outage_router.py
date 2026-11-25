"""Zone-aware takeover on top of :class:`router.Router`.

``OutageRouter(router, sites)`` wraps an existing deadline-mode ``Router``.
The publishing side installs a complete deployment plan: every bucket has a
steady primary site and a fixed eligibility list, and every site has a number
of bucket slots it can absorb during one zone outage.  ``install`` proves that
each possible zone failure can be covered entirely, records the deterministic
backup assignment and returns a revision number.

``get(bucket, path, budget=..., outage_zone=...)`` then sends a real GET only
to the one site the currently approved revision allows for that bucket: the
primary under normal conditions, or the out-of-zone backup when the bucket's
primary zone is declared failed.  There is no rotation or failover between
sites; a failing chosen site follows the ordinary single-candidate error
semantics of the wrapped router.

Only the standard library is used.
"""
from collections import OrderedDict, deque
from collections.abc import Mapping
import math
import re
import threading

from router import DeadlineExceeded, Router, Unavailable

__all__ = ["OutageRouter", "NoFeasiblePlan", "DeadlineExceeded",
           "Unavailable"]


class NoFeasiblePlan(Exception):
    """The configuration is well-formed but some zone cannot be covered."""

    def __init__(self, zones):
        self.zones = tuple(zones)
        super().__init__(
            "no complete takeover plan for zone(s): %s"
            % ", ".join(repr(zone) for zone in self.zones)
        )


_ID_PATTERN = re.compile(r"[A-Za-z0-9_-]{1,32}\Z")
_MAX_BUCKETS = 4096
_MAX_CANDIDATES = 15


def _valid_id(value):
    return isinstance(value, str) and _ID_PATTERN.fullmatch(value) is not None


def _is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def _finite_positive(value):
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value > 0
    )


_MAX_CACHED_PATH = 2048
_CACHE_LIMIT = 128
_WAIT_POLL_SECONDS = 0.005
_ETAG_RE = re.compile(rb'^"[A-Za-z0-9._-]{1,64}"\Z')
_MAX_AGE_RE = re.compile(rb"^max-age=([0-9]+)\Z", re.IGNORECASE)


class _CacheEntry:
    __slots__ = ("body", "etag", "max_age", "expires_at")

    def __init__(self, body, etag, max_age, expires_at):
        self.body = body
        self.etag = etag
        self.max_age = max_age
        self.expires_at = expires_at

    def fresh(self, now):
        return now < self.expires_at


class _Flight:
    """One actual origin exchange for one cache identity."""

    __slots__ = ("entry", "done", "kind", "status", "body", "error", "max_age")

    def __init__(self, entry):
        self.entry = entry
        self.done = False
        self.kind = None  # shared | private | error | abandoned
        self.status = None
        self.body = None
        self.error = None
        self.max_age = None


def _parse_max_age(headers):
    values = headers.get(b"cache-control", ())
    if len(values) != 1:
        return None
    match = _MAX_AGE_RE.fullmatch(values[0].strip())
    if match is None:
        return None
    digits = match.group(1).lstrip(b"0") or b"0"
    if len(digits) > 4:
        return None
    max_age = int(digits)
    if not 1 <= max_age <= 3600:
        return None
    return max_age


def _parse_strong_etag(headers):
    values = headers.get(b"etag", ())
    if len(values) != 1:
        return None
    etag = values[0].strip()
    if _ETAG_RE.fullmatch(etag) is None:
        return None
    return etag


def _vary_allows_language_only(headers):
    values = headers.get(b"vary")
    if not values:
        return True
    tokens = []
    for value in values:
        tokens.extend(part.strip().lower() for part in value.split(b","))
    tokens = [token for token in tokens if token]
    return bool(tokens) and all(token == b"accept-language" for token in tokens)


def _shared_200_metadata(headers):
    if headers.get(b"set-cookie"):
        return None
    max_age = _parse_max_age(headers)
    if max_age is None:
        return None
    etag = _parse_strong_etag(headers)
    if etag is None or not _vary_allows_language_only(headers):
        return None
    return etag, max_age


class _Plan:
    """One approved revision; never mutated after publication."""

    __slots__ = ("by_bucket", "zones")

    def __init__(self, by_bucket, zones):
        self.by_bucket = by_bucket  # bucket -> (primary, zone, backup)
        self.zones = zones          # frozenset of known zone ids


class _Dinic:
    """Small deterministic max-flow implementation (integer capacities)."""

    __slots__ = ("size", "graph", "_level", "_current")

    def __init__(self, size):
        self.size = size
        self.graph = [[] for _ in range(size)]
        self._level = []
        self._current = []

    def add_edge(self, source, target, capacity):
        """Add an edge and return the mutable forward edge record."""
        forward = [target, capacity, len(self.graph[target])]
        backward = [source, 0, len(self.graph[source])]
        self.graph[source].append(forward)
        self.graph[target].append(backward)
        return forward

    def max_flow(self, source, sink):
        flow = 0
        while self._build_levels(source, sink):
            self._current = [0] * self.size
            while True:
                pushed = self._augment_once(source, sink)
                if not pushed:
                    break
                flow += pushed
        return flow

    def _build_levels(self, source, sink):
        level = [-1] * self.size
        level[source] = 0
        queue = deque((source,))
        graph = self.graph
        while queue:
            node = queue.popleft()
            next_level = level[node] + 1
            for edge in graph[node]:
                target = edge[0]
                if edge[1] > 0 and level[target] < 0:
                    level[target] = next_level
                    queue.append(target)
        self._level = level
        return level[sink] >= 0

    def _augment_once(self, source, sink):
        graph = self.graph
        level = self._level
        current = self._current
        nodes = [source]
        path = []
        while nodes:
            node = nodes[-1]
            if node == sink:
                pushed = min(edge[1] for edge in path)
                for edge in path:
                    edge[1] -= pushed
                    graph[edge[0]][edge[2]][1] += pushed
                return pushed

            edges = graph[node]
            advanced = False
            while current[node] < len(edges):
                edge = edges[current[node]]
                target = edge[0]
                if edge[1] > 0 and level[target] == level[node] + 1:
                    path.append(edge)
                    nodes.append(target)
                    advanced = True
                    break
                current[node] += 1
            if advanced:
                continue

            nodes.pop()
            if path:
                path.pop()
                current[nodes[-1]] += 1
        return 0


class OutageRouter:
    """Approve zone-outage plans and route each bucket to its chosen site."""

    _MAX_SITES = 16
    _MIN_SITES = 2

    def __init__(self, router, sites):
        if not isinstance(router, Router) or not router.deadline_mode:
            raise ValueError(
                "OutageRouter requires a Router built with deadline_mode=True")
        if not isinstance(sites, Mapping):
            raise ValueError("sites must be a mapping of site id -> (zone, endpoint)")
        if not self._MIN_SITES <= len(sites) <= self._MAX_SITES:
            raise ValueError("sites must contain 2-16 entries")

        site_zone = {}
        site_endpoint = {}
        for site_id, value in sites.items():
            if not _valid_id(site_id):
                raise ValueError(
                    "site ids must be 1-32 ASCII letters, digits, '_' or '-'")
            if not isinstance(value, (tuple, list)) or len(value) != 2:
                raise ValueError("each site must map to (zone, endpoint)")
            zone, endpoint = value
            if not _valid_id(zone):
                raise ValueError(
                    "zone ids must be 1-32 ASCII letters, digits, '_' or '-'")
            if not isinstance(endpoint, (tuple, list)) or len(endpoint) != 2:
                raise ValueError("endpoint must be a (host, port) pair")
            endpoint = tuple(endpoint)
            if endpoint not in router.endpoints:
                raise ValueError(
                    "site endpoints must be the router's endpoints")
            site_zone[site_id] = zone
            site_endpoint[site_id] = endpoint

        if len(set(site_endpoint.values())) != len(site_endpoint):
            raise ValueError("each router endpoint must belong to one site")
        if set(site_endpoint.values()) != set(router.endpoints):
            raise ValueError(
                "sites must correspond one-to-one with the router endpoints")
        zones = frozenset(site_zone.values())
        if len(zones) < 2:
            raise ValueError("sites must span at least two zones")

        self.router = router
        self._site_zone = site_zone
        self._site_endpoint = site_endpoint
        self._site_order = tuple(sorted(site_zone))
        self._zone_order = tuple(sorted(zones))
        self._zones = zones
        self._install_lock = threading.Lock()
        self._plan = None
        self._revision = 0

        # Cache state is guarded by one condition.  Network I/O is always
        # performed outside the lock so an in-flight identity cannot block
        # hits for unrelated identities.
        self._cache_condition = threading.Condition()
        self._cache = OrderedDict()
        self._in_flight = {}
        self._cache_waiter_count = 0

    # ------------------------------------------------------------------
    # Plan approval
    # ------------------------------------------------------------------
    def install(self, buckets, spare):
        """Replace the whole deployment plan atomically.

        Returns ``(revision, assignments)``.  Invalid structure raises
        ``ValueError``; a well-formed configuration that cannot cover some
        zone raises :class:`NoFeasiblePlan`.  On failure no state changes.
        """
        parsed = self._validate_buckets(buckets)
        free_slots = self._validate_spare(spare)

        by_bucket, infeasible = self._build_assignments(parsed, free_slots)
        if infeasible:
            raise NoFeasiblePlan(infeasible)

        plan = _Plan(by_bucket, self._zones)
        assignments = {
            bucket: by_bucket[bucket][2] for bucket in sorted(by_bucket)
        }
        with self._install_lock:
            revision = self._revision + 1
            self._plan = plan
            self._revision = revision
        return revision, assignments

    def _validate_buckets(self, buckets):
        if not isinstance(buckets, Mapping):
            raise ValueError("buckets must be a mapping")
        if not 1 <= len(buckets) <= _MAX_BUCKETS:
            raise ValueError("buckets must contain 1-4096 entries")

        parsed = {}
        for bucket, value in buckets.items():
            if not _is_int(bucket) or not 0 <= bucket <= 4095:
                raise ValueError("bucket ids must be integers in 0-4095")
            if not isinstance(value, (tuple, list)) or len(value) != 2:
                raise ValueError(
                    "each bucket must map to (primary_site, eligible_sites)")
            primary, candidates = value
            if not isinstance(primary, str) or primary not in self._site_zone:
                raise ValueError("bucket primary must be a known site id")
            if not isinstance(candidates, (tuple, list)):
                raise ValueError("eligible sites must be a list or tuple")
            if len(candidates) > _MAX_CANDIDATES:
                raise ValueError("at most 15 eligible sites are allowed")
            seen = set()
            for candidate in candidates:
                if not isinstance(candidate, str) \
                        or candidate not in self._site_zone:
                    raise ValueError("eligible sites must be known site ids")
                if candidate in seen:
                    raise ValueError("eligible sites must not repeat")
                seen.add(candidate)
            # Canonical ordering makes the result independent of the input
            # candidate order.
            parsed[bucket] = (primary, tuple(sorted(seen)))
        return parsed

    def _validate_spare(self, spare):
        if not isinstance(spare, Mapping):
            raise ValueError("spare must be a mapping")
        if set(spare) != set(self._site_zone):
            raise ValueError("spare must provide exactly every known site")
        result = {}
        for site_id, slots in spare.items():
            if not _is_int(slots) or not 0 <= slots <= _MAX_BUCKETS:
                raise ValueError(
                    "spare slots must be integers in 0-4096 for every site")
            result[site_id] = slots
        return result

    def _build_assignments(self, parsed, free_slots):
        buckets_by_zone = {}
        for bucket in sorted(parsed):
            primary, _ = parsed[bucket]
            buckets_by_zone.setdefault(
                self._site_zone[primary], []).append(bucket)

        by_bucket = {}
        infeasible = []
        for zone in self._zone_order:
            zone_buckets = buckets_by_zone.get(zone, ())
            if not zone_buckets:
                continue
            chosen = self._cover_zone(zone, zone_buckets, parsed, free_slots)
            if chosen is None:
                infeasible.append(zone)
                continue
            for bucket, site_id in chosen.items():
                primary = parsed[bucket][0]
                by_bucket[bucket] = (primary, zone, site_id)
        return by_bucket, infeasible

    def _cover_zone(self, zone, zone_buckets, parsed, free_slots):
        """Assign every bucket of ``zone`` one out-of-zone site, or None."""
        usable_sites = [
            site_id for site_id in self._site_order
            if self._site_zone[site_id] != zone and free_slots[site_id] > 0
        ]
        site_index = {site_id: index for index, site_id in enumerate(usable_sites)}
        bucket_count = len(zone_buckets)
        site_offset = 1 + bucket_count
        sink = site_offset + len(usable_sites)
        flow = _Dinic(sink + 1)

        for index, bucket in enumerate(zone_buckets):
            flow.add_edge(0, 1 + index, 1)
            _, candidates = parsed[bucket]
            for site_id in candidates:  # already canonical-sorted
                node = site_index.get(site_id)
                if node is not None:
                    # The mutable forward record tells us the final flow.
                    flow.add_edge(1 + index, site_offset + node, 1)
        for node, site_id in enumerate(usable_sites):
            flow.add_edge(site_offset + node, sink, free_slots[site_id])

        if flow.max_flow(0, sink) != bucket_count:
            return None

        chosen = {}
        for index, bucket in enumerate(zone_buckets):
            node = 1 + index
            for edge in flow.graph[node]:
                target = edge[0]
                if (site_offset <= target < sink
                        and edge[1] == 0
                        and flow.graph[target][edge[2]][1] == 1):
                    chosen[bucket] = usable_sites[target - site_offset]
                    break
        if len(chosen) != bucket_count:  # pragma: no cover - defensive
            return None
        return chosen

    # ------------------------------------------------------------------
    # Gateway routing
    # ------------------------------------------------------------------
    def get(self, bucket, path, *, budget, outage_zone=None):
        """Send one GET to the site the approved plan allows for ``bucket``."""
        # Snapshot the complete plan once; an install racing with this call is
        # either entirely visible or entirely invisible.
        plan = self._plan
        if plan is None:
            raise Unavailable(path)
        if not _is_int(bucket) or bucket not in plan.by_bucket:
            raise ValueError("bucket is not part of the approved plan")
        if outage_zone is not None:
            if not isinstance(outage_zone, str) or outage_zone not in plan.zones:
                raise ValueError("outage zone is not in the site directory")

        primary, zone, backup = plan.by_bucket[bucket]
        if outage_zone is not None and outage_zone == zone:
            endpoint = self._site_endpoint[backup]
        else:
            endpoint = self._site_endpoint[primary]
        return self.router.get_from(endpoint, path, budget=budget)

    # ------------------------------------------------------------------
    # Shared cached GET
    # ------------------------------------------------------------------
    def get_cached(self, bucket, path, *, budget, language="zh",
                   outage_zone=None):
        """GET a public resource, coalescing identical origins and caching 200s.

        Returns the same ``(status, body)`` pair as :meth:`get`.  Only qualified
        200 responses are resident cache entries; revalidation uses a strong
        ETag.  Waiting callers share one in-flight origin exchange.
        """
        plan = self._plan
        if plan is None:
            raise Unavailable(path)
        if not _is_int(bucket) or bucket not in plan.by_bucket:
            raise ValueError("bucket is not part of the approved plan")
        if outage_zone is not None:
            if not isinstance(outage_zone, str) \
                    or outage_zone not in plan.zones:
                raise ValueError("outage zone is not in the site directory")

        primary, zone, backup = plan.by_bucket[bucket]
        if outage_zone is not None and outage_zone == zone:
            endpoint = self._site_endpoint[backup]
        else:
            endpoint = self._site_endpoint[primary]

        if language not in ("zh", "en"):
            raise ValueError("language must be 'zh' or 'en'")
        if not _finite_positive(budget):
            raise ValueError("budget must be a finite positive number")
        if (not isinstance(path, str)
                or len(path) > _MAX_CACHED_PATH
                or not Router._valid_deadline_path(path)):
            raise ValueError("expected a local ASCII path of at most 2048 chars")

        # The cached entry is still one router call for purposes of the
        # instance-wide 32-call limit.  Waiting therefore occupies a call slot
        # but never an endpoint/origin slot.
        deadline = self.router.clock() + budget
        self.router._acquire_call_slot(path)
        try:
            identity = (bucket, endpoint, path, language)
            return self._cached_wait_or_fetch(
                identity, endpoint, path, language, deadline)
        finally:
            self.router._release_call_slot()

    def _cached_wait_or_fetch(self, identity, endpoint, path, language,
                              deadline):
        observed = None
        while True:
            leader = None
            with self._cache_condition:
                while True:
                    now = self.router.clock()
                    if now >= deadline:
                        raise DeadlineExceeded(path)

                    # A terminal flight belonging to this call must be handled
                    # before looking for a successor in the identity map.  This
                    # preserves an upstream failure verdict for every member of
                    # the original batch.
                    if observed is not None and observed.done:
                        if observed.kind == "error":
                            raise observed.error
                        if observed.kind == "shared":
                            return 200, observed.body
                        # private/abandoned: this caller may compete to continue.
                        observed = None
                        continue

                    entry = self._cache.get(identity)
                    if entry is not None and entry.fresh(now):
                        self._cache.move_to_end(identity)
                        return 200, entry.body

                    current = self._in_flight.get(identity)
                    if current is None:
                        leader = _Flight(entry)
                        self._in_flight[identity] = leader
                        observed = leader
                        break

                    observed = current
                    if current.done:
                        continue
                    remaining = deadline - now
                    self._cache_waiter_count += 1
                    try:
                        self._cache_condition.wait(
                            timeout=min(
                                _WAIT_POLL_SECONDS, max(0.0, remaining)))
                    finally:
                        self._cache_waiter_count -= 1

            return self._run_cache_leader(
                leader, identity, endpoint, path, language, deadline)

    def _run_cache_leader(self, flight, identity, endpoint, path, language,
                          deadline):
        clock = self.router.clock
        now = clock()
        if now >= deadline:
            self._abandon_flight(identity, flight)
            raise DeadlineExceeded(path)

        entry = flight.entry
        request_headers = [("Accept-Language", language)]
        validator = None
        if entry is not None:
            request_headers.append(
                ("If-None-Match", entry.etag.decode("ascii")))

            def validator(status, headers, _body):
                if status != 304:
                    return True
                max_age = self._valid_304_metadata(headers, entry)
                if max_age is None:
                    return False
                flight.max_age = max_age
                return True

        remaining = deadline - now
        try:
            status, body, headers = self.router.request_from(
                endpoint, path, budget=remaining,
                extra_headers=request_headers,
                allow_empty_not_modified=True,
                response_validator=validator,
                call_already_admitted=True)
        except DeadlineExceeded:
            # The initiating caller's deadline cancels only this origin
            # exchange.  Its socket and endpoint token have already been
            # closed/released by Router before the exception arrives.
            self._abandon_flight(identity, flight)
            raise
        except Unavailable as exc:
            self._fail_flight(identity, flight, exc)
            if clock() >= deadline:
                raise DeadlineExceeded(path) from exc
            raise
        except BaseException as exc:  # defensive: never strand batch waiters
            self._fail_flight(identity, flight, exc)
            raise

        confirmed_at = clock()
        caller_expired = confirmed_at >= deadline

        if status == 304:
            max_age = flight.max_age
            if entry is None or max_age is None:
                error = Unavailable(path)
                self._fail_flight(identity, flight, error)
                if caller_expired:
                    raise DeadlineExceeded(path) from error
                raise error

            body = entry.body
            with self._cache_condition:
                entry.expires_at = confirmed_at + max_age
                self._cache[identity] = entry
                self._cache.move_to_end(identity)
                self._evict_cache_locked()
                self._finish_shared_locked(identity, flight, body)
            if caller_expired:
                raise DeadlineExceeded(path)
            return 200, body

        metadata = _shared_200_metadata(headers) if status == 200 else None
        if metadata is not None:
            etag, max_age = metadata
            cached = _CacheEntry(
                body, etag, max_age, confirmed_at + max_age)
            with self._cache_condition:
                self._cache[identity] = cached
                self._cache.move_to_end(identity)
                self._evict_cache_locked()
                self._finish_shared_locked(identity, flight, body)
            if caller_expired:
                raise DeadlineExceeded(path)
            return status, body

        # A deliverable but non-shareable response is private to this actual
        # permitted origin caller.  It must not be broadcast or retained.
        with self._cache_condition:
            self._cache.pop(identity, None)
            if caller_expired:
                self._finish_abandoned_locked(identity, flight)
            else:
                self._finish_private_locked(
                    identity, flight, status, body)
        if caller_expired:
            raise DeadlineExceeded(path)
        return status, body

    @staticmethod
    def _valid_304_metadata(headers, entry):
        max_age = _parse_max_age(headers)
        etag = _parse_strong_etag(headers)
        if max_age is None or etag != entry.etag:
            return None
        if headers.get(b"set-cookie"):
            return None
        if not _vary_allows_language_only(headers):
            return None

        values = headers.get(b"content-length")
        if values is not None:
            if len(values) != 1:
                return None
            value = values[0].strip()
            if not value.isdigit():
                return None
            digits = value.lstrip(b"0") or b"0"
            if len(digits) > len(str(Router._MAX_BODY_BYTES)):
                return None
            if int(digits) != len(entry.body):
                return None
        return max_age

    def _evict_cache_locked(self):
        while len(self._cache) > _CACHE_LIMIT:
            self._cache.popitem(last=False)

    def _finish_shared_locked(self, identity, flight, body):
        if self._in_flight.get(identity) is flight:
            del self._in_flight[identity]
        flight.kind = "shared"
        flight.body = body
        flight.done = True
        self._cache_condition.notify_all()

    def _finish_private_locked(self, identity, flight, status, body):
        if self._in_flight.get(identity) is flight:
            del self._in_flight[identity]
        flight.kind = "private"
        flight.status = status
        flight.body = body
        flight.done = True
        self._cache_condition.notify_all()

    def _finish_abandoned_locked(self, identity, flight):
        if self._in_flight.get(identity) is flight:
            del self._in_flight[identity]
        flight.kind = "abandoned"
        flight.done = True
        self._cache_condition.notify_all()

    def _abandon_flight(self, identity, flight):
        with self._cache_condition:
            self._finish_abandoned_locked(identity, flight)

    def _fail_flight(self, identity, flight, error):
        with self._cache_condition:
            if self._in_flight.get(identity) is flight:
                del self._in_flight[identity]
            flight.kind = "error"
            flight.error = error
            flight.done = True
            self._cache_condition.notify_all()
