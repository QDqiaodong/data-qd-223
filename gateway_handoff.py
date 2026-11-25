"""Durable local handoff store for gateway incident collection.

The store sits between the passive observation stream produced by
``OutageRouter`` and the offline reconstruction in :mod:`gateway_watch`.
Collectors register observations under an explicit per-slice ordinal, then
``seal`` each slice with the exact number of records they distributed.  A
named ``checkpoint`` freezes both the seen observations and the collection
coverage at one durable point in time, so a later reviewer can rebuild exactly
the evidence that existed when the handoff was made -- even after somebody
backfills the live store.

Only the Python standard library is used.  Observations live in a local
SQLite database and are streamed back into ``gateway_watch.evaluate``; they are
never materialised as one Python list.
"""

import json
import os
import re
import sqlite3

import gateway_watch as _watch

__all__ = ["HandoffStore"]

_DB_NAME = "handoff.sqlite3"
_FORMAT_VERSION = "1"

_MAX_BATCH = 4096
_MAX_CHECKPOINTS = 8
_MAX_ORDINAL = 1_999_999
_MAX_EXPECTED = 2_000_000
_MAX_OBSERVATIONS = _watch._MAX_OBSERVATIONS

_STEP_MS = _watch._STEP_MS
_NAME_RE = re.compile(r"[A-Za-z0-9_-]{1,32}\Z")

_OUTCOME_TO_CODE = {"delivered": 0, "deadline": 1, "unavailable": 2}
_CODE_TO_OUTCOME = ("delivered", "deadline", "unavailable")

_CHUNK = 800


class HandoffStore:
    """A single-writer, crash-recoverable incident handoff library.

    ``HandoffStore(directory)`` either creates a new store (empty directory
    plus the three configuration arguments) or reopens an existing one using
    only the directory.  The configuration follows
    :func:`gateway_watch.evaluate` exactly: ``origin_ms`` is the absolute start
    of the store's monotonic time axis, ``duration_s`` its length, and
    ``scopes`` the ``(revision, site) -> (lo, hi)`` duty intervals relative to
    ``origin_ms``.  A store belongs to one gateway instance and one timeline;
    configuration is fixed for its lifetime.
    """

    def __init__(self, directory, *, origin_ms=None, duration_s=None,
                 scopes=None):
        self._directory = os.fspath(directory)
        self._conn = None
        self._closed = False

        supplied = (origin_ms is not None, duration_s is not None,
                    scopes is not None)
        if any(supplied) and not all(supplied):
            raise ValueError(
                "origin_ms, duration_s and scopes must be supplied together")
        configured = all(supplied)
        normalized = None
        if configured:
            normalized = _validate_configuration(origin_ms, duration_s, scopes)

        if not os.path.isdir(self._directory):
            if os.path.exists(self._directory):
                raise ValueError("directory is not a directory")
            os.makedirs(self._directory)

        path = os.path.join(self._directory, _DB_NAME)
        existing = os.path.exists(path)
        if not existing:
            if os.listdir(self._directory):
                raise ValueError(
                    "directory must be empty to create a handoff store")
            if not configured:
                raise ValueError(
                    "a new handoff store requires origin_ms, duration_s and "
                    "scopes")

        conn = None
        try:
            conn = sqlite3.connect(
                path, isolation_level=None, timeout=30.0)
            try:
                conn.execute("PRAGMA journal_mode=WAL")
            except sqlite3.DatabaseError as exc:
                raise ValueError("not a handoff store") from exc
            # WAL + NORMAL survives an abrupt process exit; only an OS or
            # power failure can lose the most recent commit, which is outside
            # this store's recovery contract.
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=-2048")
            conn.execute("PRAGMA busy_timeout=30000")
            if existing:
                origin_ms, duration_s, scopes = self._load_configuration(
                    conn, normalized)
            else:
                origin_ms, duration_s, scopes = normalized
                self._create_schema(conn, origin_ms, duration_s, scopes)
        except BaseException:
            if conn is not None:
                conn.close()
            if not existing:
                # Do not leave a half-initialised database behind that would
                # block a corrected retry.
                _remove_store_files(self._directory)
            raise

        self._conn = conn
        self._origin_ms = origin_ms
        self._duration_s = duration_s
        self._scopes = scopes
        self._observation_count = conn.execute(
            "SELECT observation_count FROM state WHERE id = 1").fetchone()[0]
        self._load_slices(conn)

    # ------------------------------------------------------------------ setup

    @staticmethod
    def _create_schema(conn, origin_ms, duration_s, scopes):
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                "CREATE TABLE meta (key TEXT PRIMARY KEY, "
                "value TEXT NOT NULL)")
            conn.execute(
                "CREATE TABLE state (id INTEGER PRIMARY KEY CHECK (id = 1), "
                "observation_count INTEGER NOT NULL)")
            conn.execute(
                "CREATE TABLE slices (id INTEGER PRIMARY KEY, "
                "revision INTEGER NOT NULL, site TEXT NOT NULL, "
                "start_ms INTEGER NOT NULL, "
                "UNIQUE (revision, site, start_ms))")
            conn.execute(
                "CREATE TABLE observations (id INTEGER PRIMARY KEY, "
                "slice_id INTEGER NOT NULL, ordinal INTEGER NOT NULL, "
                "completed_ms INTEGER NOT NULL, latency_ms INTEGER NOT NULL, "
                "outcome INTEGER NOT NULL, "
                "UNIQUE (slice_id, ordinal))")
            conn.execute(
                "CREATE TABLE seal_events (id INTEGER PRIMARY KEY, "
                "slice_id INTEGER NOT NULL UNIQUE, "
                "expected INTEGER NOT NULL)")
            conn.execute(
                "CREATE TABLE checkpoints (name TEXT PRIMARY KEY, "
                "obs_id_max INTEGER NOT NULL, "
                "seal_id_max INTEGER NOT NULL)")

            encoded_scopes = json.dumps(
                sorted([revision, site, lo, hi]
                       for (revision, site), (lo, hi) in scopes.items()),
                separators=(",", ":"))
            for key, value in (
                    ("format_version", _FORMAT_VERSION),
                    ("origin_ms", str(origin_ms)),
                    ("duration_s", str(duration_s)),
                    ("scopes", encoded_scopes)):
                conn.execute(
                    "INSERT INTO meta (key, value) VALUES (?, ?)",
                    (key, value))
            conn.execute(
                "INSERT INTO state (id, observation_count) VALUES (1, 0)")

            rows = []
            for (revision, site), (lo, hi) in scopes.items():
                start = lo
                while start < hi:
                    rows.append((revision, site, start))
                    start += _STEP_MS
            conn.executemany(
                "INSERT INTO slices (revision, site, start_ms) "
                "VALUES (?, ?, ?)", rows)
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise

    @staticmethod
    def _load_configuration(conn, normalized):
        try:
            values = dict(conn.execute("SELECT key, value FROM meta"))
        except sqlite3.DatabaseError as exc:
            raise ValueError("not a handoff store") from exc
        if values.get("format_version") != _FORMAT_VERSION:
            raise ValueError("unsupported handoff store format")
        try:
            origin_ms = int(values["origin_ms"])
            duration_s = int(values["duration_s"])
            raw_scopes = json.loads(values["scopes"])
            scopes = {}
            for revision, site, lo, hi in raw_scopes:
                scopes[(revision, site)] = (lo, hi)
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("corrupt handoff store configuration") from exc

        if normalized is not None:
            if (normalized[0] != origin_ms
                    or normalized[1] != duration_s
                    or normalized[2] != scopes):
                raise ValueError(
                    "configuration does not match the existing handoff store")
        return origin_ms, duration_s, scopes

    def _load_slices(self, conn):
        self._slice_by_key = {}
        self._slice_info = {}
        for slice_id, revision, site, start_ms in conn.execute(
                "SELECT id, revision, site, start_ms FROM slices"):
            self._slice_by_key[(revision, site, start_ms)] = slice_id
            self._slice_info[slice_id] = (revision, site, start_ms)

    # -------------------------------------------------------------- lifecycle

    def close(self):
        """Release the database handle.  Safe to call more than once."""
        if self._conn is not None:
            conn, self._conn = self._conn, None
            conn.close()
        self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    def _require_open(self):
        if self._closed or self._conn is None:
            raise ValueError("handoff store is closed")

    # ---------------------------------------------------------------- helpers

    def _lookup_slice(self, revision, site, start_ms):
        if (not _watch._is_integer(revision)
                or not _watch._REVISION_MIN <= revision
                <= _watch._REVISION_MAX):
            raise ValueError("slice revision is invalid")
        if not _watch._valid_site(site):
            raise ValueError("slice site is invalid")
        if not _watch._is_integer(start_ms):
            raise ValueError("slice start_ms must be an integer")
        scope = self._scopes.get((revision, site))
        if scope is None:
            raise ValueError("slice does not belong to a scope")
        lo, hi = scope
        if (start_ms % _STEP_MS != 0 or not lo <= start_ms < hi):
            raise ValueError("slice start_ms is not a grid-aligned slice")
        slice_id = self._slice_by_key.get((revision, site, start_ms))
        if slice_id is None:
            raise ValueError("slice does not exist")
        return slice_id

    @staticmethod
    def _validate_add_item(item):
        if not isinstance(item, (tuple, list)) or len(item) != 2:
            raise ValueError("each record must be an (ordinal, observation)")
        ordinal, record = item
        if (not _watch._is_integer(ordinal)
                or not 0 <= ordinal <= _MAX_ORDINAL):
            raise ValueError("ordinal must be an integer 0..1999999")

        if (not isinstance(record, dict)
                or record.keys() != _watch._OBSERVATION_KEYS):
            raise ValueError(
                "each observation must be a dict with exactly the five "
                "required attribution keys")

        revision = record["revision"]
        site = record["site"]
        completed_ms = record["completed_ms"]
        latency_ms = record["latency_ms"]
        outcome = record["outcome"]

        if (not _watch._is_integer(revision)
                or not _watch._REVISION_MIN <= revision
                <= _watch._REVISION_MAX):
            raise ValueError("observation revision is invalid")
        if not _watch._valid_site(site):
            raise ValueError("observation site is invalid")
        if (not _watch._is_integer(completed_ms)
                or not 0 <= completed_ms <= _watch._TIME_MAX):
            raise ValueError("completed_ms must be an integer 0..2**63-1")
        if (not _watch._is_integer(latency_ms)
                or not 0 <= latency_ms <= 1000):
            raise ValueError("latency_ms must be an integer 0..1000")
        if outcome not in _watch._OUTCOMES:
            raise ValueError("observation outcome is invalid")

        return ordinal, revision, site, completed_ms, latency_ms, outcome

    # ------------------------------------------------------------- collection

    def add(self, records):
        """Atomically append one batch of ``(ordinal, observation)`` pairs.

        The input is a one-time iterable of at most 4096 pairs.  Each pair is
        placed in the 10-second slice containing its ``completed_ms`` and
        addressed by ``(revision, site, slice start, ordinal)``.  A batch is
        accepted in full or rejected in full; a rejected batch leaves no rows
        behind.  Re-sending an already stored identity with identical content
        is a no-op, while different content or an out-of-range ordinal after
        the slice was sealed raises :class:`ValueError`.
        """
        self._require_open()
        try:
            iterator = iter(records)
        except TypeError as exc:
            raise ValueError("records must be a one-time iterable") from exc

        unique = {}
        count = 0
        for item in iterator:
            count += 1
            if count > _MAX_BATCH:
                raise ValueError("at most 4096 records are accepted per batch")
            ordinal, revision, site, completed_ms, latency_ms, outcome = (
                self._validate_add_item(item))

            scope = self._scopes.get((revision, site))
            if scope is None:
                raise ValueError("observation does not belong to a scope")
            relative_ms = completed_ms - self._origin_ms
            lo, hi = scope
            if not lo <= relative_ms < hi:
                raise ValueError(
                    "observation completed_ms is outside its sequence interval")
            start_ms = (relative_ms // _STEP_MS) * _STEP_MS
            slice_id = self._slice_by_key.get((revision, site, start_ms))
            if slice_id is None:  # pragma: no cover - interval is grid aligned
                raise ValueError("observation does not belong to a slice")

            identity = (slice_id, ordinal)
            content = (completed_ms, latency_ms,
                       _OUTCOME_TO_CODE[outcome])
            previous = unique.get(identity)
            if previous is not None:
                if previous != content:
                    raise ValueError(
                        "batch contains conflicting content for one identity")
            else:
                unique[identity] = content

        if not unique:
            return
        self._write_batch(unique)

    def _write_batch(self, unique):
        conn = self._conn
        conn.execute("BEGIN IMMEDIATE")
        committed = False
        try:
            slice_ids = sorted({slice_id for slice_id, _ in unique})
            sealed = {}
            for chunk in _chunks(slice_ids, _CHUNK):
                placeholders = ",".join("?" * len(chunk))
                query = ("SELECT slice_id, expected FROM seal_events "
                         "WHERE slice_id IN (%s)" % placeholders)
                for slice_id, expected in conn.execute(query, chunk):
                    sealed[slice_id] = expected

            by_slice = {}
            for (slice_id, ordinal), content in unique.items():
                expected = sealed.get(slice_id)
                if expected is not None and ordinal >= expected:
                    raise ValueError(
                        "ordinal is outside the sealed slice range")
                by_slice.setdefault(slice_id, {})[ordinal] = content

            new_rows = []
            for slice_id, ordinals in by_slice.items():
                ordered = sorted(ordinals)
                existing = {}
                for chunk in _chunks(ordered, _CHUNK):
                    placeholders = ",".join("?" * len(chunk))
                    query = ("SELECT ordinal, completed_ms, latency_ms, "
                             "outcome FROM observations "
                             "WHERE slice_id = ? AND ordinal IN (%s)"
                             % placeholders)
                    for ordinal, completed, latency, outcome in conn.execute(
                            query, [slice_id] + list(chunk)):
                        existing[ordinal] = (completed, latency, outcome)
                for ordinal in ordered:
                    content = ordinals[ordinal]
                    previous = existing.get(ordinal)
                    if previous is None:
                        new_rows.append(
                            (slice_id, ordinal) + content)
                    elif previous != content:
                        raise ValueError(
                            "identity already stored with different content")

            if new_rows:
                new_count = self._observation_count + len(new_rows)
                if new_count > _MAX_OBSERVATIONS:
                    raise ValueError(
                        "at most 2000000 distinct observations are supported")
                conn.executemany(
                    "INSERT INTO observations "
                    "(slice_id, ordinal, completed_ms, latency_ms, outcome) "
                    "VALUES (?, ?, ?, ?, ?)", new_rows)
                conn.execute(
                    "UPDATE state SET observation_count = ? WHERE id = 1",
                    (new_count,))
            else:
                new_count = self._observation_count

            conn.execute("COMMIT")
            committed = True
            self._observation_count = new_count
        except BaseException:
            if not committed:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:  # pragma: no cover - defensive
                    pass
            raise

    def seal(self, revision, site, start_ms, expected):
        """Declare the exact number of records in one slice.

        ``expected`` must be an integer 0..2000000 (bools are rejected) and
        declares that the slice's valid ordinals are exactly
        ``[0, expected)``.  Sealing may happen before or after the records.
        Repeating the same credential is a no-op; a conflicting credential,
        or one that contradicts ordinals already received, raises
        :class:`ValueError` without changing anything.
        """
        self._require_open()
        slice_id = self._lookup_slice(revision, site, start_ms)
        if (not _watch._is_integer(expected)
                or not 0 <= expected <= _MAX_EXPECTED):
            raise ValueError("expected must be an integer 0..2000000")

        conn = self._conn
        conn.execute("BEGIN IMMEDIATE")
        committed = False
        try:
            row = conn.execute(
                "SELECT expected FROM seal_events WHERE slice_id = ?",
                (slice_id,)).fetchone()
            if row is not None:
                if row[0] != expected:
                    raise ValueError("slice is already sealed differently")
            else:
                highest = conn.execute(
                    "SELECT MAX(ordinal) FROM observations "
                    "WHERE slice_id = ?", (slice_id,)).fetchone()[0]
                if highest is not None and highest >= expected:
                    raise ValueError(
                        "credential contradicts already received ordinals")
                conn.execute(
                    "INSERT INTO seal_events (slice_id, expected) "
                    "VALUES (?, ?)", (slice_id, expected))
            conn.execute("COMMIT")
            committed = True
        except BaseException:
            if not committed:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:  # pragma: no cover - defensive
                    pass
            raise

    def missing(self, revision, site, start_ms):
        """Return the ordinal gaps of one slice, or ``None`` if unsealed.

        The result is a lazy ascending iterable of half-open ``(lo, hi)``
        ordinal intervals.  Adjacent gaps are merged.  The caller must consume
        it before the next store operation.  An unknown slice or invalid field
        raises :class:`ValueError` immediately.
        """
        self._require_open()
        slice_id = self._lookup_slice(revision, site, start_ms)
        row = self._conn.execute(
            "SELECT expected FROM seal_events WHERE slice_id = ?",
            (slice_id,)).fetchone()
        if row is None:
            return None
        expected = row[0]
        conn = self._conn

        def intervals():
            cursor = conn.execute(
                "SELECT ordinal FROM observations "
                "WHERE slice_id = ? AND ordinal < ? ORDER BY ordinal",
                (slice_id, expected))
            try:
                next_ordinal = 0
                for (ordinal,) in cursor:
                    if ordinal > next_ordinal:
                        yield (next_ordinal, ordinal)
                    next_ordinal = ordinal + 1
                if next_ordinal < expected:
                    yield (next_ordinal, expected)
            finally:
                cursor.close()

        return intervals()

    # ------------------------------------------------------------- checkpoints

    def checkpoint(self, name):
        """Freeze the current evidence under ``name``.

        The same name never moves an existing handoff, and later collection
        writes cannot change what it contains.  At most eight distinct names
        may exist.  Invalid or over-limit names raise :class:`ValueError`.
        """
        self._require_open()
        _validate_name(name)
        conn = self._conn
        conn.execute("BEGIN IMMEDIATE")
        committed = False
        try:
            row = conn.execute(
                "SELECT 1 FROM checkpoints WHERE name = ?",
                (name,)).fetchone()
            if row is None:
                count = conn.execute(
                    "SELECT COUNT(*) FROM checkpoints").fetchone()[0]
                if count >= _MAX_CHECKPOINTS:
                    raise ValueError("at most 8 checkpoints are supported")
                obs_id_max = conn.execute(
                    "SELECT COALESCE(MAX(id), 0) FROM observations"
                ).fetchone()[0]
                seal_id_max = conn.execute(
                    "SELECT COALESCE(MAX(id), 0) FROM seal_events"
                ).fetchone()[0]
                conn.execute(
                    "INSERT INTO checkpoints "
                    "(name, obs_id_max, seal_id_max) VALUES (?, ?, ?)",
                    (name, obs_id_max, seal_id_max))
            conn.execute("COMMIT")
            committed = True
        except BaseException:
            if not committed:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:  # pragma: no cover - defensive
                    pass
            raise

    def evaluate_checkpoint(self, name):
        """Re-evaluate the frozen evidence of a named handoff.

        Returns the same ``(windows, actions)`` pair as
        :func:`gateway_watch.evaluate`.  Observations added after the handoff
        are excluded, and coverage is granted only to slices that were sealed
        *and* fully received at that same instant.
        """
        self._require_open()
        _validate_name(name)
        row = self._conn.execute(
            "SELECT obs_id_max, seal_id_max FROM checkpoints WHERE name = ?",
            (name,)).fetchone()
        if row is None:
            raise ValueError("unknown checkpoint name")
        obs_id_max, seal_id_max = row

        covered = {}
        query = ("SELECT e.slice_id, s.revision, s.site, s.start_ms, "
                 "e.expected FROM seal_events AS e "
                 "JOIN slices AS s ON s.id = e.slice_id "
                 "WHERE e.id <= ?")
        for slice_id, revision, site, start_ms, expected in self._conn.execute(
                query, (seal_id_max,)):
            if expected:
                received = self._conn.execute(
                    "SELECT COUNT(*) FROM observations "
                    "WHERE slice_id = ? AND ordinal < ? AND id <= ?",
                    (slice_id, expected, obs_id_max)).fetchone()[0]
                if received != expected:
                    continue
            covered.setdefault((revision, site), []).append(
                (start_ms, start_ms + _STEP_MS))

        coverage = {}
        for key, parts in covered.items():
            parts.sort()
            merged = [list(parts[0])]
            for lo, hi in parts[1:]:
                if merged[-1][1] == lo:
                    merged[-1][1] = hi
                else:
                    merged.append([lo, hi])
            coverage[key] = [tuple(interval) for interval in merged]

        slice_info = self._slice_info
        conn = self._conn

        def observations():
            cursor = conn.execute(
                "SELECT slice_id, completed_ms, latency_ms, outcome "
                "FROM observations WHERE id <= ?", (obs_id_max,))
            try:
                for slice_id, completed_ms, latency_ms, outcome in cursor:
                    revision, site, _start = slice_info[slice_id]
                    yield {
                        "revision": revision,
                        "site": site,
                        "completed_ms": completed_ms,
                        "latency_ms": latency_ms,
                        "outcome": _CODE_TO_OUTCOME[outcome],
                    }
            finally:
                cursor.close()

        return _watch.evaluate(
            observations(),
            origin_ms=self._origin_ms,
            duration_s=self._duration_s,
            scopes=self._scopes,
            coverage=coverage,
        )


# ------------------------------------------------------------------ utilities


def _chunks(values, size):
    for index in range(0, len(values), size):
        yield values[index:index + size]


def _remove_store_files(directory):
    for suffix in ("", "-wal", "-shm"):
        path = os.path.join(directory, _DB_NAME + suffix)
        try:
            os.remove(path)
        except FileNotFoundError:
            pass


def _validate_name(name):
    if not isinstance(name, str) or _NAME_RE.fullmatch(name) is None:
        raise ValueError(
            "checkpoint name must be 1-32 ASCII letters, digits, underscores "
            "or hyphens")


def _validate_configuration(origin_ms, duration_s, scopes):
    if (not _watch._is_integer(origin_ms)
            or not 0 <= origin_ms <= _watch._TIME_MAX):
        raise ValueError("origin_ms must be an integer 0..2**63-1")
    if (not _watch._is_integer(duration_s)
            or not 60 <= duration_s <= 3600
            or duration_s % 10 != 0):
        raise ValueError(
            "duration_s must be an integer multiple of 10 from 60 to 3600")
    duration_ms = duration_s * 1000
    return origin_ms, duration_s, _watch._validate_scopes(scopes, duration_ms)
