"""Stream verified byte ranges of immutable, replicated objects.

This entry point fetches fixed 64 KiB manifest chunks through a deadline-mode
:class:`router.Router`.  Only chunks intersecting the requested byte range are
requested, and a chunk's complete bytes and SHA-256 are verified before its
intersection is handed to the reader.

The implementation is deliberately demand-driven: at most one chunk attempt is
active for a stream.  Consequently a slow consumer applies backpressure and no
whole artifact is retained.  Router endpoint slots and cooldown rules continue
to own all real network access.
"""

from __future__ import annotations

import hashlib
import math
import re
from threading import Lock

import router

DeadlineExceeded = router.DeadlineExceeded
Unavailable = router.Unavailable

__all__ = ["open_stream", "DeadlineExceeded", "Unavailable"]

CHUNK_SIZE = 65536
MIN_LENGTH = 1
MAX_LENGTH = 128 * 1024 * 1024
MAX_REPLICAS = 16
_PATH_MAX = 2048

_CONTENT_RANGE_RE = re.compile(rb"bytes ([0-9]+)-([0-9]+)/([0-9]+)\Z")
_HEX_CHARS = frozenset("0123456789abcdef")


def _strict_int(value):
    """Return true for real ints, excluding bool."""
    return isinstance(value, int) and not isinstance(value, bool)


def _finite_positive(value):
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value > 0
    )


def _valid_local_path(path):
    # Keep this identical to Router's deadline-mode path validation.
    if not isinstance(path, str) or len(path) > _PATH_MAX:
        return False
    if not path.startswith("/") or path.startswith("//"):
        return False
    try:
        encoded = path.encode("ascii")
    except UnicodeEncodeError:
        return False
    return all(0x21 <= byte <= 0x7E for byte in encoded)


def _normalize_replicas(replicas, router_obj):
    if not isinstance(replicas, (list, tuple)):
        raise ValueError("replicas must be a list or tuple")
    if not 1 <= len(replicas) <= MAX_REPLICAS:
        raise ValueError("replicas must contain 1-16 endpoints")

    try:
        configured = {tuple(endpoint) for endpoint in router_obj.endpoints}
    except (AttributeError, TypeError) as exc:
        raise ValueError("router must expose configured endpoints") from exc

    normalized = []
    seen = set()
    for endpoint in replicas:
        if not isinstance(endpoint, (list, tuple)) or len(endpoint) != 2:
            raise ValueError("each replica must be a (host, port) endpoint")
        key = (endpoint[0], endpoint[1])
        try:
            hash(key)
        except TypeError as exc:
            raise ValueError("invalid replica endpoint") from exc
        if key in seen:
            raise ValueError("replicas must be distinct")
        if key not in configured:
            raise ValueError("replica is not configured on router")
        seen.add(key)
        normalized.append(key)
    return tuple(normalized)


def _validate_open_args(router_obj, path, length, sha256s, replicas, lo, hi,
                        budget):
    if not getattr(router_obj, "deadline_mode", False):
        raise ValueError("router must have deadline_mode=True")
    if not callable(getattr(router_obj, "request_from", None)):
        raise ValueError("router does not support endpoint requests")
    if not callable(getattr(router_obj, "clock", None)):
        raise ValueError("router must provide its monotonic clock")

    if not _valid_local_path(path):
        raise ValueError("path must be a local ASCII path of at most 2048 chars")

    if not _strict_int(length) or not MIN_LENGTH <= length <= MAX_LENGTH:
        raise ValueError("length must be an integer from 1 to 128 MiB")

    chunk_count = (length + CHUNK_SIZE - 1) // CHUNK_SIZE
    if not isinstance(sha256s, (list, tuple)):
        raise ValueError("sha256s must be a list or tuple")
    if len(sha256s) != chunk_count:
        raise ValueError("sha256s must contain one digest per fixed chunk")
    normalized_hashes = []
    for digest in sha256s:
        if (not isinstance(digest, str) or len(digest) != 64
                or any(char not in _HEX_CHARS for char in digest)):
            raise ValueError("each chunk digest must be 64 lowercase hex chars")
        normalized_hashes.append(digest)

    normalized_replicas = _normalize_replicas(replicas, router_obj)

    if not _strict_int(lo) or not _strict_int(hi):
        raise ValueError("range endpoints must be integers")
    if not 0 <= lo < hi <= length:
        raise ValueError("range must satisfy 0 <= lo < hi <= length")

    if not _finite_positive(budget):
        raise ValueError("budget must be a finite positive number")

    return tuple(normalized_hashes), normalized_replicas


class _ByteRangeStream:
    """Context manager and forward iterator over verified chunk slices."""

    __slots__ = (
        "_router", "_path", "_length", "_sha256s", "_replicas",
        "_lo", "_hi", "_deadline", "_first_chunk", "_last_chunk",
        "_next_chunk", "_terminal", "_call_lock",
    )

    def __init__(self, router_obj, path, length, sha256s, replicas, lo, hi,
                 budget):
        hashes, replica_tuple = _validate_open_args(
            router_obj, path, length, sha256s, replicas, lo, hi, budget)

        self._router = router_obj
        self._path = path
        self._length = length
        self._sha256s = hashes
        self._replicas = replica_tuple
        self._lo = lo
        self._hi = hi

        # The single absolute deadline is established at open_stream() time.
        self._deadline = router_obj.clock() + budget

        self._first_chunk = lo // CHUNK_SIZE
        self._last_chunk = (hi - 1) // CHUNK_SIZE
        self._next_chunk = self._first_chunk
        self._terminal = False
        self._call_lock = Lock()

    # Context/iterator protocol ---------------------------------------
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False

    def __iter__(self):
        return self

    def close(self):
        """Terminate the stream and wait for any started attempt to finish.

        The one-reader-thread contract means contention normally only occurs
        if another coordination thread calls close while a read is blocked.
        Waiting on the call lock guarantees that Router has closed the socket
        and released its endpoint slot before close returns.
        """
        self._terminal = True
        with self._call_lock:
            return None

    def __next__(self):
        if self._terminal:
            raise StopIteration

        with self._call_lock:
            if self._terminal:
                raise StopIteration
            return self._next_once()

    def _next_once(self):
        try:
            # EOF takes precedence over a deadline reached while the consumer
            # was processing the final delivered segment: there are no bytes
            # left to withhold and no network work to start.
            if self._next_chunk > self._last_chunk:
                self._terminal = True
                raise StopIteration

            self._check_deadline()

            chunk_index = self._next_chunk
            body = self._fetch_verified_chunk(chunk_index)

            # Never deliver if the open-time deadline has arrived by the time
            # the complete verified chunk is in hand.
            self._check_deadline()

            part = self._intersection(chunk_index, body)
            self._next_chunk = chunk_index + 1
            return part
        except StopIteration:
            self._terminal = True
            raise
        except BaseException:
            # A read failure terminates this stream permanently.  Router has
            # already closed the socket and released its endpoint slot before
            # its exception escapes request_from().
            self._terminal = True
            raise

    # Internals --------------------------------------------------------
    def _check_deadline(self):
        if self._router.clock() >= self._deadline:
            raise router.DeadlineExceeded(self._path)

    def _chunk_bounds(self, chunk_index):
        start = chunk_index * CHUNK_SIZE
        end = min(self._length, start + CHUNK_SIZE)
        return start, end

    def _intersection(self, chunk_index, body):
        chunk_start, chunk_end = self._chunk_bounds(chunk_index)
        slice_start = max(self._lo, chunk_start) - chunk_start
        slice_end = min(self._hi, chunk_end) - chunk_start
        return body[slice_start:slice_end]

    def _validate_response(self, status, headers, body, *, chunk_index,
                           start, end):
        """Router response_validator: false means ordinary protocol failure."""
        try:
            if status != 206:
                return False

            encoding = headers.get(b"content-encoding")
            if encoding is not None:
                if (len(encoding) != 1
                        or encoding[0].strip().lower() != b"identity"):
                    return False

            ranges = headers.get(b"content-range")
            if not isinstance(ranges, list) or len(ranges) != 1:
                return False
            match = _CONTENT_RANGE_RE.fullmatch(ranges[0])
            if match is None:
                return False

            range_start, range_end, total = (
                int(match.group(1)), int(match.group(2)), int(match.group(3)))
            if (range_start != start
                    or range_end != end - 1
                    or total != self._length):
                return False

            expected_length = end - start
            if not isinstance(body, (bytes, bytearray)):
                return False
            if len(body) != expected_length:
                return False

            digest = hashlib.sha256(body).hexdigest()
            if digest != self._sha256s[chunk_index]:
                return False

            return True
        except Exception:
            return False

    def _fetch_verified_chunk(self, chunk_index):
        start, end = self._chunk_bounds(chunk_index)
        range_header = ("Range", "bytes=%d-%d" % (start, end - 1))

        def validator(status, headers, body):
            return self._validate_response(
                status, headers, body,
                chunk_index=chunk_index, start=start, end=end)

        last_failure = None
        for endpoint in self._replicas:
            if self._terminal:
                raise StopIteration
            self._check_deadline()

            remaining = self._deadline - self._router.clock()
            if remaining <= 0:
                raise router.DeadlineExceeded(self._path)

            try:
                result = self._router.request_from(
                    endpoint,
                    self._path,
                    budget=remaining,
                    extra_headers=(range_header,),
                    response_validator=validator,
                )
                # Public Router returns (status, body, headers), but only the
                # body is needed here; validation already inspected headers.
                body = result[1]
            except router.DeadlineExceeded:
                raise
            except router.Unavailable as exc:
                # Busy/cooling skip, transport/protocol rejection, or this
                # endpoint's single failed attempt.  This chunk may move only
                # to its next published replica.
                last_failure = exc
                if self._router.clock() >= self._deadline:
                    raise router.DeadlineExceeded(self._path) from exc
                continue

            if self._router.clock() >= self._deadline:
                raise router.DeadlineExceeded(self._path)
            return body

        if self._router.clock() >= self._deadline:
            raise router.DeadlineExceeded(self._path)
        raise router.Unavailable(self._path) from last_failure


def open_stream(router, path, *, length, sha256s, replicas, lo, hi,
                budget):
    """Open a verified byte-range stream without performing network I/O.

    Returns a context manager that is also its own forward iterator.  The
    absolute budget starts when this function is called.
    """
    # Validation and snapshotting happen in the constructor.  Keeping that
    # work there means invalid calls raise before any socket work is possible.
    return _ByteRangeStream(
        router, path, length, sha256s, replicas, lo, hi, budget)
