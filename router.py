"""Request routing with bounded upstream waits and passive availability tracking.

Two modes are supported.

``Router(endpoints, clock=..., cooldown=..., timeout=...)`` keeps the original
behaviour: each socket operation gets ``timeout`` seconds and ``get(path)``
returns ``(status, body)``.

``Router(..., deadline_mode=True)`` makes ``get(path, budget=seconds)`` obey an
end-to-end request budget.  The budget covers rotation, connecting, sending,
reading the complete response and switching upstreams.  Every actual upstream
attempt additionally has one absolute ``timeout`` seconds.  At most one live
network attempt may use an endpoint at a time; busy and cooling endpoints are
skipped rather than queued for.

Only plain HTTP over IPv4 literals is required.  Responses must have a complete
header section of at most 16 KiB, a valid ``Content-Length`` and at most 64 KiB
of body.  DNS, TLS and chunked transfer encoding are deliberately unsupported.
"""
from http.client import HTTPConnection, HTTPException
from threading import Lock
import errno
import ipaddress
import math
import select
import socket


class Unavailable(Exception):
    """No usable upstream was available for the request."""


class DeadlineExceeded(Exception):
    """The caller's end-to-end request budget expired."""


class _AttemptTimeout(Exception):
    def __init__(self, at):
        super().__init__("upstream attempt deadline exceeded")
        self.at = at


class _RequestTimeout(Exception):
    pass


class _UpstreamFailure(Exception):
    def __init__(self, at):
        super().__init__("upstream failure")
        self.at = at


_OK = "ok"
_CONNECTING = (errno.EINPROGRESS, errno.EWOULDBLOCK, errno.EALREADY,
               errno.EINTR)


def _finite_positive(value):
    """True for a finite, strictly positive real number (not bool)."""
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value > 0
    )


class Router:
    _MAX_ENDPOINTS = 16
    _MAX_CONCURRENT_CALLS = 32
    _MAX_HEADER_BYTES = 16 * 1024
    _MAX_BODY_BYTES = 64 * 1024
    # An injected clock may be virtual, so wake periodically instead of
    # sleeping for the whole remaining (possibly virtual) deadline.
    _POLL_SECONDS = 0.005

    def __init__(self, endpoints, *, clock, cooldown=5, timeout=0.5,
                 deadline_mode=False):
        endpoints = tuple(endpoints)
        if deadline_mode:
            endpoints = tuple(self._validate_deadline_configuration(
                endpoints, cooldown, timeout, clock))
        else:
            if (not endpoints or len(set(endpoints)) != len(endpoints)
                    or cooldown <= 0 or timeout <= 0):
                raise ValueError(
                    "distinct endpoints and positive timeout/cooldown required")

        self.endpoints = endpoints
        self.clock, self.cooldown, self.timeout = clock, cooldown, timeout
        self.deadline_mode = bool(deadline_mode)
        self.next_index = 0

        if self.deadline_mode:
            self._states = {
                endpoint: {"busy": None, "excluded_until": float("-inf")}
                for endpoint in self.endpoints
            }
            self._state_lock = Lock()
            self._call_lock = Lock()
            self._active_calls = 0
        else:
            self.excluded_until = {}
            self.lock = Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def get(self, path, budget=None):
        if not self.deadline_mode:
            if budget is not None:
                raise ValueError("budget requires deadline_mode=True")
            return self._get_legacy(path)

        if not _finite_positive(budget):
            raise ValueError("budget must be a finite positive number")
        if not self._valid_deadline_path(path):
            raise ValueError("expected a local path")

        with self._call_lock:
            if self._active_calls >= self._MAX_CONCURRENT_CALLS:
                raise Unavailable(path)
            self._active_calls += 1
        try:
            return self._get_with_deadline(path, budget)
        finally:
            with self._call_lock:
                self._active_calls -= 1

    # ------------------------------------------------------------------
    # Original mode
    # ------------------------------------------------------------------
    def _get_legacy(self, path):
        if not path.startswith("/") or path.startswith("//"):
            raise ValueError("expected a local path")
        with self.lock:
            start = self.next_index
            self.next_index = (start + 1) % len(self.endpoints)
        for endpoint in self.endpoints[start:] + self.endpoints[:start]:
            with self.lock:
                if self.excluded_until.get(endpoint, float("-inf")) > self.clock():
                    continue
            connection = HTTPConnection(*endpoint, timeout=self.timeout)
            try:
                connection.request("GET", path)
                response = connection.getresponse()
                body = response.read()
                if response.status < 500:
                    return response.status, body
            except (OSError, HTTPException):
                pass
            finally:
                connection.close()
            with self.lock:
                self.excluded_until[endpoint] = self.clock() + self.cooldown
        raise Unavailable(path)

    # ------------------------------------------------------------------
    # Deadline mode: request orchestration
    # ------------------------------------------------------------------
    def _get_with_deadline(self, path, budget):
        request_deadline = self.clock() + budget

        with self._state_lock:
            start = self.next_index
            self.next_index = (start + 1) % len(self.endpoints)
        order = self.endpoints[start:] + self.endpoints[:start]

        for endpoint in order:
            if self.clock() >= request_deadline:
                raise DeadlineExceeded(path)

            token = self._claim(endpoint)
            if token is None:
                continue

            try:
                try:
                    status, body = self._attempt(
                        endpoint, path, request_deadline)
                except _RequestTimeout:
                    raise DeadlineExceeded(path)
                except _AttemptTimeout as failure:
                    # The attempt deadline preceded the request deadline: the
                    # endpoint is unhealthy from the moment the attempt failed.
                    self._finish_failed(endpoint, token, failure.at)
                    token = None
                    if self.clock() >= request_deadline:
                        raise DeadlineExceeded(path)
                    continue
                except _UpstreamFailure as failure:
                    self._finish_failed(endpoint, token, failure.at)
                    token = None
                    if self.clock() >= request_deadline:
                        raise DeadlineExceeded(path)
                    continue
                else:
                    # The connection was closed by _attempt's finally clause
                    # before the slot is made available again.
                    self._release(endpoint, token)
                    token = None
                    if self.clock() >= request_deadline:
                        raise DeadlineExceeded(path)
                    return status, body
            finally:
                if token is not None:
                    self._release(endpoint, token)

        if self.clock() >= request_deadline:
            raise DeadlineExceeded(path)
        raise Unavailable(path)

    def _attempt(self, endpoint, path, request_deadline):
        """Run one real attempt, closing its socket before returning/raising."""
        now = self.clock()
        attempt_deadline = now + self.timeout
        if attempt_deadline > request_deadline:
            attempt_deadline = request_deadline

        connection = None
        try:
            self._check_deadlines(attempt_deadline, request_deadline)
            connection = self._connect(
                endpoint, attempt_deadline, request_deadline)
            self._send_request(
                connection, endpoint, path, attempt_deadline, request_deadline)
            status, body = self._read_response(
                connection, attempt_deadline, request_deadline)
            # A complete, valid result is deliverable only if it was read
            # before the earlier of the two deadlines.
            self._check_deadlines(attempt_deadline, request_deadline)
            return status, body
        finally:
            self._close_socket(connection)

    # ------------------------------------------------------------------
    # Slot bookkeeping
    # ------------------------------------------------------------------
    def _claim(self, endpoint):
        with self._state_lock:
            state = self._states[endpoint]
            if state["busy"] is not None:
                return None
            if state["excluded_until"] > self.clock():
                return None
            token = object()
            state["busy"] = token
            return token

    def _release(self, endpoint, token):
        with self._state_lock:
            state = self._states[endpoint]
            if state["busy"] is token:
                state["busy"] = None

    def _finish_failed(self, endpoint, token, failed_at):
        """Publish a failure before the slot becomes claimable again."""
        with self._state_lock:
            state = self._states[endpoint]
            if state["busy"] is not token:
                # A late event from an already finished attempt must not
                # rewrite health or release a newer attempt's slot.
                return
            until = failed_at + self.cooldown
            if until > state["excluded_until"]:
                state["excluded_until"] = until
            state["busy"] = None

    # ------------------------------------------------------------------
    # Deadline helpers
    # ------------------------------------------------------------------
    def _check_deadlines(self, attempt_deadline, request_deadline):
        """Raise the signal belonging to the earlier reached deadline.

        The request deadline wins ties.  Because ``attempt_deadline`` is
        clamped to ``request_deadline``, the attempt can only be strictly
        earlier.
        """
        now = self.clock()
        if now >= attempt_deadline:
            if attempt_deadline < request_deadline:
                raise _AttemptTimeout(now)
            raise _RequestTimeout()
        if now >= request_deadline:  # defensive; normally unreachable
            raise _RequestTimeout()

    def _select_timeout(self, deadline):
        remaining = deadline - self.clock()
        if remaining <= 0:
            return 0
        return remaining if remaining < self._POLL_SECONDS else self._POLL_SECONDS

    def _network_failure(self, attempt_deadline, request_deadline):
        """Attribute a socket error, giving an already-reached deadline priority."""
        self._check_deadlines(attempt_deadline, request_deadline)
        raise _UpstreamFailure(self.clock())

    def _protocol_failure(self, attempt_deadline, request_deadline):
        self._check_deadlines(attempt_deadline, request_deadline)
        raise _UpstreamFailure(self.clock())

    # ------------------------------------------------------------------
    # Non-blocking socket operations
    # ------------------------------------------------------------------
    def _connect(self, endpoint, attempt_deadline, request_deadline):
        host, port = endpoint
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.setblocking(False)
            error = sock.connect_ex((host, port))
            if error not in (0,) + _CONNECTING:
                self._network_failure(attempt_deadline, request_deadline)
            if error != 0:
                while True:
                    self._check_deadlines(attempt_deadline, request_deadline)
                    try:
                        _, writable, exceptional = select.select(
                            (), (sock,), (sock,),
                            self._select_timeout(attempt_deadline))
                    except (OSError, ValueError):
                        self._network_failure(
                            attempt_deadline, request_deadline)
                    self._check_deadlines(attempt_deadline, request_deadline)
                    if writable or exceptional:
                        error = sock.getsockopt(
                            socket.SOL_SOCKET, socket.SO_ERROR)
                        if error:
                            self._network_failure(
                                attempt_deadline, request_deadline)
                        break
            return sock
        except BaseException:
            self._close_socket(sock)
            raise

    def _send_request(self, sock, endpoint, path, attempt_deadline,
                      request_deadline):
        host, port = endpoint
        request = (
            "GET %s HTTP/1.1\r\n"
            "Host: %s:%d\r\n"
            "Connection: close\r\n"
            "\r\n" % (path, host, port)
        ).encode("ascii")
        view = memoryview(request)
        while view:
            self._check_deadlines(attempt_deadline, request_deadline)
            try:
                sent = sock.send(view)
            except (BlockingIOError, InterruptedError):
                sent = None
            except OSError:
                self._network_failure(attempt_deadline, request_deadline)
            if sent == 0:
                self._protocol_failure(attempt_deadline, request_deadline)
            if not sent:
                self._wait_for(sock, write=True, deadline=attempt_deadline,
                               attempt_deadline=attempt_deadline,
                               request_deadline=request_deadline)
                continue
            view = view[sent:]

    def _recv(self, sock, size, attempt_deadline, request_deadline):
        while True:
            self._check_deadlines(attempt_deadline, request_deadline)
            try:
                return sock.recv(size)
            except (BlockingIOError, InterruptedError):
                self._wait_for(sock, write=False, deadline=attempt_deadline,
                               attempt_deadline=attempt_deadline,
                               request_deadline=request_deadline)
            except OSError:
                self._network_failure(attempt_deadline, request_deadline)

    def _wait_for(self, sock, *, write, deadline, attempt_deadline,
                  request_deadline):
        try:
            if write:
                select.select((), (sock,), (sock,),
                              self._select_timeout(deadline))
            else:
                select.select((sock,), (), (sock,),
                              self._select_timeout(deadline))
        except (OSError, ValueError):
            self._network_failure(attempt_deadline, request_deadline)
        self._check_deadlines(attempt_deadline, request_deadline)

    # ------------------------------------------------------------------
    # HTTP response parsing
    # ------------------------------------------------------------------
    def _read_response(self, sock, attempt_deadline, request_deadline):
        raw_headers, body_prefix = self._read_headers(
            sock, attempt_deadline, request_deadline)
        status, headers = self._parse_headers(
            raw_headers, attempt_deadline, request_deadline)

        # A 5xx is conclusive as soon as its complete header section arrives;
        # its (possibly endless) body never needs to be read.
        if status >= 500:
            self._check_deadlines(attempt_deadline, request_deadline)
            raise _UpstreamFailure(self.clock())

        if b"transfer-encoding" in headers:
            self._protocol_failure(attempt_deadline, request_deadline)
        values = headers.get(b"content-length")
        if not values or len(set(values)) != 1:
            self._protocol_failure(attempt_deadline, request_deadline)
        value = values[0]
        digits = value.lstrip(b"0")
        if (not value or not value.isdigit()
                or len(digits) > len(str(self._MAX_BODY_BYTES))):
            # Non-numeric, or too many significant digits to be an allowed
            # body size: never convert an unbounded integer.
            self._protocol_failure(attempt_deadline, request_deadline)
        length = int(digits or b"0")
        if length > self._MAX_BODY_BYTES:
            # A declared oversize response is an upstream failure; do not
            # attempt to drain it.
            self._protocol_failure(attempt_deadline, request_deadline)

        body = bytearray(body_prefix)
        if len(body) > length:
            del body[length:]
        while len(body) < length:
            chunk = self._recv(
                sock, min(65536, length - len(body)),
                attempt_deadline, request_deadline)
            if not chunk:
                # Premature disconnect / short body.
                self._protocol_failure(attempt_deadline, request_deadline)
            body.extend(chunk)

        self._check_deadlines(attempt_deadline, request_deadline)
        return status, bytes(body)

    def _read_headers(self, sock, attempt_deadline, request_deadline):
        buffer = bytearray()
        while True:
            marker = buffer.find(b"\r\n\r\n")
            if marker >= 0:
                header_end = marker + 4
                if header_end > self._MAX_HEADER_BYTES:
                    self._protocol_failure(
                        attempt_deadline, request_deadline)
                return bytes(buffer[:marker]), bytes(buffer[header_end:])
            if len(buffer) >= self._MAX_HEADER_BYTES:
                # No delimiter can fit inside the header limit any more.
                self._protocol_failure(attempt_deadline, request_deadline)
            chunk = self._recv(sock, 8192, attempt_deadline, request_deadline)
            if not chunk:
                self._protocol_failure(attempt_deadline, request_deadline)
            buffer.extend(chunk)

    def _parse_headers(self, raw_headers, attempt_deadline, request_deadline):
        lines = raw_headers.split(b"\r\n")
        if not lines or not lines[0]:
            self._protocol_failure(attempt_deadline, request_deadline)
        status_parts = lines[0].split(b" ", 2)
        if len(status_parts) < 2 or not status_parts[0].startswith(b"HTTP/1."):
            self._protocol_failure(attempt_deadline, request_deadline)
        status_text = status_parts[1]
        if len(status_text) != 3 or not status_text.isdigit():
            self._protocol_failure(attempt_deadline, request_deadline)
        status = int(status_text)
        if status < 100 or status > 599:
            self._protocol_failure(attempt_deadline, request_deadline)

        headers = {}
        for line in lines[1:]:
            if not line or line[:1] in (b" ", b"\t"):
                self._protocol_failure(attempt_deadline, request_deadline)
            name, separator, value = line.partition(b":")
            if not separator or not name:
                self._protocol_failure(attempt_deadline, request_deadline)
            headers.setdefault(name.strip().lower(), []).append(value.strip())
        return status, headers

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------
    def _close_socket(self, sock):
        if sock is None:
            return
        try:
            sock.close()
        except OSError:
            pass

    @staticmethod
    def _valid_deadline_path(path):
        if not isinstance(path, str):
            return False
        if not path.startswith("/") or path.startswith("//"):
            return False
        try:
            encoded = path.encode("ascii")
        except UnicodeEncodeError:
            return False
        return all(0x21 <= byte <= 0x7E for byte in encoded)

    @classmethod
    def _validate_deadline_configuration(cls, endpoints, cooldown, timeout,
                                         clock):
        if not callable(clock):
            raise ValueError("clock must be callable")
        if not _finite_positive(timeout) or not _finite_positive(cooldown):
            raise ValueError("timeout and cooldown must be finite and positive")
        if not 1 <= len(endpoints) <= cls._MAX_ENDPOINTS:
            raise ValueError("deadline mode requires 1-16 endpoints")

        normalized = []
        for endpoint in endpoints:
            if (not isinstance(endpoint, (tuple, list))
                    or len(endpoint) != 2):
                raise ValueError("endpoints must be (IPv4 literal, port)")
            host, port = endpoint
            if not isinstance(host, str):
                raise ValueError("endpoints must use IPv4 string literals")
            try:
                address = ipaddress.IPv4Address(host)
            except ipaddress.AddressValueError:
                raise ValueError("endpoints must use IPv4 string literals")
            if str(address) != host:
                raise ValueError("endpoints must use canonical IPv4 literals")
            if (not isinstance(port, int) or isinstance(port, bool)
                    or not 1 <= port <= 65535):
                raise ValueError("endpoint ports must be integers in 1-65535")
            normalized.append((host, port))

        if len(set(normalized)) != len(normalized):
            raise ValueError("endpoints must be distinct")
        return normalized
