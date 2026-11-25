"""Controllable raw-socket upstream used by the deadline-mode tests.

The fixture deliberately does not use ``http.server``: tests need to stall
before/inside the response, drip a body slowly, and observe when the router
actually closes its side of the connection.
"""
import select
import socket
import threading
import time
from contextlib import AbstractContextManager


class Connection:
    def __init__(self, server, sock):
        self.server = server
        self.sock = sock
        self.path = None
        self.client_closed = threading.Event()
        self.closed_at = None
        self._lock = threading.Lock()

    # -- writing -------------------------------------------------------
    def write(self, data):
        view = memoryview(data)
        while view:
            try:
                sent = self.sock.send(view)
            except BlockingIOError:
                select.select((), (self.sock,), (), 1.0)
                continue
            if not sent:
                raise OSError("peer closed")
            view = view[sent:]

    def send_head(self, status=200, body=b"", *, content_length=None,
                  extra=()):
        if content_length is None:
            content_length = len(body)
        lines = [
            "HTTP/1.1 %d Fixture" % status,
            "Content-Length: %d" % content_length,
            "Connection: close",
        ]
        lines.extend(extra)
        self.write(("\r\n".join(lines) + "\r\n\r\n").encode("latin-1"))
        if body:
            self.write(body)

    # -- closing -------------------------------------------------------
    def mark_closed(self):
        with self._lock:
            if self.closed_at is None:
                self.closed_at = time.monotonic()
                self.client_closed.set()

    def poll_closed(self):
        """Return True once the peer has closed (or reset) the socket."""
        if self.client_closed.is_set():
            return True
        try:
            readable, _, _ = select.select((self.sock,), (), (), 0)
        except (OSError, ValueError):
            self.mark_closed()
            return True
        if readable:
            try:
                data = self.sock.recv(4096)
            except (OSError, ValueError):
                self.mark_closed()
                return True
            if not data:
                self.mark_closed()
                return True
        return False


class FixtureServer(AbstractContextManager):
    def __init__(self):
        self.stopping = threading.Event()
        self.lock = threading.Lock()
        self.dials = 0
        self.paths = []
        self.connections = []
        self.chunks_sent = 0
        self.first_connection = threading.Event()
        self.behavior = None

        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(64)
        listener.settimeout(0.05)
        self._listener = listener
        self.address = listener.getsockname()
        self._accept_thread = threading.Thread(
            target=self._accept_loop, daemon=True)

    # -- lifecycle -----------------------------------------------------
    def __enter__(self):
        self._accept_thread.start()
        return self

    def __exit__(self, *exc_info):
        self.stopping.set()
        try:
            self._listener.close()
        except OSError:
            pass
        self._accept_thread.join(2.0)
        with self.lock:
            connections = list(self.connections)
        for connection in connections:
            try:
                connection.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                connection.sock.close()
            except OSError:
                pass
        return False

    def _accept_loop(self):
        while not self.stopping.is_set():
            try:
                sock, _ = self._listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            connection = Connection(self, sock)
            with self.lock:
                self.dials += 1
                self.connections.append(connection)
            self.first_connection.set()
            threading.Thread(
                target=self._serve, args=(connection,), daemon=True).start()

    def _serve(self, connection):
        try:
            request = b""
            while b"\r\n\r\n" not in request:
                chunk = connection.sock.recv(4096)
                if not chunk:
                    return
                request += chunk
            line = request.split(b"\r\n", 1)[0].decode("latin-1")
            parts = line.split(" ")
            connection.path = parts[1] if len(parts) > 1 else ""
            with self.lock:
                self.paths.append(connection.path)
            with self.lock:
                behavior = self.behavior
            if behavior is None:
                behavior = respond(200, b"ok")
            connection.sock.setblocking(False)
            behavior(connection)
            # Wait briefly for the client to close so tests can observe it.
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline:
                if connection.poll_closed():
                    break
                time.sleep(0.01)
        except (OSError, ValueError):
            pass
        finally:
            connection.mark_closed()
            try:
                connection.sock.close()
            except OSError:
                pass

    # -- helpers for tests --------------------------------------------
    def wait_dials(self, count, timeout=5.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self.lock:
                if self.dials >= count:
                    return True
            time.sleep(0.005)
        return False

    def connection(self, index):
        with self.lock:
            return self.connections[index]


# -- reusable behaviours -------------------------------------------------
def respond(status=200, body=b"ok", *, extra=()):
    def behavior(connection):
        connection.send_head(status, body, extra=extra)
    return behavior


def wait_then_respond(release, body=b"late", status=200):
    def behavior(connection):
        release.wait(10.0)
        connection.send_head(status, body)
    return behavior


def stall_headers(stop):
    def behavior(connection):
        stop.wait(10.0)
    return behavior


def headers_5xx_then_block(release, status=503, content_length=60000):
    def behavior(connection):
        connection.send_head(status, content_length=content_length)
        release.wait(10.0)
    return behavior


def slow_drip(server, *, interval=0.025, chunk=b"0123456789",
              length=60000):
    def behavior(connection):
        connection.send_head(200, content_length=length)
        sent = 0
        while sent < length:
            if server.stopping.wait(interval):
                return
            if connection.poll_closed():
                return
            try:
                connection.write(chunk)
            except OSError:
                return
            sent += len(chunk)
            with server.lock:
                server.chunks_sent += 1
    return behavior
