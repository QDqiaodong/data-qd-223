"""Request routing with bounded upstream waits and passive availability tracking."""
from http.client import HTTPConnection, HTTPException
from threading import Lock


class Unavailable(Exception):
    pass


class Router:
    def __init__(self, endpoints, *, clock, cooldown=5, timeout=0.5):
        self.endpoints = tuple(endpoints)
        if (not self.endpoints or len(set(self.endpoints)) != len(self.endpoints)
                or cooldown <= 0 or timeout <= 0):
            raise ValueError("distinct endpoints and positive timeout/cooldown required")
        self.clock, self.cooldown, self.timeout = clock, cooldown, timeout
        self.excluded_until = {}
        self.next_index = 0
        self.lock = Lock()

    def get(self, path):
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
