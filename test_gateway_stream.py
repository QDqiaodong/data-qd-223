"""Acceptance tests for gateway_stream.open_stream."""

import hashlib
import json
import math
import os
import re
import subprocess
import sys
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_DIR))

from gateway_stream import CHUNK_SIZE, open_stream  # noqa: E402
from router import DeadlineExceeded, Router, Unavailable  # noqa: E402


class FakeClock:
    def __init__(self, start=1000.0):
        self.now = start
        self.lock = threading.Lock()

    def __call__(self):
        with self.lock:
            return self.now

    def set(self, value):
        with self.lock:
            self.now = value


def chunk_bytes(index, length):
    seed = hashlib.sha256(b"chunk:%d" % index).digest()
    return (seed * ((length + len(seed) - 1) // len(seed)))[:length]


def make_object(length):
    return b"".join(
        chunk_bytes(i, min(CHUNK_SIZE, length - i * CHUNK_SIZE))
        for i in range((length + CHUNK_SIZE - 1) // CHUNK_SIZE)
    )


def make_manifest(data):
    return [
        hashlib.sha256(
            data[i * CHUNK_SIZE:min(len(data), (i + 1) * CHUNK_SIZE)]
        ).hexdigest()
        for i in range((len(data) + CHUNK_SIZE - 1) // CHUNK_SIZE)
    ]


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        return

    def do_GET(self):
        httpd = self.server
        record = {
            "requestline": self.raw_requestline.decode("latin-1").strip(),
            "path": self.path,
            "range": self.headers.get("Range"),
            "headers": list(self.headers.items()),
        }
        with httpd.lock:
            httpd.requests.append(record)

        spec = httpd.behavior(record, self)
        if spec is None:
            return

        body = spec.get("body", b"")
        self.send_response(spec.get("status", 206))
        for name, value in spec.get("headers", ()):
            self.send_header(name, value)
        if not spec.get("omit_content_length"):
            self.send_header(
                "Content-Length",
                str(spec.get("content_length", len(body))))
        self.send_header("Connection", "close")
        self.end_headers()
        if body:
            self.wfile.write(body)


class ScriptedServer:
    def __init__(self, behavior):
        self.httpd = HTTPServer(("127.0.0.1", 0), _Handler)
        self.httpd.daemon_threads = True
        self.httpd.lock = threading.Lock()
        self.httpd.requests = []
        self.httpd.behavior = behavior
        self.address = self.httpd.server_address
        self.thread = threading.Thread(
            target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def stop(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(2.0)

    @property
    def requests(self):
        with self.httpd.lock:
            return list(self.httpd.requests)


def range_parts(record):
    match = re.fullmatch(r"bytes=(\d+)-(\d+)", record["range"] or "")
    if match is None:
        raise AssertionError("bad Range header: %r" % record["range"])
    return int(match.group(1)), int(match.group(2))


def correct_spec(data, record, *, content_range=None, body=None):
    start, end = range_parts(record)
    if body is None:
        body = data[start:end + 1]
    if content_range is None:
        content_range = "bytes %d-%d/%d" % (start, end, len(data))
    return {
        "status": 206,
        "body": body,
        "headers": [("Content-Range", content_range)],
    }


def artifact_behavior(data, *, bad_digest_chunks=(), bad_position_chunks=()):
    def behavior(record, handler):
        if record["path"] in ("/health", "/ok"):
            return {"status": 200, "body": b"healthy"}
        start, end = range_parts(record)
        index = start // CHUNK_SIZE
        body = bytearray(data[start:end + 1])
        content_range = None
        if index in bad_digest_chunks and body:
            body[0] ^= 0x01
        if index in bad_position_chunks:
            # Body remains the correct chunk, but its named position is wrong.
            content_range = "bytes 0-%d/%d" % (CHUNK_SIZE - 1, len(data))
        return correct_spec(
            data, record, content_range=content_range, body=bytes(body))
    return behavior


class GatewayStreamAcceptanceTest(unittest.TestCase):
    def setUp(self):
        self.servers = []

    def tearDown(self):
        for server in reversed(self.servers):
            server.stop()

    def start_server(self, behavior):
        server = ScriptedServer(behavior)
        self.servers.append(server)
        return server

    def router(self, endpoints, *, clock=None, cooldown=30.0, timeout=5.0):
        if clock is None:
            clock = time.monotonic
        return Router(
            endpoints,
            clock=clock,
            cooldown=cooldown,
            timeout=timeout,
            deadline_mode=True,
        )

    def test_unaligned_range_uses_digest_failover_without_refetching_first(self):
        length = 200_000
        data = make_object(length)
        digests = make_manifest(data)
        path = "/artifact?rev=42"
        lo = CHUNK_SIZE - 10
        hi = CHUNK_SIZE + 20

        first = self.start_server(artifact_behavior(
            data, bad_digest_chunks={1}))
        second = self.start_server(artifact_behavior(data))
        endpoints = [first.address, second.address]
        router = self.router(endpoints)

        stream = open_stream(
            router, path, length=length, sha256s=digests,
            replicas=endpoints, lo=lo, hi=hi, budget=15.0)

        part1 = next(stream)
        self.assertEqual(part1, data[lo:CHUNK_SIZE])
        self.assertEqual(len(part1), 10)

        # The first complete chunk is delivered before chunk two is confirmed
        # or even requested by this demand-driven implementation.
        first_requests = first.requests
        self.assertEqual(len(first_requests), 1)
        self.assertEqual(first_requests[0]["path"], path)
        self.assertEqual(
            first_requests[0]["range"], "bytes=0-%d" % (CHUNK_SIZE - 1))
        self.assertEqual(second.requests, [])
        range_headers = [
            value for name, value in first_requests[0]["headers"]
            if name.lower() == "range"
        ]
        self.assertEqual(range_headers, ["bytes=0-65535"])
        self.assertTrue(first_requests[0]["requestline"].startswith("GET "))

        part2 = next(stream)
        self.assertEqual(part2, data[CHUNK_SIZE:hi])
        self.assertEqual(len(part2), 20)

        first_ranges = [item["range"] for item in first.requests]
        second_ranges = [item["range"] for item in second.requests]
        self.assertEqual(first_ranges, [
            "bytes=0-65535",
            "bytes=65536-131071",
        ])
        self.assertEqual(second_ranges, ["bytes=65536-131071"])
        self.assertEqual(first.requests[1]["path"], path)
        self.assertEqual(second.requests[0]["path"], path)

        # Digest rejection follows Router cooldown semantics.
        self.assertGreater(
            router._states[first.address]["excluded_until"], router.clock())

        with self.assertRaises(StopIteration):
            next(stream)

    def test_wrong_content_range_position_is_rejected_even_with_good_hash(self):
        length = 200_000
        data = make_object(length)
        digests = make_manifest(data)
        lo = CHUNK_SIZE - 5
        hi = CHUNK_SIZE + 7

        bad = self.start_server(artifact_behavior(
            data, bad_position_chunks={1}))
        good = self.start_server(artifact_behavior(data))
        endpoints = [bad.address, good.address]
        router = self.router(endpoints)

        with open_stream(
                router, "/artifact", length=length, sha256s=digests,
                replicas=endpoints, lo=lo, hi=hi, budget=15.0) as stream:
            part1 = next(stream)
            self.assertEqual(part1, data[lo:CHUNK_SIZE])
            part2 = next(stream)
            self.assertEqual(part2, data[CHUNK_SIZE:hi])
            with self.assertRaises(StopIteration):
                next(stream)

        self.assertEqual(
            [item["range"] for item in bad.requests],
            ["bytes=0-65535", "bytes=65536-131071"])
        self.assertEqual(
            [item["range"] for item in good.requests],
            ["bytes=65536-131071"])
        self.assertGreater(
            router._states[bad.address]["excluded_until"], router.clock())

    def test_all_replicas_failing_second_chunk_keeps_prefix_then_terminates(self):
        length = 200_000
        data = make_object(length)
        digests = make_manifest(data)
        lo = CHUNK_SIZE - 12
        hi = CHUNK_SIZE + 34

        first = self.start_server(artifact_behavior(
            data, bad_digest_chunks={1}))
        second = self.start_server(artifact_behavior(
            data, bad_digest_chunks={1}))
        endpoints = [first.address, second.address]
        router = self.router(endpoints)

        stream = open_stream(
            router, "/artifact", length=length, sha256s=digests,
            replicas=endpoints, lo=lo, hi=hi, budget=15.0)
        prefix = next(stream)
        self.assertEqual(prefix, data[lo:CHUNK_SIZE])

        with self.assertRaises(Unavailable):
            next(stream)

        # A failed stream is terminal: later iteration is normal StopIteration.
        with self.assertRaises(StopIteration):
            next(stream)

        self.assertEqual(
            [item["range"] for item in first.requests],
            ["bytes=0-65535", "bytes=65536-131071"])
        self.assertEqual(
            [item["range"] for item in second.requests],
            ["bytes=65536-131071"])
        request_count = len(first.requests) + len(second.requests)
        with self.assertRaises(StopIteration):
            next(stream)
        self.assertEqual(
            request_count, len(first.requests) + len(second.requests))

    def test_absolute_clock_deadline_and_early_context_exit_release_router(self):
        length = 3 * CHUNK_SIZE + 77
        data = make_object(length)
        digests = make_manifest(data)
        lo = CHUNK_SIZE - 3
        hi = CHUNK_SIZE + 5

        server = self.start_server(artifact_behavior(data))
        clock = FakeClock(500.0)
        router = self.router([server.address], clock=clock)

        stream = open_stream(
            router, "/artifact", length=length, sha256s=digests,
            replicas=[server.address], lo=lo, hi=hi, budget=10.0)
        self.assertEqual(next(stream), data[lo:CHUNK_SIZE])
        self.assertEqual(len(server.requests), 1)

        # Advance only the injected clock to the already-open absolute deadline.
        clock.set(510.0)
        with self.assertRaises(DeadlineExceeded):
            next(stream)
        self.assertEqual(len(server.requests), 1)

        stream.close()
        self.assertIsNone(router._states[server.address]["busy"])
        self.assertLessEqual(
            router._states[server.address]["excluded_until"], clock())

        status, body = router.get_from(
            server.address, "/health", budget=2.0)
        self.assertEqual((status, body), (200, b"healthy"))

        # Early exit from a fresh stream starts no further range requests.
        clock.set(600.0)
        with open_stream(
                router, "/artifact", length=length, sha256s=digests,
                replicas=[server.address], lo=lo, hi=hi,
                budget=10.0) as early:
            self.assertEqual(next(early), data[lo:CHUNK_SIZE])
            requests_at_exit = len(server.requests)

        with self.assertRaises(StopIteration):
            next(early)
        self.assertEqual(len(server.requests), requests_at_exit)
        self.assertIsNone(router._states[server.address]["busy"])
        self.assertLessEqual(
            router._states[server.address]["excluded_until"], clock())
        status, body = router.get_from(
            server.address, "/health", budget=2.0)
        self.assertEqual((status, body), (200, b"healthy"))

    def test_eof_takes_precedence_over_later_clock_deadline(self):
        data = b"0123456789"
        digests = make_manifest(data)
        server = self.start_server(artifact_behavior(data))
        clock = FakeClock(0.0)
        router = self.router([server.address], clock=clock)
        stream = open_stream(
            router, "/artifact", length=10, sha256s=digests,
            replicas=[server.address], lo=0, hi=10, budget=1.0)

        self.assertEqual(next(stream), data)
        clock.set(100.0)
        with self.assertRaises(StopIteration):
            next(stream)
        stream.close()
        stream.close()
        with self.assertRaises(StopIteration):
            next(stream)
        self.assertEqual(len(server.requests), 1)

    def test_invalid_arguments_raise_before_network_and_inputs_are_snapshotted(self):
        length = 10
        digests = [hashlib.sha256(b"0123456789").hexdigest()]
        server = self.start_server(artifact_behavior(b"0123456789"))
        endpoint = server.address
        router = self.router([endpoint])
        disabled_router = Router(
            [endpoint], clock=time.monotonic, deadline_mode=False)

        valid = dict(
            router=router, path="/artifact", length=length,
            sha256s=digests, replicas=[endpoint], lo=0, hi=10, budget=1.0)

        invalid_cases = [
            dict(router=disabled_router),
            dict(path="artifact"),
            dict(path="//artifact"),
            dict(path="/" + "a" * 2048),
            dict(length=0),
            dict(length=-1),
            dict(length=True),
            dict(length=128 * 1024 * 1024 + 1),
            dict(length=1.5),
            dict(sha256s=[]),
            dict(sha256s=[digests[0], digests[0]]),
            dict(sha256s=[digests[0].upper()]),
            dict(sha256s=["x" * 64]),
            dict(replicas=[]),
            dict(replicas=[endpoint, endpoint]),
            dict(replicas=[("127.0.0.1", 1)]),
            dict(replicas=tuple([endpoint]) * 17),
            dict(lo=True),
            dict(hi=True),
            dict(lo=-1),
            dict(hi=11),
            dict(lo=5, hi=5),
            dict(budget=0),
            dict(budget=-2.0),
            dict(budget=math.inf),
            dict(budget=math.nan),
            dict(budget=True),
            dict(budget="1"),
        ]
        for changes in invalid_cases:
            kwargs = dict(valid)
            kwargs.update(changes)
            with self.subTest(changes=changes):
                with self.assertRaises(ValueError):
                    open_stream(**kwargs)
        self.assertEqual(server.requests, [])

        # Later caller mutation cannot change an opened stream.
        mutable_hashes = [digests[0]]
        mutable_replicas = [list(endpoint)]
        stream = open_stream(
            router, "/artifact", length=length, sha256s=mutable_hashes,
            replicas=mutable_replicas, lo=0, hi=10, budget=5.0)
        mutable_hashes[0] = "f" * 64
        mutable_replicas[0][1] = 1
        mutable_replicas.append(("127.0.0.1", 1))
        with stream:
            self.assertEqual(next(stream), b"0123456789")
            with self.assertRaises(StopIteration):
                next(stream)


_CHILD_MEMORY_CODE = r"""
import resource

limit = 48 * 1024 * 1024
resource.setrlimit(resource.RLIMIT_AS, (limit, limit))

import hashlib
import json
import sys
import time

payload = json.load(sys.stdin)
from router import Router
from gateway_stream import open_stream

endpoint = (payload["host"], payload["port"])
router = Router(
    [endpoint],
    clock=time.monotonic,
    cooldown=30.0,
    timeout=10.0,
    deadline_mode=True,
)

count = 0
chunks = 0
last_length = 0
overall = hashlib.sha256()

with open_stream(
    router,
    "/large-artifact",
    length=payload["length"],
    sha256s=payload["sha256s"],
    replicas=[endpoint],
    lo=0,
    hi=payload["length"],
    budget=45.0,
) as stream:
    for part in stream:
        if not part:
            raise AssertionError("empty delivered segment")
        overall.update(part)
        count += len(part)
        chunks += 1
        last_length = len(part)

print(json.dumps({
    "bytes": count,
    "chunks": chunks,
    "last_length": last_length,
    "digest": overall.hexdigest(),
}))
"""


@unittest.skipUnless(sys.platform.startswith("linux"), "Linux RLIMIT_AS test")
class MemoryLimitAcceptanceTest(unittest.TestCase):
    def test_read_whole_65mib_plus_object_under_48mib_as(self):
        length = 65 * 1024 * 1024 + 7
        digests = []
        overall = hashlib.sha256()
        for index in range((length + CHUNK_SIZE - 1) // CHUNK_SIZE):
            body = chunk_bytes(
                index, min(CHUNK_SIZE, length - index * CHUNK_SIZE))
            digests.append(hashlib.sha256(body).hexdigest())
            overall.update(body)

        def behavior(record, handler):
            start, end = range_parts(record)
            index = start // CHUNK_SIZE
            body = chunk_bytes(index, end - start + 1)
            return {
                "status": 206,
                "body": body,
                "headers": [
                    ("Content-Range",
                     "bytes %d-%d/%d" % (start, end, length))
                ],
            }

        server = ScriptedServer(behavior)
        self.addCleanup(server.stop)

        payload = json.dumps({
            "host": server.address[0],
            "port": server.address[1],
            "length": length,
            "sha256s": digests,
        })
        env = dict(os.environ)
        env["PYTHONPATH"] = str(PROJECT_DIR)
        completed = subprocess.run(
            [sys.executable, "-B", "-c", _CHILD_MEMORY_CODE],
            input=payload,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=60,
        )
        self.assertEqual(
            completed.returncode, 0,
            "48 MiB reader child failed:\nSTDOUT=%s\nSTDERR=%s"
            % (completed.stdout, completed.stderr))
        result = json.loads(completed.stdout)
        self.assertEqual(result["bytes"], length)
        self.assertEqual(result["chunks"], len(digests))
        self.assertEqual(result["last_length"], 7)
        self.assertEqual(result["digest"], overall.hexdigest())

        # One full-chunk request per manifest chunk, no range gaps.
        ranges = [item["range"] for item in server.requests]
        self.assertEqual(len(ranges), len(digests))
        self.assertEqual(ranges[0], "bytes=0-65535")
        self.assertEqual(
            ranges[-1],
            "bytes=%d-%d" % (
                (len(digests) - 1) * CHUNK_SIZE, length - 1))


if __name__ == "__main__":
    unittest.main()
