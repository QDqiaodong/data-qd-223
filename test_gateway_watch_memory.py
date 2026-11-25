"""Bounded-memory acceptance test for the two-million-record entry point."""

import json
import os
import subprocess
import sys
import unittest
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent

_CHILD_CODE = r"""
import resource

_LIMIT = 96 * 1024 * 1024
resource.setrlimit(resource.RLIMIT_AS, (_LIMIT, _LIMIT))

import json
from gateway_watch import evaluate

N = 2_000_000

# All 24 maximum-length scopes allocate the full compact histogram space
# (24 * 355 windows * 1001 uint32 bins), while every record is attributed to
# one known sequence.  The generator is deliberately one-shot and yields
# timestamps out of order.
scopes = {(1, "mem"): (0, 3_600_000)}
for revision in range(2, 25):
    scopes[(revision, "load")] = (0, 3_600_000)
coverage = {key: [(0, 3_600_000)] for key in scopes}


def generated_observations():
    for i in range(N):
        j = i // 2
        if i & 1:
            completed_ms = 10_000 + (j * 991) % 1000
        else:
            completed_ms = (j * 997) % 1000
        yield {
            "revision": 1,
            "site": "mem",
            "completed_ms": completed_ms,
            "latency_ms": j % 1000,
            "outcome": "delivered",
        }


windows, actions = evaluate(
    generated_observations(),
    origin_ms=0,
    duration_s=3600,
    scopes=scopes,
    coverage=coverage,
)
wanted = [
    window for window in windows
    if window[1] == "mem" and window[2] in (60_000, 70_000)
]
print(json.dumps({"windows": wanted, "actions": actions}))
"""


@unittest.skipUnless(sys.platform.startswith("linux"),
                     "Linux RLIMIT_AS test")
class MemoryAcceptanceTest(unittest.TestCase):
    def test_two_million_unordered_records_under_96_mib(self):
        env = dict(os.environ)
        env["PYTHONPATH"] = str(PROJECT_DIR)
        completed = subprocess.run(
            [sys.executable, "-B", "-c", _CHILD_CODE],
            cwd=str(PROJECT_DIR),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=60,
        )
        self.assertEqual(
            completed.returncode, 0,
            "memory child failed:\n%s" % completed.stderr)

        data = json.loads(completed.stdout)
        windows = {window[2]: window for window in data["windows"]}

        first = windows[60_000]
        second = windows[70_000]

        self.assertEqual(first[0:5], [1, "mem", 60_000, 2_000_000, 0])
        self.assertEqual(first[5], 949)
        self.assertTrue(first[6])
        self.assertEqual(first[7], "bad")

        self.assertEqual(second[0:5], [1, "mem", 70_000, 1_000_000, 0])
        self.assertEqual(second[5], 949)
        self.assertTrue(second[6])
        self.assertEqual(second[7], "bad")

        self.assertEqual(
            data["actions"],
            [[1, "mem", 70_000, "open_incident"]])


if __name__ == "__main__":
    unittest.main()
