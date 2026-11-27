# SPDX-License-Identifier: AGPL-3.0-only (imports the server; LICENSING.md)
"""Spans for deploy/monitoring/check.sh: one message stuck for 10 minutes, one moved to a dead-letter queue a
minute ago, and one handled and deleted. Usage: python seed.py http://127.0.0.1:4318"""
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "python" / "tests" / "server"))
from spans import SEC, Consumer, to_proto  # noqa: E402  (the test suite's span builder: contract-valid spans)

base = sys.argv[1].rstrip("/")
now = time.time_ns()
c = Consumer()
spans = [
    c.send(now - 600 * SEC, "stuck-1", queue="orders"),
    c.receive(now - 599 * SEC, "stuck-1", "rh-stuck", queue="orders"),
    c.receive(now - 60 * SEC, "dead-1", "rh-dead", count=4, queue="orders-dlq", dlq_source="orders"),
    c.send(now - 30 * SEC, "done-1", queue="orders"),
    c.receive(now - 29 * SEC, "done-1", "rh-done", queue="orders"),
    c.delete(now - 28 * SEC, "rh-done", queue="orders"),
]
req = urllib.request.Request(base + "/v1/traces", data=to_proto(spans).SerializeToString(), method="POST",
                             headers={"Content-Type": "application/x-protobuf"})
with urllib.request.urlopen(req, timeout=10) as r:
    assert r.status == 200, r.status
for path in ("/api/messages", "/api/messages/stuck-1", "/api/stuck", "/api/health", "/stuck"):
    urllib.request.urlopen(base + path, timeout=10).read()  # some page traffic, for the http_* series
print(f"seeded {len(spans)} spans into {base}")
