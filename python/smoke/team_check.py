"""Check the team install (docker-compose.yaml): send one span with the token, read it back.

    python team_check.py http://127.0.0.1:4318 send  <message-id>    # waits for /api/ready, sends one send span
    python team_check.py http://127.0.0.1:4318 check <message-id>    # the message's timeline is there

The token is read from $TRAILBEAM_TOKEN. Standard library only. ``send`` also checks that ingest and the API
refuse a request without the token. Run ``check`` after
``docker compose down && docker compose up`` to see the data survived. Exits non-zero on the first failure.
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request


def call(base: str, path: str, token: str | None, body: bytes | None = None) -> tuple[int, dict]:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(base + path, data=body, headers=headers, method="POST" if body else "GET")
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw or b"{}")
        except ValueError:
            return e.code, {"raw": raw.decode(errors="replace")}


def check(cond: bool, what: str) -> None:
    if not cond:
        raise SystemExit(f"FAIL: {what}")
    print(f"ok: {what}")


def wait_ready(base: str, timeout_s: float = 120) -> None:
    deadline, last = time.time() + timeout_s, None
    while time.time() < deadline:
        try:
            status, _ = call(base, "/api/ready", None)
            if status == 200:
                return
            last = f"status {status}"
        except OSError as e:
            last = e
        time.sleep(1)
    raise SystemExit(f"FAIL: {base}/api/ready not 200 after {timeout_s:.0f} s: {last}")


def send_span(mid: str) -> bytes:
    """One send span (span contract section 3), stamped now so retention keeps it."""
    now = time.time_ns()
    attrs = {
        "trailbeam.schema_version": 3, "trailbeam.kind": "sqs.send", "trailbeam.source": "client",
        "messaging.system.name": "aws.sqs", "messaging.operation.type": "send",
        "messaging.operation.name": "SendMessage", "messaging.destination.name": "team-check",
        "aws.sqs.queue.url": "http://localhost:9324/000000000000/team-check",
        "messaging.client.id": "team-check", "process.pid": os.getpid(), "thread.name": "main",
        "messaging.message.id": mid,
    }

    def value(v):
        return {"intValue": str(v)} if isinstance(v, int) else {"stringValue": str(v)}

    return json.dumps({"resourceSpans": [{
        "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "team-check"}}]},
        "scopeSpans": [{"scope": {"name": "trailbeam", "version": "0.3.0"}, "spans": [{
            "traceId": os.urandom(16).hex(), "spanId": os.urandom(8).hex(), "name": "trailbeam send", "kind": 3,
            "startTimeUnixNano": str(now - 5_000_000), "endTimeUnixNano": str(now),
            "attributes": [{"key": k, "value": value(v)} for k, v in attrs.items()]}]}]}]}).encode()


def main(argv: list[str]) -> int:
    if len(argv) != 4 or argv[2] not in ("send", "check"):
        print(__doc__, file=sys.stderr)
        return 2
    base, action, mid = argv[1].rstrip("/"), argv[2], argv[3]
    token = os.environ.get("TRAILBEAM_TOKEN")
    if not token:
        print("FAIL: set TRAILBEAM_TOKEN", file=sys.stderr)
        return 2
    wait_ready(base)
    if action == "send":
        check(call(base, "/v1/traces", None, send_span(mid))[0] == 401, "ingest without the token is refused (401)")
        check(call(base, "/api/messages", None)[0] == 401, "the API without the token is refused (401)")
        status, out = call(base, "/v1/traces", token, send_span(mid))
        check(status == 200 and not out.get("partialSuccess"), f"one span accepted ({status} {out})")
    status, out = call(base, f"/api/messages/{mid}", token)
    kinds = [e.get("kind") or e.get("type") for e in out.get("events", out.get("timeline", []))] if status == 200 else []
    check(status == 200, f"message {mid} has a timeline ({status} {out if status != 200 else ''})")
    print(f"   events: {kinds}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
