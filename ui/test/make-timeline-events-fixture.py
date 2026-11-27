"""Record GET /api/timeline-events in test/fixtures/timeline-events-parity.json (QV-580).

The server makes the Event explorer's events of each message's timeline with a Python port of @trailbeam/ui's
eventsOfTimeline (server timeline_events.py). The fixture holds what the port was given for each message of the
window (its timeline, queue URLs, the spans' 3.2 attributes and its kept body) and what the endpoint answered, for
contract/fixtures/contract-3.2-spans.json with every queue's bodies stored, and a few Lambda messages the contract spans
don't have (an inferred delete, an invocation with no outcome, a failure then a retry), one hour after the last span.
test/timeline-events.test.ts gives the same inputs to eventsOfTimeline and must get the same events;
python/tests/server/test_timeline_events.py checks the server still answers what is recorded.

    pip install -e ./python -e ./server   # from the repository root
    python ui/test/make-timeline-events-fixture.py
"""
import base64
import json
import sys
from pathlib import Path

from trailbeam.server import server, timeline_events
from trailbeam.server.db import Database
from trailbeam.server.otlp import decode

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "ui" / "test" / "fixtures" / "timeline-events-parity.json"
HOUR_NS = 3600 * 10**9
SEC = 10**9
sys.path.insert(0, str(ROOT / "python" / "tests" / "server"))


def lambda_spans(t0: int) -> list:
    """Lambda messages from t0 (ns): one that succeeds, one whose invocation never reports an outcome, and one that
    fails, becomes visible again and succeeds on its second receive (spans.py, the server tests' span builder)."""
    from spans import Consumer
    fn, producer = Consumer("2026/11/16/[$LATEST]fn", service="billing-fn", source="lambda", queue="billing"), \
        Consumer("producer", service="checkout", queue="billing")
    out = [producer.send(t0, "m-fn-ok"), fn.receive(t0 + SEC, "m-fn-ok", "rh-ok", lambda_inv="inv-ok", sent_ms=t0 // 10**6),
            fn.outcome(t0 + 2 * SEC, "m-fn-ok", "success", "inv-ok"),
            producer.send(t0 + 3 * SEC, "m-fn-quiet"), fn.receive(t0 + 4 * SEC, "m-fn-quiet", "rh-q", lambda_inv="inv-q"),
            fn.receive(t0 + 5 * SEC, "m-fn-retry", "rh-r1", count=1, lambda_inv="inv-r1"),
            fn.outcome(t0 + 6 * SEC, "m-fn-retry", "failed_exception", "inv-r1", error_type="RuntimeError"),
            fn.receive(t0 + 45 * SEC, "m-fn-retry", "rh-r2", count=2, lambda_inv="inv-r2"),
            fn.outcome(t0 + 46 * SEC, "m-fn-retry", "success", "inv-r2")]
    for i, sp in enumerate(out, 1):  # fixed IDs, so a new recording is the same file
        sp.trace_id, sp.span_id = i.to_bytes(16, "big"), (0xF000 + i).to_bytes(8, "big")
    return out


def record(db=None) -> dict:
    """The fixture, from a new SQLite store, or ``db``: an empty store that stores every queue's bodies."""
    spans = json.loads((ROOT / "contract" / "fixtures" / "contract-3.2-spans.json").read_text())
    db = Database(":memory:", store_bodies=frozenset({"*"})) if db is None else db
    for e in spans["exports"]:
        d = decode(base64.b64decode(e["base64"]), e["contentType"])
        assert not d.rejected, d.rejected
        db.insert(d.records)
    last_ns = max(int(r["time_ns"]) for e in spans["exports"]
                  for r in decode(base64.b64decode(e["base64"]), e["contentType"]).records)
    from spans import to_proto
    d = decode(to_proto(lambda_spans(last_ns - HOUR_NS // 2), service="billing-fn").SerializeToString(),
               "application/x-protobuf")
    assert not d.rejected, d.rejected
    db.insert(d.records)
    now_ns = last_ns + HOUR_NS
    now_ms = now_ns // 10**6
    app = server.App(db, ui_dir=None, now=lambda: now_ns)
    window = {"from_ms": str(now_ms - 2 * 3_600_000), "to_ms": str(now_ms)}
    got = timeline_events.inputs(db, int(window["from_ms"]), now_ms, now_ns, app.lambda_timeout_s, app.max_receive_counts)
    return {
        "note": "GET /api/timeline-events for contract/fixtures/contract-3.2-spans.json (every queue's bodies stored), "
                "and what the server's port of eventsOfTimeline was given for each message. eventsOfTimeline must make "
                "the same events of the same inputs (test/timeline-events.test.ts). Made by "
                "test/make-timeline-events-fixture.py.",
        "nowMs": now_ms,
        "fromMs": int(window["from_ms"]),
        "toMs": now_ms,
        "inputs": got["messages"],
        "answer": app.api("timeline-events", window),
    }


def main() -> None:
    out = record()
    OUT.write_text(json.dumps(out, indent=1) + "\n")
    print(OUT.name, len(out["inputs"]), "messages,", len(out["answer"]["events"]), "events")


if __name__ == "__main__":
    main()
