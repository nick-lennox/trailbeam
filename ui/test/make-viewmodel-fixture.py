"""Record self-hosted Trailbeam's answers in test/fixtures/viewmodel-parity.json (One UI row 0d, QV-578).

Both products' pages are built by the same view-model functions (@trailbeam/ui/model): eventsOfTimeline, overviewData
and toCause. The fixture holds what each backend's API answers for the same spans, and both must give the same result
through those functions. This script writes ``answers.python``: GET /api/messages/{id} for every message, GET
/api/flows and GET /api/causes (its groups, and its messages with their cause for the Issues pages), for contract/fixtures/contract-3.2-spans.json, one hour after the last span. ``answers.python.queue_now`` is
GET /api/queues/{queue}/now for every queue, 5 seconds after the last span (messages in flight, the past minute's
deletes) and at that same hour (their timeouts ran out), with billing's maxReceiveCount set (QV-579). The
hosted app records ``answers.hosted`` from its own engine (trailbeam-app scripts/record-viewmodel-answers.ts), and
``expected`` is what the functions make of them (test/make-viewmodel-expected.ts). Answers already in the file from
the other side are kept.

    pip install -e ./python -e ./server   # from the repository root
    python ui/test/make-viewmodel-fixture.py
"""
import base64
import json
from pathlib import Path

from trailbeam.server import server
from trailbeam.server.db import Database
from trailbeam.server.otlp import decode

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "ui" / "test" / "fixtures" / "viewmodel-parity.json"
HOUR_NS = 3600 * 10**9
# "In ‹queue› now" (QV-579): read soon after the last span and at the fixture's hour, with billing's maxReceiveCount set
NOW_AFTER_NS = (5 * 10**9, HOUR_NS)
NOW_MAX_RECEIVE_COUNTS = {"billing": 3}
# what the shared issueOf reads of a cause's message (@trailbeam/ui/model issues.ts, IssueItem)
ISSUE_ITEM = ("message_id", "cause", "state", "current_queue", "in_dead_letter_queue", "deliveries", "first_seen", "last_seen",
              "consumers", "purge")


def record() -> tuple:
    """(now_ms, from_ms, answers): what the server answers for the spans, one hour after the last one."""
    spans = json.loads((ROOT / "contract" / "fixtures" / "contract-3.2-spans.json").read_text())
    db = Database(":memory:")
    for e in spans["exports"]:
        d = decode(base64.b64decode(e["base64"]), e["contentType"])
        assert not d.rejected, d.rejected
        db.insert(d.records)
    now_ns = max(int(r["time_ns"]) for r in db.query("SELECT time_ns FROM spans")) + HOUR_NS
    now_ms = now_ns // 10**6
    app = server.App(db, ui_dir=None, now=lambda: now_ns)
    mids = [r["mid"] for r in db.query("SELECT DISTINCT mid FROM spans WHERE mid IS NOT NULL ORDER BY mid")]
    window = {"from_ms": str(now_ms - 2 * 3_600_000), "to_ms": str(now_ms)}
    causes = app.api("causes", {"limit": "1000"})
    assert causes["listed"] == len(causes["messages"]), "every cause message fits in one page"
    answers = {
        "timelines": [app.api(f"messages/{m}", {}) for m in mids],
        "flows": app.api("flows", window)["flows"],
        "causes": causes["groups"],
        "cause_messages": [{k: m[k] for k in ISSUE_ITEM if k in m} for m in causes["messages"]],
        "queue_now": queue_now(db, now_ns - HOUR_NS, [f["name"] for f in app.api("flows", window)["flows"]]),
    }
    return now_ms, int(window["from_ms"]), answers


def queue_now(db, last_ns: int, queues: list) -> list:
    """GET /api/queues/{queue}/now for every queue, at each of NOW_AFTER_NS after the last span."""
    out = []
    for after in NOW_AFTER_NS:
        at_ns = last_ns + after
        app = server.App(db, ui_dir=None, now=lambda: at_ns, max_receive_counts=NOW_MAX_RECEIVE_COUNTS)
        for q in queues:
            out.append({"queue": q, "nowMs": at_ns // 10**6, "maxReceiveCount": NOW_MAX_RECEIVE_COUNTS.get(q),
                        "view": app.api(f"queues/{q}/now", {})})
    return out


def main() -> None:
    now_ms, from_ms, answers = record()
    old = json.loads(OUT.read_text()) if OUT.exists() else {}
    out = {
        "note": "The same spans (contract/fixtures/contract-3.2-spans.json) through each backend's API, and what the shared "
                "view-model functions (@trailbeam/ui/model) make of each answer: the two must be the same. "
                "answers.python is made by ui/test/make-viewmodel-fixture.py, answers.hosted by the hosted app's "
                "scripts/record-viewmodel-answers.ts, expected by ui/test/make-viewmodel-expected.ts.",
        "nowMs": now_ms,
        "fromMs": from_ms,
        "answers": {**old.get("answers", {}), "python": answers},
        **({"expected": old["expected"]} if "expected" in old else {}),
    }
    OUT.write_text(json.dumps(out, indent=1, sort_keys=False) + "\n")
    print(OUT.name, len(answers["timelines"]), "timelines,", len(answers["flows"]), "flows,", len(answers["causes"]), "cause groups,",
          len(answers["cause_messages"]), "cause messages,", len(answers["queue_now"]), "queue views")


if __name__ == "__main__":
    main()
