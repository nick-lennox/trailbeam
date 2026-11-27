"""Record self-hosted Trailbeam's answers in test/fixtures/viewmodel-parity.json (One UI row 0d, QV-578).

Both products' pages are built by the same view-model functions (@trailbeam/ui/model): eventsOfTimeline, overviewData
and toCause. The fixture holds what each backend's API answers for the same spans, and both must give the same result
through those functions. This script writes ``answers.python``: GET /api/messages/{id} for every message, GET
/api/flows and GET /api/causes, for contract/fixtures/contract-3.2-spans.json, one hour after the last span. The
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
    answers = {
        "timelines": [app.api(f"messages/{m}", {}) for m in mids],
        "flows": app.api("flows", window)["flows"],
        "causes": app.api("causes", {})["groups"],
    }
    return now_ms, int(window["from_ms"]), answers


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
    print(OUT.name, len(answers["timelines"]), "timelines,", len(answers["flows"]), "flows,", len(answers["causes"]), "cause groups")


if __name__ == "__main__":
    main()
