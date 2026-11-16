"""Make the UI's fixtures: Trailbeam's own GET /api/messages/{id} for each message in recorded spans, so the UI
is tested on what Trailbeam really returns.

- test/fixtures/timelines.json: the smoke-test spans (python/smoke/recorded-spans.pb)
- test/fixtures/contract-3.2-timelines.json: the contract 3.2 spans the Node.js package recorded
  (contract/fixtures/contract-3.2-spans.json), which the hosted app's tests read too

    pip install -e "python[server]"   # from the repository root
    python ui/test/make-fixtures.py
"""
import base64
import json
from pathlib import Path

from trailbeam.server import queries
from trailbeam.server.db import Database
from trailbeam.server.otlp import decode

ROOT = Path(__file__).resolve().parents[2]


def timelines(blobs: list[tuple[bytes, str]]) -> list[dict]:
    db = Database(":memory:")
    for body, ctype in blobs:
        d = decode(body, ctype)
        assert not d.rejected, d.rejected
        db.insert(d.records)
    now = max(int(r["time_ns"]) for r in db.query("SELECT time_ns FROM spans")) + 3600 * 10**9
    mids = [r["mid"] for r in db.query("SELECT DISTINCT mid FROM spans WHERE mid IS NOT NULL ORDER BY mid")]
    return [queries.message(db, m, now, 900) for m in mids]


def write(name: str, note: str, ts: list[dict]) -> None:
    (ROOT / "ui" / "test" / "fixtures" / name).write_text(json.dumps({"note": note, "timelines": ts}, indent=1) + "\n")
    print(name, len(ts), "timelines,", sum(len(t["events"]) for t in ts), "events")


write("timelines.json",
      "GET /api/messages/{id} for the messages in python/smoke/recorded-spans.pb, made by Trailbeam itself (ui/test/make-fixtures.py).",
      timelines([((ROOT / "python" / "smoke" / "recorded-spans.pb").read_bytes(), "application/x-protobuf")]))
spans = json.loads((ROOT / "contract" / "fixtures" / "contract-3.2-spans.json").read_text())
write("contract-3.2-timelines.json",
      "GET /api/messages/{id} for the messages in contract/fixtures/contract-3.2-spans.json, made by Trailbeam itself "
      "(ui/test/make-fixtures.py), one hour after the last span, with the default Lambda timeout of 900 s.",
      timelines([(base64.b64decode(e["base64"]), e["contentType"]) for e in spans["exports"]]))
