"""Make test/fixtures/timelines.json: Trailbeam's own GET /api/messages/{id} for each message in the recorded
smoke-test spans, so the UI is tested on what Trailbeam really returns.

    pip install -e "python[server]"   # from the repository root
    python ui/test/make-fixtures.py
"""
import json
from pathlib import Path

from trailbeam.server import queries
from trailbeam.server.db import Database
from trailbeam.server.otlp import decode

ROOT = Path(__file__).resolve().parents[2]
body = (ROOT / "python" / "smoke" / "recorded-spans.pb").read_bytes()
db = Database(":memory:")
db.insert(decode(body, "application/x-protobuf").records)
now = max(int(r["time_ns"]) for r in db.query("SELECT time_ns FROM spans")) + 3600 * 10**9
mids = [r["mid"] for r in db.query("SELECT DISTINCT mid FROM spans WHERE mid IS NOT NULL ORDER BY mid")]
out = {"note": "GET /api/messages/{id} for the messages in python/smoke/recorded-spans.pb, made by Trailbeam itself (ui/test/make-fixtures.py).",
       "timelines": [queries.message(db, m, now, 900) for m in mids]}
(ROOT / "ui" / "test" / "fixtures" / "timelines.json").write_text(json.dumps(out, indent=1) + "\n")
print(len(out["timelines"]), "timelines,", sum(len(t["events"]) for t in out["timelines"]), "events")
