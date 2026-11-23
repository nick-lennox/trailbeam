"""Make the UI's fixtures: Trailbeam's own GET /api/messages/{id} for each message in recorded spans, so the UI
is tested on what Trailbeam really returns.

- test/fixtures/timelines.json: the smoke-test spans (python/smoke/recorded-spans.pb)
- test/fixtures/contract-3.2-timelines.json: the contract 3.2 spans the Node.js package recorded
  (contract/fixtures/contract-3.2-spans.json), which the hosted app's tests read too
- test/fixtures/purge-timelines.json: contract 3.3 PurgeQueue (section 18): a message sent before a purge (purged),
  one sent during it (possibly purged), and one received after it (not purged), from the server tests' span builder

    pip install -e "python[server]"   # from the repository root
    python ui/test/make-fixtures.py
"""
import base64
import json
import sys
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
sys.path.insert(0, str(ROOT / "python" / "tests" / "server"))
from spans import SEC, T0, Consumer, to_proto  # noqa: E402

P, C = Consumer("producer", service="orders-api"), Consumer("orders-worker")
write("purge-timelines.json",
      "GET /api/messages/{id} after a PurgeQueue call (contract 3.3, section 18), made by Trailbeam itself "
      "(ui/test/make-fixtures.py) from python/tests/server/spans.py: purge-sent-before is purged, purge-sent-during "
      "possibly purged, purge-received-after (received after the 60 s window) not affected.",
      timelines([(to_proto([P.send(T0, "purge-sent-before"), C.receive(T0 + SEC, "purge-sent-before", "rh-1"),
                            P.send(T0 + 9 * SEC + 1, "purge-sent-during"), P.send(T0, "purge-received-after"),
                            P.purge(T0 + 10 * SEC, start=T0 + 9 * SEC),
                            C.receive(T0 + 80 * SEC, "purge-received-after", "rh-2")]).SerializeToString(),
                  "application/x-protobuf")]))
