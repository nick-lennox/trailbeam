"""Make test/fixtures/amqp-timelines.json: Trailbeam's own GET /api/messages/{id} and the Messages list's rows for
every RabbitMQ message of the contract's fixture (contract/fixtures/amqp-4.0-spans.json, contract 4.0), so the UI's
RabbitMQ wording (QV-308) is tested on what Trailbeam really returns: a publish acked once, requeues, a nack loop,
channel closes to a dead-letter queue, a refusal, a fanout to two queues, a publisher retry, an unroutable publish, a
classic queue, a consumer timeout and a delivery never settled.

    pip install -e ./python -e ./server   # from the repository root
    python ui/test/make-amqp-fixture.py
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "python" / "tests" / "conformance"))

from test_amqp_conformance import FIXTURE, load  # noqa: E402
from trailbeam.server import queries, systems  # noqa: E402
from trailbeam.server.db import SqliteStore  # noqa: E402

LT = 900
NOW = max(sp["end_time_unix_nano"] for sp in FIXTURE["spans"]) + 10 ** 9

store = SqliteStore()
load(store)
items = queries.recent(store, NOW, LT, 100)
ts = [queries.message(store, it["message_id"], NOW, LT, None) for it in items]
assert ts and all(systems.system_of(t["message_id"]) == systems.RABBITMQ for t in ts)
out = ROOT / "ui" / "test" / "fixtures" / "amqp-timelines.json"
out.write_text(json.dumps({
    "note": "GET /api/messages/{id} and the Messages list's rows for the RabbitMQ messages of contract/fixtures/"
            "amqp-4.0-spans.json (contract 4.0), made by Trailbeam itself (ui/test/make-amqp-fixture.py).",
    "items": items,
    "timelines": sorted(ts, key=lambda t: t["message_id"]),
}, indent=1) + "\n")
store.close()
print(f"{len(ts)} timelines")
