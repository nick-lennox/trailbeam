"""Make the UI's RabbitMQ fixtures (QV-308) from the contract's RabbitMQ spans.

test/fixtures/amqp-timelines.json: Trailbeam's own GET /api/messages/{id} and the Messages list's rows for
every RabbitMQ message of the contract's fixture (contract/fixtures/amqp-4.0-spans.json, contract 4.0), so the UI's
RabbitMQ wording (QV-308) is tested on what Trailbeam really returns: a publish acked once, requeues, a nack loop,
channel closes to a dead-letter queue, a refusal, a fanout to two queues, a publisher retry, an unroutable publish, a
classic queue, a consumer timeout and a delivery never settled.

test/fixtures/amqp-timeline-events-parity.json: GET /api/timeline-events over the same spans, and what the server's
port of eventsOfTimeline (timeline_events.py) was given for each message, as timeline-events-parity.json records it
for SQS: test/rabbitmq.test.ts gives eventsOfTimeline the same inputs and must get the same events, and
python/tests/server/test_timeline_events.py checks the server still answers what is recorded.

    pip install -e ./python -e ./server   # from the repository root
    python ui/test/make-amqp-fixture.py
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "python" / "tests" / "conformance"))

from test_amqp_conformance import FIXTURE, load  # noqa: E402
from trailbeam.server import queries, server, systems, timeline_events  # noqa: E402
from trailbeam.server.db import SqliteStore  # noqa: E402

LT = 900
NOW = max(sp["end_time_unix_nano"] for sp in FIXTURE["spans"]) + 10 ** 9
FIXTURES = ROOT / "ui" / "test" / "fixtures"
TIMELINES, PARITY = FIXTURES / "amqp-timelines.json", FIXTURES / "amqp-timeline-events-parity.json"


def record() -> tuple[dict, dict]:
    """(amqp-timelines.json, amqp-timeline-events-parity.json), from a new SQLite store of the contract's spans."""
    store = SqliteStore()
    try:
        load(store)
        items = queries.recent(store, NOW, LT, 100)
        ts = [queries.message(store, it["message_id"], NOW, LT, None) for it in items]
        assert ts and all(systems.system_of(t["message_id"]) == systems.RABBITMQ for t in ts)
        timelines = {
            "note": "GET /api/messages/{id} and the Messages list's rows for the RabbitMQ messages of contract/fixtures/"
                    "amqp-4.0-spans.json (contract 4.0), made by Trailbeam itself (ui/test/make-amqp-fixture.py).",
            "items": items,
            "timelines": sorted(ts, key=lambda t: t["message_id"]),
        }
        now_ms = NOW // 10 ** 6
        app = server.App(store, ui_dir=None, now=lambda: NOW)
        window = {"from_ms": str(now_ms - 2 * 3_600_000), "to_ms": str(now_ms)}
        got = timeline_events.inputs(store, int(window["from_ms"]), now_ms, NOW, app.lambda_timeout_s,
                                     app.max_receive_counts)
        answer = app.api("timeline-events", window)
        assert answer["events"] and all(e.get("system") == systems.RABBITMQ for e in answer["events"])
        parity = {
            "note": "GET /api/timeline-events for contract/fixtures/amqp-4.0-spans.json, and what the server's port of "
                    "eventsOfTimeline was given for each message. eventsOfTimeline must make the same events of the same "
                    "inputs (test/rabbitmq.test.ts). Made by test/make-amqp-fixture.py.",
            "nowMs": now_ms, "fromMs": int(window["from_ms"]), "toMs": now_ms,
            "inputs": got["messages"],
            "answer": answer,
        }
        return timelines, parity
    finally:
        store.close()


def main() -> None:
    timelines, parity = record()
    TIMELINES.write_text(json.dumps(timelines, indent=1) + "\n")
    PARITY.write_text(json.dumps(parity, indent=1) + "\n")
    print(f"{len(timelines['timelines'])} timelines, {len(parity['answer']['events'])} events")


if __name__ == "__main__":
    main()
