"""Make the UI's fixtures: Trailbeam's own GET /api/messages/{id} for each message in recorded spans, so the UI
is tested on what Trailbeam really returns.

- test/fixtures/timelines.json: the smoke-test spans (python/smoke/recorded-spans.pb)
- test/fixtures/contract-3.2-timelines.json: the contract 3.2 spans the Node.js package recorded
  (contract/fixtures/contract-3.2-spans.json), which the hosted app's tests read too
- test/fixtures/purge-timelines.json: contract 3.3 PurgeQueue (section 18): a message sent before a purge (purged),
  one sent during it (possibly purged), and one received after it (not purged), from the server tests' span builder
- test/fixtures/sns-timelines.json: contract 3.4 SNS fan-out (section 21): one publish that reached two queues, one
  through the envelope (raw delivery off) and one through the publish ID (raw delivery on), and a message that
  arrived via SNS from a publisher with no Trailbeam package, from the same span builder
- test/fixtures/origin-timelines.json: contract 3.5 (section 23): a message an EventBridge rule sent, one an S3
  notification sent (body captured: with the key), and S3's test message, from the same span builder
- test/fixtures/put-event-timelines.json: contract 3.6 EventBridge PutEvents (section 25): one recorded entry on
  orders-bus, with a sender-given Time, that reached two queues, and one on the default bus, from the same span builder

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
T0_MS = T0 // 10**6
write("sns-timelines.json",
      "GET /api/messages/{id} after an SNS fan-out (contract 3.4, section 21), made by Trailbeam itself "
      "(ui/test/make-fixtures.py) from python/tests/server/spans.py: one Publish to orders-topic reached sns-orders "
      "(raw delivery off: joined by the envelope, with an SNS delivery time) and sns-billing (raw delivery on: joined "
      "by the publish ID, no delivery time); sns-arrived names the topic in its envelope, and no publish was recorded.",
      timelines([(to_proto([P.publish(T0 + SEC, "sns-msg-1", publish_id="pub-1", start=T0),
                            C.sns_receive(T0 + 3 * SEC, "sns-orders", "rh-o", sns_id="sns-msg-1", sns_ts_ms=T0_MS + 1000,
                                          sent_ms=T0_MS + 1040, queue="orders", start=T0 + 2 * SEC),
                            C.sns_receive(T0 + 3 * SEC, "sns-billing", "rh-b", publish_id="pub-1", queue="billing",
                                          sent_ms=T0_MS + 1050, start=T0 + 2 * SEC),
                            C.delete(T0 + 4 * SEC, "rh-o", "sns-orders", queue="orders", start=T0 + 3 * SEC + 500_000_000),
                            C.sns_receive(T0 + 9 * SEC, "sns-arrived", "rh-x", sns_id="sns-msg-2",
                                          topic="audit-topic", sns_ts_ms=T0_MS + 7000, sent_ms=T0_MS + 7025,
                                          start=T0 + 8 * SEC)]).SerializeToString(),
                  "application/x-protobuf")]))

EB_ATTRS = {"trailbeam.eventbridge.event_id": "6a7e8feb-b491-4cf7-a9f1-bf3703467718",
            "trailbeam.eventbridge.source": "com.example.orders", "trailbeam.eventbridge.detail_type": "OrderPlaced",
            "trailbeam.eventbridge.account": "111122223333", "trailbeam.eventbridge.region": "eu-west-1",
            "trailbeam.eventbridge.time_ms": T0_MS}
S3_ATTRS = {"trailbeam.s3.event_name": "ObjectCreated:Put", "trailbeam.s3.bucket": "uploads",
            "trailbeam.s3.region": "us-west-2", "trailbeam.s3.event_time_ms": T0_MS + 5000,
            "trailbeam.s3.configuration_id": "new-uploads", "trailbeam.s3.sequencer": "0055AED6DCD90281E5",
            "trailbeam.s3.object_key": "red+flower.jpg"}
S3_TEST_ATTRS = {"trailbeam.s3.event_name": "s3:TestEvent", "trailbeam.s3.bucket": "uploads",
                 "trailbeam.s3.event_time_ms": T0_MS + 8000}
write("origin-timelines.json",
      "GET /api/messages/{id} for messages an EventBridge rule or an S3 notification sent (contract 3.5, section 23), "
      "made by Trailbeam itself (ui/test/make-fixtures.py) from python/tests/server/spans.py: eb-order (an EventBridge "
      "event, delivered 1.2 s after its time), s3-upload (ObjectCreated:Put with the key, from a captured queue) and "
      "s3-test (S3's test message).",
      timelines([(to_proto([C.receive_with(T0 + 2 * SEC, "eb-order", "rh-e", EB_ATTRS, sent_ms=T0_MS + 1200,
                                           start=T0 + SEC),
                            C.delete(T0 + 3 * SEC, "rh-e", "eb-order", queue="orders", start=T0 + 2 * SEC + 500_000_000),
                            C.receive_with(T0 + 7 * SEC, "s3-upload", "rh-s", S3_ATTRS, sent_ms=T0_MS + 5400,
                                           start=T0 + 6 * SEC),
                            C.receive_with(T0 + 9 * SEC, "s3-test", "rh-t", S3_TEST_ATTRS, sent_ms=T0_MS + 8100,
                                           start=T0 + 8 * SEC)]).SerializeToString(),
                  "application/x-protobuf")]))

EVENT_A, EVENT_B = "11710aed-b79e-4468-a20b-bb3c0c3b4860", "5f0c2d1e-8a3b-4c7d-9e6f-0a1b2c3d4e5f"
write("put-event-timelines.json",
      "GET /api/messages/{id} for messages an EventBridge rule sent from a recorded PutEvents entry (contract 3.6, "
      "section 25), made by Trailbeam itself (ui/test/make-fixtures.py) from python/tests/server/spans.py: one entry "
      "on orders-bus, with a Time the sender gave, reached pe-orders (handled and deleted) and pe-billing; a failed "
      "entry in the same call joins nothing; pe-default came from an entry on the default bus, timed by EventBridge.",
      timelines([(to_proto([P.put_event(T0 + SEC, EVENT_A, bus="orders-bus", time_given=True, start=T0),
                            P.put_event(T0 + SEC, error="ThrottlingException", index=1, start=T0),
                            C.eb_receive(T0 + 3 * SEC, "pe-orders", "rh-po", EVENT_A, time_ms=T0_MS,
                                         sent_ms=T0_MS + 400, queue="orders", start=T0 + 2 * SEC),
                            C.eb_receive(T0 + 3 * SEC, "pe-billing", "rh-pb", EVENT_A, time_ms=T0_MS,
                                         sent_ms=T0_MS + 450, queue="billing", start=T0 + 2 * SEC),
                            C.delete(T0 + 4 * SEC, "rh-po", "pe-orders", queue="orders", start=T0 + 3 * SEC + 500_000_000),
                            P.put_event(T0 + 6 * SEC, EVENT_B, detail_type="OrderShipped", start=T0 + 5 * SEC),
                            C.eb_receive(T0 + 8 * SEC, "pe-default", "rh-pd", EVENT_B, detail_type="OrderShipped",
                                         time_ms=T0_MS + 6000, sent_ms=T0_MS + 6300, start=T0 + 7 * SEC)]).SerializeToString(),
                  "application/x-protobuf")]))
