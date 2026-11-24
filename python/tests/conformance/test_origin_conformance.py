"""EventBridge and S3 notification origins (contract section 22.3, revision 3.5), through the Store interface only:
every backend keeps the receive's section 22 attributes, and a message's timeline gets the inferred first step. What
the step says is origin.py and timeline.py, the same for every backend."""
import json
import sys
from pathlib import Path

from trailbeam.server import queries
from trailbeam.server.otlp import decode

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "server"))
from spans import SEC, T0, Consumer, to_proto  # noqa: E402

NOW = T0 + 3600 * SEC
LT = 900
T0_MS = T0 // 1_000_000
C = Consumer()
P = Consumer("producer", service="orders-api")

VECTORS = {c["name"].split(":")[0] + ":" + str(i): c for i, c in enumerate(json.loads(
    (Path(__file__).resolve().parents[3] / "contract" / "span-contract-vectors.json").read_text("utf-8"))["origin"]["receive"])}
EB = VECTORS["EventBridge:0"]["attributes"]          # aws.ec2, time 2017-12-22T18:43:48Z
S3_KEY = VECTORS["S3:7"]["attributes"]               # ObjectCreated:Put with the key (body captured)
S3_TEST = VECTORS["S3:11"]["attributes"]             # s3:TestEvent


def put(s, *spans):
    d = decode(to_proto(list(spans)).SerializeToString(), "application/x-protobuf")
    assert d.rejected == []
    return s.insert(d.records)


def receive(t, mid, handle, attrs, **kw):
    sp = C.receive(t, mid, handle, **kw)
    sp.attrs.update(attrs)
    return sp


def types(t):
    return [e["type"] for e in t["events"]]


def test_eventbridge_rule(new_store):
    s = new_store()
    sent = EB["trailbeam.eventbridge.time_ms"] + 250
    put(s, receive(T0 + 2 * SEC, "sqs-a", "rh-a", EB, sent_ms=sent, start=T0 + SEC))
    t = queries.message(s, "sqs-a", NOW, LT)
    assert types(t) == ["arrived_via_eventbridge", "received"]
    ev = t["events"][0]
    assert (ev["inferred"], ev["source"], ev["time"]) == (True, "eventbridge_event", None)
    assert (ev["eventbridge_source"], ev["detail_type"], ev["event_id"], ev["account"], ev["region"]) == (
        "aws.ec2", "EC2 Instance State-change Notification", "6a7e8feb-b491-4cf7-a9f1-bf3703467718", "111122223333",
        "us-west-1")
    assert ev["event_time"] == "2017-12-22T18:43:48.000000Z" and ev["delivery_ms"] == 250
    assert ev["reported_on_delivery"] == 1 and ev["window"]["from"] is None
    assert ev["after"] == [] and ev["before"]["source"] == "inferred_from"
    assert "EventBridge event" in ev["before"]["why"] and "does not name the rule" in ev["reason"]


def test_s3_object_created_with_key(new_store):
    s = new_store()
    put(s, receive(T0 + 2 * SEC, "sqs-a", "rh-a", S3_KEY, sent_ms=1040))
    ev = queries.message(s, "sqs-a", NOW, LT)["events"][0]
    assert (ev["type"], ev["inferred"], ev["source"]) == ("s3_event", True, "s3_event")
    assert (ev["s3_event_name"], ev["bucket"], ev["object_key"], ev["sequencer"], ev["configuration_id"]) == (
        "ObjectCreated:Put", "amzn-s3-demo-bucket", "red+flower.jpg", "0055AED6DCD90281E5", "testConfigRule")
    assert ev["test_event"] is False and ev["delivery_ms"] == 1040  # eventTime 1970-01-01T00:00:00.000Z
    assert "object red+flower.jpg in bucket amzn-s3-demo-bucket" in ev["reason"]


def test_s3_without_key_and_test_message(new_store):
    s = new_store()
    no_key = {k: v for k, v in S3_KEY.items() if k != "trailbeam.s3.object_key"}
    put(s, receive(T0 + 2 * SEC, "sqs-a", "rh-a", no_key), receive(T0 + 3 * SEC, "sqs-t", "rh-t", S3_TEST))
    a = queries.message(s, "sqs-a", NOW, LT)["events"][0]
    assert "object_key" not in a and "on bucket amzn-s3-demo-bucket" in a["reason"] and "delivery_ms" not in a
    tst = queries.message(s, "sqs-t", NOW, LT)["events"][0]
    assert (tst["type"], tst["s3_event_name"], tst["test_event"]) == ("s3_event", "s3:TestEvent", True)
    assert "notification configuration" in tst["reason"]


def test_not_shown_when_a_send_was_seen(new_store):
    s = new_store()
    put(s, P.send(T0 + SEC, "sqs-a", start=T0), receive(T0 + 3 * SEC, "sqs-a", "rh-a", EB, start=T0 + 2 * SEC))
    assert types(queries.message(s, "sqs-a", NOW, LT)) == ["sent", "received"]


def test_an_sns_envelope_wins(new_store):
    s = new_store()
    sp = C.sns_receive(T0 + 2 * SEC, "sqs-a", "rh-a", sns_id="dc1e94d9-56c5-5e96-808d-cc7f68faa162")
    sp.attrs.update(EB)
    put(s, sp)
    assert types(queries.message(s, "sqs-a", NOW, LT)) == ["arrived_via_sns", "received"]


def test_before_the_earliest_receive_and_nothing_else_changes(new_store):
    s = new_store()
    put(s, receive(T0 + 2 * SEC, "sqs-a", "rh-1", EB, count=1, sent_ms=T0_MS),
        receive(T0 + 60 * SEC, "sqs-a", "rh-2", EB, count=2, sent_ms=T0_MS))
    put(s, C.receive(T0 + 2 * SEC, "sqs-b", "rh-1", count=1, sent_ms=T0_MS),
        C.receive(T0 + 60 * SEC, "sqs-b", "rh-2", count=2, sent_ms=T0_MS))
    a, b = queries.message(s, "sqs-a", NOW, LT), queries.message(s, "sqs-b", NOW, LT)
    assert types(a) == ["arrived_via_eventbridge"] + types(b)
    assert a["events"][0]["reported_on_delivery"] == 1
    for k in ("state", "first_seen", "last_seen", "first_seen_clock"):
        assert a.get(k) == b.get(k), k
