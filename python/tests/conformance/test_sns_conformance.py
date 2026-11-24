"""SNS fan-out (contract section 21, revision 3.4), through the Store interface only: every backend stores publish
spans apart from messages, finds them by the keys a receive carries, lists the receives a publish reached, and drops
them by their own time on a retention pass. What a join means for a timeline is sns.py and timeline.py, the same for
every backend."""
import sys
from pathlib import Path

import pytest

from trailbeam.server import queries, sns
from trailbeam.server.otlp import decode
from trailbeam.server.store import Cutoffs

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "server"))
from spans import SEC, T0, Consumer, to_proto  # noqa: E402

NOW = T0 + 3600 * SEC
LT = 900
FOREVER = 2 ** 63 - 1
T0_MS = T0 // 1_000_000

SNS_ID = "dc1e94d9-56c5-5e96-808d-cc7f68faa162"
PID = "5f0c1b9e-2a33-4c1d-9d1e-6b1f3c2a4e10"

P = Consumer("publisher", service="orders-api")
C = Consumer()                                       # queue orders: raw delivery off
B = Consumer("billing-worker", queue="billing")      # queue billing: raw delivery on


def put(s, *spans):
    d = decode(to_proto(list(spans)).SerializeToString(), "application/x-protobuf")
    assert d.rejected == []
    return s.insert(d.records)


def types(t):
    return [e["type"] for e in t["events"]]


def fanout(s):
    """One publish to a topic with a raw (billing) and a non-raw (orders) queue subscription."""
    put(s, P.publish(T0 + SEC, SNS_ID, publish_id=PID, start=T0),
        C.sns_receive(T0 + 3 * SEC, "sqs-a", "rh-a", sns_id=SNS_ID, sns_ts_ms=T0_MS + 1000,
                      sent_ms=T0_MS + 1040, start=T0 + 2 * SEC),
        B.sns_receive(T0 + 4 * SEC, "sqs-b", "rh-b", publish_id=PID, sent_ms=T0_MS + 1050, start=T0 + 2 * SEC))


def test_a_publish_is_not_a_message(new_store):
    s = new_store()
    put(s, P.publish(T0 + SEC, SNS_ID, start=T0))
    assert s.recent_messages(100) == []
    assert s.counts()["messages"] == 0
    assert s.counts()["spans"] == {"publish": 1}
    assert queries.message(s, SNS_ID, NOW, LT) is None


def test_both_subscriptions_join_the_one_publish(new_store):
    s = new_store()
    fanout(s)
    a = queries.message(s, "sqs-a", NOW, LT)
    b = queries.message(s, "sqs-b", NOW, LT)
    assert types(a) == ["published", "received"] and types(b) == ["published", "received"]
    pa, pb = a["events"][0], b["events"][0]
    assert pa["span"] == pb["span"]
    assert pa["joined_by"] == ["envelope_message_id"] and pb["joined_by"] == ["publish_id"]
    assert (pa["topic"], pa["topic_arn"], pa["sns_message_id"], pa["publish_id"]) == (
        "orders-topic", "arn:aws:sns:us-east-1:111122223333:orders-topic", SNS_ID, PID)
    assert pa["queue"] == "orders-topic" and pa["inferred"] is False and pa["source"] == "sns_publish"
    want = [{"queue": "billing", "message_ids": ["sqs-b"]}, {"queue": "orders", "message_ids": ["sqs-a"]}]
    assert pa["reached"] == want and pb["reached"] == want and pa["reached_truncated"] is False
    # SNS delivery time: SentTimestamp - envelope Timestamp, only with an envelope
    assert pa["sns_delivery_ms"] == 40 and "sns_delivery_ms" not in pb
    # ordered by the hosts' clocks: the publish ended before the receive started
    assert [x["source"] for x in a["events"][1]["after"]] == ["clock"]
    assert a["first_seen_clock"] == "sqs"  # the publish is the topic's step: first seen is still SQS's SentTimestamp


def test_the_publish_may_arrive_after_the_receive(new_store):
    s = new_store()
    put(s, C.sns_receive(T0 + 3 * SEC, "sqs-a", "rh-a", sns_id=SNS_ID, start=T0 + 2 * SEC))
    assert types(queries.message(s, "sqs-a", NOW, LT)) == ["arrived_via_sns", "received"]
    put(s, P.publish(T0 + SEC, SNS_ID, start=T0))
    assert types(queries.message(s, "sqs-a", NOW, LT)) == ["published", "received"]


def test_a_receive_is_not_after_its_publish_without_a_clock_or_tick(new_store):
    # 21.5 "Order": the hosts' times overlap, so nothing orders the publish before the receive; "a receive comes
    # after its publish" is not a source.
    s = new_store()
    put(s, P.publish(T0 + 3 * SEC, SNS_ID, start=T0),
        C.sns_receive(T0 + 4 * SEC, "sqs-a", "rh-a", sns_id=SNS_ID, start=T0 + 2 * SEC))
    m = queries.message(s, "sqs-a", NOW, LT)
    assert types(m) == ["published", "received"]
    assert m["order"]["edges"] == [] and all(e["after"] == [] for e in m["events"])
    assert m["events"][0]["group"] == m["events"][1]["group"]


def test_a_publish_delivered_twice_to_one_queue_joins_both_messages(new_store):
    # 21.5: SNS can deliver one publish to a queue more than once; each delivery is its own SQS message.
    s = new_store()
    put(s, P.publish(T0 + SEC, SNS_ID, start=T0),
        C.sns_receive(T0 + 3 * SEC, "sqs-a", "rh-a", sns_id=SNS_ID, start=T0 + 2 * SEC),
        C.sns_receive(T0 + 5 * SEC, "sqs-a2", "rh-a2", sns_id=SNS_ID, start=T0 + 4 * SEC))
    want = [{"queue": "orders", "message_ids": ["sqs-a", "sqs-a2"]}]
    for mid in ("sqs-a", "sqs-a2"):
        m = queries.message(s, mid, NOW, LT)
        assert types(m) == ["published", "received"] and m["events"][0]["reached"] == want


def test_the_envelope_key_needs_the_same_topic(new_store):
    s = new_store()
    put(s, P.publish(T0 + SEC, SNS_ID, topic="other-topic", start=T0),
        C.sns_receive(T0 + 3 * SEC, "sqs-a", "rh-a", sns_id=SNS_ID, start=T0 + 2 * SEC))
    assert types(queries.message(s, "sqs-a", NOW, LT)) == ["arrived_via_sns", "received"]


def test_arrived_via_sns_without_a_recorded_publish(new_store):
    s = new_store()
    put(s, C.sns_receive(T0 + 3 * SEC, "sqs-a", "rh-a", sns_id=SNS_ID, sns_ts_ms=T0_MS + 1000,
                         sent_ms=T0_MS + 1040, start=T0 + 2 * SEC))
    t = queries.message(s, "sqs-a", NOW, LT)
    ev = t["events"][0]
    assert (ev["type"], ev["inferred"], ev["source"], ev["topic"]) == ("arrived_via_sns", False, "sns_envelope",
                                                                       "orders-topic")
    assert ev["window"] == {"from": None, "to": "2025-11-14T00:26:43.000000Z"}
    assert ev["sns_delivery_ms"] == 40 and ev["sns_timestamp_ms"] == T0_MS + 1000
    assert ev["after"] == [] and ev["before"]["source"] == "sns_envelope"
    assert t["send_observed"] is False


def test_a_publish_id_join_is_not_made_for_a_message_with_a_send(new_store):
    """A consumer that re-sent a raw-delivered message with all its attributes copied the publish ID."""
    s = new_store()
    put(s, P.publish(T0 + SEC, SNS_ID, publish_id=PID, start=T0),
        B.sns_receive(T0 + 4 * SEC, "sqs-b", "rh-b", publish_id=PID, start=T0 + 2 * SEC),
        P.send(T0 + 5 * SEC, "resent", queue="later", start=T0 + 5 * SEC - 1),
        B.sns_receive(T0 + 6 * SEC, "resent", "rh-c", publish_id=PID, queue="later", start=T0 + 6 * SEC - 1))
    assert types(queries.message(s, "resent", NOW, LT)) == ["sent", "received"]
    b = queries.message(s, "sqs-b", NOW, LT)
    assert b["events"][0]["reached"] == [{"queue": "billing", "message_ids": ["sqs-b"]}]


def test_a_receive_with_a_malformed_key_joins_nothing(new_store):
    s = new_store()
    sp = C.sns_receive(T0 + 3 * SEC, "sqs-a", "rh-a", publish_id=PID, start=T0 + 2 * SEC)
    sp.attrs["trailbeam.sns.publish_id"] = "not a valid id!"
    put(s, P.publish(T0 + SEC, SNS_ID, publish_id=PID, start=T0), sp)
    assert types(queries.message(s, "sqs-a", NOW, LT)) == ["received"]


def test_publishes_are_found_by_either_key(new_store):
    s = new_store()
    put(s, P.publish(T0 + SEC, SNS_ID, publish_id=PID, start=T0),
        P.publish(T0 + 2 * SEC, "other-id", batch_entry="e1", start=T0 + SEC))
    arn = "arn:aws:sns:us-east-1:111122223333:orders-topic"
    by_env = s.publishes(sns.Keys(frozenset([(arn, SNS_ID)]), frozenset()))
    by_pid = s.publishes(sns.Keys(frozenset(), frozenset([PID])))
    assert [p["message_id"] for p in by_env] == [SNS_ID] == [p["message_id"] for p in by_pid]
    p = by_env[0]
    assert (p["queue"], p["operation"], p["start_ns"], p["time_ns"]) == ("orders-topic", "Publish", T0, T0 + SEC)
    assert s.publishes(sns.NO_KEYS) == []
    assert s.publishes(sns.Keys(frozenset([(arn.replace("orders", "x"), SNS_ID)]), frozenset())) == []
    [b] = s.publishes(sns.Keys(frozenset([(arn, "other-id")]), frozenset()))
    assert (b["operation"], b["batch_entry_id"]) == ("PublishBatch", "e1")


def test_an_exporter_retry_of_a_publish_changes_nothing(new_store):
    s = new_store()
    sp = P.publish(T0 + SEC, SNS_ID, start=T0)
    assert put(s, sp) == (1, 0)
    assert put(s, sp) == (0, 1)
    assert s.counts()["spans"] == {"publish": 1}


def test_retention_drops_publishes_by_their_own_time(new_store):
    s = new_store()
    fanout(s)
    resume = None
    while True:
        step = s.purge_step(Cutoffs(FOREVER), resume, 500)
        resume = step.resume
        if resume is None:
            break
    assert s.counts()["spans"] == {} and s.counts()["messages"] == 0
    arn = "arn:aws:sns:us-east-1:111122223333:orders-topic"
    assert s.publishes(sns.Keys(frozenset([(arn, SNS_ID)]), frozenset([PID]))) == []
    assert s.sns_receivers(sns.Keys(frozenset([(arn, SNS_ID)]), frozenset([PID]))) == []


@pytest.mark.parametrize("missing", ["aws.sns.topic.arn", "messaging.message.id", "messaging.destination.name"])
def test_a_publish_span_without_its_keys_is_rejected(missing):
    sp = P.publish(T0 + SEC, SNS_ID, start=T0)
    del sp.attrs[missing]
    d = decode(to_proto([sp]).SerializeToString(), "application/x-protobuf")
    assert d.records == [] and len(d.rejected) == 1 and d.rejected[0].startswith("publish span without")
