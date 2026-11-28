"""EventBridge PutEvents (contract section 25, revision 3.6), through the Store interface only: every backend stores
put_event spans apart from messages, finds the accepted ones by the event ID a receive's body names, lists the
receives an entry reached, and drops them by their own time on a retention pass. What a join means for a timeline is
eventbridge.py and timeline.py, the same for every backend."""
import sys
from pathlib import Path

import pytest

from trailbeam.server import queries
from trailbeam.server.otlp import decode
from trailbeam.server.store import Cutoffs

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "server"))
from spans import SEC, T0, Consumer, to_proto  # noqa: E402

NOW = T0 + 3600 * SEC
LT = 900
FOREVER = 2 ** 63 - 1
T0_MS = T0 // 1_000_000

EID = "11710aed-b79e-4468-a20b-bb3c0c3b4860"
OTHER = "d804d26a-88db-4b66-9eaf-9a11c708ae82"

P = Consumer("publisher", service="orders-api")
C = Consumer()                                       # queue orders
B = Consumer("billing-worker", queue="billing")      # queue billing: a second rule's target


def put(s, *spans):
    d = decode(to_proto(list(spans)).SerializeToString(), "application/x-protobuf")
    assert d.rejected == []
    return s.insert(d.records)


def types(t):
    return [e["type"] for e in t["events"]]


def codes(t):
    return [n["code"] for n in t["notices"]]


def fanout(s):
    """One PutEvents entry that two rules send to two queues."""
    put(s, P.put_event(T0 + SEC, EID, bus="orders-bus", start=T0),
        C.eb_receive(T0 + 3 * SEC, "sqs-a", "rh-a", EID, time_ms=T0_MS, sent_ms=T0_MS + 1300, start=T0 + 2 * SEC),
        B.eb_receive(T0 + 4 * SEC, "sqs-b", "rh-b", EID, time_ms=T0_MS, sent_ms=T0_MS + 1350, start=T0 + 2 * SEC))


def test_an_entry_is_not_a_message(new_store):
    s = new_store()
    put(s, P.put_event(T0 + SEC, EID, start=T0), P.put_event(T0 + SEC, error="ThrottlingException", index=1, start=T0))
    assert s.recent_messages(100) == []
    assert s.counts()["messages"] == 0
    assert s.counts()["spans"] == {"put_event": 2}
    assert queries.message(s, EID, NOW, LT) is None


def test_both_queues_join_the_one_entry(new_store):
    s = new_store()
    fanout(s)
    a = queries.message(s, "sqs-a", NOW, LT)
    b = queries.message(s, "sqs-b", NOW, LT)
    # the joined entry replaces the inferred "Arrived via an EventBridge rule" (section 23.3)
    assert types(a) == ["put_event", "received"] and types(b) == ["put_event", "received"]
    assert codes(a) == codes(b) == []  # QV-728: no send_not_observed under a joined entry
    ea, eb = a["events"][0], b["events"][0]
    assert ea["span"] == eb["span"]
    assert ea["inferred"] is False and ea["source"] == "eventbridge_put_events" and ea["joined_by"] == "event_id"
    assert (ea["event_bus"], ea["event_id"], ea["eventbridge_source"], ea["detail_type"]) == (
        "orders-bus", EID, "com.example.orders", "OrderPlaced")
    assert ea["queue"] == "orders-bus" and ea["event_time_given"] is False and ea["entry_index"] == 0
    want = [{"queue": "billing", "message_ids": ["sqs-b"]}, {"queue": "orders", "message_ids": ["sqs-a"]}]
    assert ea["reached"] == want and eb["reached"] == want and ea["reached_truncated"] is False
    # EventBridge delivery time: SentTimestamp - the event's time, of each message's own first receive
    assert ea["eventbridge_delivery_ms"] == 1300 and eb["eventbridge_delivery_ms"] == 1350
    assert ea["event_time_ms"] == T0_MS
    assert [x["source"] for x in a["events"][1]["after"]] == ["clock"]
    assert a["first_seen_clock"] == "sqs"  # the entry is the bus's step: first seen is still SQS's SentTimestamp


def test_the_entry_may_arrive_after_the_receive(new_store):
    s = new_store()
    put(s, C.eb_receive(T0 + 3 * SEC, "sqs-a", "rh-a", EID, start=T0 + 2 * SEC))
    t = queries.message(s, "sqs-a", NOW, LT)
    assert types(t) == ["arrived_via_eventbridge", "received"] and "send_not_observed" in codes(t)
    put(s, P.put_event(T0 + SEC, EID, start=T0))
    t = queries.message(s, "sqs-a", NOW, LT)
    # QV-728: the PutEvents caller is instrumented and on the page; the SQS send was EventBridge's own
    assert types(t) == ["put_event", "received"] and "send_not_observed" not in codes(t)


def test_a_failed_entry_or_another_event_joins_nothing(new_store):
    s = new_store()
    put(s, P.put_event(T0 + SEC, OTHER, start=T0), P.put_event(T0 + SEC, error="InternalFailure", index=1, start=T0),
        C.eb_receive(T0 + 3 * SEC, "sqs-a", "rh-a", EID, start=T0 + 2 * SEC))
    assert types(queries.message(s, "sqs-a", NOW, LT)) == ["arrived_via_eventbridge", "received"]
    assert s.put_events(frozenset([EID])) == []


def test_a_message_with_a_send_still_joins(new_store):
    # 25.3: a process re-sent the body (a DLQ redrive); the event still came from that entry.
    s = new_store()
    put(s, P.put_event(T0 + SEC, EID, start=T0), C.send(T0 + 2 * SEC, "sqs-a", start=T0 + SEC + 1),
        C.eb_receive(T0 + 4 * SEC, "sqs-a", "rh-a", EID, start=T0 + 3 * SEC))
    assert types(queries.message(s, "sqs-a", NOW, LT)) == ["put_event", "sent", "received"]


def test_a_sender_given_time_is_flagged(new_store):
    s = new_store()
    put(s, P.put_event(T0 + SEC, EID, time_given=True, start=T0),
        C.eb_receive(T0 + 3 * SEC, "sqs-a", "rh-a", EID, time_ms=T0_MS - 5000, sent_ms=T0_MS + 1000,
                     start=T0 + 2 * SEC))
    e = queries.message(s, "sqs-a", NOW, LT)["events"][0]
    assert e["event_time_given"] is True and e["eventbridge_delivery_ms"] == 6000


def test_an_exporter_retry_of_an_entry_changes_nothing(new_store):
    s = new_store()
    sp = P.put_event(T0 + SEC, EID, start=T0)
    assert put(s, sp) == (1, 0)
    assert put(s, sp) == (0, 1)
    assert s.counts()["spans"] == {"put_event": 1}


def test_retention_drops_entries_by_their_own_time(new_store):
    s = new_store()
    fanout(s)
    put(s, P.put_event(T0 + SEC, error="ThrottlingException", index=1, start=T0))
    resume = None
    while True:
        step = s.purge_step(Cutoffs(FOREVER), resume, 500)
        resume = step.resume
        if resume is None:
            break
    assert s.counts()["spans"] == {} and s.counts()["messages"] == 0
    assert s.put_events(frozenset([EID])) == []
    assert s.eventbridge_receivers(frozenset([EID])) == []


@pytest.mark.parametrize("change,why", [
    (lambda a: a.pop("messaging.destination.name"), "put_event span without queue"),
    (lambda a: a.pop("trailbeam.result"), "put_event span without result"),
    (lambda a: a.update({"messaging.destination.name": "no spaces allowed"}), "put_event span without a valid event bus"),
    (lambda a: a.pop("messaging.message.id"), "put_event span with trailbeam.result ok and no valid event ID"),
    (lambda a: a.update({"messaging.message.id": "x" * 65}), "put_event span with trailbeam.result ok and no valid"),
    (lambda a: a.update({"trailbeam.result": "maybe"}), "trailbeam.result 'maybe' is not ok or failed"),
])
def test_a_put_event_span_without_its_fields_is_rejected(change, why):
    sp = P.put_event(T0 + SEC, EID, start=T0)
    change(sp.attrs)
    d = decode(to_proto([sp]).SerializeToString(), "application/x-protobuf")
    assert d.records == [] and len(d.rejected) == 1 and d.rejected[0].startswith(why), d.rejected
