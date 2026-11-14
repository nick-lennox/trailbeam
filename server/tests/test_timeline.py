"""Timelines, inferred steps, duplicate deliveries and stuck messages."""
import pytest

from qv2_store import queries
from qv2_store.timeline import iso
from spans import KEY, SEC, T0, Consumer

NOW = T0 + 3600 * SEC
LT = 900


def tl(store, mid, now=NOW, lambda_timeout=LT, mrc=None):
    return queries.message(store, mid, now, lambda_timeout, mrc)


def types(t):
    return [(e["type"], e["inferred"]) for e in t["events"]]


def check_marking(t):
    """Inferred steps are never shown as observed: no time, a window instead, and a reason."""
    for e in t["events"]:
        if e["inferred"]:
            assert e["time"] is None and "window" in e and e["reason"] and "span" not in e
            assert e["type"] != "moved_to_dead_letter_queue"  # contract 3.1: never inferred
        elif e["type"] == "moved_to_dead_letter_queue":  # observed, but SQS gives it no time (section 9b)
            assert e["source"] == "sqs_dlq_source" and e["time"] is None and "span" not in e
            assert e["reported_by"] == "reported by SQS (DeadLetterQueueSourceArn on the DLQ receive)"
        else:
            assert e["time"] and e["span"]["span_id"] and "window" not in e


def test_happy_path(store, ingest):
    p, c = Consumer("producer", service="orders-api"), Consumer()
    ingest(p.send(T0, "m1"), c.receive(T0 + SEC, "m1", "rh-1", sent_ms=T0 // 1_000_000), c.delete(T0 + 2 * SEC, "rh-1"))
    t = tl(store, "m1")
    check_marking(t)
    assert types(t) == [("sent", False), ("received", False), ("deleted", False)]
    assert t["events"][2]["status"] == "final" and t["events"][2]["delivery"] == 1
    assert t["events"][0]["consumer"]["service_name"] == "orders-api"
    assert (t["state"], t["deliveries"], t["successful_deliveries"], t["notices"]) == ("deleted", 1, 1, [])
    assert t["events"][1]["time"] == "2025-11-14T00:26:41.000000Z"


def test_visibility_timeout_expiry_is_inferred_and_late_delete_is_stale(store, ingest):
    """Consumer A is too slow: the message is delivered to B, then both delete (double processing)."""
    a, b = Consumer("consumer-a"), Consumer("consumer-b")
    ingest(Consumer("producer").send(T0, "m1"),
           a.receive(T0 + SEC, "m1", "rh-1", count=1),
           b.receive(T0 + 32 * SEC, "m1", "rh-2", count=2),
           a.delete(T0 + 40 * SEC, "rh-1"),
           b.delete(T0 + 41 * SEC, "rh-2"))
    t = tl(store, "m1")
    check_marking(t)
    assert types(t) == [("sent", False), ("received", False), ("visibility_timeout_expired", True),
                        ("received", False), ("deleted", False), ("deleted", False)]
    exp = t["events"][2]
    assert (exp["after_delivery"], exp["before_delivery"]) == (1, 2)
    assert exp["window"] == {"from": t["events"][1]["time"], "to": t["events"][3]["time"]}
    assert [(e["delivery"], e["status"]) for e in t["events"][4:]] == [(1, "stale_handle"), (2, "final")]
    assert t["state"] == "deleted" and t["successful_deliveries"] == 2

    dup = queries.duplicates(store, NOW, LT, 100)
    assert [d["message_id"] for d in dup] == ["m1"]
    assert dup[0]["reasons"] == ["received_more_than_once", "succeeded_more_than_once"]
    assert {c["client_id"] for c in dup[0]["consumers"]} == {"consumer-a", "consumer-b"}


def test_delete_accepted_before_a_redelivery_is_stale(store, ingest):
    """The delete came after the visibility timeout: SQS accepted it, yet delivered the message again."""
    a, b = Consumer("consumer-a"), Consumer("consumer-b")
    ingest(a.receive(T0, "m1", "rh-1", count=1), a.delete(T0 + 31 * SEC, "rh-1"),
           b.receive(T0 + 32 * SEC, "m1", "rh-2", count=2))
    t = tl(store, "m1")
    assert t["events"][1]["status"] == "stale_handle"
    assert "came too late" in next(e for e in t["events"] if e["type"] == "visibility_timeout_expired")["reason"]
    assert t["state"] == "not_deleted"
    assert [s["message_id"] for s in queries.stuck(store, NOW, LT, 300, 100)] == ["m1"]


def test_change_visibility_to_zero_is_the_reason_for_redelivery(store, ingest):
    c = Consumer()
    ingest(c.receive(T0, "m1", "rh-1", count=1), c.change_visibility(T0 + SEC, "rh-1", 0),
           c.receive(T0 + 2 * SEC, "m1", "rh-2", count=2), c.delete(T0 + 3 * SEC, "rh-2"))
    t = tl(store, "m1")
    check_marking(t)
    assert types(t) == [("received", False), ("visibility_changed", False), ("visibility_timeout_expired", True),
                        ("received", False), ("deleted", False)]
    assert t["events"][1]["visibility_timeout_s"] == 0 and t["events"][1]["delivery"] == 1
    assert "to 0" in t["events"][2]["reason"]


def poison(c, d, dlq_count=4, dlq_source="orders"):
    """Three receives on orders, then a receive on orders-dlq (same message ID), deleted there."""
    return (Consumer("producer").send(T0, "m1"),
            *[c.receive(T0 + (1 + 31 * i) * SEC, "m1", f"rh-{i}", count=i + 1) for i in range(3)],
            d.receive(T0 + 200 * SEC, "m1", "rh-dlq", count=dlq_count, dlq_source=dlq_source),
            d.delete(T0 + 201 * SEC, "rh-dlq"))


def test_move_to_dead_letter_queue_is_observed_from_dead_letter_queue_source_arn(store, ingest):
    """Contract 3.1, section 9b: the DLQ receive carries DeadLetterQueueSourceArn, so SQS reported the move."""
    c, d = Consumer(), Consumer("dlq-worker", queue="orders-dlq")
    ingest(*poison(c, d))
    t = tl(store, "m1")
    check_marking(t)
    assert types(t) == [("sent", False), ("received", False), ("visibility_timeout_expired", True),
                        ("received", False), ("visibility_timeout_expired", True), ("received", False),
                        ("moved_to_dead_letter_queue", False), ("received", False), ("deleted", False)]
    last_src, move, dlq_rcv = t["events"][5:8]
    assert (move["from_queue"], move["to_queue"]) == ("orders", "orders-dlq")
    assert move["source_arn"] == "arn:aws:sqs:us-east-1:000000000000:orders"
    assert move["reported_on_delivery"] == dlq_rcv["delivery"] == 4
    # Placed after the last receive on orders and before the DLQ receive that reports it, by those two only.
    assert [(a["event"], a["sources"]) for a in move["after"]] == [(last_src["id"], ["sqs_dlq_source"])]
    assert (move["before"]["event"], move["before"]["sources"]) == (dlq_rcv["id"], ["sqs_dlq_source"])
    assert "DeadLetterQueueSourceArn" in move["before"]["why"] and "orders" in move["before"]["why"]
    assert move["window"] == {"from": last_src["start"], "to": dlq_rcv["end"]}
    assert "sqs_dlq_source" not in [s for e in t["order"]["edges"] for s in e["sources"]]  # not a new order source
    assert t["events"][8]["status"] == "final"
    assert (t["state"], t["source_queue"], t["current_queue"], t["in_dead_letter_queue"],
            t["dead_letter_queue_move"]) == ("deleted", "orders", "orders-dlq", True, "observed")
    assert not [n for n in t["notices"] if n["code"] == "dlq_move_not_seen"]


def test_receive_on_another_queue_without_the_attribute_shows_no_move(store, ingest):
    """No report from SQS, no move: the message ID on another queue is not enough (contract 3.1)."""
    c, d = Consumer(), Consumer("dlq-worker", queue="orders-dlq")
    ingest(*poison(c, d, dlq_source=None))
    t = tl(store, "m1", mrc={"orders": 3})
    check_marking(t)
    assert "moved_to_dead_letter_queue" not in [e["type"] for e in t["events"]]
    assert "moved_back_to_source_queue" not in [e["type"] for e in t["events"]]
    assert (t["current_queue"], t["in_dead_letter_queue"], t["dead_letter_queue_move"]) == \
        ("orders-dlq", False, None)  # deleted, so not "not seen yet" either


def test_receive_count_reached_max_but_not_seen_on_the_dead_letter_queue(store, ingest):
    c = Consumer()
    ingest(Consumer("producer").send(T0, "m1"),
           *[c.receive(T0 + (1 + 31 * i) * SEC, "m1", f"rh-{i}", count=i + 1) for i in range(3)])
    t = tl(store, "m1", mrc={"orders": 3})
    check_marking(t)
    assert "moved_to_dead_letter_queue" not in [e["type"] for e in t["events"]]
    assert (t["dead_letter_queue_move"], t["in_dead_letter_queue"]) == ("not_seen_on_dlq_yet", False)
    [n] = [n for n in t["notices"] if n["code"] == "dlq_move_not_seen"]
    assert n["message"].startswith("Receive count exceeded maxReceiveCount; not seen on the DLQ yet.")
    assert "receive count 3" in n["message"] and "maxReceiveCount is 3" in n["message"]
    assert "does not receive from the dead-letter queue" in n["message"]
    # Not configured, or below the limit: nothing to say.
    assert tl(store, "m1")["dead_letter_queue_move"] is None
    assert tl(store, "m1", mrc={"orders": 4})["dead_letter_queue_move"] is None
    assert tl(store, "m1", mrc={"other": 1})["dead_letter_queue_move"] is None


def test_deleted_at_max_receive_count_is_not_waiting_for_the_dead_letter_queue(store, ingest):
    c = Consumer()
    ingest(c.send(T0, "m1"), c.receive(T0 + SEC, "m1", "rh-1"), c.delete(T0 + 2 * SEC, "rh-1"))
    t = tl(store, "m1", mrc={"orders": 1})
    assert (t["state"], t["dead_letter_queue_move"], t["notices"]) == ("deleted", None, [])


def test_move_reported_by_the_first_of_several_dead_letter_receives(store, ingest):
    """Every receive from the DLQ carries the attribute; the move is one step, before the first of them."""
    c, d = Consumer(), Consumer("dlq-worker", queue="orders-dlq")
    ingest(c.send(T0, "m1"), c.receive(T0 + SEC, "m1", "rh-1"),
           d.receive(T0 + 50 * SEC, "m1", "rh-d1", count=2, dlq_source="orders"),
           d.receive(T0 + 90 * SEC, "m1", "rh-d2", count=3, dlq_source="orders"))
    t = tl(store, "m1")
    check_marking(t)
    assert [e["type"] for e in t["events"]] == ["sent", "received", "moved_to_dead_letter_queue", "received",
                                                "visibility_timeout_expired", "received"]
    move = t["events"][2]
    assert move["reported_on_delivery"] == 2 and move["before"]["event"] == t["events"][3]["id"]


def test_move_when_no_receive_on_the_source_queue_was_observed(store, ingest):
    """The consumers of orders run without qv2: the move is still reported, placed after the send."""
    p, d = Consumer("producer"), Consumer("dlq-worker", queue="orders-dlq")
    ingest(p.send(T0, "m1"), d.receive(T0 + 50 * SEC, "m1", "rh-d1", count=4, dlq_source="orders"))
    t = tl(store, "m1")
    check_marking(t)
    assert [e["type"] for e in t["events"]] == ["sent", "moved_to_dead_letter_queue", "received"]
    assert t["events"][1]["after"][0]["event"] == t["events"][0]["id"]
    assert t["in_dead_letter_queue"] is True


def test_move_is_ordered_against_other_steps_only_through_its_anchors(store, ingest):
    """A delete on orders that overlaps the last receive there is not placed against the move by any guess."""
    c, d = Consumer(), Consumer("dlq-worker", queue="orders-dlq")
    ingest(c.send(T0, "m1"), c.receive(T0 + SEC, "m1", "rh-1", start=T0 + SEC // 2),
           c.delete(T0 + SEC, "rh-1", start=T0 + SEC // 2),  # overlaps receive 1: not proven after it
           d.receive(T0 + 50 * SEC, "m1", "rh-d1", count=2, dlq_source="orders"))
    t = tl(store, "m1")
    move = next(e for e in t["events"] if e["type"] == "moved_to_dead_letter_queue")
    assert [a["sources"] for a in move["after"]] == [["sqs_dlq_source"]]
    rcv1 = next(e for e in t["events"] if e["type"] == "received" and e["delivery"] == 1)
    assert move["after"][0]["event"] == rcv1["id"]


def test_message_waiting_in_dead_letter_queue_is_stuck(store, ingest):
    c, d = Consumer(), Consumer("dlq-peek", queue="orders-dlq")
    ingest(c.send(T0, "m1"), c.receive(T0 + SEC, "m1", "rh-1"),
           d.receive(T0 + 100 * SEC, "m1", "rh-2", count=2, dlq_source="orders"))
    s = queries.stuck(store, NOW, LT, 300, 100)
    assert [(x["message_id"], x["current_queue"], x["in_dead_letter_queue"], x["dead_letter_queue_move"])
            for x in s] == [("m1", "orders-dlq", True, "observed")]


def test_lambda_success_infers_the_delete(store, ingest):
    fn = Consumer("2026/11/13/[$LATEST]abc", source="lambda")
    ingest(Consumer("producer").send(T0, "m1"), fn.receive(T0 + SEC, "m1", "rh-1", lambda_inv="inv-1"),
           fn.outcome(T0 + 2 * SEC, "m1", "success", "inv-1"))
    t = tl(store, "m1")
    check_marking(t)
    assert types(t) == [("sent", False), ("received", False), ("lambda_outcome", False), ("deleted_by_lambda", True)]
    assert t["events"][2]["delivery"] == 1 and t["events"][3]["delivery"] == 1
    assert t["state"] == "deleted_inferred" and t["successful_deliveries"] == 1
    assert queries.stuck(store, NOW, LT, 300, 100) == []


def test_lambda_failure_then_success(store, ingest):
    fn = Consumer("env-1", source="lambda")
    ingest(fn.receive(T0, "m1", "rh-1", count=1, lambda_inv="inv-1"),
           fn.outcome(T0 + SEC, "m1", "failed_exception", "inv-1", error_type="RuntimeError"),
           fn.receive(T0 + 40 * SEC, "m1", "rh-2", count=2, lambda_inv="inv-2"),
           fn.outcome(T0 + 41 * SEC, "m1", "success", "inv-2"))
    t = tl(store, "m1")
    assert types(t) == [("received", False), ("lambda_outcome", False), ("visibility_timeout_expired", True),
                        ("received", False), ("lambda_outcome", False), ("deleted_by_lambda", True)]
    assert t["events"][1]["error_type"] == "RuntimeError"
    assert t["state"] == "deleted_inferred"
    assert queries.duplicates(store, NOW, LT, 100, reason="succeeded_more_than_once") == []
    assert [d["message_id"] for d in queries.duplicates(store, NOW, LT, 100, reason="received_more_than_once")] == ["m1"]


def test_lambda_receive_without_outcome_is_inferred_killed_after_the_timeout(store, ingest):
    fn = Consumer("env-1", source="lambda")
    ingest(fn.send(T0, "m1"), fn.receive(T0 + SEC, "m1", "rh-1", lambda_inv="inv-1"))
    during = tl(store, "m1", now=T0 + 60 * SEC, lambda_timeout=900)
    assert types(during) == [("sent", False), ("received", False)] and during["state"] == "processing"
    after = tl(store, "m1", now=T0 + 120 * SEC, lambda_timeout=60)
    check_marking(after)
    assert types(after) == [("sent", False), ("received", False), ("lambda_invocation_ended_without_outcome", True)]
    assert after["events"][2]["faas_invocation_id"] == "inv-1" and after["state"] == "not_deleted"


def test_unobserved_deliveries_are_inferred_from_the_receive_count(store, ingest):
    c = Consumer()
    ingest(c.send(T0, "m1"), c.receive(T0 + 100 * SEC, "m1", "rh-3", count=3),
           c.receive(T0 + 200 * SEC, "m1", "rh-5", count=5), c.delete(T0 + 201 * SEC, "rh-5"))
    t = tl(store, "m1")
    check_marking(t)
    gaps = [e for e in t["events"] if e["type"] == "deliveries_not_observed"]
    assert [g["count"] for g in gaps] == [2, 1]
    assert types(t)[:3] == [("sent", False), ("deliveries_not_observed", True), ("received", False)]


def test_batch_delete_in_the_timeline_of_each_message(store, ingest):
    c, other = Consumer(), Consumer("deleter")
    tr = b"\x07" * 16
    ingest(*[c.receive(T0, m, f"rh-{m}") for m in ("a", "b", "c")])
    ingest(*[other.delete(T0 + SEC, f"rh-{m}", batch_entry=str(i), trace_id=tr) for i, m in enumerate("abc")])
    for i, m in enumerate("abc"):
        t = tl(store, m)
        d = t["events"][-1]
        assert (d["type"], d["status"], d["batch_entry_id"], d["span"]["trace_id"]) == ("deleted", "final", str(i), tr.hex())
        assert d["consumer"]["client_id"] == "deleter" and t["state"] == "deleted"


def test_failed_delete_does_not_settle(store, ingest):
    c = Consumer()
    ingest(c.send(T0, "m1"), c.receive(T0 + SEC, "m1", "rh-1"), c.delete(T0 + 2 * SEC, "rh-1", ok=False))
    t = tl(store, "m1")
    assert (t["events"][-1]["status"], t["events"][-1]["error_code"]) == ("failed", "ReceiptHandleIsInvalid")
    assert t["state"] == "not_deleted" and t["successful_deliveries"] == 0
    assert [s["message_id"] for s in queries.stuck(store, NOW, LT, 300, 100)] == ["m1"]


def test_observed_steps_are_never_reordered_by_cause(store, ingest):
    """Contract section 9a: the order shown is the order the clocks prove, nothing else. If b's clock is
    1 s behind, its delete is shown before the receive it settles; the store does not move it."""
    a, b = Consumer("a"), Consumer("b")
    ingest(a.receive(T0 + 5 * SEC, "m1", "rh-1", start=T0 + 5 * SEC - 1000),
           b.delete(T0 + 4 * SEC, "rh-1", start=T0 + 4 * SEC - 1000))
    t = tl(store, "m1")
    assert [(e["type"], e["group"]) for e in t["events"]] == [("deleted", 0), ("received", 1)]
    assert t["events"][0]["delivery"] == 1 and t["events"][0]["status"] == "final"  # the join is unchanged


# -- order: concurrent groups (contract section 9a) ----------------------------------------------

MS = 1_000_000


def groups(t):
    """Event types per group, in display order; inferred events between groups as their own entry."""
    out = []
    for e in t["events"]:
        if e["group"] is None:
            out.append(e["type"])
        elif out and isinstance(out[-1], tuple) and out[-1][0] == e["group"]:
            out[-1][1].append(e["type"])
        else:
            out.append((e["group"], [e["type"]]))
    return [x if isinstance(x, str) else x[1] for x in out]


def test_overlapping_send_and_receive_are_one_concurrent_group(store, ingest):
    """The QV-20 case: the receive's response came back before the send's, but SQS acted inside both
    intervals and they overlap, so the store cannot say which came first and does not pretend to."""
    p, c = Consumer("producer"), Consumer()
    ingest(p.send(T0 + 30 * MS, "m1", start=T0),                        # 0 to 30 ms
           c.receive(T0 + 20 * MS, "m1", "rh-1", start=T0 - 900 * MS),   # a long poll, ends at 20 ms
           c.delete(T0 + 50 * MS, "rh-1", start=T0 + 40 * MS))
    t = tl(store, "m1")
    check_marking(t)
    assert groups(t) == [["received", "sent"], ["deleted"]]
    assert t["groups"][0] == {"group": 0, "concurrent": True, "label": "concurrent, order unknown",
                              "observed_events": 2, "inferred_events": 0, "ordered_inside": 0,
                              "start": "2025-11-14T00:26:39.100000Z", "end": "2025-11-14T00:26:40.030000Z"}
    assert t["groups"][1]["concurrent"] is False and t["groups"][1]["label"] is None
    assert t["concurrent_groups"] == 1
    sent = t["events"][1]
    assert (sent["start"], sent["end"], sent["start_observed"]) == (
        "2025-11-14T00:26:40.000000Z", "2025-11-14T00:26:40.030000Z", True)
    assert (sent["start_unix_nano"], sent["end_unix_nano"]) == (str(T0), str(T0 + 30 * MS))


def test_a_step_is_before_another_only_if_it_ended_before_the_other_started(store, ingest):
    c = Consumer()
    # ended before: ordered
    ingest(c.send(T0 + 10, "a", start=T0), c.receive(T0 + 20, "a", "rh-a", start=T0 + 11))
    assert groups(tl(store, "a")) == [["sent"], ["received"]]
    # touching (end == start): not proven, so concurrent
    ingest(c.send(T0 + 10, "b", start=T0), c.receive(T0 + 20, "b", "rh-b", start=T0 + 10))
    assert groups(tl(store, "b")) == [["sent", "received"]]


def test_a_chain_of_overlaps_is_one_group(store, ingest):
    """A overlaps B and B overlaps C: one group, even though A ended before C started. Splitting it
    would show A before B or B before C, which the clocks do not prove."""
    c = Consumer()
    ingest(c.send(T0 + 10, "m1", start=T0),
           c.receive(T0 + 30, "m1", "rh-1", start=T0 + 5),
           c.delete(T0 + 40, "rh-1", start=T0 + 20),
           c.delete(T0 + 60, "rh-1", start=T0 + 50, ok=False))
    t = tl(store, "m1")
    assert groups(t) == [["sent", "received", "deleted"], ["deleted"]]


def test_lambda_receive_has_no_start_so_it_is_concurrent_with_what_came_before(store, ingest):
    fn = Consumer("env-1", source="lambda")
    ingest(Consumer("producer").send(T0, "m1", start=T0 - 5 * MS),
           fn.receive(T0 + SEC, "m1", "rh-1", lambda_inv="inv-1", start=T0 + SEC),
           fn.outcome(T0 + 2 * SEC, "m1", "success", "inv-1"))
    t = tl(store, "m1")
    rcv = t["events"][1]
    assert (rcv["type"], rcv["start"], rcv["start_unix_nano"], rcv["start_observed"]) == ("received", None, None, False)
    assert rcv["end"] == "2025-11-14T00:26:41.000000Z"
    assert groups(t) == [["sent", "received"], ["lambda_outcome"], "deleted_by_lambda"]
    assert t["groups"][0]["start"] is None  # one member's start is unknown


def test_inferred_step_between_groups_is_placed_by_its_anchors_not_by_a_time(store, ingest):
    c = Consumer()
    ingest(c.send(T0 + 10, "m1", start=T0),
           c.receive(T0 + SEC, "m1", "rh-1", start=T0 + 100),
           c.change_visibility(T0 + SEC + 20, "rh-1", 0, start=T0 + SEC + 10),
           c.receive(T0 + 2 * SEC, "m1", "rh-2", count=2, start=T0 + SEC + 5),  # overlaps the visibility change
           c.delete(T0 + 3 * SEC, "rh-2", start=T0 + 2 * SEC + 10))
    t = tl(store, "m1")
    check_marking(t)
    assert groups(t) == [["sent"], ["received"], ["visibility_changed", "visibility_timeout_expired", "received"],
                         ["deleted"]]
    expiry = next(e for e in t["events"] if e["type"] == "visibility_timeout_expired")
    # Its anchors (delivery 1's receive, delivery 2's receive) are in different groups, but delivery 2 is
    # in a concurrent group: the expiry is shown inside that group, never ordered against its members.
    assert expiry["group"] == 2
    assert expiry["window"] == {"from": "2025-11-14T00:26:40.000000Z", "to": "2025-11-14T00:26:42.000000Z"}
    assert t["groups"][2]["inferred_events"] == 1 and t["groups"][2]["observed_events"] == 2


def test_inferred_step_between_two_single_groups_has_no_group(store, ingest):
    c, d = Consumer("c"), Consumer("d")
    ingest(c.send(T0, "m1"), c.receive(T0 + SEC, "m1", "rh-1", start=T0 + SEC - 10),
           d.receive(T0 + 100 * SEC, "m1", "rh-2", count=2, start=T0 + 99 * SEC))
    t = tl(store, "m1")
    assert groups(t) == [["sent"], ["received"], "visibility_timeout_expired", ["received"]]
    expiry = t["events"][2]
    assert expiry["group"] is None
    # The window runs from the start of the receive that hid the message to the end of the one that got it.
    assert expiry["window"]["from"] == iso(T0 + SEC - 10) and expiry["window"]["to"] == iso(T0 + 100 * SEC)


def test_every_observed_event_has_start_end_and_group(store, ingest):
    c = Consumer()
    ingest(c.send(T0 + 5, "m1", start=T0), c.receive(T0 + SEC, "m1", "rh-1", start=T0 + 10))
    for e in tl(store, "m1")["events"]:
        assert {"start", "end", "start_unix_nano", "end_unix_nano", "start_observed", "group"} <= set(e)
        assert e["time"] == e["end"]


def test_notices(store, ingest):
    dflt = Consumer(key="qv2-default-receipt-hash-key")
    ingest(dflt.receive(T0, "m1", "rh-1"), dflt.delete(T0 + SEC, "rh-1"))
    assert [n["code"] for n in tl(store, "m1")["notices"]] == ["default_key", "send_not_observed"]

    c, wrong = Consumer(), Consumer("w", key="another-key-000000")
    ingest(c.send(T0, "m2"), c.receive(T0, "m2", "rh-2"), wrong.delete(T0 + SEC, "rh-2", mid="m2"))
    t = tl(store, "m2")
    assert [n["code"] for n in t["notices"]] == ["key_mismatch"]
    assert t["events"][-1]["status"] == "unmatched_handle" and t["state"] == "not_deleted"


class TestStuck:
    def setup(self, ingest):
        p, c = Consumer("producer"), Consumer()
        ingest(p.send(T0, "never-received"),
               p.send(T0, "deleted"), c.receive(T0 + SEC, "deleted", "rh-d"), c.delete(T0 + 2 * SEC, "rh-d"),
               p.send(NOW - 60 * SEC, "recent"),
               c.receive(T0 + 10 * SEC, "no-send", "rh-n", sent_ms=(T0 + 5 * SEC) // 1_000_000),
               Consumer("o", queue="payments").send(T0, "other-queue"))

    def test_lists_only_old_undeleted_messages(self, store, ingest):
        self.setup(ingest)
        s = queries.stuck(store, NOW, LT, 300, 100)
        assert [x["message_id"] for x in s] == ["never-received", "other-queue", "no-send"]
        assert s[0]["age_s"] == 3600 and s[0]["deliveries"] == 0 and s[0]["state"] == "not_deleted"
        assert s[2]["age_s"] == 3595  # from SQS SentTimestamp, since the send was not observed
        assert [x["age_across_clocks"] for x in s] == [False, False, True]  # and it says so
        assert [x["first_seen_clock"] for x in s] == ["host", "host", "sqs"]

    def test_an_observed_send_is_never_replaced_by_an_earlier_sqs_timestamp(self, store, ingest):
        """QV-32 section 4: first seen took the minimum of host send times and SQS SentTimestamps. With SQS's
        clock 2 s behind the producer's, that moved first seen (and the stuck age and cutoff) by 2 s."""
        p, c = Consumer("producer"), Consumer()
        ingest(p.send(NOW - 299 * SEC, "skew"),
               c.receive(NOW - 298 * SEC, "skew", "rh-k", sent_ms=(NOW - 301 * SEC) // 1_000_000))
        t = tl(store, "skew")
        assert (t["first_seen"], t["first_seen_clock"]) == (iso(NOW - 299 * SEC), "host")
        assert queries.stuck(store, NOW, LT, 300, 100) == []  # 299 s on the host's clock: not stuck yet
        s = queries.stuck(store, NOW, LT, 290, 100)
        assert [(x["message_id"], x["age_s"], x["age_across_clocks"]) for x in s] == [("skew", 299.0, False)]

    def test_threshold_and_queue_filter(self, store, ingest):
        self.setup(ingest)
        assert "recent" in [x["message_id"] for x in queries.stuck(store, NOW, LT, 30, 100)]
        assert [x["message_id"] for x in queries.stuck(store, NOW, LT, 300, 100, queue="payments")] == ["other-queue"]
        assert queries.stuck(store, NOW, LT, 7200, 100) == []
        assert len(queries.stuck(store, NOW, LT, 300, 2)) == 2


def test_duplicates_excludes_single_deliveries(store, ingest):
    c = Consumer()
    ingest(c.receive(T0, "once", "rh-1"), c.delete(T0 + SEC, "rh-1"),
           c.delete(T0 + 2 * SEC, "rh-1"))  # a retried delete of the same handle is one success
    assert queries.duplicates(store, NOW, LT, 100) == []
    assert tl(store, "once")["successful_deliveries"] == 1


@pytest.mark.parametrize("n", [3])
def test_duplicate_delivery_with_the_same_receive_count(store, ingest, n):
    """Standard queues deliver at least once: two consumers may get the same delivery number."""
    ingest(*[Consumer(f"c{i}").receive(T0 + i, "m1", f"rh-{i}") for i in range(n)])
    t = tl(store, "m1")
    assert t["deliveries"] == n and not any(e["inferred"] for e in t["events"])
    assert queries.duplicates(store, NOW, LT, 100)[0]["reasons"] == ["received_more_than_once"]


# -- inferred steps are never ordered against steps they were not inferred from (QV-29 report §3) --

def _redelivered(c, mid, *between):
    """Send, receive 1 at 20.002 s, the given spans, redelivery at 35.000 s, final delete."""
    return [c.send(T0 + 20 * SEC - 2 * MS, mid, start=T0 + 20 * SEC - 4 * MS),
            c.receive(T0 + 20 * SEC + 2 * MS, mid, "rh-1", start=T0 + 20 * SEC + MS),
            *between,
            c.receive(T0 + 35 * SEC, mid, "rh-2", count=2, start=T0 + 35 * SEC - MS),
            c.delete(T0 + 35 * SEC + 10 * MS, "rh-2", start=T0 + 35 * SEC + 5 * MS)]


def test_expiry_is_not_shown_after_a_rejected_delete_it_was_not_inferred_from(store, ingest):
    """The reviewer's case: visibility timeout 10 s, so the message expired at about 30.002 s; the
    delete of delivery 1 at 32.000 s was rejected because of that. The store cannot tell the expiry and
    the delete apart in time (it does not know the timeout), so they are one concurrent group."""
    c = Consumer()
    ingest(*_redelivered(c, "m1", c.delete(T0 + 32 * SEC, "rh-1", ok=False, start=T0 + 32 * SEC - 2 * MS)))
    t = tl(store, "m1")
    check_marking(t)
    assert groups(t) == [["sent"], ["received"], ["deleted", "visibility_timeout_expired"], ["received"],
                         ["deleted"]]
    g = t["groups"][2]
    assert (g["concurrent"], g["label"], g["observed_events"], g["inferred_events"]) == (
        True, "concurrent, order unknown", 1, 1)
    assert t["concurrent_groups"] == 1


def test_expiry_is_not_shown_after_an_unrelated_failed_visibility_change(store, ingest):
    c = Consumer()
    ingest(*_redelivered(c, "m1", c.change_visibility(T0 + 32 * SEC, "rh-1", 30, ok=False,
                                                     start=T0 + 32 * SEC - 2 * MS)))
    t = tl(store, "m1")
    assert groups(t) == [["sent"], ["received"], ["visibility_changed", "visibility_timeout_expired"],
                         ["received"], ["deleted"]]


def test_steps_in_several_groups_inside_the_window_are_merged_with_the_expiry(store, ingest):
    """A failed visibility change and a rejected delete, ordered against each other, both inside the
    expiry's window: showing them in order would put the expiry before or after one of them."""
    c = Consumer()
    ingest(*_redelivered(c, "m1",
                         c.change_visibility(T0 + 25 * SEC, "rh-1", 30, ok=False, start=T0 + 25 * SEC - MS),
                         c.delete(T0 + 32 * SEC, "rh-1", ok=False, start=T0 + 32 * SEC - MS)))
    t = tl(store, "m1")
    assert groups(t) == [["sent"], ["received"], ["visibility_changed", "deleted", "visibility_timeout_expired"],
                         ["received"], ["deleted"]]
    assert [e["group"] for e in t["events"]] == [0, 1, 2, 2, 2, 3, 4]
    assert len(t["groups"]) == 5


def test_an_accepted_visibility_change_before_the_redelivery_is_an_anchor_of_the_expiry(store, ingest):
    """A slow consumer extends the timeout twice and then stops: the expiry came after the last accepted
    extension, so it is placed after it, not merged with the extensions."""
    c = Consumer()
    ingest(*_redelivered(c, "m1",
                         c.change_visibility(T0 + 25 * SEC, "rh-1", 5, start=T0 + 25 * SEC - MS),
                         c.change_visibility(T0 + 29 * SEC, "rh-1", 5, start=T0 + 29 * SEC - MS)))
    t = tl(store, "m1")
    assert groups(t) == [["sent"], ["received"], ["visibility_changed"], ["visibility_changed"],
                         "visibility_timeout_expired", ["received"], ["deleted"]]
    expiry = next(e for e in t["events"] if e["type"] == "visibility_timeout_expired")
    assert expiry["window"]["from"] == iso(T0 + 29 * SEC - MS)
    assert expiry["reason"] == "The visibility timeout, last changed to 5 s, expired before the message was deleted."


def test_expiry_is_not_shown_after_a_send_that_overlaps_the_first_receive(store, ingest):
    """The long-poll case: the send may have ended after the expiry as far as the clocks show, so the
    expiry is in the send's group, not after it."""
    p, c = Consumer("producer"), Consumer()
    ingest(p.send(T0 + 30 * MS, "m1", start=T0),
           c.receive(T0 + 20 * MS, "m1", "rh-1", start=T0 - 900 * MS),
           c.receive(T0 + 40 * SEC, "m1", "rh-2", count=2, start=T0 + 39 * SEC))
    t = tl(store, "m1")
    assert groups(t) == [["received", "sent", "visibility_timeout_expired"], ["received"]]
    assert t["groups"][0]["inferred_events"] == 1


# -- order sources: each edge says which observed fact proves it (contract section 9a, QV-31) ------

def ev(t, type_, n=0):
    return [e for e in t["events"] if e["type"] == type_][n]


def edge(t, a, b):
    """The sources of the direct edge a -> b in the API's order.edges, or None."""
    return next((e["sources"] for e in t["order"]["edges"] if (e["from"], e["to"]) == (a["id"], b["id"])), None)


def shown_before(t, a, b):
    """a is proven before b through the API's edges (a chain is fine)."""
    succ: dict = {}
    for e in t["order"]["edges"]:
        succ.setdefault(e["from"], []).append(e["to"])
    todo, seen = [a["id"]], set()
    while todo:
        x = todo.pop()
        for y in succ.get(x, []):
            if y == b["id"]:
                return True
            if y not in seen:
                seen.add(y)
                todo.append(y)
    return False


def long_poll(c, mid, rh="rh-1", **kw):
    """A receive whose long poll started before the send (T0) and returned at 20 ms."""
    return c.receive(T0 + 20 * MS, mid, rh, start=T0 - 900 * MS, **kw)


def test_process_order_orders_a_delete_after_its_own_receive_when_the_clocks_tie(store, ingest):
    """The delete was issued in the same millisecond the receive returned: the clocks can't order them,
    the process's call counter can."""
    c = Consumer(scope="proc-1")
    ingest(c.receive(T0 + 20 * MS, "m1", "rh-1", start=T0, order=(1, 2)),
           c.delete(T0 + 25 * MS, "rh-1", start=T0 + 20 * MS, order=(3, 4)))
    t = tl(store, "m1")
    r, d = ev(t, "received"), ev(t, "deleted")
    assert edge(t, r, d) == ["process_order"]
    assert groups(t) == [["received"], ["deleted"]]
    assert d["after"] == [{"event": r["id"], "type": "received", "sources": ["process_order"],
                           "source": "process_order",
                           "why": "Same process: receive 1 returned before this call was issued."}]
    assert t["order"]["edges_by_source"] == {"clock": 0, "process_order": 1, "receive_count": 0, "sqs_timestamps": 0}


def test_process_order_is_never_inferred_across_processes(store, ingest):
    a, b = Consumer("a", scope="proc-a"), Consumer("b", scope="proc-b")
    ingest(a.receive(T0 + 20 * MS, "m1", "rh-1", start=T0, order=(1, 2)),
           b.delete(T0 + 25 * MS, "rh-1", start=T0 + 20 * MS, order=(3, 4)))
    t = tl(store, "m1")
    assert t["order"]["edges"] == [] and groups(t) == [["received", "deleted"]]


def test_process_order_needs_the_call_to_have_returned_before_the_other_was_issued(store, ingest):
    """Two calls in flight at once in one process (issued 1, 2; returned 3, 4): no order."""
    c = Consumer(scope="proc-1")
    ingest(c.receive(T0 + 20 * MS, "m1", "rh-1", start=T0, order=(1, 3)),
           c.delete(T0 + 25 * MS, "rh-1", start=T0 + 10 * MS, order=(2, 4)))
    assert tl(store, "m1")["order"]["edges"] == []


def test_receive_count_orders_deliveries_whose_intervals_overlap(store, ingest):
    a, b = Consumer("a"), Consumer("b")
    ingest(a.receive(T0 + 20 * MS, "m1", "rh-1", start=T0),
           b.receive(T0 + 30 * MS, "m1", "rh-2", count=2, start=T0 + 10 * MS))
    t = tl(store, "m1")
    r1, r2 = ev(t, "received"), ev(t, "received", 1)
    assert (r1["receive_count"], r2["receive_count"]) == (1, 2)
    assert edge(t, r1, r2) == ["receive_count"]
    assert r2["after"][0]["why"] == ("SQS's receive count shows receive 1 (receive count 1) came before this one "
                                    "(receive count 2).")
    # The expiry between them is placed by its anchors; nothing else is in between.
    assert groups(t) == [["received"], "visibility_timeout_expired", ["received"]]


def test_equal_receive_counts_prove_nothing(store, ingest):
    ingest(Consumer("a").receive(T0 + 20 * MS, "m1", "rh-1", start=T0),
           Consumer("b").receive(T0 + 30 * MS, "m1", "rh-2", start=T0 + 10 * MS))
    assert tl(store, "m1")["order"]["edges"] == []


def test_sqs_timestamps_order_the_send_before_the_first_receive(store, ingest):
    """The long-poll case: the clocks can't order the send and the receive, SQS's own clock can."""
    p, c = Consumer("producer"), Consumer()
    sent = T0 // MS
    ingest(p.send(T0 + 30 * MS, "m1", start=T0), long_poll(c, "m1", sent_ms=sent + 3, first_ms=sent + 5))
    t = tl(store, "m1")
    s, r = ev(t, "sent"), ev(t, "received")
    assert edge(t, s, r) == ["sqs_timestamps"]
    assert groups(t) == [["sent"], ["received"]]
    assert r["after"][0]["why"] == ("SQS's timestamps show this message was sent (2025-11-14T00:26:40.003000Z) "
                                    "before its first receive (2025-11-14T00:26:40.005000Z).")


def test_sqs_timestamps_in_the_same_millisecond_prove_nothing(store, ingest):
    p, c = Consumer("producer"), Consumer()
    sent = T0 // MS
    ingest(p.send(T0 + 30 * MS, "m1", start=T0), long_poll(c, "m1", sent_ms=sent + 3, first_ms=sent + 3))
    t = tl(store, "m1")
    assert t["order"]["edges"] == [] and groups(t) == [["received", "sent"]]


def test_sqs_timestamps_order_only_the_first_receive(store, ingest):
    """Receive count 2 carries the same first-receive timestamp; it says nothing about receive 2 itself."""
    p, c = Consumer("producer"), Consumer()
    sent = T0 // MS
    ingest(p.send(T0 + 30 * MS, "m1", start=T0),
           long_poll(c, "m1", "rh-2", count=2, sent_ms=sent + 3, first_ms=sent + 5))
    assert tl(store, "m1")["order"]["edges"] == []


def test_order_is_transitive_across_sources_and_the_chain_is_in_the_api(store, ingest):
    """send -(SQS's timestamps)-> receive 1 -(receive count)-> receive 2 -(process order)-> delete, with
    every pair overlapping on the clocks."""
    p, a, b = Consumer("producer"), Consumer("a"), Consumer("b", scope="proc-b")
    sent = T0 // MS
    ingest(p.send(T0 + 30 * MS, "m1", start=T0),
           long_poll(a, "m1", sent_ms=sent + 3, first_ms=sent + 5),
           b.receive(T0 + 30 * MS, "m1", "rh-2", count=2, start=T0 + 10 * MS, order=(1, 2)),
           b.delete(T0 + 50 * MS, "rh-2", start=T0 + 30 * MS, order=(3, 4)))
    t = tl(store, "m1")
    s, r1, r2, d = ev(t, "sent"), ev(t, "received"), ev(t, "received", 1), ev(t, "deleted")
    assert (edge(t, s, r1), edge(t, r1, r2), edge(t, r2, d)) == (["sqs_timestamps"], ["receive_count"],
                                                                 ["process_order"])
    assert edge(t, s, d) is None and shown_before(t, s, d)
    assert [e["type"] for e in t["events"]] == ["sent", "received", "visibility_timeout_expired", "received",
                                                "deleted"]
    assert t["concurrent_groups"] == 0


def test_sources_that_contradict_each_other_order_nothing(store, ingest):
    """The host clock jumped back 1 s between two calls of one process: the clocks put the delete first,
    the process's counter puts the receive first. Neither is shown; a notice says why."""
    c = Consumer(scope="proc-1")
    ingest(c.receive(T0 + 5 * SEC, "m1", "rh-1", start=T0 + 5 * SEC - MS, order=(1, 2)),
           c.delete(T0 + 4 * SEC, "rh-1", start=T0 + 4 * SEC - MS, order=(3, 4)))
    t = tl(store, "m1")
    assert t["order"]["edges"] == [] and groups(t) == [["deleted", "received"]]
    assert {(x["sources"][0]) for x in t["order"]["conflicts"]} == {"clock", "process_order"}
    assert "order_conflict" in [n["code"] for n in t["notices"]]


def test_spans_without_call_order_still_work(store, ingest):
    """Contract v2 spans carry no qv2.order.*: only the other sources apply."""
    c = Consumer()
    ingest(c.receive(T0 + 20 * MS, "m1", "rh-1", start=T0), c.delete(T0 + 25 * MS, "rh-1", start=T0 + 20 * MS))
    assert tl(store, "m1")["order"]["edges"] == []


def test_every_event_has_an_id_and_its_after_list_is_proven(store, ingest):
    c = Consumer(scope="p")
    ingest(c.send(T0 + 5, "m1", start=T0, order=(1, 2)), c.receive(T0 + SEC, "m1", "rh-1", start=T0 + 10, order=(3, 4)))
    t = tl(store, "m1")
    assert [e["id"] for e in t["events"]] == ["e1", "e2"]
    assert t["events"][1]["after"][0]["sources"] == ["clock", "process_order"]
    assert t["events"][1]["after"][0]["why"].startswith("The send ended (")


def test_qv29_lambda_retry_is_ordered_by_what_the_event_carries(store, ingest):
    """The QV-29 report's case, one "concurrent, order unknown" box of four before QV-31: send at 0 s,
    Lambda receive 1 by 1 s, failed outcome at 2 s, receive 2 by 40 s, success at 41 s. The records carry
    the receive count and SQS's timestamps; the wrapper records the call order in its process.

    Now: SQS's timestamps put the send before receive 1, the receive count puts receive 1 before receive 2,
    and each handler's outcome is after its own receive. What stays unknown is whether Lambda's poller
    received delivery 2 before or after handler 1 failed: the wrapper never sees that call, so the
    failure, the expiry and receive 2 are one concurrent box, and nothing else is."""
    p = Consumer("producer")
    fn = Consumer("2026/11/12/[$LATEST]env", source="lambda", scope="env-1")
    sent = T0 // MS + 2
    ingest(p.send(T0 + 10 * MS, "m1", start=T0),
           fn.receive(T0 + SEC, "m1", "rh-1", lambda_inv="inv-1", sent_ms=sent, first_ms=sent + 40, order=(None, 1)),
           fn.outcome(T0 + 2 * SEC, "m1", "failed_exception", "inv-1", error_type="RuntimeError", order=(2, 2)),
           fn.receive(T0 + 40 * SEC, "m1", "rh-2", count=2, lambda_inv="inv-2", sent_ms=sent, first_ms=sent + 40,
                      order=(None, 3)),
           fn.outcome(T0 + 41 * SEC, "m1", "success", "inv-2", order=(4, 4)))
    t = tl(store, "m1")
    check_marking(t)
    assert groups(t) == [["sent"], ["received"], ["lambda_outcome", "visibility_timeout_expired", "received"],
                         ["lambda_outcome"], "deleted_by_lambda"]
    s, r1, r2 = ev(t, "sent"), ev(t, "received"), ev(t, "received", 1)
    o1, o2 = ev(t, "lambda_outcome"), ev(t, "lambda_outcome", 1)
    assert edge(t, s, r1) == ["sqs_timestamps"]
    assert edge(t, r1, r2) == ["receive_count"]
    assert edge(t, r1, o1) == ["clock", "process_order"] and edge(t, r2, o2) == ["clock", "process_order"]
    assert not shown_before(t, o1, r2) and not shown_before(t, r2, o1)
    assert shown_before(t, o1, o2)  # the clocks: outcome 1 at 2 s, outcome 2 at 41 s
    assert t["concurrent_groups"] == 1
