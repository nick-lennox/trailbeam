"""Timelines, inferred steps, duplicate deliveries and stuck messages."""
import pytest

from qv2_store import queries
from qv2_store.timeline import iso
from spans import KEY, SEC, T0, Consumer

NOW = T0 + 3600 * SEC
LT = 900


def tl(store, mid, now=NOW, lambda_timeout=LT):
    return queries.message(store, mid, now, lambda_timeout)


def types(t):
    return [(e["type"], e["inferred"]) for e in t["events"]]


def check_marking(t):
    """Inferred steps are never shown as observed: no time, a window instead, and a reason."""
    for e in t["events"]:
        if e["inferred"]:
            assert e["time"] is None and "window" in e and e["reason"] and "span" not in e
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


def test_move_to_dead_letter_queue_is_inferred(store, ingest):
    """Poison message: three receives on orders, then it shows up on orders-dlq (same message ID)."""
    c, d = Consumer(), Consumer("dlq-worker", queue="orders-dlq")
    ingest(Consumer("producer").send(T0, "m1"),
           *[c.receive(T0 + (1 + 31 * i) * SEC, "m1", f"rh-{i}", count=i + 1) for i in range(3)],
           d.receive(T0 + 200 * SEC, "m1", "rh-dlq", count=4),
           d.delete(T0 + 201 * SEC, "rh-dlq"))
    t = tl(store, "m1")
    check_marking(t)
    assert types(t) == [("sent", False), ("received", False), ("visibility_timeout_expired", True),
                        ("received", False), ("visibility_timeout_expired", True), ("received", False),
                        ("moved_to_dead_letter_queue", True), ("received", False), ("deleted", False)]
    move = t["events"][6]
    assert (move["from_queue"], move["to_queue"]) == ("orders", "orders-dlq")
    assert move["window"] == {"from": t["events"][5]["time"], "to": t["events"][7]["time"]}
    assert t["events"][8]["status"] == "final"
    assert (t["state"], t["source_queue"], t["current_queue"], t["in_dead_letter_queue"]) == \
        ("deleted", "orders", "orders-dlq", True)


def test_message_waiting_in_dead_letter_queue_is_stuck(store, ingest):
    c, d = Consumer(), Consumer("dlq-peek", queue="orders-dlq")
    ingest(c.send(T0, "m1"), c.receive(T0 + SEC, "m1", "rh-1"), d.receive(T0 + 100 * SEC, "m1", "rh-2", count=2))
    s = queries.stuck(store, NOW, LT, 300, 100)
    assert [(x["message_id"], x["current_queue"], x["in_dead_letter_queue"]) for x in s] == [("m1", "orders-dlq", True)]


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
                              "observed_events": 2, "inferred_events": 0,
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
