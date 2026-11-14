"""Timelines, inferred steps, duplicate deliveries and stuck messages."""
import pytest

from qv2_store import queries
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


def test_settle_is_ordered_after_its_receive_despite_clock_skew(store, ingest):
    a, b = Consumer("a"), Consumer("b")
    ingest(a.receive(T0 + 5 * SEC, "m1", "rh-1"), b.delete(T0 + 4 * SEC, "rh-1"))  # b's clock is 1 s behind
    t = tl(store, "m1")
    assert [e["type"] for e in t["events"]] == ["received", "deleted"]
    assert t["events"][1]["time"] < t["events"][0]["time"]  # the real timestamps are kept


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
