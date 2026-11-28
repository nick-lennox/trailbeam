"""GET /api/queues/{queue}/now (QV-579): "In ‹queue› now" from spans alone, the hosted InQueueView without SQS
totals (queue_now.py)."""
import os
import sys
from pathlib import Path

import pytest

from spans import SEC, T0, Consumer, to_proto
from trailbeam.server.otlp import decode
from trailbeam.server.server import ApiError, App

MS = T0 // 1_000_000


def now_of(db, at_ns, path="queues/orders/now", mrc=None, **q):
    app = App(db, now=lambda: at_ns, max_receive_counts=mrc if mrc is not None else {"orders": 3})
    return app.api(path, {k: str(v) for k, v in q.items()})


def by_id(d):
    return {m["id"]: m for m in d["held"]}


def test_a_sent_message_is_visible_and_a_received_one_in_flight(db, ingest):
    api, worker = Consumer("api", service="orders-api"), Consumer("w")
    ingest(api.send(T0, "m1"), api.send(T0, "m2"), service="orders-api")
    ingest(worker.receive(T0 + 2 * SEC, "m2", "h2"))
    d = now_of(db, T0 + 12 * SEC)
    assert d["queue"]["name"] == "orders"
    m = by_id(d)
    assert m["m1"]["state"] == "visible" and m["m1"]["receiveCount"] == 0 and m["m1"]["consumer"] == ""
    assert m["m1"]["producer"] == "orders-api" and m["m1"]["sentMs"] == MS
    two = m["m2"]
    assert two["state"] == "in_flight" and two["receiveCount"] == 1 and two["consumer"] == "orders-worker"
    # SQS's default 30 s timeout from the receive at 2 s: 20 s left at 12 s, a third used
    assert two["timeoutS"] == 30 and two["untilMs"] == MS + 32_000 and two["leftS"] == 20
    assert two["used"] == pytest.approx(1 / 3) and "lastError" not in two
    assert d["instrumented"] == {"visible": 1, "inFlight": 1}
    assert d["totals"] is None and d["uninstrumented"] is None
    assert d["config"]["visibilityKnown"] is False and d["config"]["maxReceiveCount"] == 3
    assert d["producers"] == [{"name": "orders-api", "n": 2}]
    assert sorted(e["event"] for e in two["events"]) == ["receive", "send"]


def test_a_deleted_message_is_not_in_the_queue_and_counts_as_gone_in_the_past_minute(db, ingest):
    w = Consumer("w")
    ingest(w.send(T0, "m1"), w.receive(T0 + SEC, "m1", "h1"), w.delete(T0 + 2 * SEC, "h1"))
    d = now_of(db, T0 + 30 * SEC)
    assert d["held"] == [] and d["deleted"] == [{"id": "m1", "atMs": MS + 2000}]
    assert now_of(db, T0 + 90 * SEC)["deleted"] == []  # more than a minute ago


def test_after_its_timeout_a_message_is_visible_again_and_says_why(db, ingest):
    w = Consumer("w")
    ingest(w.send(T0, "m1"), w.receive(T0 + SEC, "m1", "h1"))
    [m] = now_of(db, T0 + 40 * SEC)["held"]
    assert m["state"] == "visible" and m["untilMs"] is None and m["timeoutS"] == 30
    assert m["lastError"] == ("Inferred: receive 1 was not deleted before its visibility timeout ran out (no delete of "
                              "it was stored; the timeout used is SQS's default 30 s, as the queue's setting was not read)")


def test_a_visibility_change_on_the_latest_receive_sets_its_timer(db, ingest):
    w = Consumer("w")
    ingest(w.send(T0, "m1"), w.receive(T0 + SEC, "m1", "h1"), w.change_visibility(T0 + 5 * SEC, "h1", 120))
    [m] = now_of(db, T0 + 65 * SEC)["held"]
    assert m["state"] == "in_flight" and m["timeoutS"] == 120 and m["untilMs"] == MS + 125_000 and m["leftS"] == 60


def test_a_message_at_max_receive_count_whose_timeout_ran_out_is_not_seen_on_the_dlq_yet(db, ingest):
    w = Consumer("w")
    ingest(w.send(T0, "m1"), *[w.receive(T0 + (1 + 40 * i) * SEC, "m1", f"h{i}", count=i + 1) for i in range(3)])
    d = now_of(db, T0 + 200 * SEC)
    assert d["held"] == []
    [n] = d["notSeenOnDlq"]
    assert n["id"] == "m1" and n["receiveCount"] == 3 and n["timedOutMs"] == MS + 111_000
    # with no maxReceiveCount configured it stays in the queue, visible
    assert [m["state"] for m in now_of(db, T0 + 200 * SEC, mrc={})["held"]] == ["visible"]


def test_a_message_moved_to_its_dlq_has_left_and_the_move_is_counted(db, ingest):
    w = Consumer("w")
    ingest(w.send(T0, "m1"), w.receive(T0 + SEC, "m1", "h1"),
           w.receive(T0 + 50 * SEC, "m1", "h2", count=1, queue="orders-dlq", dlq_source="orders"))
    d = now_of(db, T0 + 60 * SEC)
    assert d["held"] == [] and d["seenOnDlq"] == 1
    [m] = now_of(db, T0 + 60 * SEC, path="queues/orders-dlq/now")["held"]
    assert m["id"] == "m1" and m["state"] == "in_flight"


def test_a_failed_lambda_invocation_is_the_last_error(db, ingest):
    w = Consumer("w")
    ingest(w.send(T0, "m1"), w.receive(T0 + SEC, "m1", "h1", lambda_inv="inv-1"),
           w.outcome(T0 + 2 * SEC, "m1", "failed_exception", "inv-1", error_type="ValueError"))
    [m] = now_of(db, T0 + 5 * SEC)["held"]
    assert m["state"] == "in_flight" and m["lastError"].startswith("The handler threw")


def test_a_queue_of_the_same_name_elsewhere_is_another_queue(db, ingest):
    a = Consumer("a")
    ingest(a.send(T0, "x1"))
    b = Consumer("b")
    s = b.send(T0, "y1")
    s.attrs["aws.sqs.queue.url"] = "https://sqs.eu-west-1.amazonaws.com/123456789012/orders"
    ingest(s)
    d = now_of(db, T0 + SEC, path="queues/123456789012%2Feu-west-1%2Forders/now")
    assert [m["id"] for m in d["held"]] == ["y1"]
    with pytest.raises(ApiError) as e:
        now_of(db, T0 + SEC)
    assert e.value.status == 400


ELSEWHERE = "https://sqs.eu-west-1.amazonaws.com/123456789012/orders"


def _same_name_elsewhere(ingest_into):
    """A message sent to 000000000000//orders, and one sent, received and deleted on 123456789012/eu-west-1/orders."""
    a, b = Consumer("a"), Consumer("b")
    ingest_into([a.send(T0, "x1")])
    spans = [b.send(T0, "y1"), b.receive(T0 + SEC, "y1", "hy"), b.delete(T0 + 2 * SEC, "hy")]
    for s in spans:
        s.attrs["aws.sqs.queue.url"] = ELSEWHERE
    ingest_into(spans)


def test_a_delete_on_a_queue_of_the_same_name_elsewhere_has_not_left_this_one(db, ingest):
    _same_name_elsewhere(lambda spans: ingest(*spans))
    d = now_of(db, T0 + 10 * SEC, path="queues/000000000000%2F%2Forders/now")
    assert [m["id"] for m in d["held"]] == ["x1"] and d["deleted"] == []
    there = now_of(db, T0 + 10 * SEC, path="queues/123456789012%2Feu-west-1%2Forders/now")
    assert there["held"] == [] and there["deleted"] == [{"id": "y1", "atMs": MS + 2000}]


def test_the_other_queues_steps_do_not_take_this_ones_places_in_the_limit(db, ingest):
    from trailbeam.server import queue_now
    here, there = Consumer("a"), Consumer("b")
    mine = [s for i in range(2) for s in (here.receive(T0 + i * SEC, f"x{i}", f"hx{i}"),
                                          here.delete(T0 + i * SEC + 1, f"hx{i}"))]
    theirs = [s for i in range(5) for s in (there.receive(T0 + 10 * SEC + i, f"y{i}", f"hy{i}"),
                                            there.delete(T0 + 10 * SEC + i + 1, f"hy{i}"))]
    for s in theirs:
        s.attrs["aws.sqs.queue.url"] = ELSEWHERE
    ingest(*mine, *theirs)
    # newest first, the other queue's five deletes come before this one's two: a page of 2 holds none of them
    got = queue_now._steps_of(db, {"000000000000//orders"}, "orders", T0, T0 + 60 * SEC, limit=2)
    assert [r["time_ns"] for r in got] == [T0 + SEC + 1, T0 + 1]


def test_the_other_queues_messages_do_not_cap_this_ones_list(db, ingest):
    here, there = Consumer("a"), Consumer("b")
    theirs = [there.send(T0 + i, f"y{i}") for i in range(5)]  # older than this queue's, so read first
    for s in theirs:
        s.attrs["aws.sqs.queue.url"] = ELSEWHERE
    ingest(*theirs, *[here.send(T0 + SEC + i, f"x{i}") for i in range(2)])
    d = now_of(db, T0 + 10 * SEC, limit=2, path="queues/000000000000%2F%2Forders/now")
    assert [m["id"] for m in d["held"]] == ["x0", "x1"] and d["capped"] is False
    there_now = now_of(db, T0 + 10 * SEC, limit=2, path="queues/123456789012%2Feu-west-1%2Forders/now")
    assert [m["id"] for m in there_now["held"]] == ["y0", "y1"] and there_now["capped"] is True


def test_the_list_is_capped(db, ingest):
    w = Consumer("w")
    ingest(*[w.send(T0 + i * SEC, f"m{i}") for i in range(5)])
    d = now_of(db, T0 + 10 * SEC, limit=3)
    assert [m["id"] for m in d["held"]] == ["m0", "m1", "m2"] and d["capped"] is True
    assert now_of(db, T0 + 10 * SEC)["capped"] is False


def test_a_message_older_than_sqs_keeps_any_has_left(db, ingest):
    w = Consumer("w")
    ingest(w.send(T0, "m1"))
    assert now_of(db, T0 + (14 * 86_400 + 1) * SEC)["held"] == []


def _scenario(ingest_into):
    api, w = Consumer("api", service="orders-api"), Consumer("w")
    ingest_into([api.send(T0, "m1"), api.send(T0, "m2"), api.send(T0, "m3")], "orders-api")
    ingest_into([w.receive(T0 + SEC, "m2", "h2"), w.change_visibility(T0 + 2 * SEC, "h2", 90),
                 w.receive(T0 + SEC, "m3", "h3"), w.delete(T0 + 3 * SEC, "h3"),
                 *[w.receive(T0 + (1 + 40 * i) * SEC, "m4", f"h4-{i}", count=i + 1) for i in range(3)]],
                "orders-worker")


@pytest.mark.skipif(not os.environ.get("TRAILBEAM_TEST_POSTGRES_URL"), reason="TRAILBEAM_TEST_POSTGRES_URL is not set")
def test_a_postgres_store_answers_as_sqlite_does(db):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "conformance"))
    from pg_helpers import postgres_store
    pg = postgres_store()
    try:
        for store in (db, pg):
            def into(spans, service, store=store):
                store.insert(decode(to_proto(spans, service=service).SerializeToString(), "application/x-protobuf").records)
            _scenario(into)
        at = T0 + 150 * SEC
        assert now_of(pg, at) == now_of(db, at)
        assert [m["id"] for m in now_of(db, at)["held"]] == ["m1", "m2"]
    finally:
        pg.close()


@pytest.mark.skipif(not os.environ.get("TRAILBEAM_TEST_POSTGRES_URL"), reason="TRAILBEAM_TEST_POSTGRES_URL is not set")
def test_a_postgres_store_keeps_a_same_named_queue_apart_as_sqlite_does(db):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "conformance"))
    from pg_helpers import postgres_store
    pg = postgres_store()
    try:
        for store in (db, pg):
            def into(spans, store=store):
                store.insert(decode(to_proto(spans, service="orders").SerializeToString(), "application/x-protobuf").records)
            _same_name_elsewhere(into)
        for path in ("queues/000000000000%2F%2Forders/now", "queues/123456789012%2Feu-west-1%2Forders/now"):
            assert now_of(pg, T0 + 10 * SEC, path=path) == now_of(db, T0 + 10 * SEC, path=path)
        assert now_of(pg, T0 + 10 * SEC, path="queues/000000000000%2F%2Forders/now")["deleted"] == []
    finally:
        pg.close()
