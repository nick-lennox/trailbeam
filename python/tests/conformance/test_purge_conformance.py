"""PurgeQueue calls (contract section 18, revision 3.3), through the Store interface only: every backend stores
purge spans apart from messages, gives them back by queue, leaves purged messages off Stuck, and drops them by
their own time on a retention pass. What a purge means for a message is purge.py, the same for every backend."""
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


def put(s, *spans):
    d = decode(to_proto(list(spans)).SerializeToString(), "application/x-protobuf")
    assert d.rejected == []
    return s.insert(d.records)


def stuck_ids(s, queue=None):
    return [m["message_id"] for m in queries.stuck(s, NOW, LT, 0, 100, queue)]


def item(s, mid):
    return next(m for m in queries.recent(s, NOW, LT, 100) if m["message_id"] == mid)


P = Consumer("producer", service="orders-api")
C = Consumer()


def test_a_message_sent_before_a_purge_is_purged_not_stuck(new_store):
    s = new_store()
    put(s, P.send(T0, "gone"), P.send(T0, "other", queue="billing"),
        P.purge(T0 + 10 * SEC, start=T0 + 9 * SEC))
    assert stuck_ids(s) == ["other"]  # billing was not purged
    it = item(s, "gone")
    assert it["state"] == "purged"
    assert it["purge"]["status"] == "purged" and it["purge"]["queue"] == "orders"
    assert it["purge"]["start"] == "2025-11-14T00:26:49.000000Z"
    assert "purge" not in item(s, "other")
    assert s.counts()["spans"]["purge"] == 1
    assert s.recent_messages(100) and all(m in ("gone", "other") for m in s.recent_messages(100))


def test_the_purge_may_arrive_before_the_message(new_store):
    s = new_store()
    put(s, P.purge(T0 + 10 * SEC, start=T0 + 9 * SEC))
    put(s, P.send(T0, "gone"))
    assert stuck_ids(s) == [] and item(s, "gone")["state"] == "purged"


def test_a_message_in_the_purge_window_stays_stuck_as_possibly_purged(new_store):
    s = new_store()
    put(s, P.send(T0 + 9 * SEC + 1, "during"),  # ended after the purge started
        P.send(T0 + 70 * SEC, "edge"),           # ended exactly 60 s after the purge ended: still in the window
        P.send(T0 + 70 * SEC + 1, "after"),      # after the window: not affected
        P.purge(T0 + 10 * SEC, start=T0 + 9 * SEC))
    assert stuck_ids(s) == ["during", "edge", "after"]
    assert item(s, "during")["state"] == "not_deleted"
    assert item(s, "during")["purge"]["status"] == "possibly_purged"
    assert item(s, "edge")["purge"]["window_end"] == "2025-11-14T00:27:50.000000Z"
    assert "purge" not in item(s, "after")


def test_a_receive_after_the_purge_means_it_was_not_purged_by_it(new_store):
    s = new_store()
    put(s, P.send(T0, "kept"), P.purge(T0 + 10 * SEC, start=T0 + 9 * SEC),
        C.receive(T0 + 200 * SEC, "kept", "rh-1"))
    assert stuck_ids(s) == ["kept"] and "purge" not in item(s, "kept")


def test_a_failed_purge_is_stored_and_changes_no_message(new_store):
    s = new_store()
    put(s, P.send(T0, "m"), P.purge(T0 + 10 * SEC, start=T0 + 9 * SEC, ok=False))
    assert stuck_ids(s) == ["m"] and "purge" not in item(s, "m")
    [p] = s.purges(["orders"])
    assert (p["result"], p["error_code"], p["queue"]) == ("failed", "PurgeQueueInProgress", "orders")


def test_a_deleted_message_is_not_purged(new_store):
    s = new_store()
    put(s, P.send(T0, "done"), C.receive(T0 + SEC, "done", "rh-1"), C.delete(T0 + 2 * SEC, "rh-1"),
        P.purge(T0 + 10 * SEC, start=T0 + 9 * SEC))
    assert item(s, "done")["state"] == "deleted" and "purge" not in item(s, "done")


def test_the_earliest_purge_that_removed_it_is_shown(new_store):
    s = new_store()
    put(s, P.send(T0, "m"), P.purge(T0 + 100 * SEC, start=T0 + 99 * SEC),
        P.purge(T0 + 10 * SEC, start=T0 + 9 * SEC))
    assert item(s, "m")["purge"]["start"] == "2025-11-14T00:26:49.000000Z"


def test_a_dead_lettered_message_is_judged_on_the_dead_letter_queue(new_store):
    s = new_store()
    dlq = Consumer("dlq-worker", queue="orders-dlq")
    put(s, P.send(T0, "m"), C.receive(T0 + SEC, "m", "rh-1"),
        dlq.receive(T0 + 50 * SEC, "m", "rh-2", count=2, dlq_source="orders"),
        P.purge(T0 + 100 * SEC, start=T0 + 99 * SEC))  # the source queue: the message has left it
    assert stuck_ids(s) == ["m"]
    put(s, P.purge(T0 + 101 * SEC, queue="orders-dlq", start=T0 + 100 * SEC))
    assert stuck_ids(s) == [] and item(s, "m")["purge"]["queue"] == "orders-dlq"


def test_purges_are_read_by_queue(new_store):
    s = new_store()
    put(s, P.purge(T0 + 10 * SEC, start=T0 + 9 * SEC), P.purge(T0 + 20 * SEC, queue="billing", start=T0 + 19 * SEC))
    assert [p["queue"] for p in s.purges(["orders"])] == ["orders"]
    assert sorted(p["queue"] for p in s.purges(["orders", "billing", "nope"])) == ["billing", "orders"]
    assert s.purges([]) == []
    p = s.purges(["orders"])[0]
    assert (p["start_ns"], p["time_ns"], p["result"]) == (T0 + 9 * SEC, T0 + 10 * SEC, "ok")
    assert s.recent_messages(100) == []  # a purge is not a message
    assert s.counts()["messages"] == 0


US = "https://sqs.us-east-1.amazonaws.com/000000000000/orders"
EU = "https://sqs.eu-west-1.amazonaws.com/123456789012/orders"


def at(span, url):
    span.attrs["aws.sqs.queue.url"] = url
    return span


def test_a_purge_of_a_same_named_queue_in_another_account_or_region_purges_nothing_here(new_store):
    # QV-725: purges were matched by queue name, so this purge marked x1 purged
    s = new_store()
    put(s, at(P.send(T0, "x1"), US), at(P.purge(T0 + 5 * SEC, start=T0 + 5 * SEC), EU))
    assert stuck_ids(s) == ["x1"] and stuck_ids(s, "orders") == ["x1"]
    assert item(s, "x1")["state"] == "not_deleted" and "purge" not in item(s, "x1")
    t = queries.message(s, "x1", NOW, LT)
    assert t["state"] == "not_deleted" and "queue_purged" not in [e["type"] for e in t["events"]]
    # the same queue's purge does purge it
    put(s, at(P.purge(T0 + 10 * SEC, start=T0 + 9 * SEC), US))
    assert stuck_ids(s) == [] and item(s, "x1")["state"] == "purged"
    assert queries.message(s, "x1", NOW, LT)["purge"]["start"] == "2025-11-14T00:26:49.000000Z"


def test_a_purge_whose_key_says_less_is_the_one_queue_it_fits(new_store):
    # an emulator's URL names no region: 000000000000//orders is 000000000000/us-east-1/orders when that is the only
    # queue it fits (queues.resolve's rule), and no queue's when two fit
    s = new_store()
    put(s, at(P.send(T0, "x1"), US), P.purge(T0 + 5 * SEC, start=T0 + 5 * SEC))
    assert stuck_ids(s) == [] and item(s, "x1")["state"] == "purged"
    s = new_store(name="b")
    put(s, at(P.send(T0, "x1"), US), at(P.send(T0, "x2"), US.replace("us-east-1", "eu-west-1")),
        P.purge(T0 + 5 * SEC, start=T0 + 5 * SEC))
    assert stuck_ids(s) == ["x1", "x2"] and item(s, "x1")["state"] == "not_deleted"


def test_an_exporter_retry_of_a_purge_changes_nothing(new_store):
    s = new_store()
    sp = P.purge(T0 + 10 * SEC, start=T0 + 9 * SEC)
    assert put(s, sp) == (1, 0)
    assert put(s, sp) == (0, 1)
    assert len(s.purges(["orders"])) == 1


def test_retention_drops_purges_by_their_own_time(new_store):
    s = new_store()
    put(s, P.purge(T0 + 10 * SEC, start=T0 + 9 * SEC), P.send(T0 + 500 * SEC, "late"))
    deleted, resume = 0, None
    while True:
        step = s.purge_step(Cutoffs(FOREVER), resume, 500)
        deleted, resume = deleted + step.deleted, step.resume
        if resume is None:
            break
    assert s.purges(["orders"]) == [] and s.counts()["spans"] == {}


def test_the_timeline_shows_the_purge_as_a_step_after_the_message(new_store):
    s = new_store()
    put(s, P.send(T0, "gone"), P.purge(T0 + 10 * SEC, start=T0 + 9 * SEC))
    t = queries.message(s, "gone", NOW, LT)
    assert t["state"] == "purged" and t["purge"]["status"] == "purged"
    assert [e["type"] for e in t["events"]] == ["sent", "queue_purged"]
    ev = t["events"][1]
    assert (ev["inferred"], ev["source"], ev["removal"], ev["result"]) == (False, "purge_queue", "per_sqs_documentation", "ok")
    assert [a["source"] for a in ev["after"]] == ["clock"]
    assert t["last_seen"] == "2025-11-14T00:26:40.000000Z"  # the purge is the queue's step, not the message's


@pytest.mark.parametrize("ok", [True, False])
def test_a_purge_span_without_a_result_is_rejected(ok):
    sp = P.purge(T0, start=T0 - SEC, ok=ok)
    del sp.attrs["trailbeam.result"]
    d = decode(to_proto([sp]).SerializeToString(), "application/x-protobuf")
    assert d.records == [] and d.rejected == ["purge span without result"]
