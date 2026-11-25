"""The timeline, summary, Duplicates and Stuck of RabbitMQ messages (contract/span-contract-amqp.md A5 to A10;
QV-307), on the scenarios of the contract's fixture (contract/fixtures/amqp-4.0-spans.json): the cause of each
return to the queue, the dead-letter move from x-death with its reason, the routed and unroutable steps, the
requeue loop warning, a publish made twice, a copy per queue, and x-delivery-limit. SQS wording never shows."""
import json
import sys
from pathlib import Path

import pytest

from trailbeam.server import queries, summary, systems, timeline
from trailbeam.server.db import SqliteStore

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "conformance"))
from test_amqp_conformance import FIXTURE, load  # noqa: E402

LT = 900
NOW = max(sp["end_time_unix_nano"] for sp in FIXTURE["spans"]) + 10 ** 9
SQS_WORDS = ("SQS", "visibility", "ReceiveMessage", "DeleteMessage", "maxReceiveCount", "receipt")


@pytest.fixture(scope="module")
def store():
    s = SqliteStore()
    load(s)
    yield s
    s.close()


def tl(store, mid, limits=None):
    t = queries.message(store, systems.message_key(systems.RABBITMQ, mid), NOW, LT, limits)
    assert t is not None
    return t


def types(t, inferred=None):
    return [e["type"] for e in t["events"] if inferred is None or e["inferred"] is inferred]


def one(t, type_):
    evs = [e for e in t["events"] if e["type"] == type_]
    assert len(evs) == 1, (type_, types(t))
    return evs[0]


def _words(t) -> str:
    out = []
    for e in t["events"]:
        out += [e.get("reason", "")] + [x["why"] for x in e.get("after", [])]
        if "before" in e:
            out.append(e["before"]["why"])
    out += [n["message"] for n in t["notices"]]
    return " ".join(out)


@pytest.mark.parametrize("mid", sorted({sp["attributes"].get("messaging.message.id") for sp in FIXTURE["spans"]}
                                       - {None}))
def test_no_sqs_wording_on_a_rabbitmq_timeline(store, mid):
    t = tl(store, mid, {"orders": 2})
    text = _words(t)
    assert not [w for w in SQS_WORDS if w in text], text
    assert t["system"] == "rabbitmq"
    assert "visibility_timeout_expired" not in types(t)


def test_happy_publish_routed_delivered_acked(store):
    t = tl(store, "m-happy")
    assert types(t) == ["sent", "routed", "received", "acked"]
    assert t["state"] == "deleted" and one(t, "acked")["status"] == "final"
    r = one(t, "routed")
    assert r["queues"] == ["orders"] and r["exchange"] == "amq.default" and r["lower_bound"] is True
    assert one(t, "received")["receive_count_source"] == "first_delivery"


def test_a_requeue_is_the_observed_cause_of_the_return(store):
    t = tl(store, "m-requeue")
    assert types(t) == ["sent", "routed", "received", "requeued", "returned_to_queue", "received", "acked"]
    back = one(t, "returned_to_queue")
    assert back["cause"] == "requeued" and back["cause_observed"] is True and back["inferred"] is True
    assert "basic.nack (requeue = true)" in back["reason"]
    assert back["after"][0]["type"] == "requeued"  # placed after the requeue call, which proved it came first
    assert t["state"] == "deleted" and t["notices"] == []


def test_a_nack_loop_warns_and_never_reaches_the_delivery_limit(store):
    t = tl(store, "m-loop", {"orders": 2})
    assert types(t).count("returned_to_queue") == 5
    loop = [n for n in t["notices"] if n["code"] == "requeue_loop"]
    assert len(loop) == 1 and loop[0]["requeues"] == 5 and loop[0]["queue"] == "orders"
    assert "does not count toward x-delivery-limit" in loop[0]["message"]
    assert t["dead_letter_queue_move"] is None  # nacks don't raise x-delivery-count


def test_a_crash_loop_is_channel_closed_then_moved_by_delivery_limit(store):
    t = tl(store, "m-crash", {"orders": 2})
    backs = [e for e in t["events"] if e["type"] == "returned_to_queue"]
    assert [b["cause"] for b in backs] == ["channel_closed", "channel_closed"]
    assert "connection was lost" in backs[0]["reason"]
    assert backs[0]["after"][0]["type"] == "received"  # a close is when the client saw it: placed after the delivery
    move = one(t, "moved_to_dead_letter_queue")
    assert move["source"] == "amqp_x_death" and move["inferred"] is False
    assert (move["from_queue"], move["to_queue"], move["dead_letter_reason"]) == ("orders", "orders.dlq", "delivery_limit")
    assert "x-delivery-limit" in move["reason"] and "x-death" in move["before"]["why"]
    assert t["dead_letter_queue_move"] == "observed" and t["in_dead_letter_queue"] is True
    assert t["state"] == "deleted"
    # the dead-letter queue's first delivery has count 4 there: not read as 3 deliveries not observed (A7)
    assert "deliveries_not_observed" not in types(t)
    assert one(t, "routed")["queues"] == ["orders"]  # the dead-letter queue is not routed to


def test_rejected_to_the_dlq_with_x_death_reason_and_time(store):
    t = tl(store, "m-reject")
    rej = one(t, "rejected")
    assert rej["requeue"] is False and "dead-letters" in rej["reason"] and "not been seen since" not in rej["reason"]
    move = one(t, "moved_to_dead_letter_queue")
    assert move["dead_letter_reason"] == "rejected" and "first_dead_lettered" in move
    assert "requeue = false" in move["reason"]
    assert t["state"] == "deleted"


def test_fanout_is_two_copies_not_a_duplicate(store):
    t = tl(store, "m-fan")
    r = one(t, "routed")
    assert r["count"] == 2 and r["queues"] == ["fan.a", "fan.b"] and r["exchange"] == "fan"
    assert [e["status"] for e in t["events"] if e["type"] == "acked"] == ["final", "final"]
    assert t["state"] == "deleted" and [c["ended"] for c in t["copies"]] == ["acked", "acked"]
    s = summary.summarize(store.message_rows(t["message_id"]))
    assert not s["dup_received"] and not s["dup_succeeded"] and not s["stuck_open"]


def test_a_publisher_retry_is_published_twice_and_a_duplicate(store):
    t = tl(store, "m-retry")
    sends = [e for e in t["events"] if e["type"] == "sent"]
    assert [e["publish"] for e in sends] == [1, 2] and sends[1]["published_again"] is True
    assert "returned_to_queue" not in types(t)  # two first deliveries: two messages, not a redelivery
    assert [n["code"] for n in t["notices"]] == ["published_twice"]
    assert t["publishes"] == 2
    s = summary.summarize(store.message_rows(t["message_id"]))
    assert s["dup_received"] and s["dup_succeeded"]  # the work was done twice


def test_unroutable_is_a_returned_step_and_not_stuck(store):
    t = tl(store, "m-noroute")
    assert types(t) == ["returned", "sent"] or types(t) == ["sent", "returned"]
    ret = one(t, "returned")
    assert ret["reply_code"] == 312 and "Unroutable" in ret["reason"] and ret["exchange"] == "amq.default"
    s = summary.summarize(store.message_rows(t["message_id"]))
    assert not s["stuck_open"] and t["state"] == "not_deleted"


def test_a_classic_redelivery_with_no_count_is_a_return(store):
    t = tl(store, "m-classic")
    rec = [e for e in t["events"] if e["type"] == "received"]
    assert rec[1]["receive_count"] is None and rec[1]["redelivered"] is True
    assert one(t, "returned_to_queue")["cause"] == "requeued"


def test_a_broker_cancel_is_the_cause(store):
    t = tl(store, "m-timeout")
    back = one(t, "returned_to_queue")
    assert back["cause"] == "consumer_cancelled" and "consumer_timeout" in back["reason"]


def test_a_store_without_cancels_says_not_observed():
    class NoCancels(SqliteStore):
        def consumer_cancels(self, queues, from_ns, to_ns):
            return []
    s = NoCancels()
    load(s, {"consumer-timeout"})
    t = tl(s, "m-timeout")
    assert one(t, "returned_to_queue")["cause"] == "not_observed"
    assert one(t, "returned_to_queue")["cause_observed"] is False
    s.close()


def test_stuck_is_a_copy_never_settled(store):
    t = tl(store, "m-stuck")
    assert t["state"] == "not_deleted" and [c["ended"] for c in t["copies"]] == [None]
    assert summary.summarize(store.message_rows(t["message_id"]))["stuck_open"]


@pytest.mark.parametrize("scenario", sorted(FIXTURE["scenarios"]))
def test_the_summary_says_what_the_timeline_says(store, scenario):
    mids = {sp["attributes"].get("messaging.message.id") for sp in FIXTURE["spans"] if sp["scenario"] == scenario}
    for mid in mids - {None}:
        t = tl(store, mid, {"orders": 2})
        s = summary.summarize(store.message_rows(t["message_id"]))
        it = summary.item(s, NOW, LT, {"orders": 2})
        for k in ("state", "source_queue", "current_queue", "in_dead_letter_queue", "dead_letter_queue_move",
                  "deliveries", "successful_deliveries"):
            assert it[k] == t[k], (mid, k)


def _with_limit_reached():
    """The crash loop on its source queue only: three deliveries whose channels close (the third with
    x-delivery-count 2, so its close makes 3), and no dead-letter delivery yet."""
    return [sp for sp in FIXTURE["spans"] if sp["scenario"] == "crash-loop"
            and sp["attributes"]["messaging.destination.name"] == "orders"]


def test_x_delivery_limit_passed_and_not_seen_on_the_dlq_yet():
    from test_amqp_conformance import otlp_json
    from trailbeam.server.otlp import decode
    s = SqliteStore()
    s.insert(decode(otlp_json(_with_limit_reached()), "application/json").records)
    t = tl(s, "m-crash", {"orders": 2})
    assert t["dead_letter_queue_move"] == "not_seen_on_dlq_yet"
    n = [x for x in t["notices"] if x["code"] == "dlq_move_not_seen"][0]
    assert "x-delivery-limit is 2" in n["message"] and "3 time(s)" in n["message"]
    assert tl(s, "m-crash", {"orders": 3})["dead_letter_queue_move"] is None
    it = summary.item(summary.summarize(s.message_rows(t["message_id"])), NOW, LT, {"orders": 2})
    assert it["dead_letter_queue_move"] == "not_seen_on_dlq_yet"
    s.close()


def test_receive_counts_from_different_headers_order_nothing():
    """A9: x-acquired-count 3 and x-delivery-count 1 say nothing about each other."""
    from trailbeam.server import order
    a = {"kind": "receive", "queue": "q", "receive_count": 4, "count_source": "x-acquired-count", "start_ns": None,
         "time_ns": 2, "order_scope": None, "order_returned": None, "order_issued": None}
    b = dict(a, receive_count=2, count_source="x-delivery-count", time_ns=1)
    assert "receive_count" not in order._edge_sources(b, a)
    c = dict(a, receive_count=1, count_source="first_delivery", time_ns=0)
    assert "receive_count" in order._edge_sources(c, a)



# ---- the web screen and the queue page's events (QV-307 part 2) ---------------------------------------------

PAGE_SQS_WORDS = ("SQS", "visibility", "Visibility", "ReceiveMessage", "DeleteMessage", "maxReceiveCount",
                  "receipt", "DeadLetterQueueSourceArn")


def _page(store, mid):
    import re
    from html import unescape
    from trailbeam.server import web

    def api(path, params):
        assert path.startswith("messages/")
        return queries.message(store, systems.message_key(systems.RABBITMQ, mid), NOW, LT, {"orders": 2})
    status, _, body = web.handle(api, Exception, "/message", {"id": mid})
    assert status == 200
    html = body.decode()
    main = html[html.index("<main"):html.index("</main>")]
    main = main[:main.index('<p class="muted">The message body')]  # the body link names the API path
    return html, re.sub(r"\s+", " ", unescape(re.sub(r"<[^>]+>", " ", main)))


@pytest.mark.parametrize("mid", sorted({sp["attributes"].get("messaging.message.id") for sp in FIXTURE["spans"]}
                                       - {None}))
def test_no_sqs_wording_on_a_rabbitmq_message_page(store, mid):
    _, text = _page(store, mid)
    assert not [w for w in PAGE_SQS_WORDS if w in text], text
    assert "~rabbitmq~" not in text  # the ID as the application set it


def test_the_message_page_names_rabbitmq_steps_causes_and_x_death(store):
    html, text = _page(store, "m-crash")
    for words in ("Published", "Routed", "Delivered", "Channel closed", "Returned to the queue", "Dead-lettered",
                  "Acked (basic.ack)", "cause its channel closed before an ack", "x-death reason delivery_limit",
                  "reported by RabbitMQ", "Copies", "Deliveries"):
        assert words in text, words
    assert 'data-source="amqp_x_death"' in html and 'class="event observed reported"' in html
    _, text = _page(store, "m-timeout")
    assert "cause the broker cancelled the consumer" in text
    _, text = _page(store, "m-noroute")
    assert "Returned to the publisher (basic.return)" in text and "reply text NO_ROUTE" in text
    assert "no ack seen" in text
    _, text = _page(store, "m-fan")
    assert "fan.a : 1 delivery, acked; fan.b : 1 delivery, acked" in text
    _, text = _page(store, "m-reject")
    assert "Refused, not requeued" in text and "x-death reason rejected" in text


def test_queue_events_follow_routing_and_x_death(store):
    from trailbeam.server import queues

    def evs(mid):
        return [(e[1], e[2], e[6]) for e in queues.events(store.message_rows(systems.message_key(systems.RABBITMQ, mid)))]
    url = "amqp://rabbitmq/%2F/"
    # the publish counts on each queue it reached, not on the exchange
    assert [k for k, q, _ in evs("m-fan") if k == "send"] == ["send", "send"]
    assert {q for k, q, _ in evs("m-fan") if k == "send"} == {url + "fan.a", url + "fan.b"}
    # x-death moves: dlq_receive on the dead-letter queue, dlq_seen on the source, keyed as a delivery on it was
    crash = evs("m-crash")
    assert ("dlq_receive", url + "orders.dlq", url + "orders") in crash
    assert ("dlq_seen", url + "orders", None) in crash
    assert ("send", url + "orders", None) in crash
    # a requeue is not a visibility change, and no at_max is written for RabbitMQ
    assert not [k for k, _, _ in evs("m-requeue") + evs("m-stuck") if k in ("visibility", "at_max")]
    # an unroutable publish stays on its exchange
    assert [k for k, _, _ in evs("m-noroute")] == ["send"]


def test_stuck_causes_of_a_rabbitmq_message_say_unacked_not_visibility(store):
    from trailbeam.server import causes
    t = tl(store, "m-stuck")
    x = causes.cause_of(t, NOW / 1e6 + 3600 * 1000)
    assert x["kind"] == "in_flight" and x["amqp"] is True and "assumedVisibility" not in x
    g = causes.group([x])[0]
    assert g["label"].endswith("has the delivery and hasn't acked it (basic.ack)")
    assert "consumer_timeout" in g["why"]
    assert not [w for w in PAGE_SQS_WORDS if w in g["label"] + g["why"]]


def test_deduplicated_sends_takes_rows_without_a_message_id():
    """The Postgres version-7 upgrade passes FIFO sends with only kind, queue, time and row (QV-307 CI)."""
    sends = [{"kind": "send", "queue": "orders.fifo", "time_ns": 1, "row": 1},
             {"kind": "send", "queue": "orders.fifo", "time_ns": 2, "row": 2}]
    assert timeline.deduplicated_sends(sends) == {2}


# ---- a queue is (virtual host, queue) (A3, A6; QV-528) -------------------------------------------------------

def _in_vhost(vhost, shift_ns, settle=True):
    """The happy scenario's spans in ``vhost``, ``shift_ns`` later, on their own channel; without the ack when not
    ``settle``."""
    out = []
    for sp in FIXTURE["spans"]:
        if sp["scenario"] != "happy" or (not settle and sp["attributes"]["trailbeam.kind"] == "amqp.ack"):
            continue
        a = dict(sp["attributes"], **{"trailbeam.amqp.vhost": vhost, "messaging.message.id": "order-1"})
        if "trailbeam.amqp.channel.id" in a:
            a["trailbeam.amqp.channel.id"] = f"{a['trailbeam.amqp.channel.id']}-{vhost}"
        out.append(dict(sp, attributes=a, start_time_unix_nano=sp["start_time_unix_nano"] + shift_ns,
                        end_time_unix_nano=sp["end_time_unix_nano"] + shift_ns))
    return out


def _vhost_store(spans):
    from test_amqp_conformance import otlp_json
    from trailbeam.server.otlp import decode
    s = SqliteStore()
    s.insert(decode(otlp_json(spans), "application/json").records)
    return s


def test_one_id_on_one_queue_name_in_two_vhosts_is_two_copies_not_a_duplicate():
    """Queue orders of tenant-a and orders of tenant-b are two queues: order-1 published, delivered and acked once
    in each is two copies, each acked once, not a message received and acked twice."""
    s = _vhost_store(_in_vhost("tenant-a", 0) + _in_vhost("tenant-b", 10 ** 9))
    t = tl(s, "order-1")
    assert t["deliveries"] == 2 and t["state"] == "deleted"
    assert [(c["vhost"], c["queue"], c["deliveries"], c["ended"]) for c in t["copies"]] == [
        ("tenant-a", "orders", 1, "acked"), ("tenant-b", "orders", 1, "acked")]
    assert [e["status"] for e in t["events"] if e["type"] == "acked"] == ["final", "final"]
    assert "returned_to_queue" not in types(t)
    assert one(t, "routed")["queues"] == ["orders"]  # tenant-a's exchange routes only in tenant-a
    sm = summary.summarize(s.message_rows(t["message_id"]))
    assert not sm["dup_received"] and not sm["dup_succeeded"] and not sm["stuck_open"]
    assert s.duplicate_messages(10) == []
    assert s.stuck_messages(NOW, 10) == []
    html, _ = _page(s, "order-1")
    assert "(vhost <code>tenant-a</code>)" in html and "(vhost <code>tenant-b</code>)" in html
    s.close()


def test_an_unacked_copy_in_one_vhost_is_stuck_though_the_other_vhost_acked_later():
    s = _vhost_store(_in_vhost("tenant-b", 0, settle=False) + _in_vhost("tenant-a", 10 ** 9))
    t = tl(s, "order-1")
    assert t["state"] == "not_deleted"
    assert {c["vhost"]: c["ended"] for c in t["copies"]} == {"tenant-a": "acked", "tenant-b": None}
    sm = summary.summarize(s.message_rows(t["message_id"]))
    assert sm["stuck_open"] and not sm["dup_received"] and not sm["dup_succeeded"]
    assert [c.message_id for c in s.stuck_messages(NOW, 10)] == [t["message_id"]]
    s.close()


# ---- x-death is publisher-controlled: it can't close a copy still held, or acked (A8; QV-527) ----------------

FORGED_X_DEATH = {"trailbeam.dead_letter.source_queue": "payments", "trailbeam.dead_letter.reason": "rejected",
                  "trailbeam.dead_letter.count": 1, "trailbeam.dead_letter.exchange": "amq.default"}


def _happy_on(queue, mid, shift_ns, settle=True, extra=None, channel=""):
    """The happy scenario's spans for ``mid`` on ``queue``, ``shift_ns`` later; without the ack when not ``settle``;
    ``extra``: attributes added to the delivery."""
    out = []
    for sp in FIXTURE["spans"]:
        kind = sp["attributes"]["trailbeam.kind"]
        if sp["scenario"] != "happy" or (not settle and kind == "amqp.ack"):
            continue
        a = dict(sp["attributes"], **{"messaging.message.id": mid})
        a["rabbitmq.destination.routing_key"] = queue
        if kind != "amqp.publish":
            a["messaging.destination.name"] = queue
            a["trailbeam.amqp.channel.id"] = a["trailbeam.amqp.channel.id"] + channel
        for k in ("trailbeam.order.issued", "trailbeam.order.returned"):
            if k in a:
                a[k] += shift_ns // 10 ** 6
        if kind == "amqp.deliver" and extra:
            a.update(extra)
        out.append(dict(sp, attributes=a, start_time_unix_nano=sp["start_time_unix_nano"] + shift_ns,
                        end_time_unix_nano=sp["end_time_unix_nano"] + shift_ns))
    return out


def test_a_forged_x_death_does_not_hide_a_stuck_message():
    """The QV-526 reproduction: pay-1 delivered on payments and never acked; then a message published to another
    queue, attacker-q, under the same ID with x-death naming payments, delivered and acked. The held delivery on
    payments was not in the queue to be dead-lettered, so this is no move: pay-1 stays on Stuck."""
    s = _vhost_store(_happy_on("payments", "pay-1", 0, settle=False)
                     + _happy_on("attacker-q", "pay-1", 10 ** 9, extra=FORGED_X_DEATH, channel="-attacker"))
    t = tl(s, "pay-1")
    assert "moved_to_dead_letter_queue" not in types(t)
    assert t["state"] == "not_deleted" and t["dead_letter_queue_move"] is None and t["in_dead_letter_queue"] is False
    assert {c["queue"]: c["ended"] for c in t["copies"]} == {"payments": None, "attacker-q": "acked"}
    n = [x for x in t["notices"] if x["code"] == "x_death_not_a_move"]
    assert len(n) == 1 and n[0]["queue"] == "payments" and n[0]["vhost"] == "/"
    assert "still held by its consumer" in n[0]["message"] and "publisher can set x-death" in n[0]["message"]
    sm = summary.summarize(s.message_rows(t["message_id"]))
    assert sm["stuck_open"] and not sm["final_delete"] and not sm["in_dlq"]
    it = summary.item(sm, NOW, LT, {})
    for k in ("state", "in_dead_letter_queue", "dead_letter_queue_move"):
        assert it[k] == t[k], k
    assert [c.message_id for c in s.stuck_messages(NOW + 2 * 10 ** 9, 10)] == [t["message_id"]]
    from trailbeam.server import queues
    assert not [e for e in queues.events(s.message_rows(t["message_id"])) if e[1] in ("dlq_receive", "dlq_seen")]
    s.close()


def test_an_x_death_naming_a_queue_whose_delivery_was_acked_is_not_a_move():
    s = _vhost_store(_happy_on("payments", "pay-2", 0)
                     + _happy_on("attacker-q", "pay-2", 10 ** 9, extra=FORGED_X_DEATH, channel="-attacker"))
    t = tl(s, "pay-2")
    assert "moved_to_dead_letter_queue" not in types(t) and t["dead_letter_queue_move"] is None
    assert t["state"] == "deleted"  # both copies acked
    assert "was acked" in [x for x in t["notices"] if x["code"] == "x_death_not_a_move"][0]["message"]
    s.close()
