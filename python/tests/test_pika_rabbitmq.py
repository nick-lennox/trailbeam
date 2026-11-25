"""The pika hooks against a real RabbitMQ (contract/span-contract-amqp.md): the scenarios of the QV-303 proof and of
the contract's fixture, played with pika's BlockingConnection (and one with its asyncio adapter), each span checked
against the server's ingest (A12) and the messages' timelines read back from a Trailbeam store.

Needs a broker: TRAILBEAM_TEST_AMQP_URL, such as amqp://guest:guest@127.0.0.1:5672/%2F (CI runs rabbitmq:4).
Skipped without one. Every test declares queues of its own, so tests can share one broker.
"""
import asyncio
import json
import os
import socket
import time
import uuid

import pytest

pika = pytest.importorskip("pika")

from opentelemetry.sdk.trace.export import SimpleSpanProcessor  # noqa: E402
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter  # noqa: E402

import trailbeam.pika  # noqa: E402
from trailbeam.server import queries, systems  # noqa: E402
from trailbeam.server.db import SqliteStore  # noqa: E402
from trailbeam.server.otlp import decode, to_record  # noqa: E402

URL = os.environ.get("TRAILBEAM_TEST_AMQP_URL")
pytestmark = pytest.mark.skipif(not URL, reason="no RabbitMQ: set TRAILBEAM_TEST_AMQP_URL")


@pytest.fixture
def exporter():
    exp = InMemorySpanExporter()
    trailbeam.pika.install(span_processor=SimpleSpanProcessor(exp))
    yield exp
    trailbeam.pika.uninstall()


@pytest.fixture
def names():
    """Queue and exchange names of this test's own, deleted afterwards."""
    made = []
    run = uuid.uuid4().hex[:8]

    def name(base):
        n = f"tb-{base}-{run}"
        made.append(n)
        return n

    yield name
    try:
        c = pika.BlockingConnection(pika.URLParameters(URL))
        for n in made:
            ch = c.channel()
            try:
                ch.queue_delete(n)
            except Exception:
                ch = c.channel()
            try:
                ch.exchange_delete(n)
            except Exception:
                pass
        c.close()
    except Exception:
        pass


def connect():
    return pika.BlockingConnection(pika.URLParameters(URL))


def quorum(ch, name, **arguments):
    ch.queue_declare(name, durable=True, arguments={"x-queue-type": "quorum", **arguments})


def spans_of(exporter, *names):
    return [s for s in exporter.get_finished_spans() if not names or s.name in names]


def a(span):
    return dict(span.attributes)


def wait_for(cond, conn, timeout=10.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if cond():
            return True
        conn.process_data_events(time_limit=0.05)
    return cond()


def accepted(spans):
    """Every span passes the server's A12 checks."""
    for s in spans:
        rec, reason = to_record(dict(s.resource.attributes), format(s.context.trace_id, "032x"),
                                format(s.context.span_id, "016x"), s.start_time, s.end_time, dict(s.attributes))
        assert rec is not None, f"{s.name}: {reason}"


def timeline(spans, message_id, limits=None):
    """The message's timeline, from a Trailbeam store holding these spans."""
    accepted(spans)
    out = []
    for s in spans:
        attrs = [{"key": k, "value": _value(v)} for k, v in s.attributes.items()]
        res = [{"key": k, "value": _value(v)} for k, v in s.resource.attributes.items()]
        out.append({"resource": {"attributes": res}, "scopeSpans": [{"scope": {"name": "trailbeam"}, "spans": [{
            "traceId": format(s.context.trace_id, "032x"), "spanId": format(s.context.span_id, "016x"),
            "name": s.name, "startTimeUnixNano": str(s.start_time), "endTimeUnixNano": str(s.end_time),
            "attributes": attrs}]}]})
    d = decode(json.dumps({"resourceSpans": out}).encode(), "application/json")
    assert d.rejected == []
    store = SqliteStore()
    try:
        store.insert(d.records)
        t = queries.message(store, systems.message_key(systems.RABBITMQ, message_id), time.time_ns() + 10 ** 9, 900,
                            limits)
    finally:
        store.close()
    assert t is not None
    return t


def _value(v):
    if isinstance(v, bool):
        return {"boolValue": v}
    if isinstance(v, int):
        return {"intValue": str(v)}
    return {"stringValue": str(v)}


def types(t):
    return [e["type"] for e in t["events"]]


def publish(ch, routing_key, message_id, exchange="", body=b'{"order": 1}', **kw):
    ch.basic_publish(exchange, routing_key, body, properties=pika.BasicProperties(message_id=message_id,
                                                                                   delivery_mode=2), **kw)


def take(conn, ch, queue, n=1, timeout=10.0):
    """Consume until n deliveries arrived; returns [(method, properties, body)], not settled."""
    got = []
    tag = ch.basic_consume(queue, lambda _ch, m, p, b: got.append((m, p, b)))
    assert wait_for(lambda: len(got) >= n, conn, timeout), f"{len(got)} of {n} deliveries from {queue}"
    return tag, got


# -- the scenarios ----------------------------------------------------------------------------------------------

def test_publish_deliver_ack(exporter, names):
    q = names("happy")
    conn = connect()
    ch = conn.channel()
    quorum(ch, q)
    ch.confirm_delivery()
    publish(ch, q, "m-happy")
    tag, got = take(conn, ch, q)
    ch.basic_ack(got[0][0].delivery_tag)
    ch.basic_cancel(tag)
    conn.close()

    pub, = spans_of(exporter, "trailbeam publish")
    assert a(pub) | {} == a(pub) and a(pub)["trailbeam.amqp.confirmed"] is True and a(pub)["trailbeam.result"] == "ok"
    assert a(pub)["messaging.destination.name"] == "amq.default" and a(pub)["rabbitmq.destination.routing_key"] == q
    assert a(pub)["messaging.message.body.size"] == len(b'{"order": 1}')
    assert a(pub)["trailbeam.body.capture_status"] == "off"
    deliver, = spans_of(exporter, "trailbeam deliver")
    d = a(deliver)
    assert d["messaging.destination.name"] == q and d["messaging.operation.name"] == "basic.deliver"
    assert d["trailbeam.receive_count"] == 1 and d["trailbeam.receive_count.source"] == "first_delivery"
    assert d["trailbeam.start_observed"] is False and deliver.start_time == deliver.end_time
    ack, = spans_of(exporter, "trailbeam ack")
    assert a(ack)["trailbeam.amqp.channel.id"] == d["trailbeam.amqp.channel.id"]
    assert a(ack)["rabbitmq.message.delivery_tag"] == d["rabbitmq.message.delivery_tag"]
    assert a(ack)["trailbeam.result"] == "ok" and a(ack)["messaging.message.id"] == "m-happy"
    # the publish's interval contains the broker's confirm, so it ends after it started and before the delivery
    assert pub.start_time < pub.end_time and a(pub)["trailbeam.order.returned"] < d["trailbeam.order.returned"]
    cancel, = spans_of(exporter, "trailbeam cancel")
    assert a(cancel)["trailbeam.amqp.cancelled_by"] == "client" and a(cancel)["trailbeam.result"] == "ok"
    assert a(cancel)["trailbeam.amqp.unsettled_count"] == 0
    assert spans_of(exporter, "trailbeam channel_closed") == []  # nothing was open when the connection closed

    t = timeline(spans_of(exporter), "m-happy")
    assert types(t) == ["sent", "routed", "received", "acked"], types(t)


def test_nack_requeue_then_ack_counts_from_x_acquired_count(exporter, names):
    q = names("requeue")
    conn = connect()
    ch = conn.channel()
    quorum(ch, q)
    ch.confirm_delivery()
    publish(ch, q, "m-requeue")
    seen = []

    def on_message(_ch, m, p, b):
        seen.append(m.delivery_tag)
        if len(seen) == 1:
            _ch.basic_nack(m.delivery_tag, requeue=True)
        else:
            _ch.basic_ack(m.delivery_tag)

    ch.basic_consume(q, on_message)
    assert wait_for(lambda: len(seen) >= 2, conn)
    conn.close()

    d1, d2 = spans_of(exporter, "trailbeam deliver")
    assert a(d1)["trailbeam.receive_count"] == 1 and a(d1)["trailbeam.amqp.redelivered"] is False
    assert a(d2)["trailbeam.amqp.redelivered"] is True
    assert a(d2)["trailbeam.receive_count.source"] == "x-acquired-count"
    assert a(d2)["trailbeam.receive_count"] == a(d2)["trailbeam.amqp.acquired_count"] + 1 == 2
    nack, = spans_of(exporter, "trailbeam nack")
    assert a(nack)["trailbeam.amqp.requeue"] is True and a(nack)["trailbeam.amqp.multiple"] is False
    t = timeline(spans_of(exporter), "m-requeue")
    assert types(t) == ["sent", "routed", "received", "requeued", "returned_to_queue", "received", "acked"], types(t)


def test_publisher_set_counter_headers_are_ignored_on_a_first_delivery(exporter, names):
    # QV-529: RabbitMQ passes these headers through when the publisher sets them; only a redelivery or x-death
    # makes them the broker's.
    q = names("forged")
    conn = connect()
    ch = conn.channel()
    quorum(ch, q)
    ch.confirm_delivery()
    for headers in (None, {"x-acquired-count": 41, "x-delivery-count": 41}):
        ch.basic_publish("", q, b"{}", properties=pika.BasicProperties(message_id="ord-9", delivery_mode=2,
                                                                         headers=headers))
        tag, got = take(conn, ch, q)
        ch.basic_ack(got[0][0].delivery_tag)
        ch.basic_cancel(tag)
    assert got[0][1].headers["x-acquired-count"] == 41 and got[0][0].redelivered is False  # the broker kept it
    conn.close()
    for d in map(a, spans_of(exporter, "trailbeam deliver")):
        assert d["trailbeam.receive_count"] == 1 and d["trailbeam.receive_count.source"] == "first_delivery"
        assert "trailbeam.amqp.acquired_count" not in d and "trailbeam.amqp.delivery_count" not in d
    t = timeline(spans_of(exporter), "ord-9")
    assert "deliveries_not_observed" not in types(t) and "returned_to_queue" not in types(t), types(t)


def test_reject_to_a_dead_letter_queue_reports_x_death(exporter, names):
    q, dlq, dlx = names("orders"), names("orders-dlq"), names("dlx")
    conn = connect()
    ch = conn.channel()
    ch.exchange_declare(dlx, "fanout")
    quorum(ch, dlq)
    ch.queue_bind(dlq, dlx)
    quorum(ch, q, **{"x-dead-letter-exchange": dlx})
    ch.confirm_delivery()
    publish(ch, q, "m-dead")
    tag, got = take(conn, ch, q)
    ch.basic_reject(got[0][0].delivery_tag, requeue=False)
    ch.basic_cancel(tag)
    tag2, got2 = take(conn, ch, dlq)
    ch.basic_ack(got2[0][0].delivery_tag)
    conn.close()

    reject, = spans_of(exporter, "trailbeam reject")
    assert a(reject)["trailbeam.amqp.requeue"] is False and "trailbeam.amqp.multiple" not in a(reject)
    on_dlq = [s for s in spans_of(exporter, "trailbeam deliver") if a(s)["messaging.destination.name"] == dlq]
    d = a(on_dlq[0])
    assert d["trailbeam.dead_letter.source_queue"] == q and d["trailbeam.dead_letter.reason"] == "rejected"
    assert d["trailbeam.dead_letter.count"] == 1 and d["trailbeam.dead_letter.exchange"] == "amq.default"
    assert abs(d["trailbeam.dead_letter.time_ms"] - time.time() * 1000) < 120_000
    assert d["trailbeam.dead_letter.first_queue"] == q and d["trailbeam.amqp.exchange"] == dlx
    t = timeline(spans_of(exporter), "m-dead")
    assert types(t) == ["sent", "routed", "received", "rejected", "moved_to_dead_letter_queue", "received", "acked"], types(t)
    move = next(e for e in t["events"] if e["type"] == "moved_to_dead_letter_queue")
    assert move["inferred"] is False, move


def test_mandatory_publish_no_queue_takes(exporter, names):
    conn = connect()
    ch = conn.channel()
    ch.confirm_delivery()
    with pytest.raises(pika.exceptions.UnroutableError):
        publish(ch, "tb-nobody-" + uuid.uuid4().hex, "m-unroutable", mandatory=True)
    conn.close()
    ret, pub = spans_of(exporter, "trailbeam return", "trailbeam publish")
    assert ret.name == "trailbeam return" and pub.name == "trailbeam publish"
    assert a(ret)["trailbeam.amqp.reply_code"] == 312 and a(ret)["trailbeam.amqp.reply_text"] == "NO_ROUTE"
    assert a(ret)["messaging.message.id"] == "m-unroutable"
    # A4.1: pika raises UnroutableError, but the broker acked the publish
    assert a(pub)["trailbeam.result"] == "ok" and a(pub)["trailbeam.amqp.mandatory"] is True
    accepted([ret, pub])


def test_publish_to_a_missing_exchange_fails_with_the_reply_code_name(exporter):
    conn = connect()
    ch = conn.channel()
    ch.confirm_delivery()
    with pytest.raises(pika.exceptions.ChannelClosedByBroker):
        publish(ch, "k", "m-nope", exchange="tb-missing-" + uuid.uuid4().hex)
    conn.close()
    pub, = spans_of(exporter, "trailbeam publish")
    assert a(pub)["trailbeam.result"] == "failed" and a(pub)["trailbeam.error_code"] == "NOT_FOUND"
    accepted([pub])


def test_publish_without_confirms_and_without_message_id(exporter, names):
    q = names("noid")
    conn = connect()
    ch = conn.channel()
    quorum(ch, q)
    ch.basic_publish("", q, b"plain")
    tag, got = take(conn, ch, q)
    ch.basic_ack(got[0][0].delivery_tag)
    conn.close()
    pub, = spans_of(exporter, "trailbeam publish")
    assert a(pub)["trailbeam.amqp.confirmed"] is False and a(pub)["trailbeam.result"] == "ok"
    assert "messaging.message.id" not in a(pub)
    deliver, = spans_of(exporter, "trailbeam deliver")
    assert "messaging.message.id" not in a(deliver)
    accepted(spans_of(exporter))


def test_basic_get_is_the_interval_of_the_call_and_get_empty_gives_no_span(exporter, names):
    q = names("pull")
    conn = connect()
    ch = conn.channel()
    quorum(ch, q)
    ch.confirm_delivery()
    publish(ch, q, "m-pull")
    method, props, body = None, None, None
    end = time.monotonic() + 10
    while method is None and time.monotonic() < end:
        method, props, body = ch.basic_get(q)
    assert method is not None
    ch.basic_ack(method.delivery_tag)
    assert ch.basic_get(q) == (None, None, None)
    conn.close()
    gets = spans_of(exporter, "trailbeam deliver")
    assert len(gets) == 1
    g = a(gets[0])
    assert g["messaging.operation.name"] == "basic.get" and "trailbeam.start_observed" not in g
    assert g["trailbeam.order.issued"] < g["trailbeam.order.returned"]
    assert "trailbeam.amqp.consumer_tag" not in g
    ack, = spans_of(exporter, "trailbeam ack")
    assert a(ack)["rabbitmq.message.delivery_tag"] == g["rabbitmq.message.delivery_tag"]
    accepted(spans_of(exporter))


def test_ack_multiple_covers_every_open_delivery(exporter, names):
    q = names("multi")
    conn = connect()
    ch = conn.channel()
    quorum(ch, q)
    ch.confirm_delivery()
    for i in range(3):
        publish(ch, q, f"m-multi-{i}")
    tag, got = take(conn, ch, q, 3)
    ch.basic_ack(got[-1][0].delivery_tag, multiple=True)
    conn.close()
    acks = spans_of(exporter, "trailbeam ack")
    assert sorted(a(s)["messaging.message.id"] for s in acks) == ["m-multi-0", "m-multi-1", "m-multi-2"]
    assert len({(s.start_time, s.end_time) for s in acks}) == 1
    assert all(a(s)["trailbeam.amqp.multiple"] is True for s in acks)


def test_client_closes_the_connection_with_a_delivery_open(exporter, names):
    q = names("open")
    conn = connect()
    ch = conn.channel()
    quorum(ch, q)
    ch.confirm_delivery()
    publish(ch, q, "m-open")
    take(conn, ch, q)
    conn.close()
    closed, = spans_of(exporter, "trailbeam channel_closed")
    c = a(closed)
    assert c["messaging.operation.name"] == "connection.close" and c["trailbeam.amqp.close.initiator"] == "client"
    assert c["messaging.message.id"] == "m-open" and "trailbeam.amqp.close.reply_code" not in c
    accepted([closed])


def test_client_closes_the_channel_with_a_delivery_open(exporter, names):
    q = names("chclose")
    conn = connect()
    ch = conn.channel()
    quorum(ch, q)
    ch.confirm_delivery()
    publish(ch, q, "m-chclose")
    take(conn, ch, q)
    ch.close()
    conn.close()
    closed, = spans_of(exporter, "trailbeam channel_closed")
    assert a(closed)["messaging.operation.name"] == "channel.close"
    assert a(closed)["trailbeam.amqp.close.initiator"] == "client"


def test_broker_closes_the_channel_after_a_settle_it_refused(exporter, names):
    """A4.4: an ack of an unknown tag (here one already acked) gets PRECONDITION_FAILED; the deliveries still
    open on the channel are reported closed by the broker."""
    q = names("refused")
    conn = connect()
    ch = conn.channel()
    quorum(ch, q)
    ch.confirm_delivery()
    publish(ch, q, "m-refused-1")
    publish(ch, q, "m-refused-2")
    tag, got = take(conn, ch, q, 2)
    first = got[0][0].delivery_tag
    ch.basic_ack(first)
    ch.basic_ack(first)  # not recorded any more: no span; the broker closes the channel
    try:  # pika 1.5 raises the broker's close here; 1.3 only closes the channel
        assert wait_for(lambda: ch.is_closed, conn)
    except pika.exceptions.ChannelClosedByBroker:
        pass
    conn.close()
    acks = spans_of(exporter, "trailbeam ack")
    assert len(acks) == 1
    closed, = spans_of(exporter, "trailbeam channel_closed")
    c = a(closed)
    assert c["messaging.message.id"] == "m-refused-2" and c["trailbeam.amqp.close.initiator"] == "broker"
    assert c["messaging.operation.name"] == "channel.close" and c["trailbeam.amqp.close.reply_code"] == 406
    assert c["trailbeam.amqp.close.reply_text"].startswith("PRECONDITION_FAILED - unknown delivery tag")
    accepted([closed])


def test_connection_lost_with_a_delivery_open(exporter, names):
    q = names("lost")
    conn = connect()
    ch = conn.channel()
    quorum(ch, q)
    ch.confirm_delivery()
    publish(ch, q, "m-lost")
    take(conn, ch, q)
    conn._impl._transport._sock.shutdown(socket.SHUT_RDWR)  # the network goes away under the client
    with pytest.raises(pika.exceptions.AMQPConnectionError):
        wait_for(lambda: False, conn, 5)
    closed, = spans_of(exporter, "trailbeam channel_closed")
    c = a(closed)
    assert c["messaging.operation.name"] == "connection.lost" and c["trailbeam.amqp.close.initiator"] == "network"
    assert "trailbeam.amqp.close.reply_code" not in c
    # and the broker requeued it: the next delivery counts it
    conn2 = connect()
    ch2 = conn2.channel()
    tag, got = take(conn2, ch2, q)
    ch2.basic_ack(got[0][0].delivery_tag)
    conn2.close()
    d2 = a(spans_of(exporter, "trailbeam deliver")[-1])
    assert d2["trailbeam.amqp.redelivered"] is True and d2["trailbeam.receive_count"] == 2
    t = timeline(spans_of(exporter), "m-lost")
    assert types(t).count("received") == 2 and "channel_closed" in types(t), types(t)


def test_broker_cancels_the_consumer_when_its_queue_is_deleted(exporter, names):
    q = names("gone")
    conn = connect()
    ch = conn.channel()
    quorum(ch, q)
    ch.confirm_delivery()
    publish(ch, q, "m-gone")
    tag, got = take(conn, ch, q)
    other = connect()
    other.channel().queue_delete(q)
    other.close()
    assert wait_for(lambda: bool(spans_of(exporter, "trailbeam cancel")), conn)
    conn.close()
    cancel, = spans_of(exporter, "trailbeam cancel")
    c = a(cancel)
    assert c["trailbeam.amqp.cancelled_by"] == "broker" and c["trailbeam.amqp.consumer_tag"] == tag
    assert c["trailbeam.amqp.unsettled_count"] == 1 and c["trailbeam.start_observed"] is False
    assert "trailbeam.result" not in c
    accepted([cancel])


def test_fanout_delivers_a_copy_to_each_queue(exporter, names):
    fan, qa, qb = names("fan"), names("fan-a"), names("fan-b")
    conn = connect()
    ch = conn.channel()
    ch.exchange_declare(fan, "fanout")
    for q in (qa, qb):
        quorum(ch, q)
        ch.queue_bind(q, fan)
    ch.confirm_delivery()
    publish(ch, "", "m-fan", exchange=fan)
    for q in (qa, qb):
        tag, got = take(conn, ch, q)
        ch.basic_ack(got[0][0].delivery_tag)
    conn.close()
    pub, = spans_of(exporter, "trailbeam publish")
    assert a(pub)["messaging.destination.name"] == fan and "rabbitmq.destination.routing_key" not in a(pub)
    delivered = sorted(a(s)["messaging.destination.name"] for s in spans_of(exporter, "trailbeam deliver"))
    assert delivered == sorted([qa, qb])
    t = timeline(spans_of(exporter), "m-fan")
    assert types(t).count("received") == 2 and types(t).count("acked") == 2, types(t)


def test_classic_queue_redelivery_has_no_count(exporter, names):
    q = names("classic")
    conn = connect()
    ch = conn.channel()
    ch.queue_declare(q, durable=True, arguments={"x-queue-type": "classic"})
    ch.confirm_delivery()
    publish(ch, q, "m-classic")
    seen = []

    def on_message(_ch, m, p, b):
        seen.append(m)
        (_ch.basic_nack(m.delivery_tag) if len(seen) == 1 else _ch.basic_ack(m.delivery_tag))

    ch.basic_consume(q, on_message)
    assert wait_for(lambda: len(seen) >= 2, conn)
    conn.close()
    d1, d2 = spans_of(exporter, "trailbeam deliver")
    assert a(d1)["trailbeam.receive_count"] == 1
    assert "trailbeam.receive_count" not in a(d2) and a(d2)["trailbeam.amqp.redelivered"] is True
    accepted(spans_of(exporter))


def test_auto_ack_deliveries_are_not_left_open(exporter, names):
    q = names("autoack")
    conn = connect()
    ch = conn.channel()
    quorum(ch, q)
    ch.confirm_delivery()
    publish(ch, q, "m-auto")
    got = []
    ch.basic_consume(q, lambda _ch, m, p, b: got.append(m), auto_ack=True)
    assert wait_for(lambda: got, conn)
    conn.close()
    assert len(spans_of(exporter, "trailbeam deliver")) == 1
    assert spans_of(exporter, "trailbeam channel_closed") == []


def test_tenant_header_and_body_capture(names):
    exp = InMemorySpanExporter()
    q = names("tenant")
    trailbeam.pika.install(span_processor=SimpleSpanProcessor(exp), tenant={"message_attribute": "tenant"},
                           capture={"body_queues": [q]})
    try:
        conn = connect()
        ch = conn.channel()
        quorum(ch, q)
        ch.confirm_delivery()
        ch.basic_publish("", q, b'{"order": 7, "password": "hunter2"}',
                         properties=pika.BasicProperties(message_id="m-tenant", headers={"tenant": "acme"}))
        ch.basic_publish("", q, b"\xff\xfe", properties=pika.BasicProperties(message_id="m-binary"))
        tag, got = take(conn, ch, q, 2)
        for m, _, _ in got:
            ch.basic_ack(m.delivery_tag)
        conn.close()
    finally:
        trailbeam.pika.uninstall()
    d1, d2 = [s for s in exp.get_finished_spans() if s.name == "trailbeam deliver"]
    assert a(d1)["trailbeam.tenant"] == "acme" and a(d1)["trailbeam.tenant.source"] == "header"
    assert a(d1)["trailbeam.body.capture_status"] == "captured"
    assert "hunter2" not in a(d1)["trailbeam.body"] and a(d1)["trailbeam.body.redacted_count"] == 1
    assert a(d2)["trailbeam.body.capture_status"] == "not_text" and "trailbeam.body" not in a(d2)
    pub = [s for s in exp.get_finished_spans() if s.name == "trailbeam publish"][0]
    assert a(pub)["trailbeam.tenant"] == "acme"
    assert a(pub)["trailbeam.body.capture_status"] == "off"  # bodies of publishes: listed by exchange (A11)
    accepted(exp.get_finished_spans())


def test_set_message_id_option(names):
    exp = InMemorySpanExporter()
    q = names("setid")
    trailbeam.pika.install(span_processor=SimpleSpanProcessor(exp), set_message_id=True)
    try:
        conn = connect()
        ch = conn.channel()
        quorum(ch, q)
        ch.confirm_delivery()
        mine = pika.BasicProperties(delivery_mode=2)
        ch.basic_publish("", q, b"x", properties=mine)
        assert mine.message_id is None  # the application's properties object is not changed
        tag, got = take(conn, ch, q)
        ch.basic_ack(got[0][0].delivery_tag)
        conn.close()
    finally:
        trailbeam.pika.uninstall()
    pub = [s for s in exp.get_finished_spans() if s.name == "trailbeam publish"][0]
    mid = a(pub)["messaging.message.id"]
    assert uuid.UUID(mid) and a(pub)["trailbeam.amqp.message_id.source"] == "package"
    deliver = [s for s in exp.get_finished_spans() if s.name == "trailbeam deliver"][0]
    assert a(deliver)["messaging.message.id"] == mid == got[0][1].message_id


def test_the_asyncio_adapter_is_followed_too(exporter, names):
    """The hooks are on pika.channel.Channel, under every connection type."""
    from pika.adapters.asyncio_connection import AsyncioConnection

    q = names("asyncio")
    done = asyncio.Event()

    async def main():
        loop = asyncio.get_running_loop()
        state = {}

        def on_open(conn):
            conn.channel(on_open_callback=on_channel)

        def on_channel(ch):
            state["ch"] = ch
            ch.queue_declare(q, durable=True, arguments={"x-queue-type": "quorum"}, callback=on_declared)

        def on_declared(_frame):
            ch = state["ch"]
            ch.confirm_delivery(lambda frame: ch.basic_consume(q, on_message), callback=lambda _f: publish_one())

        def publish_one():
            state["ch"].basic_publish("", q, b"{}", properties=pika.BasicProperties(message_id="m-asyncio"))

        def on_message(ch, m, p, b):
            ch.basic_ack(m.delivery_tag)
            state["conn"].close()

        state["conn"] = AsyncioConnection(pika.URLParameters(URL), on_open_callback=on_open,
                                          on_close_callback=lambda *_: done.set(), custom_ioloop=loop)
        await asyncio.wait_for(done.wait(), 20)

    asyncio.run(main())
    names_ = [s.name for s in spans_of(exporter)]
    assert names_[:3] == ["trailbeam publish", "trailbeam deliver", "trailbeam ack"], names_
    assert a(spans_of(exporter, "trailbeam publish")[0])["trailbeam.result"] == "ok"
    accepted(spans_of(exporter))


def test_uninstall_stops_the_spans(names):
    exp = InMemorySpanExporter()
    trailbeam.pika.install(span_processor=SimpleSpanProcessor(exp))
    trailbeam.pika.uninstall()
    q = names("off")
    conn = connect()
    ch = conn.channel()
    quorum(ch, q)
    ch.basic_publish("", q, b"x")
    conn.close()
    assert exp.get_finished_spans() == ()
    assert not hasattr(pika.channel.Channel.basic_publish, "_trailbeam_orig")


def test_the_thread_safe_connection_of_pika_1_5_is_followed_too(exporter, names):
    """pika 1.5's Connection marshals a publish in the caller's thread and writes it on its IOLoop thread,
    without Channel.basic_publish; settles and deliveries go through the same Channel as the other adapters."""
    ts = pytest.importorskip("pika.adapters.thread_safe_connection")
    q = names("threadsafe")
    got = []
    with ts.Connection(pika.URLParameters(URL)) as conn:
        ch = conn.channel()
        ch.queue_declare(q, durable=True, arguments={"x-queue-type": "quorum"})
        ch.confirm_delivery(lambda frame: None)
        ch.basic_publish("", q, b"{}", properties=pika.BasicProperties(message_id="m-threadsafe"))

        def on_message(_ch, m, p, b):
            got.append(m)
            _ch.basic_ack(m.delivery_tag)

        ch.basic_consume(q, on_message)
        end = time.monotonic() + 10
        while not spans_of(exporter, "trailbeam ack") and time.monotonic() < end:
            time.sleep(0.05)
    pub, = spans_of(exporter, "trailbeam publish")
    assert a(pub)["trailbeam.amqp.confirmed"] is True and a(pub)["trailbeam.result"] == "ok"
    assert a(pub)["messaging.message.id"] == "m-threadsafe"
    deliver, = spans_of(exporter, "trailbeam deliver")
    ack, = spans_of(exporter, "trailbeam ack")
    assert a(ack)["rabbitmq.message.delivery_tag"] == a(deliver)["rabbitmq.message.delivery_tag"]
    accepted(spans_of(exporter))
