"""The pika hooks against the contract's RabbitMQ vectors (contract/span-contract-amqp-vectors.json, A13).

Each ``emit`` case is played on a real ``pika.channel.Channel`` over a fake connection (tests/pika_fake.py): the
application's calls go through pika's own methods, the broker's frames through the handlers pika uses for them.
Every span the hooks emit must also be accepted by the server's ingest (A12). The same hooks against a real
RabbitMQ are tests/test_pika_rabbitmq.py.
"""
import json
from pathlib import Path

import pytest

pika = pytest.importorskip("pika")

from opentelemetry.sdk.trace.export import SimpleSpanProcessor  # noqa: E402
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter  # noqa: E402

import trailbeam.pika  # noqa: E402
from trailbeam import _amqp  # noqa: E402
from trailbeam.server.otlp import to_record  # noqa: E402

import pika_fake as fake  # noqa: E402

VECTORS = json.loads((Path(__file__).resolve().parents[2] / "contract" / "span-contract-amqp-vectors.json")
                     .read_text("utf-8"))
PRESENT = VECTORS["emit_present"]
PRESENT_ON_CHANNEL_SPANS = VECTORS["emit_present_on_channel_spans"]
NOT_CHANNEL_SPANS = {"trailbeam publish", "trailbeam return"}


@pytest.fixture
def exporter():
    exp = InMemorySpanExporter()
    trailbeam.pika.install(span_processor=SimpleSpanProcessor(exp))
    yield exp
    trailbeam.pika.uninstall()


def _channel(event):
    ctx = VECTORS["context"]
    conn = fake.FakeConnection(ctx["server.address"], ctx["server.port"], ctx["vhost"])
    return conn, fake.open_channel(conn)


def _record(ch, recorded):
    """Deliveries the package recorded on the channel and not settled: delivered through the hooks."""
    tags = set()
    for r in recorded:
        if r["consumer_tag"] not in tags and r["consumer_tag"] not in ch._consumers:
            fake.consume(ch, r["queue"], r["consumer_tag"])
            tags.add(r["consumer_tag"])
        mid = {"message_id": r["message_id"]} if r.get("message_id") else {}
        fake.deliver(ch, r["consumer_tag"], r["delivery_tag"], exchange="", routing_key=r["queue"],
                     properties=fake.props(mid))


def _consumer(ch, event):
    c = event.get("consumer")
    if c and c["consumer_tag"] not in ch._consumers:
        fake.consume(ch, c["queue"], c["consumer_tag"])


def _play(event, exporter):
    conn, ch = _channel(event)
    _consumer(ch, event)
    _record(ch, event.get("recorded", []))
    exporter.clear()
    broker = event.get("broker") or {}
    call = event.get("call")

    if call == "basic.publish":
        if event["confirm_mode"]:
            ch.confirm_delivery(lambda frame: None)
            fake.reply(ch, pika.spec.Confirm.SelectOk())
        if isinstance(event.get("client"), dict):
            conn.fail_writes = getattr(pika.exceptions, event["client"]["raised"])
        try:
            ch.basic_publish(event["exchange"], event["routing_key"], b'{"n": 1}',
                             properties=fake.props(event["properties"]), mandatory=event["mandatory"])
        except Exception:
            pass
        if "basic.return" in broker:
            r = broker["basic.return"]
            fake.returned(ch, r["reply_code"], r["reply_text"], event["exchange"], event["routing_key"],
                          fake.props(event["properties"]))
        if "confirm" in broker:
            fake.confirm(ch, broker["confirm"] == "ack", 1)
        if "channel.close" in broker:
            c = broker["channel.close"]
            fake.broker_closes_channel(ch, c["reply_code"], c["reply_text"])
    elif event.get("method") == "basic.deliver":
        fake.consume(ch, event["queue"], event["consumer_tag"])
        exporter.clear()
        fake.deliver(ch, event["consumer_tag"], event["delivery_tag"], event["redelivered"], event["exchange"],
                     event["routing_key"], fake.props(event["properties"], event["headers"]))
    elif event.get("method") == "basic.get":
        ch.basic_get(event["queue"], lambda *a: None)
        if event.get("empty"):
            fake.get_empty(ch)
        else:
            fake.get_ok(ch, event["delivery_tag"], event["redelivered"], event["exchange"], event["routing_key"],
                        fake.props(event["properties"], event["headers"]))
    elif call in ("basic.ack", "basic.nack", "basic.reject"):
        if isinstance(event.get("client"), dict):
            # the library raises: the channel went away under the application
            assert event["client"]["raised"] == "ChannelWrongStateError"
            ch._set_state(ch.CLOSED)
        kwargs = {"delivery_tag": event["delivery_tag"]}
        if call != "basic.reject":
            kwargs["multiple"] = event["multiple"]
        if call != "basic.ack":
            kwargs["requeue"] = event["requeue"]
        try:
            getattr(ch, call.replace(".", "_"))(**kwargs)
        except pika.exceptions.ChannelWrongStateError:
            pass
    elif call == "basic.cancel":
        ch.basic_cancel(event["consumer"]["consumer_tag"], callback=lambda frame: None)
        if broker.get("basic.cancel-ok"):
            fake.reply(ch, pika.spec.Basic.CancelOk(consumer_tag=event["consumer"]["consumer_tag"]))
    elif "basic.cancel" in broker:
        fake.broker_cancels(ch, broker["basic.cancel"]["consumer_tag"])
    elif "channel.close" in broker:
        c = broker["channel.close"]
        fake.broker_closes_channel(ch, c["reply_code"], c["reply_text"])
    elif "network" in event:
        fake.connection_lost(ch)
    elif call in ("channel.close", "connection.close"):
        if call == "connection.close":
            setattr(conn, trailbeam.pika._CONN_CLOSING_ATTR, True)  # what the Connection.close hook sets
        fake.client_closes_channel(ch)
    else:  # pragma: no cover
        raise AssertionError(f"vector event not played: {event}")
    spans = exporter.get_finished_spans()
    if call in ("channel.close", "connection.close"):
        # pika's Channel.close cancels each consumer first (a nowait basic.cancel): real client cancels, with their
        # own spans, which the close vectors don't list
        spans = [s for s in spans if s.name != "trailbeam cancel"]
    return spans


@pytest.mark.parametrize("case", VECTORS["emit"], ids=lambda c: c["name"])
def test_the_contract_emit_vectors(case, exporter):
    spans = _play(case["event"], exporter)
    assert [s.name for s in spans] == [s["span_name"] for s in case["spans"]]
    for got, want in zip(spans, case["spans"]):
        attrs = dict(got.attributes)
        for k, v in want["attributes"].items():
            assert k in attrs, f"{got.name}: no {k}"
            assert attrs[k] == v and type(attrs[k]) is type(v), f"{got.name}: {k} = {attrs[k]!r}, not {v!r}"
        for k in PRESENT + (PRESENT_ON_CHANNEL_SPANS if got.name not in NOT_CHANNEL_SPANS else []):
            assert k in attrs, f"{got.name}: no {k}"
        for k in want.get("absent", []):
            assert k not in attrs, f"{got.name}: {k} = {attrs[k]!r} should be absent"
        if attrs.get("trailbeam.start_observed") is False:
            assert got.start_time == got.end_time and "trailbeam.order.issued" not in attrs
        else:
            assert attrs["trailbeam.order.issued"] < attrs["trailbeam.order.returned"]
            assert got.start_time <= got.end_time
        _accepted(got)
    # spans of one call share their interval and ticks
    calls = {(s.start_time, s.end_time, s.attributes.get("trailbeam.order.returned")) for s in spans
             if s.name.endswith(("ack", "nack", "reject"))}
    assert len(calls) <= 1


def _accepted(span):
    resource = dict(span.resource.attributes)
    rec, reason = to_record(resource, format(span.context.trace_id, "032x"), format(span.context.span_id, "016x"),
                            span.start_time, span.end_time, dict(span.attributes))
    assert rec is not None and reason == "", f"{span.name}: the server rejects it: {reason}"


@pytest.mark.parametrize("case", VECTORS["receive_count"], ids=lambda c: c["name"])
def test_receive_count_vectors(case):
    assert _amqp.receive_count(case["redelivered"], case["headers"]) == (case["receive_count"], case["source"])


@pytest.mark.parametrize("case", VECTORS["dead_letter"], ids=lambda c: c["name"])
def test_dead_letter_vectors(case):
    assert _amqp.dead_letter_attrs(case["x-death"]) == case["attributes"]
