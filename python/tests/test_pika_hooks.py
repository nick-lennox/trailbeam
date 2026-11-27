"""The pika hooks beyond the contract's vectors, on a real pika Channel over a fake connection (tests/pika_fake.py):
failures stay the application's own, the records stay bounded, and an uninstalled hook passes everything through."""
import pytest

pika = pytest.importorskip("pika")

from opentelemetry.sdk.trace.export import SimpleSpanProcessor  # noqa: E402
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter  # noqa: E402

import trailbeam.pika  # noqa: E402
from trailbeam import _amqp, _contract  # noqa: E402

import pika_fake as fake  # noqa: E402


@pytest.fixture
def exporter():
    exp = InMemorySpanExporter()
    trailbeam.pika.install(span_processor=SimpleSpanProcessor(exp))
    yield exp
    trailbeam.pika.uninstall()


def names(exp):
    return [s.name for s in exp.get_finished_spans()]


def test_a_publish_the_library_fails_gets_a_failed_span_and_the_error_reaches_the_application(exporter):
    conn = fake.FakeConnection()
    ch = fake.open_channel(conn)
    conn.fail_writes = pika.exceptions.StreamLostError
    with pytest.raises(pika.exceptions.StreamLostError):
        ch.basic_publish("", "orders", b"x", properties=pika.BasicProperties(message_id="m-1"))
    (span,) = exporter.get_finished_spans()
    assert span.attributes["trailbeam.result"] == "failed"
    assert span.attributes["trailbeam.error_code"] == "StreamLostError"


def test_in_confirm_mode_a_failed_write_does_not_shift_the_sequence_numbers(exporter):
    conn = fake.FakeConnection()
    ch = fake.open_channel(conn)
    ch.confirm_delivery(lambda frame: None)
    fake.reply(ch, pika.spec.Confirm.SelectOk())
    conn.fail_writes = pika.exceptions.StreamLostError
    with pytest.raises(pika.exceptions.StreamLostError):
        ch.basic_publish("", "orders", b"x", properties=pika.BasicProperties(message_id="m-lost"))
    ch.basic_publish("", "orders", b"x", properties=pika.BasicProperties(message_id="m-sent"))
    fake.confirm(ch, True, 1)  # the broker numbered only the publish it got
    spans = {s.attributes["messaging.message.id"]: s.attributes for s in exporter.get_finished_spans()}
    assert spans["m-lost"]["trailbeam.result"] == "failed" and spans["m-sent"]["trailbeam.result"] == "ok"


def test_a_multiple_confirm_ends_every_publish_up_to_its_tag(exporter):
    ch = fake.open_channel(fake.FakeConnection())
    ch.confirm_delivery(lambda frame: None)
    for i in range(3):
        ch.basic_publish("", "orders", b"x", properties=pika.BasicProperties(message_id=f"m-{i}"))
    fake.confirm(ch, True, 2, multiple=True)
    assert sorted(s.attributes["messaging.message.id"] for s in exporter.get_finished_spans()) == ["m-0", "m-1"]
    fake.confirm(ch, False, 3)
    last = exporter.get_finished_spans()[-1].attributes
    assert last["messaging.message.id"] == "m-2" and last["trailbeam.error_code"] == "nack"


def test_wrong_arguments_get_pikas_own_error(exporter):
    ch = fake.open_channel(fake.FakeConnection())
    with pytest.raises(TypeError) as plain:
        pika.channel.Channel.basic_ack._trailbeam_orig(ch, 1, False, "extra")
    with pytest.raises(TypeError) as hooked:
        ch.basic_ack(1, False, "extra")
    assert str(hooked.value) == str(plain.value)


def test_a_failing_span_export_never_breaks_the_call(exporter, monkeypatch):
    ch = fake.open_channel(fake.FakeConnection())
    fake.consume(ch, "orders", "ctag-1")

    def boom(*a, **k):
        raise RuntimeError("tracer down")

    monkeypatch.setattr(trailbeam.pika._hooks.emitter.tracer, "start_span", boom)
    got = []
    tag = ch.basic_consume("orders", lambda *a: got.append(a), consumer_tag="ctag-2")
    fake.reply(ch, pika.spec.Basic.ConsumeOk(consumer_tag=tag))
    fake.deliver(ch, "ctag-2", 1, properties=pika.BasicProperties(message_id="m-1"))
    ch.basic_ack(1)
    ch.basic_publish("", "orders", b"x")
    assert len(got) == 1


def test_a_return_of_a_package_set_id_is_marked_package():
    exp = InMemorySpanExporter()
    trailbeam.pika.install(span_processor=SimpleSpanProcessor(exp), set_message_id=True)
    try:
        conn = fake.FakeConnection()
        ch = fake.open_channel(conn)
        ch.basic_publish("", "nobody", b"x", mandatory=True)
        method, (props, body) = conn.sent[-1]
        fake.returned(ch, 312, "NO_ROUTE", "", "nobody", props)
    finally:
        trailbeam.pika.uninstall()
    pub, ret = exp.get_finished_spans()
    assert ret.attributes["messaging.message.id"] == pub.attributes["messaging.message.id"] == props.message_id
    assert ret.attributes["trailbeam.amqp.message_id.source"] == "package"


def test_set_message_id_from_the_environment(monkeypatch):
    monkeypatch.setenv("TRAILBEAM_AMQP_SET_MESSAGE_ID", "on")
    exp = InMemorySpanExporter()
    trailbeam.pika.install(span_processor=SimpleSpanProcessor(exp))
    try:
        conn = fake.FakeConnection()
        ch = fake.open_channel(conn)
        ch.basic_publish("", "orders", b"x", properties=pika.BasicProperties(message_id="mine"))
        ch.basic_publish("", "orders", b"x")
    finally:
        trailbeam.pika.uninstall()
    a, b = (s.attributes for s in exp.get_finished_spans())
    assert a["messaging.message.id"] == "mine" and a["trailbeam.amqp.message_id.source"] == "application"
    assert b["trailbeam.amqp.message_id.source"] == "package"


def _publish_return_deliver(**install):
    """A mandatory publish to topic exchange "events" that comes back, and a delivery from queue "signups", all
    with a routing key holding an email address. Returns the publish, return and deliver spans' attributes."""
    exp = InMemorySpanExporter()
    trailbeam.pika.install(span_processor=SimpleSpanProcessor(exp), **install)
    try:
        conn = fake.FakeConnection()
        ch = fake.open_channel(conn)
        ch.basic_publish("events", "user.alice@example.com.signup", b"x", mandatory=True,
                         properties=pika.BasicProperties(message_id="m-1"))
        fake.returned(ch, 312, "NO_ROUTE", "events", "user.alice@example.com.signup",
                      pika.BasicProperties(message_id="m-1"))
        fake.consume(ch, "signups", "ctag-1")
        fake.deliver(ch, "ctag-1", 1, False, "events", "user.alice@example.com.signup",
                     pika.BasicProperties(message_id="m-1"))
    finally:
        trailbeam.pika.uninstall()
    spans = {s.name: dict(s.attributes) for s in exp.get_finished_spans()}
    return [spans[n] for n in ("trailbeam publish", "trailbeam return", "trailbeam deliver")]


def test_with_body_capture_off_publish_return_and_deliver_carry_only_a_hash_of_the_routing_key():
    key = "k" * 32
    for a in _publish_return_deliver(receipt_hash_key=key):
        assert "rabbitmq.destination.routing_key" not in a
        assert "alice" not in repr(a)
        assert a["trailbeam.amqp.routing_key.hmac"] == _contract.ReceiptHasher(key)("user.alice@example.com.signup")
        assert a["trailbeam.amqp.routing_key.key_id"] == _contract.ReceiptHasher(key).key_id


def test_export_routing_keys_puts_the_routing_key_back_on_publish_return_and_deliver(monkeypatch):
    for spans in (_publish_return_deliver(export_routing_keys=True),
                  (monkeypatch.setenv("TRAILBEAM_AMQP_EXPORT_ROUTING_KEYS", "on") or _publish_return_deliver())):
        for a in spans:
            assert a["rabbitmq.destination.routing_key"] == "user.alice@example.com.signup"
            assert "trailbeam.amqp.routing_key.hmac" not in a and "trailbeam.amqp.routing_key.key_id" not in a


def test_body_capture_decides_per_queue_and_exchange():
    pub, ret, dlv = _publish_return_deliver(receipt_hash_key="k" * 32, capture={"body_queues": ["events"]})
    assert pub["rabbitmq.destination.routing_key"] == ret["rabbitmq.destination.routing_key"]  # exchange captured
    assert "rabbitmq.destination.routing_key" not in dlv and "trailbeam.amqp.routing_key.hmac" in dlv


def test_no_key_warning_when_no_routing_key_can_be_hashed(monkeypatch, caplog):
    monkeypatch.delenv("TRAILBEAM_RECEIPT_HASH_KEY", raising=False)
    monkeypatch.setattr(_contract, "_warned", set())
    _publish_return_deliver(export_routing_keys=True)
    _publish_return_deliver(capture={"body_queues": ["*"]})
    assert "receipt-hash key" not in caplog.text
    _publish_return_deliver()
    assert "receipt-hash key" in caplog.text


def test_control_characters_are_stripped_from_message_ids(exporter):
    ch = fake.open_channel(fake.FakeConnection())
    ch.basic_publish("", "orders", b"x", properties=pika.BasicProperties(message_id="m-1\n"))
    fake.consume(ch, "orders", "ctag-1")
    fake.deliver(ch, "ctag-1", 1, False, "", "orders", pika.BasicProperties(message_id="m-1\n"))
    fake.deliver(ch, "ctag-1", 2, False, "", "orders", pika.BasicProperties(message_id="\x1b\x07"))
    ch.basic_ack(0, multiple=True)
    ids = [s.attributes.get("messaging.message.id") for s in exporter.get_finished_spans()]
    assert ids == ["m-1", "m-1", None, "m-1", None]  # publish, two deliveries, two acks


def test_deliveries_of_an_unknown_consumer_or_a_server_named_queue_get_no_span(exporter):
    ch = fake.open_channel(fake.FakeConnection())
    fake.consume(ch, "", "ctag-anon")
    fake.deliver(ch, "ctag-anon", 1)
    assert names(exporter) == []


def test_the_delivery_record_is_bounded(exporter, monkeypatch):
    monkeypatch.setattr(_amqp, "MAX_DELIVERIES_PER_CHANNEL", 3)
    ch = fake.open_channel(fake.FakeConnection())
    fake.consume(ch, "orders", "ctag-1")
    for tag in range(1, 6):
        fake.deliver(ch, "ctag-1", tag)
    exporter.clear()
    ch.basic_ack(0, multiple=True)
    assert [s.attributes["rabbitmq.message.delivery_tag"] for s in exporter.get_finished_spans()] == [3, 4, 5]


def test_a_channel_opened_while_installed_passes_through_after_uninstall():
    exp = InMemorySpanExporter()
    trailbeam.pika.install(span_processor=SimpleSpanProcessor(exp))
    ch = fake.open_channel(fake.FakeConnection())
    fake.consume(ch, "orders", "ctag-1")
    trailbeam.pika.uninstall()
    exp.clear()
    fake.broker_cancels(ch, "ctag-1")  # its handler was bound while installed
    fake.deliver(ch, "ctag-1", 1)
    assert exp.get_finished_spans() == ()
    assert pika.channel.Channel._on_cancel.__name__ == "_on_cancel"
    assert not hasattr(pika.channel.Channel._on_cancel, "_trailbeam_orig")


def test_install_twice_patches_once():
    exp = InMemorySpanExporter()
    trailbeam.pika.install(span_processor=SimpleSpanProcessor(InMemorySpanExporter()))
    trailbeam.pika.install(span_processor=SimpleSpanProcessor(exp))
    try:
        orig = pika.channel.Channel.basic_publish._trailbeam_orig
        assert not hasattr(orig, "_trailbeam_orig")
        ch = fake.open_channel(fake.FakeConnection())
        ch.basic_publish("", "orders", b"x")
        assert len(exp.get_finished_spans()) == 1
    finally:
        trailbeam.pika.uninstall()
