"""Ingest of RabbitMQ spans (contract/span-contract-amqp.md A1, A12; QV-306): the contract's ingest vectors, the
version rule for SQS spans, and the message key (systems.py)."""
import json
from pathlib import Path

import pytest

from trailbeam.server import systems
from trailbeam.server.otlp import to_record

from spans import T0, Consumer, to_proto
from test_ingest import get, post

VECTORS = json.loads((Path(__file__).resolve().parents[3] / "contract" / "span-contract-amqp-vectors.json")
                     .read_text("utf-8"))
TRACE, SPAN = "0" * 31 + "1", "0" * 15 + "1"


def record(attrs, start=T0, end=T0 + 1000):
    return to_record({"service.name": "orders-worker"}, TRACE, SPAN, start, end, attrs)


@pytest.mark.parametrize("case", VECTORS["ingest"], ids=lambda c: c["name"])
def test_the_contract_ingest_vectors(case):
    rec, reason = record(case["attributes"])
    if case["accept"]:
        assert rec is not None and reason == "", reason
    else:
        assert rec is None and reason.startswith(case["reason_starts_with"]), reason


def _vector(kind):
    return dict(next(c for c in VECTORS["ingest"] if c["accept"] and c["attributes"]["trailbeam.kind"] == kind)
                ["attributes"])


def test_a_delivery_is_a_receive_keyed_by_system_and_message_id():
    rec, _ = record(_vector("amqp.deliver"))
    assert rec["kind"] == "receive" and rec["message_id"] == "~rabbitmq~m-1" and rec["queue"] == "orders"
    assert rec["receipt_hmac"] == "3f1b0c9e-58a2-4c1e-9f33-6a0d7e2b4c55:1" and rec["key_id"] == "amqp"
    assert rec["receive_count"] == 1 and rec["start_ns"] is None  # zero-length, start not observed (A4)
    assert rec["attributes"]["messaging.message.id"] == "m-1"  # the span's own ID is kept as sent


def test_a_receive_count_without_its_source_is_no_count():
    a = _vector("amqp.deliver")
    del a["trailbeam.receive_count.source"]
    rec, reason = record(a)
    assert reason == "" and rec["receive_count"] is None


@pytest.mark.parametrize("kind,requeue,expect", [
    ("amqp.nack", True, "change_visibility"), ("amqp.nack", False, "reject"),
    ("amqp.reject", True, "change_visibility"), ("amqp.reject", False, "reject"),
])
def test_requeue_decides_released_or_refused(kind, requeue, expect):
    a = {**_vector("amqp.ack"), "trailbeam.kind": kind, "messaging.operation.name": "basic." + kind[5:],
         "trailbeam.amqp.requeue": requeue}
    rec, reason = record(a)
    assert reason == "" and rec["kind"] == expect
    assert rec["visibility_timeout"] == (0 if requeue else None)


def test_sqs_spans_of_schema_version_4_are_read_as_3():
    c = Consumer()
    sp = c.receive(T0, "m1", "rh-1")
    sp.attrs["trailbeam.schema_version"] = 4
    rec, reason = record(sp.attrs)
    assert reason == "" and rec["kind"] == "receive" and rec["message_id"] == "m1"


def test_an_sqs_span_with_version_4_is_stored(server, db):
    c = Consumer()
    sp = c.send(T0, "m1")
    sp.attrs["trailbeam.schema_version"] = 4
    status, _, _ = post(server, to_proto([sp]).SerializeToString())
    assert status == 200 and [r["mid"] for r in db.query("SELECT mid FROM spans")] == ["m1"]


@pytest.mark.parametrize("system,mid,key", [
    (systems.SQS, "0f1e2d3c-aaaa-bbbb-cccc-000000000001", "0f1e2d3c-aaaa-bbbb-cccc-000000000001"),
    (systems.RABBITMQ, "order-17", "~rabbitmq~order-17"),
    (systems.RABBITMQ, "~rabbitmq~x", "~rabbitmq~~rabbitmq~x"),
    (systems.SQS, "~rabbitmq~x", "~aws.sqs~~rabbitmq~x"),  # an SQS ID never takes a RabbitMQ key
    (systems.SQS, "~", "~aws.sqs~~"),
])
def test_message_keys_are_one_to_one(system, mid, key):
    assert systems.message_key(system, mid) == key
    assert systems.split(key) == (system, mid)


def test_a_key_no_system_wrote_is_an_sqs_id():
    assert systems.split("~nope~x") == (systems.SQS, "~nope~x")
    assert systems.split("~rabbitmq~") == (systems.SQS, "~rabbitmq~")
    assert systems.message_key(systems.RABBITMQ, None) is None
    with pytest.raises(ValueError):
        systems.message_key("kafka", "x")


def test_the_fixture_through_the_server_and_a_rabbitmq_message_by_its_key(server, db):
    import sys
    import urllib.request
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "conformance"))
    from test_amqp_conformance import FIXTURE, otlp_json

    status, _, body = post(server, otlp_json(FIXTURE["spans"]), ctype="application/json")
    assert status == 200 and "partialSuccess" not in json.loads(body or b"{}")
    status, t = get(server, "/api/messages/~rabbitmq~m-happy")
    assert status == 200 and t["message_id"] == "~rabbitmq~m-happy"
    assert [e["type"] for e in t["events"] if not e["inferred"]] == ["sent", "received", "deleted"]
    assert get(server, "/api/messages/m-happy")[0] == 404  # no SQS message has that ID
    with urllib.request.urlopen(server + "/message?id=~rabbitmq~m-happy", timeout=5) as r:
        assert r.status == 200 and b"m-happy" in r.read()
