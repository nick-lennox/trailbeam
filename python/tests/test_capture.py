"""Revision 3.2 (contract section 15): body size, tenant, FIFO, body capture with redaction, Lambda
batch and error text. Shared vectors in contract/span-contract-vectors.json; Node.js checks the same."""
import copy
import hashlib
import json
import uuid
from pathlib import Path

import botocore.client
import pytest
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

import trailbeam.aws_lambda as ql
import trailbeam.boto
from trailbeam import _capture

from sdk_constants import TEST_KEY

VECTORS = json.loads(
    (Path(__file__).resolve().parents[2] / "contract" / "span-contract-vectors.json").read_text("utf-8")
)["capture"]
ON = {"capture": {"body_queues": ["*"]}, "tenant": {"message_attribute": "tenant", "body_path": "customer.id"}}


def md5(s):
    return hashlib.md5(s.encode("utf-8")).hexdigest()


# -- shared vectors ----------------------------------------------------------------------------------

@pytest.mark.parametrize("v", VECTORS["redaction"], ids=lambda v: v["name"])
def test_redaction_vectors(v):
    config = _capture.resolve_config({}, v["capture"])
    assert _capture.redact_body(v["body"], config) == (v["redacted"], v["redacted_count"], v["format"])


@pytest.mark.parametrize("v", VECTORS["tenant"], ids=lambda v: v["name"])
def test_tenant_vectors(v):
    got = _capture.tenant_from(_capture.resolve_config(v["tenant"], {}), v["message_attributes"], v["body"],
                               v["lambda_record"])
    assert (None if got is None else {"tenant": got[0], "source": got[1]}) == v["expected"]


@pytest.mark.parametrize("v", VECTORS["error_message"], ids=lambda v: v["name"])
def test_error_message_vectors(v):
    config = _capture.resolve_config({}, v["capture"])
    assert _capture.error_message(config, v["queue"], RuntimeError(v["message"])) == v["expected"]


def test_config_refuses_bad_values_and_reads_the_environment(monkeypatch):
    with pytest.raises(ValueError, match="redact key"):
        _capture.resolve_config({}, {"redact_keys": ["a b"]})
    with pytest.raises(ValueError, match="unknown redact pattern"):
        _capture.resolve_config({}, {"redact_patterns": ["nope"]})
    with pytest.raises(ValueError, match="max_body_bytes"):
        _capture.resolve_config({}, {"max_body_bytes": _capture.HARD_MAX_BODY_BYTES + 1})
    monkeypatch.setenv(_capture.ENV_BODY_QUEUES, "orders, payments")
    monkeypatch.setenv(_capture.ENV_TENANT_ATTRIBUTE, "tenant")
    monkeypatch.setenv(_capture.ENV_MAX_BODY_BYTES, "100")
    c = _capture.resolve_config()
    assert c.body_on("orders") and c.body_on("payments") and not c.body_on("other")
    assert (c.tenant_attribute, c.max_body_bytes) == ("tenant", 100)
    assert not _capture.resolve_config({}, {}).body_on("orders")  # explicit options replace the environment


def test_deep_json_fails_closed():
    config = _capture.resolve_config({}, {"body_queues": ["q"]})
    body = "[" * 200 + "]" * 200
    assert _capture.content_attrs(config, "q", body, None, None)["trailbeam.body.capture_status"] == "redaction_failed"


# -- client hooks against the emulator -----------------------------------------------------------------

@pytest.fixture
def spans_with():
    exporter = InMemorySpanExporter()

    def install(**options):
        trailbeam.boto.install(TEST_KEY, span_processor=SimpleSpanProcessor(exporter), **options)

        def get(name):
            return [s for s in exporter.get_finished_spans()
                    if s.instrumentation_scope.name == "trailbeam" and s.name == name]
        return get

    yield install
    trailbeam.boto.uninstall()


@pytest.fixture
def fifo_url(make_client):
    c = make_client()
    url = c.create_queue(QueueName=f"trailbeam-test-{uuid.uuid4().hex[:12]}.fifo",
                         Attributes={"FifoQueue": "true", "ContentBasedDeduplication": "true"})["QueueUrl"]
    yield url
    c.delete_queue(QueueUrl=url)


def test_send_and_receive_capture_redacted_body_and_tenant(spans_with, make_client, queue_url):
    spans = spans_with(**ON)
    c = make_client()
    body = '{"customer": {"id": "c-9"}, "password": "hunter2"}'
    c.send_message(QueueUrl=queue_url, MessageBody=body,
                   MessageAttributes={"tenant": {"DataType": "String", "StringValue": "acme"}})
    got = c.receive_message(QueueUrl=queue_url, WaitTimeSeconds=0)["Messages"][0]
    assert got["MessageAttributes"]["tenant"]["StringValue"] == "acme"  # the hook asked for the tenant attribute
    for s in spans("trailbeam send") + spans("trailbeam receive"):
        a = dict(s.attributes)
        assert a["trailbeam.body.capture_status"] == "captured", s.name
        assert a["trailbeam.body"] == '{"customer": {"id": "c-9"}, "password": "[REDACTED]"}'
        assert (a["trailbeam.body.format"], a["trailbeam.body.redacted_count"]) == ("json", 1)
        assert a["messaging.message.body.size"] == len(body)
        assert (a["trailbeam.tenant"], a["trailbeam.tenant.source"]) == ("acme", "message_attribute")


def test_md5_mismatch_drops_the_body():
    hooks = trailbeam.boto.SqsHooks(emitter=None, capture=_capture.resolve_config({}, {"body_queues": ["q"]}))
    a = hooks._sent("q", {"MessageBody": "hello"}, {"MD5OfMessageBody": md5("other")})
    assert a["trailbeam.body.capture_status"] == "md5_mismatch" and "trailbeam.body" not in a
    a = hooks._sent("q", {"MessageBody": "hello"}, {"MD5OfMessageBody": md5("hello")})
    assert a["trailbeam.body"] == "hello"


def test_batch_bodies_by_entry_id_and_fifo_fields(spans_with, make_client, fifo_url):
    spans = spans_with(**ON)
    c = make_client()
    c.send_message_batch(QueueUrl=fifo_url, Entries=[
        {"Id": "a", "MessageBody": "first", "MessageGroupId": "g1"},
        {"Id": "b", "MessageBody": "second", "MessageGroupId": "g2"},
    ])
    sent = {s.attributes["trailbeam.batch_entry_id"]: dict(s.attributes) for s in spans("trailbeam send")}
    assert {k: (v["trailbeam.body"], v["trailbeam.message_group_id"]) for k, v in sent.items()} == {
        "a": ("first", "g1"), "b": ("second", "g2")}
    # moto returns no SequenceNumber at all (send or receive); see test_sequence_number_from_the_responses.
    c.receive_message(QueueUrl=fifo_url, MaxNumberOfMessages=10, WaitTimeSeconds=0)
    for s in spans("trailbeam receive"):
        a = dict(s.attributes)
        mine = next(v for v in sent.values() if v["messaging.message.id"] == a["messaging.message.id"])
        assert a["trailbeam.message_group_id"] == mine["trailbeam.message_group_id"]


def test_sequence_number_from_the_responses():
    hooks = trailbeam.boto.SqsHooks(emitter=None, capture=_capture.Config())
    a = hooks._sent("q.fifo", {"MessageBody": "x", "MessageGroupId": "g"}, {"SequenceNumber": "18849496460467696128"})
    assert (a["trailbeam.message_group_id"], a["trailbeam.sequence_number"]) == ("g", "18849496460467696128")
    params = {"QueueUrl": "u"}
    trailbeam.boto.add_receive_attribute_names(params)
    assert {"MessageGroupId", "SequenceNumber"} <= set(params["MessageSystemAttributeNames"])


def test_status_off_limit_and_s3_pointer(spans_with, make_client, queue_url):
    pointer = '["software.amazon.payloadoffloading.PayloadS3Pointer",{"s3BucketName":"b","s3Key":"k"}]'
    name = queue_url.rsplit("/", 1)[-1]
    c = make_client()
    other = c.create_queue(QueueName=f"trailbeam-test-{uuid.uuid4().hex[:12]}")["QueueUrl"]
    spans = spans_with(capture={"body_queues": [name], "max_body_bytes": 100})
    c.send_message(QueueUrl=other, MessageBody="x")
    c.send_message(QueueUrl=queue_url, MessageBody="y" * 101)
    c.send_message(QueueUrl=queue_url, MessageBody=pointer)
    got = [(s.attributes["trailbeam.body.capture_status"], "trailbeam.body" in s.attributes) for s in spans("trailbeam send")]
    assert got == [("off", False), ("exceeds_capture_limit", False), ("offloaded_to_s3", True)]
    c.delete_queue(QueueUrl=other)


def test_the_hooks_never_make_an_sqs_call_of_their_own(spans_with, make_client, queue_url, monkeypatch):
    c = make_client()
    calls = []
    real = botocore.client.BaseClient._make_api_call

    def recording(self, operation, params):
        calls.append(operation)
        return real(self, operation, params)

    monkeypatch.setattr(botocore.client.BaseClient, "_make_api_call", recording)
    spans_with(tenant=ON["tenant"], capture={"body_queues": ["*"], "error_queues": ["*"]})
    c.send_message(QueueUrl=queue_url, MessageBody='{"customer": {"id": "c"}}')
    m = c.receive_message(QueueUrl=queue_url, WaitTimeSeconds=0)["Messages"][0]
    c.delete_message(QueueUrl=queue_url, ReceiptHandle=m["ReceiptHandle"])
    assert calls == ["SendMessage", "ReceiveMessage", "DeleteMessage"]


# -- Lambda wrapper ------------------------------------------------------------------------------------

def record(mid, body, attributes=None, message_attributes=None):
    return {"messageId": mid, "receiptHandle": f"rh-{mid}", "body": body, "md5OfBody": md5(body),
            "attributes": {"ApproximateReceiveCount": "1", **(attributes or {})},
            "messageAttributes": message_attributes or {}, "eventSource": "aws:sqs",
            "eventSourceARN": "arn:aws:sqs:us-east-2:123456789012:orders.fifo", "awsRegion": "us-east-2"}


EVENT = {"Records": [
    record("r1", '{"customer": {"id": "c-1"}}', {"MessageGroupId": "g", "SequenceNumber": "1"}),
    record("r2", "two", message_attributes={"tenant": {"stringValue": "acme", "dataType": "String"}}),
    record("r3", "three"),
]}
CTX = type("Ctx", (), {"aws_request_id": "req-1", "function_name": "fn"})()


def lam(fn, **options):
    exporter = InMemorySpanExporter()
    wrapped = ql.wrap(fn, receipt_hash_key=TEST_KEY, span_processor=SimpleSpanProcessor(exporter), **options)
    return wrapped, lambda name: [s for s in exporter.get_finished_spans() if s.name == name]


def test_lambda_batch_position_body_tenant_fifo():
    fn, spans = lam(lambda e, c: None, **ON)
    fn(copy.deepcopy(EVENT), CTX)
    got = [(a.get("trailbeam.batch.size"), a.get("trailbeam.batch.index"), a.get("trailbeam.body"),
            a.get("trailbeam.tenant"), a.get("trailbeam.message_group_id"))
           for a in (dict(s.attributes) for s in spans("trailbeam receive"))]
    assert got == [(3, 0, '{"customer": {"id": "c-1"}}', "c-1", "g"), (3, 1, "two", "acme", None),
                   (3, 2, "three", None, None)]


def test_lambda_reported_failure_count():
    fn, spans = lam(lambda e, c: {"batchItemFailures": [{"itemIdentifier": "r2"}]})
    fn(copy.deepcopy(EVENT), CTX)
    assert [(s.attributes["trailbeam.outcome"], s.attributes["trailbeam.batch.index"],
             s.attributes["trailbeam.batch.reported_failure_count"]) for s in spans("trailbeam outcome")] == [
        ("success", 0, 1), ("failed_reported", 1, 1), ("success", 2, 1)]
    fn, spans = lam(lambda e, c: {"batchItemFailures": [{"itemIdentifier": "nope"}]})
    fn(copy.deepcopy(EVENT), CTX)
    assert all("trailbeam.batch.reported_failure_count" not in s.attributes for s in spans("trailbeam outcome"))


def test_lambda_exception_message_redacted_only_when_on():
    def boom(e, c):
        raise TypeError("no card for ada@example.com")

    fn, spans = lam(boom, capture={"error_queues": ["orders.fifo"]})
    with pytest.raises(TypeError):
        fn(copy.deepcopy(EVENT), CTX)
    for s in spans("trailbeam outcome"):
        assert s.attributes["trailbeam.error_type"] == "TypeError"
        assert s.attributes["trailbeam.error_message"] == "no card for [REDACTED]"
        assert "trailbeam.batch.reported_failure_count" not in s.attributes
    fn, spans = lam(boom, capture={})
    with pytest.raises(TypeError):
        fn(copy.deepcopy(EVENT), CTX)
    assert all("trailbeam.error_message" not in s.attributes for s in spans("trailbeam outcome"))
