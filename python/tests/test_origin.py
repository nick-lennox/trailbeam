"""Revision 3.5 (contract section 23): what a receive records from an EventBridge event or an S3 event notification
body. Shared vectors in contract/span-contract-vectors.json ("origin"); Node.js checks the same."""
import json
from pathlib import Path

import pytest
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

import trailbeam.aws_lambda as ql
import trailbeam.boto
from trailbeam import _capture

from sdk_constants import TEST_KEY

VECTORS = json.loads(
    (Path(__file__).resolve().parents[2] / "contract" / "span-contract-vectors.json").read_text("utf-8"))["origin"]
PREFIXES = ("trailbeam.eventbridge.", "trailbeam.s3.")


def origin_only(attrs):
    return {k: v for k, v in attrs.items() if k.startswith(PREFIXES)}


def capture_for(case):
    return {"body_queues": ["orders"] if case["body_captured"] else []}


@pytest.mark.parametrize("case", VECTORS["receive"], ids=lambda c: c["name"])
def test_origin_vectors(case):
    config = _capture.resolve_config({}, capture_for(case), {})
    assert _capture.origin_receive_attrs(config, "orders", case["body"]) == case["attributes"]


@pytest.mark.parametrize("case", VECTORS["receive"], ids=lambda c: c["name"])
def test_origin_vectors_client_hooks(case):
    exporter = InMemorySpanExporter()
    trailbeam.boto.install(TEST_KEY, span_processor=SimpleSpanProcessor(exporter), capture=capture_for(case))
    try:
        hooks = trailbeam.boto._state["hooks"]
        span = object()
        hooks.request_hook(span, "sqs", "ReceiveMessage", {"QueueUrl": "http://localhost:9324/000000000000/orders"})
        m = {"MessageId": "sqs-1", "ReceiptHandle": "rh-1", "Body": case["body"],
             "Attributes": {"ApproximateReceiveCount": "1", "SentTimestamp": "1613511680100"}}
        hooks.response_hook(span, "sqs", "ReceiveMessage", {"Messages": [m]})
    finally:
        trailbeam.boto.uninstall()
    [s] = [s for s in exporter.get_finished_spans() if s.name == "trailbeam receive"]
    assert origin_only(dict(s.attributes)) == case["attributes"]


@pytest.mark.parametrize("case", VECTORS["receive"], ids=lambda c: c["name"])
def test_origin_vectors_lambda_wrapper(case):
    exporter = InMemorySpanExporter()
    fn = ql.wrap(lambda e, c: None, receipt_hash_key=TEST_KEY, span_processor=SimpleSpanProcessor(exporter),
                 capture=capture_for(case))
    fn({"Records": [{"messageId": "sqs-1", "receiptHandle": "rh-1", "body": case["body"],
                     "attributes": {"ApproximateReceiveCount": "1"}, "messageAttributes": {},
                     "eventSource": "aws:sqs", "eventSourceARN": "arn:aws:sqs:us-east-2:111122223333:orders"}]},
       type("Ctx", (), {"aws_request_id": "req-1", "function_name": "fn"})())
    [s] = [s for s in exporter.get_finished_spans() if s.name == "trailbeam receive"]
    assert origin_only(dict(s.attributes)) == case["attributes"]


def test_nothing_else_from_the_body_is_exported_when_capture_is_off():
    """With body capture off, the only body-derived attributes are the section 23 ones and the capture status."""
    case = VECTORS["receive"][0]
    config = _capture.resolve_config({}, {}, {})
    attrs = _capture.content_attrs(config, "orders", case["body"], None, None)
    assert not any(isinstance(v, str) and ("terminated" in v or "i-1234567890abcdef0" in v) for v in attrs.values())
    out = _capture.origin_receive_attrs(config, "orders", case["body"])
    assert not any("i-1234567890abcdef0" in str(v) or "terminated" in str(v) for v in out.values())


def test_an_sns_envelope_wins():
    """A body that is an SNS envelope is never also read as an EventBridge event or an S3 notification."""
    sns_case = next(c for c in json.loads(
        (Path(__file__).resolve().parents[2] / "contract" / "span-contract-vectors.json").read_text("utf-8"))["sns"]["receive"]
        if c["name"].startswith("envelope:"))
    doc = json.loads(sns_case["body"])
    doc.update({"id": "6a7e8feb-b491-4cf7-a9f1-bf3703467718", "source": "aws.ec2", "detail-type": "x",
                "account": "111122223333", "detail": {}})
    exporter = InMemorySpanExporter()
    trailbeam.boto.install(TEST_KEY, span_processor=SimpleSpanProcessor(exporter))
    try:
        hooks = trailbeam.boto._state["hooks"]
        span = object()
        hooks.request_hook(span, "sqs", "ReceiveMessage", {"QueueUrl": "http://localhost:9324/000000000000/orders"})
        hooks.response_hook(span, "sqs", "ReceiveMessage", {"Messages": [
            {"MessageId": "sqs-1", "ReceiptHandle": "rh-1", "Body": json.dumps(doc), "Attributes": {}}]})
    finally:
        trailbeam.boto.uninstall()
    [s] = [s for s in exporter.get_finished_spans() if s.name == "trailbeam receive"]
    assert "trailbeam.sns.message_id" in s.attributes
    assert origin_only(dict(s.attributes)) == {}
