"""Revision 3.6 (contract section 25): EventBridge PutEvents. One trailbeam put_event per request entry, accepted or
failed. Shared vectors in contract/span-contract-vectors.json ("eventbridge"); Node.js checks the same. The end-to-end
test runs PutEvents to a rule with an SQS target in moto."""
import copy
import json
from pathlib import Path

import boto3
import pytest
from botocore.exceptions import EndpointConnectionError
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

import trailbeam.boto
from trailbeam import _capture
from sdk_constants import TEST_KEY

VECTORS = json.loads(
    (Path(__file__).resolve().parents[2] / "contract" / "span-contract-vectors.json").read_text("utf-8"))["eventbridge"]
PUT = VECTORS["put_events"]


@pytest.fixture
def hooks():
    exporter = InMemorySpanExporter()
    trailbeam.boto.install(TEST_KEY, span_processor=SimpleSpanProcessor(exporter))

    def spans():
        return [s for s in exporter.get_finished_spans() if s.instrumentation_scope.name == "trailbeam"]
    yield trailbeam.boto._state["hooks"], spans
    trailbeam.boto.uninstall()


def check(got, case):
    assert len(got) == len(case["spans"]), [dict(s.attributes) for s in got]
    for s, want in zip(got, case["spans"]):
        assert s.name == "trailbeam put_event"
        a = dict(s.attributes)
        assert {k: a.get(k) for k in want} == want
        extra = set(a) - set(want) - set(PUT["present"]) - {"thread.name"}
        assert not extra, extra
        assert set(PUT["present"]) <= set(a)
        assert not set(PUT["absent"]) & set(a)
        assert a["trailbeam.order.issued"] < a["trailbeam.order.returned"]


@pytest.mark.parametrize("case", PUT["cases"], ids=lambda c: c["name"])
def test_put_events_vectors(hooks, case):
    h, spans = hooks
    span = object()
    params = copy.deepcopy(case["request"])
    h.request_hook(span, "events", "PutEvents", params)
    assert params == case["request"]  # the request is never changed
    if "error" in case:
        result = {"Error": {"Code": case["error"], "Message": "x"}}
    else:
        result = copy.deepcopy(case["response"])
    h.response_hook(span, "events", "PutEvents", result)
    assert params == case["request"]
    check(spans(), case)
    assert h._pending == {}


def test_a_call_that_raised_fails_every_entry_with_the_exception_type(hooks):
    h, spans = hooks
    span = object()
    params = {"Entries": [{"Source": "a.b", "DetailType": "T", "Detail": "{}"}]}
    h.request_hook(span, "events", "PutEvents", params)
    try:
        raise EndpointConnectionError(endpoint_url="http://nowhere")
    except EndpointConnectionError:
        h.response_hook(span, "events", "PutEvents", None)  # botocore calls the hook from a finally
    [s] = spans()
    assert s.attributes["trailbeam.result"] == "failed"
    assert s.attributes["trailbeam.error_code"] == "EndpointConnectionError"


def test_other_eventbridge_operations_have_no_span(hooks):
    h, spans = hooks
    span = object()
    h.request_hook(span, "events", "PutRule", {"Name": "r"})
    h.response_hook(span, "events", "PutRule", {"RuleArn": "arn:aws:events:us-east-1:111122223333:rule/r"})
    assert spans() == [] and h._pending == {}


def test_bus_names():
    assert _capture.event_bus_name(None) == "default"
    assert _capture.event_bus_name("orders") == "orders"
    assert _capture.event_bus_name("arn:aws:events:us-east-1:111122223333:event-bus/orders") == "orders"


def test_put_events_end_to_end_in_moto(monkeypatch):
    """A real boto3 events client: the span's event ID is the id of the event the rule's SQS target receives."""
    moto = pytest.importorskip("moto")
    for k, v in (("AWS_ACCESS_KEY_ID", "x"), ("AWS_SECRET_ACCESS_KEY", "x"), ("AWS_DEFAULT_REGION", "us-east-1")):
        monkeypatch.setenv(k, v)
    exporter = InMemorySpanExporter()
    trailbeam.boto.install(TEST_KEY, span_processor=SimpleSpanProcessor(exporter))
    try:
        with moto.mock_aws():
            sqs, events = boto3.client("sqs"), boto3.client("events")
            url = sqs.create_queue(QueueName="orders")["QueueUrl"]
            arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
            events.put_rule(Name="r", EventPattern=json.dumps({"source": ["com.example.orders"]}))
            events.put_targets(Rule="r", Targets=[{"Id": "t", "Arn": arn}])
            entries = [{"Source": "com.example.orders", "DetailType": "OrderPlaced", "Detail": '{"order": 1}'},
                       {"Source": "com.example.orders", "Detail": "{}"}]  # no DetailType: moto fails the entry
            r = events.put_events(Entries=entries)
            body = json.loads(sqs.receive_message(QueueUrl=url)["Messages"][0]["Body"])
    finally:
        trailbeam.boto.uninstall()
    puts = [s for s in exporter.get_finished_spans() if s.name == "trailbeam put_event"]
    assert [s.attributes["trailbeam.result"] for s in puts] == ["ok", "failed"]
    ok, failed = (dict(s.attributes) for s in puts)
    assert ok["messaging.message.id"] == r["Entries"][0]["EventId"] == body["id"]
    assert ok["messaging.destination.name"] == "default" and ok["trailbeam.eventbridge.detail_type"] == "OrderPlaced"
    assert failed["trailbeam.error_code"] == r["Entries"][1]["ErrorCode"] == "InvalidArgument"
    assert "messaging.message.id" not in failed
    assert not any("order" in str(v) for s in puts for v in s.attributes.values() if isinstance(v, str)
                   and v.startswith("{"))  # Detail is never recorded
