"""trailbeam.boto hooks against a local SQS emulator: one span per message, as contract/span-contract.md says."""
import hashlib
import hmac
import json
import logging
import os
from pathlib import Path

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError
from opentelemetry.instrumentation.botocore import BotocoreInstrumentor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.sdk.trace.sampling import ALWAYS_OFF

import trailbeam
import trailbeam.boto
from trailbeam import _contract, _order
from trailbeam._handles import HandleMap

from sdk_constants import TEST_KEY

KEY_ID = "05769f58"  # key ID of TEST_KEY, from contract/span-contract-vectors.json
COMMON = {"trailbeam.schema_version", "trailbeam.kind", "trailbeam.source", "messaging.system.name", "messaging.operation.type",
          "messaging.operation.name", "messaging.destination.name", "aws.sqs.queue.url", "messaging.client.id",
          "process.pid", "thread.name", "trailbeam.order.scope", "trailbeam.order.issued", "trailbeam.order.returned"}


def h(rh, key=TEST_KEY):
    return hmac.new(key.encode(), rh.encode(), hashlib.sha256).hexdigest()[:32]


@pytest.fixture
def trailbeam_spans():
    exporter = InMemorySpanExporter()
    trailbeam.boto.install(TEST_KEY, span_processor=SimpleSpanProcessor(exporter))

    def get(name=None):
        out = [s for s in exporter.get_finished_spans() if s.instrumentation_scope.name == "trailbeam"]
        return [s for s in out if name is None or s.name == name]

    get.exporter = exporter
    yield get
    trailbeam.boto.uninstall()


def stock_spans(exporter):
    return [s for s in exporter.get_finished_spans() if s.instrumentation_scope.name != "trailbeam"]


def check_common(span, op, queue_url, kind, op_type):
    a = dict(span.attributes)
    assert COMMON <= set(a), COMMON - set(a)
    assert a["trailbeam.schema_version"] == 3
    assert a["trailbeam.kind"] == kind
    assert a["trailbeam.source"] == "client"
    assert a["messaging.system.name"] == "aws.sqs"
    assert a["messaging.operation.type"] == op_type
    assert a["messaging.operation.name"] == op
    assert a["aws.sqs.queue.url"] == queue_url
    assert a["messaging.destination.name"] == queue_url.rsplit("/", 1)[-1]
    assert a["process.pid"] == os.getpid()
    assert a["messaging.client.id"] == span.resource.attributes["service.instance.id"]
    assert span.instrumentation_scope.version == trailbeam.__version__
    # Contract v2 section 2: an interval from just before the request to the response.
    assert span.start_time < span.end_time
    assert "trailbeam.start_observed" not in a
    # Contract v3: call order inside this process.
    assert a["trailbeam.order.scope"] == _order.scope
    assert 0 < a["trailbeam.order.issued"] < a["trailbeam.order.returned"]
    for k, v in a.items():  # absent values are omitted, never "" or -1
        assert v != "" and v != -1, k
    # The span name never carries the queue name.
    assert span.name in ("trailbeam send", "trailbeam receive", "trailbeam delete", "trailbeam change_visibility",
                         "trailbeam purge")


def check_parent(span, exporter):
    parents = {s.context.span_id: s for s in stock_spans(exporter)}
    assert span.parent is not None and span.parent.span_id in parents
    assert parents[span.parent.span_id].context.trace_id == span.context.trace_id


def no_receipt_handles(spans, handles):
    for s in spans:
        for v in s.attributes.values():
            assert not any(isinstance(v, str) and rh in v for rh in handles)


# -- send ----------------------------------------------------------------------------------------

def test_send_message(trailbeam_spans, make_client, queue_url):
    mid = make_client().send_message(QueueUrl=queue_url, MessageBody="x")["MessageId"]
    [s] = trailbeam_spans("trailbeam send")
    check_common(s, "SendMessage", queue_url, "sqs.send", "send")
    check_parent(s, trailbeam_spans.exporter)
    assert s.attributes["messaging.message.id"] == mid
    assert "trailbeam.batch_entry_id" not in s.attributes


def test_send_message_batch_one_span_per_entry(trailbeam_spans, make_client, queue_url):
    r = make_client().send_message_batch(
        QueueUrl=queue_url, Entries=[{"Id": f"e{i}", "MessageBody": str(i)} for i in range(3)])
    spans = trailbeam_spans("trailbeam send")
    assert len(spans) == 3
    got = {s.attributes["trailbeam.batch_entry_id"]: s.attributes["messaging.message.id"] for s in spans}
    assert got == {e["Id"]: e["MessageId"] for e in r["Successful"]}
    assert len({s.parent.span_id for s in spans}) == 1
    for s in spans:
        check_common(s, "SendMessageBatch", queue_url, "sqs.send", "send")


def test_failed_send_batch_entries_produce_no_span(trailbeam_spans):
    hooks = trailbeam.boto._state["hooks"]
    params = {"QueueUrl": "http://localhost/000000000000/q", "Entries": []}
    span = object()
    hooks.request_hook(span, "sqs", "SendMessageBatch", params)
    hooks.response_hook(span, "sqs", "SendMessageBatch", {
        "Successful": [{"Id": "a", "MessageId": "m-a"}],
        "Failed": [{"Id": "b", "Code": "InvalidParameterValue", "SenderFault": True}]})
    assert [s.attributes["trailbeam.batch_entry_id"] for s in trailbeam_spans("trailbeam send")] == ["a"]


# -- receive -------------------------------------------------------------------------------------

def test_receive_one_span_per_message(trailbeam_spans, make_client, queue_url):
    c = make_client()
    sent = {c.send_message(QueueUrl=queue_url, MessageBody=str(i))["MessageId"] for i in range(3)}
    msgs = c.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10)["Messages"]
    assert {m["MessageId"] for m in msgs} == sent
    spans = trailbeam_spans("trailbeam receive")
    assert len(spans) == 3
    by_id = {s.attributes["messaging.message.id"]: s for s in spans}
    for m in msgs:
        s = by_id[m["MessageId"]]
        check_common(s, "ReceiveMessage", queue_url, "sqs.receive", "receive")
        check_parent(s, trailbeam_spans.exporter)
        a = s.attributes
        assert a["trailbeam.receipt_handle.hmac"] == h(m["ReceiptHandle"])
        assert a["trailbeam.receipt_handle.key_id"] == KEY_ID
        assert a["trailbeam.receive_count"] == 1
        assert isinstance(a["trailbeam.sent_timestamp_ms"], int)
        assert isinstance(a["trailbeam.first_receive_timestamp_ms"], int)
        assert "trailbeam.aws_trace_header" not in a  # none was sent; omitted, not ""
        assert "trailbeam.dead_letter_queue_source_arn" not in a  # not a dead-letter queue
    no_receipt_handles(spans, [m["ReceiptHandle"] for m in msgs])


def test_receive_count_and_trace_header(trailbeam_spans, make_client, queue_url):
    c = make_client()
    hdr = "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1"
    c.send_message(QueueUrl=queue_url, MessageBody="x",
                   MessageSystemAttributes={"AWSTraceHeader": {"StringValue": hdr, "DataType": "String"}})
    m1 = c.receive_message(QueueUrl=queue_url, VisibilityTimeout=0)["Messages"][0]
    m2 = c.receive_message(QueueUrl=queue_url)["Messages"][0]
    s1, s2 = trailbeam_spans("trailbeam receive")
    assert (s1.attributes["trailbeam.receive_count"], s2.attributes["trailbeam.receive_count"]) == (1, 2)
    assert s2.attributes["trailbeam.aws_trace_header"] == hdr
    assert s1.attributes["trailbeam.receipt_handle.hmac"] == h(m1["ReceiptHandle"])
    assert s2.attributes["trailbeam.receipt_handle.hmac"] == h(m2["ReceiptHandle"])


def test_dead_letter_queue_source_arn_is_copied_verbatim(trailbeam_spans):
    """Schema 3.1: SQS sets DeadLetterQueueSourceArn only on a receive from a dead-letter queue. moto does
    not fill it, so the response is given here; the Node.js tests check it against ElasticMQ."""
    hooks = trailbeam.boto._state["hooks"]
    arn = "arn:aws:sqs:us-east-1:123456789012:orders"
    params = {"QueueUrl": "http://localhost:9324/000000000000/orders-dlq"}
    span = object()
    hooks.request_hook(span, "sqs", "ReceiveMessage", params)
    assert "DeadLetterQueueSourceArn" in params["MessageSystemAttributeNames"]
    hooks.response_hook(span, "sqs", "ReceiveMessage", {"Messages": [
        {"MessageId": "m1", "ReceiptHandle": "rh-1", "Attributes": {"ApproximateReceiveCount": "4",
                                                                 "DeadLetterQueueSourceArn": arn}},
        {"MessageId": "m2", "ReceiptHandle": "rh-2", "Attributes": {"DeadLetterQueueSourceArn": ""}},
    ]})
    got = [s.attributes.get("trailbeam.dead_letter_queue_source_arn") for s in trailbeam_spans("trailbeam receive")]
    assert got == [arn, None]  # empty: omitted, never ""


def test_receive_keeps_what_the_caller_asked_for(trailbeam_spans, make_client, queue_url):
    c = make_client()
    c.send_message(QueueUrl=queue_url, MessageBody="x")
    m = c.receive_message(QueueUrl=queue_url, MessageSystemAttributeNames=["SenderId"])["Messages"][0]
    assert "SenderId" in m["Attributes"]
    assert "ApproximateReceiveCount" in m["Attributes"]
    assert "MessageAttributes" not in m  # Trailbeam never asks for message attributes


def test_empty_receive_produces_no_span(trailbeam_spans, make_client, queue_url):
    assert "Messages" not in make_client().receive_message(QueueUrl=queue_url, WaitTimeSeconds=0)
    assert trailbeam_spans() == []


@pytest.mark.parametrize("given,expected", [
    ({}, {"MessageSystemAttributeNames": list(trailbeam.boto.RECEIVE_SYSTEM_ATTRIBUTES)}),
    ({"MessageSystemAttributeNames": ["All"]}, {"MessageSystemAttributeNames": ["All"]}),
    ({"AttributeNames": ["All"]}, {"AttributeNames": ["All"]}),
    ({"MessageSystemAttributeNames": ["SenderId", "SentTimestamp"]},
     {"MessageSystemAttributeNames": ["SenderId", "SentTimestamp", "ApproximateReceiveCount",
                                      "ApproximateFirstReceiveTimestamp", "AWSTraceHeader",
                                      "DeadLetterQueueSourceArn", "MessageGroupId", "SequenceNumber"]}),
    ({"AttributeNames": ["SenderId"]},
     {"AttributeNames": ["SenderId"] + list(trailbeam.boto.RECEIVE_SYSTEM_ATTRIBUTES)}),
    ({"MessageAttributeNames": ["tenant"]},
     {"MessageAttributeNames": ["tenant"],
      "MessageSystemAttributeNames": list(trailbeam.boto.RECEIVE_SYSTEM_ATTRIBUTES)}),
])
def test_add_receive_attribute_names(given, expected):
    original = json.loads(json.dumps(given))
    params = dict(given, QueueUrl="u")
    trailbeam.boto.add_receive_attribute_names(params)
    assert {k: v for k, v in params.items() if k != "QueueUrl"} == expected
    assert given == original  # the caller's lists are not modified in place


# -- delete / change visibility ------------------------------------------------------------------

def receive_one(c, url, **kw):
    c.send_message(QueueUrl=url, MessageBody="x")
    return c.receive_message(QueueUrl=url, **kw)["Messages"][0]


def test_delete_message_joins_the_receive(trailbeam_spans, make_client, queue_url):
    c = make_client()
    m = receive_one(c, queue_url)
    hooks = trailbeam.boto._state["hooks"]
    assert hooks.handles.get(m["ReceiptHandle"]) == m["MessageId"]
    c.delete_message(QueueUrl=queue_url, ReceiptHandle=m["ReceiptHandle"])
    [r] = trailbeam_spans("trailbeam receive")
    [d] = trailbeam_spans("trailbeam delete")
    check_common(d, "DeleteMessage", queue_url, "sqs.settle", "settle")
    check_parent(d, trailbeam_spans.exporter)
    a = d.attributes
    assert a["trailbeam.receipt_handle.hmac"] == r.attributes["trailbeam.receipt_handle.hmac"]
    assert a["trailbeam.receipt_handle.key_id"] == KEY_ID
    assert a["messaging.message.id"] == m["MessageId"]
    assert a["trailbeam.result"] == "ok"
    assert "trailbeam.error_code" not in a and "trailbeam.batch_entry_id" not in a
    assert hooks.handles.get(m["ReceiptHandle"]) is None  # dropped after a successful delete
    no_receipt_handles(trailbeam_spans(), [m["ReceiptHandle"]])


def test_delete_with_invalid_handle_is_failed(trailbeam_spans, make_client, queue_url):
    with pytest.raises(ClientError):
        make_client().delete_message(QueueUrl=queue_url, ReceiptHandle="not-a-handle")
    [d] = trailbeam_spans("trailbeam delete")
    assert d.attributes["trailbeam.result"] == "failed"
    assert d.attributes["trailbeam.error_code"] == "ReceiptHandleIsInvalid"
    assert d.attributes["trailbeam.receipt_handle.hmac"] == h("not-a-handle")
    assert "messaging.message.id" not in d.attributes  # not received by this process


def test_delete_batch_mixed_results(trailbeam_spans, make_client, queue_url):
    c = make_client()
    m = receive_one(c, queue_url)
    c.delete_message_batch(QueueUrl=queue_url, Entries=[
        {"Id": "good", "ReceiptHandle": m["ReceiptHandle"]}, {"Id": "bad", "ReceiptHandle": "bogus"}])
    spans = {s.attributes["trailbeam.batch_entry_id"]: s for s in trailbeam_spans("trailbeam delete")}
    assert set(spans) == {"good", "bad"}
    assert len({s.parent.span_id for s in spans.values()}) == 1
    good, bad = spans["good"].attributes, spans["bad"].attributes
    check_common(spans["good"], "DeleteMessageBatch", queue_url, "sqs.settle", "settle")
    assert (good["trailbeam.result"], good["messaging.message.id"]) == ("ok", m["MessageId"])
    assert "trailbeam.error_code" not in good
    assert (bad["trailbeam.result"], bad["trailbeam.error_code"]) == ("failed", "ReceiptHandleIsInvalid")


def test_change_visibility_zero_is_recorded(trailbeam_spans, make_client, queue_url):
    c = make_client()
    m = receive_one(c, queue_url)
    c.change_message_visibility(QueueUrl=queue_url, ReceiptHandle=m["ReceiptHandle"], VisibilityTimeout=0)
    [s] = trailbeam_spans("trailbeam change_visibility")
    check_common(s, "ChangeMessageVisibility", queue_url, "sqs.settle", "settle")
    a = s.attributes
    assert a["trailbeam.visibility_timeout"] == 0
    assert a["trailbeam.result"] == "ok"
    assert a["messaging.message.id"] == m["MessageId"]
    # change_visibility does not drop the handle from the map; the message is still held.
    assert trailbeam.boto._state["hooks"].handles.get(m["ReceiptHandle"]) == m["MessageId"]


def test_change_visibility_batch(trailbeam_spans, make_client, queue_url):
    c = make_client()
    m = receive_one(c, queue_url)
    c.change_message_visibility_batch(QueueUrl=queue_url, Entries=[
        {"Id": "a", "ReceiptHandle": m["ReceiptHandle"], "VisibilityTimeout": 30},
        {"Id": "b", "ReceiptHandle": "bogus", "VisibilityTimeout": 5}])
    spans = {s.attributes["trailbeam.batch_entry_id"]: s.attributes for s in trailbeam_spans("trailbeam change_visibility")}
    assert spans["a"]["trailbeam.visibility_timeout"] == 30 and spans["a"]["trailbeam.result"] == "ok"
    assert spans["b"]["trailbeam.visibility_timeout"] == 5 and spans["b"]["trailbeam.result"] == "failed"
    assert spans["b"]["trailbeam.error_code"] == "ReceiptHandleIsInvalid"


def test_settle_that_throws_before_a_response(trailbeam_spans, make_client):
    c = make_client("http://127.0.0.1:9", retries={"max_attempts": 1}, connect_timeout=1)
    url = "http://127.0.0.1:9/000000000000/nowhere"
    with pytest.raises(EndpointConnectionError):
        c.delete_message_batch(QueueUrl=url, Entries=[
            {"Id": "a", "ReceiptHandle": "rh-a"}, {"Id": "b", "ReceiptHandle": "rh-b"}])
    with pytest.raises(EndpointConnectionError):
        c.change_message_visibility(QueueUrl=url, ReceiptHandle="rh-c", VisibilityTimeout=0)
    spans = trailbeam_spans()
    assert [s.name for s in spans] == ["trailbeam delete", "trailbeam delete", "trailbeam change_visibility"]
    for s in spans:
        assert s.attributes["trailbeam.result"] == "failed"
        assert s.attributes["trailbeam.error_code"] == "EndpointConnectionError"
    assert trailbeam.boto._state["hooks"]._pending == {}


def test_other_services_and_operations_are_ignored(trailbeam_spans, make_client, queue_url):
    make_client().get_queue_attributes(QueueUrl=queue_url, AttributeNames=["All"])
    assert trailbeam_spans() == []
    assert trailbeam.boto._state["hooks"]._pending == {}


# -- interval (contract v2, section 2) ------------------------------------------------------------

class _Clock:
    """A fake clock for the hooks: each call returns the next tick."""

    def __init__(self, monkeypatch, start=1_000):
        self.t = start
        monkeypatch.setattr(trailbeam.boto, "_now", self)

    def __call__(self):
        self.t += 1
        return self.t


def test_interval_starts_before_the_request_and_ends_after_the_response(trailbeam_spans, make_client, queue_url):
    import time as real_time
    c = make_client()
    before = real_time.time_ns()
    c.send_message(QueueUrl=queue_url, MessageBody="x")
    msgs = c.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)["Messages"]
    c.delete_message(QueueUrl=queue_url, ReceiptHandle=msgs[0]["ReceiptHandle"])
    after = real_time.time_ns()
    by_name = {s.name: s for s in trailbeam_spans()}
    stock = {s.context.span_id: s for s in stock_spans(trailbeam_spans.exporter)}
    for name in ("trailbeam send", "trailbeam receive", "trailbeam delete"):
        s = by_name[name]
        assert before <= s.start_time < s.end_time <= after, name
        # The stock span covers the same call: ours lies within it.
        p = stock[s.parent.span_id]
        assert p.start_time <= s.start_time and s.end_time <= p.end_time, name
    # The spans of one thread's sequential calls do not overlap.
    assert by_name["trailbeam send"].end_time < by_name["trailbeam receive"].start_time
    assert by_name["trailbeam receive"].end_time < by_name["trailbeam delete"].start_time


def test_interval_uses_the_request_hook_time_and_the_response_hook_time(trailbeam_spans, monkeypatch):
    clock = _Clock(monkeypatch)
    hooks = trailbeam.boto._state["hooks"]
    url = "http://localhost/000000000000/q"
    span = object()
    hooks.request_hook(span, "sqs", "ReceiveMessage", {"QueueUrl": url})   # t = 1001
    clock.t = 5_000
    hooks.response_hook(span, "sqs", "ReceiveMessage", {"Messages": [       # t = 5001
        {"MessageId": "m1", "ReceiptHandle": "rh1", "Attributes": {"ApproximateReceiveCount": "1"}},
        {"MessageId": "m2", "ReceiptHandle": "rh2", "Attributes": {"ApproximateReceiveCount": "1"}}]})
    spans = trailbeam_spans("trailbeam receive")
    assert [(s.start_time, s.end_time) for s in spans] == [(1001, 5001), (1001, 5001)]


def test_batch_entries_share_the_call_interval(trailbeam_spans, monkeypatch):
    clock = _Clock(monkeypatch)
    hooks = trailbeam.boto._state["hooks"]
    span = object()
    hooks.request_hook(span, "sqs", "DeleteMessageBatch", {"QueueUrl": "http://h/0/q", "Entries": [
        {"Id": "a", "ReceiptHandle": "rh-a"}, {"Id": "b", "ReceiptHandle": "rh-b"}]})
    clock.t = 2_000
    hooks.response_hook(span, "sqs", "DeleteMessageBatch", {"Successful": [{"Id": "a"}],
                                                            "Failed": [{"Id": "b", "Code": "X"}]})
    assert {(s.start_time, s.end_time) for s in trailbeam_spans("trailbeam delete")} == {(1001, 2001)}


def test_settle_that_throws_has_an_interval_too(trailbeam_spans, make_client):
    c = make_client("http://127.0.0.1:9", retries={"max_attempts": 1}, connect_timeout=1)
    with pytest.raises(EndpointConnectionError):
        c.delete_message(QueueUrl="http://127.0.0.1:9/000000000000/nowhere", ReceiptHandle="rh")
    [s] = trailbeam_spans("trailbeam delete")
    assert s.start_time < s.end_time
    assert s.attributes["trailbeam.order.scope"] == _order.scope
    assert s.attributes["trailbeam.order.issued"] < s.attributes["trailbeam.order.returned"]


# -- call order inside one process (contract v3) --------------------------------------------------

def order(span):
    a = span.attributes
    return a["trailbeam.order.issued"], a["trailbeam.order.returned"]


def test_order_issued_before_returned_and_sequential_calls_are_ordered(trailbeam_spans, monkeypatch):
    _Clock(monkeypatch)
    hooks = trailbeam.boto._state["hooks"]
    url = "http://localhost/000000000000/q"
    first, second = object(), object()
    hooks.request_hook(first, "sqs", "SendMessage", {"QueueUrl": url, "MessageBody": "x"})
    hooks.response_hook(first, "sqs", "SendMessage", {"MessageId": "m1"})
    hooks.request_hook(second, "sqs", "SendMessage", {"QueueUrl": url, "MessageBody": "y"})
    hooks.response_hook(second, "sqs", "SendMessage", {"MessageId": "m2"})
    a, b = trailbeam_spans("trailbeam send")
    assert order(a)[0] < order(a)[1] < order(b)[0] < order(b)[1]
    assert a.attributes["trailbeam.order.scope"] == b.attributes["trailbeam.order.scope"] == _order.scope


def test_order_of_overlapping_calls_is_not_invented(trailbeam_spans):
    hooks = trailbeam.boto._state["hooks"]
    url = "http://localhost/000000000000/q"
    first, second = object(), object()
    hooks.request_hook(first, "sqs", "SendMessage", {"QueueUrl": url})
    hooks.request_hook(second, "sqs", "SendMessage", {"QueueUrl": url})
    hooks.response_hook(first, "sqs", "SendMessage", {"MessageId": "m1"})
    hooks.response_hook(second, "sqs", "SendMessage", {"MessageId": "m2"})
    a, b = trailbeam_spans("trailbeam send")
    # Neither returned before the other was issued.
    assert order(a)[1] > order(b)[0] and order(b)[1] > order(a)[0]


def test_order_against_the_real_sdk(trailbeam_spans, make_client, queue_url):
    c = make_client()
    c.send_message(QueueUrl=queue_url, MessageBody="x")
    msgs = c.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)["Messages"]
    c.delete_message(QueueUrl=queue_url, ReceiptHandle=msgs[0]["ReceiptHandle"])
    by_name = {s.name: s for s in trailbeam_spans()}
    send, rcv, dele = by_name["trailbeam send"], by_name["trailbeam receive"], by_name["trailbeam delete"]
    assert order(send)[1] < order(rcv)[0] and order(rcv)[1] < order(dele)[0]
    assert len({s.attributes["trailbeam.order.scope"] for s in trailbeam_spans()}) == 1


def test_every_span_of_one_call_shares_the_order_pair(trailbeam_spans):
    hooks = trailbeam.boto._state["hooks"]
    url = "http://h/0/q"
    calls = [
        ("SendMessageBatch", {"QueueUrl": url, "Entries": []},
         {"Successful": [{"Id": "a", "MessageId": "m-a"}, {"Id": "b", "MessageId": "m-b"}]}),
        ("ReceiveMessage", {"QueueUrl": url},
         {"Messages": [{"MessageId": "m1", "ReceiptHandle": "rh1"}, {"MessageId": "m2", "ReceiptHandle": "rh2"}]}),
        ("DeleteMessageBatch", {"QueueUrl": url, "Entries": [{"Id": "a", "ReceiptHandle": "rh-a"},
                                                             {"Id": "b", "ReceiptHandle": "rh-b"}]},
         {"Successful": [{"Id": "a"}], "Failed": [{"Id": "b", "Code": "X"}]}),
    ]
    pairs = []
    for op, params, result in calls:
        span = object()
        hooks.request_hook(span, "sqs", op, params)
        hooks.response_hook(span, "sqs", op, result)
        spans = [s for s in trailbeam_spans() if s.attributes["messaging.operation.name"] == op]
        assert len(spans) == 2
        assert len({order(s) for s in spans}) == 1, op
        pairs.append(order(spans[0]))
    assert pairs == sorted(pairs) and pairs[0][1] < pairs[1][0] and pairs[1][1] < pairs[2][0]


def test_order_tick_is_strictly_increasing_across_threads():
    import threading
    got: list = []

    def work():
        got.extend(_order.tick() for _ in range(2_000))

    threads = [threading.Thread(target=work) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(set(got)) == len(got) == 16_000
    assert _order.tick() > max(got)


_FORK_SCRIPT = """
import json, os, sys
from trailbeam import _order
parent_scope = _order.scope
for _ in range(3):
    _order.tick()
r, w = os.pipe()
pid = os.fork()
if pid == 0:
    os.close(r)
    os.write(w, json.dumps({"scope": _order.scope, "tick": _order.tick()}).encode())
    os._exit(0)
os.close(w)
with os.fdopen(r, "rb") as f:
    child = json.loads(f.read())
os.waitpid(pid, 0)
print(json.dumps({"parent_before": parent_scope, "parent_after": _order.scope,
                  "parent_tick": _order.tick(), "child": child}))
"""


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork")
def test_forked_child_gets_a_new_order_scope_and_counter():
    # In a fresh single-threaded interpreter: forking this multi-threaded pytest process (the moto
    # server thread) is itself unsafe.
    import subprocess
    import sys
    out = subprocess.run([sys.executable, "-c", _FORK_SCRIPT], check=True, capture_output=True, text=True,
                         timeout=60).stdout
    got = json.loads(out)
    assert got["child"]["scope"] != got["parent_before"]
    assert got["child"]["tick"] == 1          # the child's counter starts again
    assert got["parent_after"] == got["parent_before"]  # the parent keeps its scope and counter
    assert got["parent_tick"] == 4


# -- install -------------------------------------------------------------------------------------

def test_existing_instrumentation_keeps_its_hooks_and_sampling(make_client, queue_url):
    """The application instrumented botocore first, with its own hooks and a sampler that drops
    everything. Trailbeam chains its hooks, still emits its spans, and they share the stock trace ID."""
    app_exporter = InMemorySpanExporter()
    app_provider = TracerProvider(sampler=ALWAYS_OFF)
    app_provider.add_span_processor(SimpleSpanProcessor(app_exporter))
    calls = []
    BotocoreInstrumentor().instrument(
        tracer_provider=app_provider,
        request_hook=lambda span, svc, op, p: calls.append(("req", op)),
        response_hook=lambda span, svc, op, r: calls.append(("resp", op)))
    try:
        exporter = InMemorySpanExporter()
        trailbeam.boto.install(TEST_KEY, span_processor=SimpleSpanProcessor(exporter))
        mid = make_client().send_message(QueueUrl=queue_url, MessageBody="x")["MessageId"]
        [s] = [s for s in exporter.get_finished_spans() if s.name == "trailbeam send"]
        assert s.attributes["messaging.message.id"] == mid
        assert s.parent is not None and s.parent.trace_id == s.context.trace_id
        assert ("req", "SendMessage") in calls and ("resp", "SendMessage") in calls
        assert app_exporter.get_finished_spans() == ()
        trailbeam.boto.uninstall()
        assert BotocoreInstrumentor().is_instrumented_by_opentelemetry  # the app's stays
        calls.clear()
        make_client().send_message(QueueUrl=queue_url, MessageBody="y")
        assert ("req", "SendMessage") in calls
        assert len(exporter.get_finished_spans()) == 1  # nothing new from Trailbeam
    finally:
        BotocoreInstrumentor().uninstrument()


def test_service_instance_id_from_resource_env(monkeypatch, make_client, queue_url):
    monkeypatch.setenv("OTEL_RESOURCE_ATTRIBUTES", "service.instance.id=worker-7")
    monkeypatch.setenv("OTEL_SERVICE_NAME", "orders-worker")
    exporter = InMemorySpanExporter()
    trailbeam.boto.install(TEST_KEY, span_processor=SimpleSpanProcessor(exporter))
    try:
        make_client().send_message(QueueUrl=queue_url, MessageBody="x")
    finally:
        trailbeam.boto.uninstall()
    [s] = [s for s in exporter.get_finished_spans() if s.name == "trailbeam send"]
    assert s.attributes["messaging.client.id"] == "worker-7"
    assert s.resource.attributes["service.name"] == "orders-worker"


def test_generated_service_instance_id_is_a_uuid(trailbeam_spans, make_client, queue_url):
    import uuid
    make_client().send_message(QueueUrl=queue_url, MessageBody="x")
    [s] = trailbeam_spans("trailbeam send")
    assert uuid.UUID(s.attributes["messaging.client.id"]).version == 4
    assert s.resource.attributes["service.name"] == "unknown_service"


# -- key and hash --------------------------------------------------------------------------------

VECTORS = json.loads((Path(__file__).resolve().parents[2] / "contract" / "span-contract-vectors.json").read_text("utf-8"))


@pytest.mark.parametrize("v", VECTORS["vectors"], ids=lambda v: v["name"])
def test_hasher_matches_contract_vectors(v):
    hasher = _contract.ReceiptHasher(v["key"])
    assert hasher(v["receipt_handle"]) == v["hmac"]
    assert hasher.key_id == v["key_id"]
    assert v["key"] not in repr(hasher)


def test_key_precedence_and_warnings(monkeypatch, caplog):
    monkeypatch.setattr(_contract, "_warned", set())
    caplog.set_level(logging.WARNING, logger="trailbeam")
    monkeypatch.setenv("TRAILBEAM_RECEIPT_HASH_KEY", TEST_KEY)
    assert _contract.resolve_hasher().key_id == KEY_ID
    assert _contract.resolve_hasher("clé-ünïcode-key-0001").key_id == "2aee8344"  # explicit wins
    assert caplog.records == []
    monkeypatch.delenv("TRAILBEAM_RECEIPT_HASH_KEY")
    assert _contract.resolve_hasher().key_id == "b5e633f7"  # published default
    assert _contract.resolve_hasher().key_id == "b5e633f7"
    assert len([r for r in caplog.records if "default key" in r.getMessage()]) == 1  # once per process
    _contract.resolve_hasher("short")
    _contract.resolve_hasher("short2")
    short = [r for r in caplog.records if "shorter" in r.getMessage()]
    assert len(short) == 1 and "short" not in short[0].getMessage().replace("shorter", "")


# -- handle map ----------------------------------------------------------------------------------

def test_handle_map_is_bounded_and_expires():
    now = [0.0]
    m = HandleMap(max_entries=3, max_age=10, clock=lambda: now[0])
    for i in range(5):
        m.put(f"rh{i}", f"m{i}")
    assert len(m) == 3 and m.get("rh0") is None and m.get("rh4") == "m4"
    now[0] = 11
    assert m.get("rh4") is None
    m.put("new", "n")
    assert len(m) == 1
    m.discard("new")
    assert m.get("new") is None


def test_handle_map_concurrent_use():
    import threading
    m = HandleMap(max_entries=1000)

    def work(t):
        for i in range(2000):
            m.put(f"{t}-{i}", str(i))
            m.get(f"{t}-{i - 1}")
            m.discard(f"{t}-{i - 2}")

    threads = [threading.Thread(target=work, args=(t,)) for t in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(m) <= 1000


def test_default_export_is_otlp_http_to_the_configured_endpoint(monkeypatch, make_client, queue_url):
    """No exporter given: spans go as OTLP/HTTP protobuf to $TRAILBEAM_OTLP_TRACES_ENDPOINT."""
    import threading
    from http.server import BaseHTTPRequestHandler, HTTPServer

    from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest

    got = []

    class Receiver(BaseHTTPRequestHandler):
        def do_POST(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            got.append((self.path, self.headers.get("Content-Type"), ExportTraceServiceRequest.FromString(body)))
            self.send_response(200)
            self.end_headers()

        def log_message(self, *a):
            pass

    srv = HTTPServer(("127.0.0.1", 0), Receiver)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    monkeypatch.setenv("TRAILBEAM_OTLP_TRACES_ENDPOINT", f"http://127.0.0.1:{srv.server_port}/v1/traces")
    provider = trailbeam.boto.install(TEST_KEY)
    try:
        make_client().send_message(QueueUrl=queue_url, MessageBody="x")
        provider.force_flush()
    finally:
        trailbeam.boto.uninstall()
        srv.shutdown()
    assert got and all(p == "/v1/traces" and ct == "application/x-protobuf" for p, ct, _ in got)
    names = [(ss.scope.name, s.name) for _, _, req in got for rs in req.resource_spans
             for ss in rs.scope_spans for s in ss.spans]
    assert ("trailbeam", "trailbeam send") in names


# -- purge (revision 3.3, section 18) ------------------------------------------------------------

PURGE_VECTORS = json.loads(
    (Path(__file__).resolve().parents[2] / "contract" / "span-contract-vectors.json").read_text("utf-8"))["purge"]


class _Thrown(Exception):
    pass


@pytest.mark.parametrize("case", PURGE_VECTORS["cases"], ids=lambda c: c["name"])
def test_purge_matches_contract_vectors(trailbeam_spans, case):
    hooks = trailbeam.boto._state["hooks"]
    span = object()
    hooks.request_hook(span, "sqs", "PurgeQueue", {"QueueUrl": case["queue_url"]})
    response = case["response"]
    if "thrown" in response:
        # The instrumentation calls the hook from a ``finally`` while the exception is in flight.
        err = type(response["thrown"], (_Thrown,), {})
        try:
            raise err()
        except _Thrown:
            hooks.response_hook(span, "sqs", "PurgeQueue", None)
    elif "error_code" in response:
        hooks.response_hook(span, "sqs", "PurgeQueue", {"Error": {"Code": response["error_code"]}})
    else:
        hooks.response_hook(span, "sqs", "PurgeQueue", {"ResponseMetadata": {"HTTPStatusCode": 200}})
    [s] = trailbeam_spans()
    assert s.name == PURGE_VECTORS["span_name"]
    a = dict(s.attributes)
    assert {k: a.get(k) for k in case["attributes"]} == case["attributes"]
    assert set(PURGE_VECTORS["present"]) <= set(a)
    assert not set(case["absent"]) & set(a)
    assert a["trailbeam.order.issued"] < a["trailbeam.order.returned"]
    assert hooks._pending == {}


def test_purge_queue_against_the_emulator(trailbeam_spans, make_client, queue_url):
    c = make_client()
    c.send_message(QueueUrl=queue_url, MessageBody="x")
    c.purge_queue(QueueUrl=queue_url)
    [send] = trailbeam_spans("trailbeam send")
    [s] = trailbeam_spans("trailbeam purge")
    check_common(s, "PurgeQueue", queue_url, "sqs.purge", "purge")
    check_parent(s, trailbeam_spans.exporter)
    assert s.attributes["trailbeam.result"] == "ok"
    assert "trailbeam.error_code" not in s.attributes
    assert "messaging.message.id" not in s.attributes
    # The send returned before the purge was issued: section 18's "purged" rule can order them.
    assert send.attributes["trailbeam.order.returned"] < s.attributes["trailbeam.order.issued"]


def test_purge_that_throws_before_a_response(trailbeam_spans, make_client):
    c = make_client("http://127.0.0.1:9", retries={"max_attempts": 1}, connect_timeout=1)
    url = "http://127.0.0.1:9/000000000000/nowhere"
    with pytest.raises(EndpointConnectionError):
        c.purge_queue(QueueUrl=url)
    [s] = trailbeam_spans("trailbeam purge")
    assert s.attributes["trailbeam.result"] == "failed"
    assert s.attributes["trailbeam.error_code"] == "EndpointConnectionError"
    assert s.start_time < s.end_time
    assert trailbeam.boto._state["hooks"]._pending == {}


def test_purge_of_a_missing_queue_is_failed(trailbeam_spans, make_client, sqs_endpoint):
    url = f"{sqs_endpoint}/000000000000/no-such-queue-{os.getpid()}"
    with pytest.raises(ClientError):
        make_client().purge_queue(QueueUrl=url)
    [s] = trailbeam_spans("trailbeam purge")
    assert s.attributes["trailbeam.result"] == "failed"
    # The code as the SDK reports it: moto sends the legacy query code (AWS.SimpleQueueService.NonExistentQueue).
    assert s.attributes["trailbeam.error_code"] in ("QueueDoesNotExist", "AWS.SimpleQueueService.NonExistentQueue")
