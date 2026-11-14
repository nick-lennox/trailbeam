"""qv2.boto hooks against a local SQS emulator: one span per message, as docs/span-contract.md says."""
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

import qv2
import qv2.boto
from qv2 import _contract
from qv2._handles import HandleMap

from conftest import TEST_KEY

KEY_ID = "3b3edeb7"  # key ID of TEST_KEY, from docs/span-contract-vectors.json
COMMON = {"qv2.schema_version", "qv2.kind", "qv2.source", "messaging.system.name", "messaging.operation.type",
          "messaging.operation.name", "messaging.destination.name", "aws.sqs.queue.url", "messaging.client.id",
          "process.pid", "thread.name"}


def h(rh, key=TEST_KEY):
    return hmac.new(key.encode(), rh.encode(), hashlib.sha256).hexdigest()[:32]


@pytest.fixture
def qv2_spans():
    exporter = InMemorySpanExporter()
    qv2.boto.install(TEST_KEY, span_processor=SimpleSpanProcessor(exporter))

    def get(name=None):
        out = [s for s in exporter.get_finished_spans() if s.instrumentation_scope.name == "qv2"]
        return [s for s in out if name is None or s.name == name]

    get.exporter = exporter
    yield get
    qv2.boto.uninstall()


def stock_spans(exporter):
    return [s for s in exporter.get_finished_spans() if s.instrumentation_scope.name != "qv2"]


def check_common(span, op, queue_url, kind, op_type):
    a = dict(span.attributes)
    assert COMMON <= set(a), COMMON - set(a)
    assert a["qv2.schema_version"] == 1
    assert a["qv2.kind"] == kind
    assert a["qv2.source"] == "client"
    assert a["messaging.system.name"] == "aws.sqs"
    assert a["messaging.operation.type"] == op_type
    assert a["messaging.operation.name"] == op
    assert a["aws.sqs.queue.url"] == queue_url
    assert a["messaging.destination.name"] == queue_url.rsplit("/", 1)[-1]
    assert a["process.pid"] == os.getpid()
    assert a["messaging.client.id"] == span.resource.attributes["service.instance.id"]
    assert span.instrumentation_scope.version == qv2.__version__
    assert span.start_time == span.end_time
    for k, v in a.items():  # absent values are omitted, never "" or -1
        assert v != "" and v != -1, k
    # The span name never carries the queue name.
    assert span.name in ("qv2 send", "qv2 receive", "qv2 delete", "qv2 change_visibility")


def check_parent(span, exporter):
    parents = {s.context.span_id: s for s in stock_spans(exporter)}
    assert span.parent is not None and span.parent.span_id in parents
    assert parents[span.parent.span_id].context.trace_id == span.context.trace_id


def no_receipt_handles(spans, handles):
    for s in spans:
        for v in s.attributes.values():
            assert not any(isinstance(v, str) and rh in v for rh in handles)


# -- send ----------------------------------------------------------------------------------------

def test_send_message(qv2_spans, make_client, queue_url):
    mid = make_client().send_message(QueueUrl=queue_url, MessageBody="x")["MessageId"]
    [s] = qv2_spans("qv2 send")
    check_common(s, "SendMessage", queue_url, "sqs.send", "send")
    check_parent(s, qv2_spans.exporter)
    assert s.attributes["messaging.message.id"] == mid
    assert "qv2.batch_entry_id" not in s.attributes


def test_send_message_batch_one_span_per_entry(qv2_spans, make_client, queue_url):
    r = make_client().send_message_batch(
        QueueUrl=queue_url, Entries=[{"Id": f"e{i}", "MessageBody": str(i)} for i in range(3)])
    spans = qv2_spans("qv2 send")
    assert len(spans) == 3
    got = {s.attributes["qv2.batch_entry_id"]: s.attributes["messaging.message.id"] for s in spans}
    assert got == {e["Id"]: e["MessageId"] for e in r["Successful"]}
    assert len({s.parent.span_id for s in spans}) == 1
    for s in spans:
        check_common(s, "SendMessageBatch", queue_url, "sqs.send", "send")


def test_failed_send_batch_entries_produce_no_span(qv2_spans):
    hooks = qv2.boto._state["hooks"]
    params = {"QueueUrl": "http://localhost/000000000000/q", "Entries": []}
    span = object()
    hooks.request_hook(span, "sqs", "SendMessageBatch", params)
    hooks.response_hook(span, "sqs", "SendMessageBatch", {
        "Successful": [{"Id": "a", "MessageId": "m-a"}],
        "Failed": [{"Id": "b", "Code": "InvalidParameterValue", "SenderFault": True}]})
    assert [s.attributes["qv2.batch_entry_id"] for s in qv2_spans("qv2 send")] == ["a"]


# -- receive -------------------------------------------------------------------------------------

def test_receive_one_span_per_message(qv2_spans, make_client, queue_url):
    c = make_client()
    sent = {c.send_message(QueueUrl=queue_url, MessageBody=str(i))["MessageId"] for i in range(3)}
    msgs = c.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10)["Messages"]
    assert {m["MessageId"] for m in msgs} == sent
    spans = qv2_spans("qv2 receive")
    assert len(spans) == 3
    by_id = {s.attributes["messaging.message.id"]: s for s in spans}
    for m in msgs:
        s = by_id[m["MessageId"]]
        check_common(s, "ReceiveMessage", queue_url, "sqs.receive", "receive")
        check_parent(s, qv2_spans.exporter)
        a = s.attributes
        assert a["qv2.receipt_handle.hmac"] == h(m["ReceiptHandle"])
        assert a["qv2.receipt_handle.key_id"] == KEY_ID
        assert a["qv2.receive_count"] == 1
        assert isinstance(a["qv2.sent_timestamp_ms"], int)
        assert isinstance(a["qv2.first_receive_timestamp_ms"], int)
        assert "qv2.aws_trace_header" not in a  # none was sent; omitted, not ""
    no_receipt_handles(spans, [m["ReceiptHandle"] for m in msgs])


def test_receive_count_and_trace_header(qv2_spans, make_client, queue_url):
    c = make_client()
    hdr = "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1"
    c.send_message(QueueUrl=queue_url, MessageBody="x",
                   MessageSystemAttributes={"AWSTraceHeader": {"StringValue": hdr, "DataType": "String"}})
    m1 = c.receive_message(QueueUrl=queue_url, VisibilityTimeout=0)["Messages"][0]
    m2 = c.receive_message(QueueUrl=queue_url)["Messages"][0]
    s1, s2 = qv2_spans("qv2 receive")
    assert (s1.attributes["qv2.receive_count"], s2.attributes["qv2.receive_count"]) == (1, 2)
    assert s2.attributes["qv2.aws_trace_header"] == hdr
    assert s1.attributes["qv2.receipt_handle.hmac"] == h(m1["ReceiptHandle"])
    assert s2.attributes["qv2.receipt_handle.hmac"] == h(m2["ReceiptHandle"])


def test_receive_keeps_what_the_caller_asked_for(qv2_spans, make_client, queue_url):
    c = make_client()
    c.send_message(QueueUrl=queue_url, MessageBody="x")
    m = c.receive_message(QueueUrl=queue_url, MessageSystemAttributeNames=["SenderId"])["Messages"][0]
    assert "SenderId" in m["Attributes"]
    assert "ApproximateReceiveCount" in m["Attributes"]
    assert "MessageAttributes" not in m  # qv2 never asks for message attributes


def test_empty_receive_produces_no_span(qv2_spans, make_client, queue_url):
    assert "Messages" not in make_client().receive_message(QueueUrl=queue_url, WaitTimeSeconds=0)
    assert qv2_spans() == []


@pytest.mark.parametrize("given,expected", [
    ({}, {"MessageSystemAttributeNames": list(qv2.boto.RECEIVE_SYSTEM_ATTRIBUTES)}),
    ({"MessageSystemAttributeNames": ["All"]}, {"MessageSystemAttributeNames": ["All"]}),
    ({"AttributeNames": ["All"]}, {"AttributeNames": ["All"]}),
    ({"MessageSystemAttributeNames": ["SenderId", "SentTimestamp"]},
     {"MessageSystemAttributeNames": ["SenderId", "SentTimestamp", "ApproximateReceiveCount",
                                      "ApproximateFirstReceiveTimestamp", "AWSTraceHeader"]}),
    ({"AttributeNames": ["SenderId"]},
     {"AttributeNames": ["SenderId"] + list(qv2.boto.RECEIVE_SYSTEM_ATTRIBUTES)}),
    ({"MessageAttributeNames": ["tenant"]},
     {"MessageAttributeNames": ["tenant"],
      "MessageSystemAttributeNames": list(qv2.boto.RECEIVE_SYSTEM_ATTRIBUTES)}),
])
def test_add_receive_attribute_names(given, expected):
    original = json.loads(json.dumps(given))
    params = dict(given, QueueUrl="u")
    qv2.boto.add_receive_attribute_names(params)
    assert {k: v for k, v in params.items() if k != "QueueUrl"} == expected
    assert given == original  # the caller's lists are not modified in place


# -- delete / change visibility ------------------------------------------------------------------

def receive_one(c, url, **kw):
    c.send_message(QueueUrl=url, MessageBody="x")
    return c.receive_message(QueueUrl=url, **kw)["Messages"][0]


def test_delete_message_joins_the_receive(qv2_spans, make_client, queue_url):
    c = make_client()
    m = receive_one(c, queue_url)
    hooks = qv2.boto._state["hooks"]
    assert hooks.handles.get(m["ReceiptHandle"]) == m["MessageId"]
    c.delete_message(QueueUrl=queue_url, ReceiptHandle=m["ReceiptHandle"])
    [r] = qv2_spans("qv2 receive")
    [d] = qv2_spans("qv2 delete")
    check_common(d, "DeleteMessage", queue_url, "sqs.settle", "settle")
    check_parent(d, qv2_spans.exporter)
    a = d.attributes
    assert a["qv2.receipt_handle.hmac"] == r.attributes["qv2.receipt_handle.hmac"]
    assert a["qv2.receipt_handle.key_id"] == KEY_ID
    assert a["messaging.message.id"] == m["MessageId"]
    assert a["qv2.result"] == "ok"
    assert "qv2.error_code" not in a and "qv2.batch_entry_id" not in a
    assert hooks.handles.get(m["ReceiptHandle"]) is None  # dropped after a successful delete
    no_receipt_handles(qv2_spans(), [m["ReceiptHandle"]])


def test_delete_with_invalid_handle_is_failed(qv2_spans, make_client, queue_url):
    with pytest.raises(ClientError):
        make_client().delete_message(QueueUrl=queue_url, ReceiptHandle="not-a-handle")
    [d] = qv2_spans("qv2 delete")
    assert d.attributes["qv2.result"] == "failed"
    assert d.attributes["qv2.error_code"] == "ReceiptHandleIsInvalid"
    assert d.attributes["qv2.receipt_handle.hmac"] == h("not-a-handle")
    assert "messaging.message.id" not in d.attributes  # not received by this process


def test_delete_batch_mixed_results(qv2_spans, make_client, queue_url):
    c = make_client()
    m = receive_one(c, queue_url)
    c.delete_message_batch(QueueUrl=queue_url, Entries=[
        {"Id": "good", "ReceiptHandle": m["ReceiptHandle"]}, {"Id": "bad", "ReceiptHandle": "bogus"}])
    spans = {s.attributes["qv2.batch_entry_id"]: s for s in qv2_spans("qv2 delete")}
    assert set(spans) == {"good", "bad"}
    assert len({s.parent.span_id for s in spans.values()}) == 1
    good, bad = spans["good"].attributes, spans["bad"].attributes
    check_common(spans["good"], "DeleteMessageBatch", queue_url, "sqs.settle", "settle")
    assert (good["qv2.result"], good["messaging.message.id"]) == ("ok", m["MessageId"])
    assert "qv2.error_code" not in good
    assert (bad["qv2.result"], bad["qv2.error_code"]) == ("failed", "ReceiptHandleIsInvalid")


def test_change_visibility_zero_is_recorded(qv2_spans, make_client, queue_url):
    c = make_client()
    m = receive_one(c, queue_url)
    c.change_message_visibility(QueueUrl=queue_url, ReceiptHandle=m["ReceiptHandle"], VisibilityTimeout=0)
    [s] = qv2_spans("qv2 change_visibility")
    check_common(s, "ChangeMessageVisibility", queue_url, "sqs.settle", "settle")
    a = s.attributes
    assert a["qv2.visibility_timeout"] == 0
    assert a["qv2.result"] == "ok"
    assert a["messaging.message.id"] == m["MessageId"]
    # change_visibility does not drop the handle from the map; the message is still held.
    assert qv2.boto._state["hooks"].handles.get(m["ReceiptHandle"]) == m["MessageId"]


def test_change_visibility_batch(qv2_spans, make_client, queue_url):
    c = make_client()
    m = receive_one(c, queue_url)
    c.change_message_visibility_batch(QueueUrl=queue_url, Entries=[
        {"Id": "a", "ReceiptHandle": m["ReceiptHandle"], "VisibilityTimeout": 30},
        {"Id": "b", "ReceiptHandle": "bogus", "VisibilityTimeout": 5}])
    spans = {s.attributes["qv2.batch_entry_id"]: s.attributes for s in qv2_spans("qv2 change_visibility")}
    assert spans["a"]["qv2.visibility_timeout"] == 30 and spans["a"]["qv2.result"] == "ok"
    assert spans["b"]["qv2.visibility_timeout"] == 5 and spans["b"]["qv2.result"] == "failed"
    assert spans["b"]["qv2.error_code"] == "ReceiptHandleIsInvalid"


def test_settle_that_throws_before_a_response(qv2_spans, make_client):
    c = make_client("http://127.0.0.1:9", retries={"max_attempts": 1}, connect_timeout=1)
    url = "http://127.0.0.1:9/000000000000/nowhere"
    with pytest.raises(EndpointConnectionError):
        c.delete_message_batch(QueueUrl=url, Entries=[
            {"Id": "a", "ReceiptHandle": "rh-a"}, {"Id": "b", "ReceiptHandle": "rh-b"}])
    with pytest.raises(EndpointConnectionError):
        c.change_message_visibility(QueueUrl=url, ReceiptHandle="rh-c", VisibilityTimeout=0)
    spans = qv2_spans()
    assert [s.name for s in spans] == ["qv2 delete", "qv2 delete", "qv2 change_visibility"]
    for s in spans:
        assert s.attributes["qv2.result"] == "failed"
        assert s.attributes["qv2.error_code"] == "EndpointConnectionError"
    assert qv2.boto._state["hooks"]._pending == {}


def test_other_services_and_operations_are_ignored(qv2_spans, make_client, queue_url):
    make_client().get_queue_attributes(QueueUrl=queue_url, AttributeNames=["All"])
    assert qv2_spans() == []
    assert qv2.boto._state["hooks"]._pending == {}


# -- install -------------------------------------------------------------------------------------

def test_existing_instrumentation_keeps_its_hooks_and_sampling(make_client, queue_url):
    """The application instrumented botocore first, with its own hooks and a sampler that drops
    everything. qv2 chains its hooks, still emits its spans, and they share the stock trace ID."""
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
        qv2.boto.install(TEST_KEY, span_processor=SimpleSpanProcessor(exporter))
        mid = make_client().send_message(QueueUrl=queue_url, MessageBody="x")["MessageId"]
        [s] = [s for s in exporter.get_finished_spans() if s.name == "qv2 send"]
        assert s.attributes["messaging.message.id"] == mid
        assert s.parent is not None and s.parent.trace_id == s.context.trace_id
        assert ("req", "SendMessage") in calls and ("resp", "SendMessage") in calls
        assert app_exporter.get_finished_spans() == ()
        qv2.boto.uninstall()
        assert BotocoreInstrumentor().is_instrumented_by_opentelemetry  # the app's stays
        calls.clear()
        make_client().send_message(QueueUrl=queue_url, MessageBody="y")
        assert ("req", "SendMessage") in calls
        assert len(exporter.get_finished_spans()) == 1  # nothing new from qv2
    finally:
        BotocoreInstrumentor().uninstrument()


def test_service_instance_id_from_resource_env(monkeypatch, make_client, queue_url):
    monkeypatch.setenv("OTEL_RESOURCE_ATTRIBUTES", "service.instance.id=worker-7")
    monkeypatch.setenv("OTEL_SERVICE_NAME", "orders-worker")
    exporter = InMemorySpanExporter()
    qv2.boto.install(TEST_KEY, span_processor=SimpleSpanProcessor(exporter))
    try:
        make_client().send_message(QueueUrl=queue_url, MessageBody="x")
    finally:
        qv2.boto.uninstall()
    [s] = [s for s in exporter.get_finished_spans() if s.name == "qv2 send"]
    assert s.attributes["messaging.client.id"] == "worker-7"
    assert s.resource.attributes["service.name"] == "orders-worker"


def test_generated_service_instance_id_is_a_uuid(qv2_spans, make_client, queue_url):
    import uuid
    make_client().send_message(QueueUrl=queue_url, MessageBody="x")
    [s] = qv2_spans("qv2 send")
    assert uuid.UUID(s.attributes["messaging.client.id"]).version == 4
    assert s.resource.attributes["service.name"] == "unknown_service"


# -- key and hash --------------------------------------------------------------------------------

VECTORS = json.loads((Path(__file__).resolve().parents[2] / "docs" / "span-contract-vectors.json").read_text("utf-8"))


@pytest.mark.parametrize("v", VECTORS["vectors"], ids=lambda v: v["name"])
def test_hasher_matches_contract_vectors(v):
    hasher = _contract.ReceiptHasher(v["key"])
    assert hasher(v["receipt_handle"]) == v["hmac"]
    assert hasher.key_id == v["key_id"]
    assert v["key"] not in repr(hasher)


def test_key_precedence_and_warnings(monkeypatch, caplog):
    monkeypatch.setattr(_contract, "_warned", set())
    caplog.set_level(logging.WARNING, logger="qv2")
    monkeypatch.setenv("QV2_RECEIPT_HASH_KEY", TEST_KEY)
    assert _contract.resolve_hasher().key_id == KEY_ID
    assert _contract.resolve_hasher("clé-ünïcode-key-0001").key_id == "20ce29a4"  # explicit wins
    assert caplog.records == []
    monkeypatch.delenv("QV2_RECEIPT_HASH_KEY")
    assert _contract.resolve_hasher().key_id == "2bd63d10"  # published default
    assert _contract.resolve_hasher().key_id == "2bd63d10"
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
    """No exporter given: spans go as OTLP/HTTP protobuf to $QV2_OTLP_TRACES_ENDPOINT."""
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
    monkeypatch.setenv("QV2_OTLP_TRACES_ENDPOINT", f"http://127.0.0.1:{srv.server_port}/v1/traces")
    provider = qv2.boto.install(TEST_KEY)
    try:
        make_client().send_message(QueueUrl=queue_url, MessageBody="x")
        provider.force_flush()
    finally:
        qv2.boto.uninstall()
        srv.shutdown()
    assert got and all(p == "/v1/traces" and ct == "application/x-protobuf" for p, ct, _ in got)
    names = [(ss.scope.name, s.name) for _, _, req in got for rs in req.resource_spans
             for ss in rs.scope_spans for s in ss.spans]
    assert ("qv2", "qv2 send") in names
