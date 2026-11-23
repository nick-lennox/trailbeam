"""Revision 3.4 (contract section 20): SNS fan-out. The trailbeam publish span, the trailbeam.publish_id the
publish hooks add, and the SNS attributes on a receive. Shared vectors in contract/span-contract-vectors.json
("sns"); Node.js checks the same. The end-to-end tests run SNS to SQS delivery in moto, raw delivery off and on."""
import copy
import json
import re
import uuid
from pathlib import Path

import boto3
import pytest
from botocore.config import Config as BotoConfig
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

import trailbeam.aws_lambda as ql
import trailbeam.boto
from trailbeam import _capture

from sdk_constants import TEST_KEY

VECTORS = json.loads(
    (Path(__file__).resolve().parents[2] / "contract" / "span-contract-vectors.json").read_text("utf-8"))["sns"]
SNS_NAMES = ("trailbeam.sns.publish_id", "trailbeam.sns.message_id", "trailbeam.sns.topic_arn",
             "trailbeam.sns.timestamp_ms")
UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")


def expand(entry):
    """A vector entry with its {"repeat": c, "times": n} Message written out."""
    e = copy.deepcopy(entry)
    m = e.get("Message")
    if isinstance(m, dict):
        e["Message"] = m["repeat"] * m["times"]
    return e


def lambda_attributes(attrs):
    """ReceiveMessage-form message attributes in a Lambda record's form."""
    if attrs is None:
        return {}
    out = {}
    for name, a in attrs.items():
        b = {"dataType": a.get("DataType")}
        if "StringValue" in a:
            b["stringValue"] = a["StringValue"]
        if "BinaryValue" in a:
            b["binaryValue"] = a["BinaryValue"]
        out[name] = b
    return out


def sns_only(attrs):
    return {k: v for k, v in attrs.items() if k in SNS_NAMES}


# -- vectors -----------------------------------------------------------------------------------------

@pytest.mark.parametrize("value,expected", VECTORS["timestamps"], ids=lambda v: repr(v))
def test_timestamp_vectors(value, expected):
    assert _capture.sns_timestamp_ms(value) == expected


@pytest.fixture
def hooks_with():
    exporter = InMemorySpanExporter()

    def make(**sns):
        trailbeam.boto.install(TEST_KEY, span_processor=SimpleSpanProcessor(exporter), sns=sns)
        hooks = trailbeam.boto._state["hooks"]

        def spans(name=None):
            return [s for s in exporter.get_finished_spans()
                    if s.instrumentation_scope.name == "trailbeam" and (name is None or s.name == name)]
        return hooks, spans

    yield make
    trailbeam.boto.uninstall()


@pytest.mark.parametrize("case", VECTORS["receive"], ids=lambda c: c["name"])
def test_receive_vectors_client_hooks(hooks_with, case):
    hooks, spans = hooks_with(envelope_queues=["orders"] if case["envelope_on"] else [])
    span = object()
    params = {"QueueUrl": "http://localhost:9324/000000000000/orders"}
    hooks.request_hook(span, "sqs", "ReceiveMessage", params)
    assert "trailbeam.publish_id" in params["MessageAttributeNames"]
    m = {"MessageId": "sqs-1", "ReceiptHandle": "rh-1", "Body": case["body"],
         "Attributes": {"ApproximateReceiveCount": "1", "SentTimestamp": "1613511680100"}}
    if case["message_attributes"] is not None:
        m["MessageAttributes"] = case["message_attributes"]
    hooks.response_hook(span, "sqs", "ReceiveMessage", {"Messages": [m]})
    [s] = spans("trailbeam receive")
    assert sns_only(dict(s.attributes)) == case["attributes"]


@pytest.mark.parametrize("case", VECTORS["receive"], ids=lambda c: c["name"])
def test_receive_vectors_lambda_wrapper(case):
    exporter = InMemorySpanExporter()
    fn = ql.wrap(lambda e, c: None, receipt_hash_key=TEST_KEY, span_processor=SimpleSpanProcessor(exporter),
                 sns={"envelope_queues": ["orders"] if case["envelope_on"] else []})
    fn({"Records": [{"messageId": "sqs-1", "receiptHandle": "rh-1", "body": case["body"],
                     "attributes": {"ApproximateReceiveCount": "1"},
                     "messageAttributes": lambda_attributes(case["message_attributes"]),
                     "eventSource": "aws:sqs", "eventSourceARN": "arn:aws:sqs:us-east-2:111122223333:orders"}]},
       type("Ctx", (), {"aws_request_id": "req-1", "function_name": "fn"})())
    [s] = [s for s in exporter.get_finished_spans() if s.name == "trailbeam receive"]
    assert sns_only(dict(s.attributes)) == case["attributes"]


def test_body_capture_turns_the_envelope_fields_on():
    config = _capture.resolve_config({}, {"body_queues": ["orders"]}, {})
    body = next(c["body"] for c in VECTORS["receive"] if c["name"].startswith("envelope on"))
    assert _capture.sns_receive_attrs(config, "orders", body, None)["trailbeam.sns.message_id"]
    assert _capture.sns_receive_attrs(config, "other", body, None) == {}
    assert _capture.sns_receive_attrs(_capture.resolve_config({}, {}, {"envelope_queues": ["*"]}), "x", body, None)


@pytest.mark.parametrize("case", VECTORS["inject"], ids=lambda c: c["name"])
def test_inject_vectors(case):
    config = _capture.resolve_config({}, {}, {"inject": case["inject"]})
    entries = [expand(e) for e in case["entries"]]
    before = copy.deepcopy(entries)
    out, results = _capture.sns_inject(config, entries)
    assert entries == before  # the caller's objects are not changed
    assert len(out) == len(results) == len(entries)
    for e, o, r, want in zip(entries, out, results, case["results"]):
        if want == "injected":
            assert r[0] == "trailbeam.sns.publish_id" and UUID_RE.match(r[1])
            assert o is not e
            assert o["MessageAttributes"]["trailbeam.publish_id"] == {"DataType": "String", "StringValue": r[1]}
            assert len(o["MessageAttributes"]) <= VECTORS["max_attributes"]
            assert {k: v for k, v in o.items() if k != "MessageAttributes"} == \
                {k: v for k, v in e.items() if k != "MessageAttributes"}
            assert _capture.sns_entry_bytes(o) + 62 <= VECTORS["max_message_bytes"]
        else:
            assert r == ("trailbeam.sns.publish_id_skipped", want)
            assert o is e
    injected = [r[1] for r in results if r[0] == "trailbeam.sns.publish_id"]
    assert len(set(injected)) == len(injected)


@pytest.mark.parametrize("case", VECTORS["publish"]["cases"], ids=lambda c: c["name"])
def test_publish_vectors(hooks_with, case):
    hooks, spans = hooks_with(inject=False)
    span = object()
    params = copy.deepcopy(case["request"])
    hooks.request_hook(span, "sns", case["operation"], params)
    assert params == case["request"]  # injection is off: the request is unchanged
    hooks.response_hook(span, "sns", case["operation"], copy.deepcopy(case["response"]))
    got = spans()
    assert len(got) == len(case["spans"])
    for s, want in zip(got, case["spans"]):
        assert s.name == "trailbeam publish"
        a = dict(s.attributes)
        assert {k: a.get(k) for k in want} == want
        extra = set(a) - set(want) - set(VECTORS["publish"]["present"]) - {"thread.name"}
        assert not extra, extra
        assert set(VECTORS["publish"]["present"]) <= set(a)
        assert not set(VECTORS["publish"]["absent"]) & set(a)
        assert a["trailbeam.order.issued"] < a["trailbeam.order.returned"]
    assert hooks._pending == {}


def test_publish_records_the_injected_id_and_leaves_the_callers_attributes_alone(hooks_with):
    hooks, spans = hooks_with()
    mine = {"kind": {"DataType": "String", "StringValue": "order"}}
    entries = [{"Id": "a", "Message": "1", "MessageAttributes": mine}, {"Id": "b", "Message": "2"}]
    params = {"TopicArn": "arn:aws:sns:us-east-1:123456789012:t", "PublishBatchRequestEntries": entries}
    span = object()
    hooks.request_hook(span, "sns", "PublishBatch", params)
    assert mine == {"kind": {"DataType": "String", "StringValue": "order"}}
    assert entries[1] == {"Id": "b", "Message": "2"}
    sent = {e["Id"]: e["MessageAttributes"]["trailbeam.publish_id"]["StringValue"]
            for e in params["PublishBatchRequestEntries"]}
    hooks.response_hook(span, "sns", "PublishBatch",
                        {"Successful": [{"Id": "b", "MessageId": "m-b"}, {"Id": "a", "MessageId": "m-a"}]})
    got = {s.attributes["trailbeam.batch_entry_id"]: s.attributes["trailbeam.sns.publish_id"] for s in spans()}
    assert got == sent


def test_publish_without_a_topic_is_not_changed(hooks_with):
    hooks, spans = hooks_with()
    params = {"PhoneNumber": "+15555550100", "Message": "hi"}
    hooks.request_hook(object(), "sns", "Publish", params)
    assert params == {"PhoneNumber": "+15555550100", "Message": "hi"}


# -- SNS to SQS in moto --------------------------------------------------------------------------------

@pytest.fixture
def fanout(sqs_endpoint):
    """A topic with two queue subscriptions, raw delivery off and on. Yields (sns, sqs, topic, urls)."""
    session = boto3.session.Session()
    sns = session.client("sns", endpoint_url=sqs_endpoint, region_name="us-east-1")
    sqs = session.client("sqs", endpoint_url=sqs_endpoint, region_name="us-east-1",
                         config=BotoConfig(retries={"max_attempts": 1}))
    tag = uuid.uuid4().hex[:10]
    topic = sns.create_topic(Name=f"orders-{tag}")["TopicArn"]
    urls = {}
    for raw in (False, True):
        name = f"fanout-{'raw' if raw else 'envelope'}-{tag}"
        url = sqs.create_queue(QueueName=name)["QueueUrl"]
        arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        sub = sns.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=arn)["SubscriptionArn"]
        sns.set_subscription_attributes(SubscriptionArn=sub, AttributeName="RawMessageDelivery",
                                        AttributeValue="true" if raw else "false")
        urls[raw] = url
    yield sns, sqs, topic, urls
    sns.delete_topic(TopicArn=topic)
    for url in urls.values():
        sqs.delete_queue(QueueUrl=url)


def receive_all(sqs, url):
    return sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=1).get("Messages", [])


def test_fanout_joins_both_deliveries_to_the_publish(spans_with_sns, fanout):
    sns, sqs, topic, urls = fanout
    spans = spans_with_sns(sns={"envelope_queues": ["*"]})
    r = sns.publish(TopicArn=topic, Message='{"order": 1}')
    [pub] = spans("trailbeam publish")
    a = dict(pub.attributes)
    assert a["messaging.message.id"] == r["MessageId"]
    assert a["aws.sns.topic.arn"] == topic
    assert a["messaging.system.name"] == "aws.sns"
    assert a["messaging.destination.name"] == topic.rsplit(":", 1)[-1]
    assert "aws.sqs.queue.url" not in a
    publish_id = a["trailbeam.sns.publish_id"]
    assert UUID_RE.match(publish_id)

    [m] = receive_all(sqs, urls[False])  # raw delivery off: the envelope
    [m_raw] = receive_all(sqs, urls[True])  # raw delivery on: the bare message and its attributes
    assert m_raw["Body"] == '{"order": 1}'
    receives = {s.attributes["aws.sqs.queue.url"]: dict(s.attributes) for s in spans("trailbeam receive")}
    env = receives[urls[False]]
    assert env["trailbeam.sns.message_id"] == r["MessageId"]
    assert env["trailbeam.sns.topic_arn"] == topic
    assert isinstance(env["trailbeam.sns.timestamp_ms"], int)
    # SNS dwell (section 20.5): both clocks are AWS's; in moto it is a few milliseconds.
    assert 0 <= env["trailbeam.sent_timestamp_ms"] - env["trailbeam.sns.timestamp_ms"] < 60_000
    raw = receives[urls[True]]
    assert raw["trailbeam.sns.publish_id"] == publish_id
    assert "trailbeam.sns.message_id" not in raw  # no envelope: the SNS message ID does not reach the queue
    assert env["messaging.message.id"] != r["MessageId"] != raw["messaging.message.id"]


def test_fanout_without_envelope_reading_joins_only_raw_deliveries(spans_with_sns, fanout):
    sns, sqs, topic, urls = fanout
    spans = spans_with_sns()  # the default: envelopes are read only on queues whose body is captured
    sns.publish(TopicArn=topic, Message="hello")
    receive_all(sqs, urls[False])
    receive_all(sqs, urls[True])
    receives = {s.attributes["aws.sqs.queue.url"]: dict(s.attributes) for s in spans("trailbeam receive")}
    assert not set(SNS_NAMES) & set(receives[urls[False]])
    assert receives[urls[True]]["trailbeam.sns.publish_id"] == \
        spans("trailbeam publish")[0].attributes["trailbeam.sns.publish_id"]


def test_fanout_with_injection_off_changes_no_message(spans_with_sns, fanout):
    sns, sqs, topic, urls = fanout
    spans = spans_with_sns(sns={"inject": False, "envelope_queues": ["*"]})
    sns.publish(TopicArn=topic, Message="hello")
    [m] = sqs.receive_message(QueueUrl=urls[True], MessageAttributeNames=["All"], WaitTimeSeconds=1)["Messages"]
    assert "trailbeam.publish_id" not in (m.get("MessageAttributes") or {})
    receive_all(sqs, urls[False])
    [pub] = spans("trailbeam publish")
    assert pub.attributes["trailbeam.sns.publish_id_skipped"] == "off"
    env = [s for s in spans("trailbeam receive") if s.attributes["aws.sqs.queue.url"] == urls[False]]
    assert env[0].attributes["trailbeam.sns.message_id"] == pub.attributes["messaging.message.id"]


def test_publish_batch_in_moto(spans_with_sns, fanout):
    sns, sqs, topic, urls = fanout
    spans = spans_with_sns(sns={"envelope_queues": ["*"]})
    r = sns.publish_batch(TopicArn=topic, PublishBatchRequestEntries=[
        {"Id": "a", "Message": "one"}, {"Id": "b", "Message": "two"}])
    pubs = {s.attributes["trailbeam.batch_entry_id"]: dict(s.attributes) for s in spans("trailbeam publish")}
    assert {k: v["messaging.message.id"] for k, v in pubs.items()} == {e["Id"]: e["MessageId"] for e in r["Successful"]}
    assert len(receive_all(sqs, urls[True])) == 2
    receives = [dict(s.attributes) for s in spans("trailbeam receive") if s.attributes["aws.sqs.queue.url"] == urls[True]]
    assert sorted(x["trailbeam.sns.publish_id"] for x in receives) == \
        sorted(v["trailbeam.sns.publish_id"] for v in pubs.values())


def test_the_stock_traceparent_and_ours_never_pass_ten_attributes(spans_with_sns, fanout):
    """The stock botocore instrumentation adds traceparent before our request hook runs (section 20.2), so
    the hook counts it. Whatever the caller sends, raw delivery never gets more than 10 attributes."""
    sns, sqs, topic, urls = fanout
    spans = spans_with_sns()
    for n in range(6, 11):
        attrs = {f"a{i}": {"DataType": "String", "StringValue": "v"} for i in range(n)}
        sns.publish(TopicArn=topic, Message=f"n={n}", MessageAttributes=attrs)
    got = {}
    for _ in range(3):
        for m in sqs.receive_message(QueueUrl=urls[True], MaxNumberOfMessages=10, MessageAttributeNames=["All"],
                                     WaitTimeSeconds=1).get("Messages", []):
            got[m["Body"]] = set(m.get("MessageAttributes") or {})
    assert set(got) == {f"n={n}" for n in range(6, 11)}
    for body, names in got.items():
        assert len(names) <= 10, (body, names)
    assert "trailbeam.publish_id" in got["n=9"]  # 9 + ours = 10
    assert "trailbeam.publish_id" not in got["n=10"]
    # "n=10" is the only 4-byte message.
    [ten] = [s for s in spans("trailbeam publish") if s.attributes["messaging.message.body.size"] == 4]
    assert ten.attributes["trailbeam.sns.publish_id_skipped"] == "attribute_limit"


@pytest.fixture
def spans_with_sns():
    exporter = InMemorySpanExporter()

    def install(**options):
        trailbeam.boto.install(TEST_KEY, span_processor=SimpleSpanProcessor(exporter), **options)

        def get(name):
            return [s for s in exporter.get_finished_spans()
                    if s.instrumentation_scope.name == "trailbeam" and s.name == name]
        return get

    yield install
    trailbeam.boto.uninstall()


def test_environment_variables(monkeypatch):
    monkeypatch.setenv("TRAILBEAM_SNS_INJECT", "off")
    monkeypatch.setenv("TRAILBEAM_SNS_ENVELOPE_QUEUES", "a, b")
    c = _capture.resolve_config()
    assert (c.sns_inject, c.sns_envelope_queues) == (False, frozenset({"a", "b"}))
    monkeypatch.setenv("TRAILBEAM_SNS_INJECT", "on")
    assert _capture.resolve_config().sns_inject is True
    monkeypatch.delenv("TRAILBEAM_SNS_INJECT")
    monkeypatch.delenv("TRAILBEAM_SNS_ENVELOPE_QUEUES")
    c = _capture.resolve_config()
    assert (c.sns_inject, c.sns_envelope_queues) == (True, frozenset())
    # A group given explicitly replaces its environment variables as a whole.
    monkeypatch.setenv("TRAILBEAM_SNS_INJECT", "off")
    assert _capture.resolve_config(sns={}).sns_inject is True
