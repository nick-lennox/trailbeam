"""trailbeam.aws_lambda wrapper: one receive (flushed before the handler) and one outcome per SQS record."""
import copy
import hashlib
import hmac
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

import trailbeam.aws_lambda as ql
from trailbeam import _order
from conftest import TEST_KEY

FIXTURES = Path(__file__).parent / "fixtures"
QV9 = json.loads((FIXTURES / "qv9_lambda_events.json").read_text("utf-8"))
DOCS = json.loads((FIXTURES / "aws_docs_sqs_event.json").read_text("utf-8"))["event"]


class Killed(BaseException):
    """Stands in for the runtime killing the invocation (timeout, out of memory)."""


def ctx(req="req-1", fn="orders-fn"):
    return SimpleNamespace(aws_request_id=req, function_name=fn)


def h(rh):
    return hmac.new(TEST_KEY.encode(), rh.encode(), hashlib.sha256).hexdigest()[:32]


@pytest.fixture
def exporter():
    return InMemorySpanExporter()


def wrapped(fn, exporter, batch=True, **kw):
    # BatchSpanProcessor is the production processor: this checks that the wrapper's flushes export.
    proc = BatchSpanProcessor(exporter, schedule_delay_millis=60_000) if batch else SimpleSpanProcessor(exporter)
    return ql.wrap(fn, receipt_hash_key=TEST_KEY, span_processor=proc, **kw)


def spans(exporter, name=None):
    return [s for s in exporter.get_finished_spans() if name is None or s.name == name]


# -- the 0.1 "done means" test: every delivery gets a receive; every one not killed, an outcome ----
# The killed delivery gets no outcome span (nothing runs after the kill); the store infers its outcome,
# tested end to end in server/tests/test_lambda_end_to_end.py.

def behave(spec):
    def handler(event, context):
        if spec["kind"] == "raise":
            raise {"RuntimeError": RuntimeError}[spec["error_type"]]("synthetic failure")
        if spec["kind"] == "killed":
            raise Killed()
        return {"batchItemFailures": [{"itemIdentifier": i} for i in spec["batchItemFailures"]]}
    return handler


def test_recorded_qv9_events_every_delivery_gets_a_receive_and_every_unkilled_one_an_outcome(monkeypatch):
    """Replays the 24 invocations (34 deliveries) recorded in QV-9 through the wrapper."""
    assert len(QV9["invocations"]) == 24
    deliveries = 0
    for inv in QV9["invocations"]:
        monkeypatch.setenv("AWS_LAMBDA_LOG_STREAM_NAME", inv["log_stream_name"])
        exporter = InMemorySpanExporter()  # shutdown() below also stops the exporter
        records = inv["event"]["Records"]
        seen_before_handler = []

        def handler(event, context, _inner=behave(inv["behaviour"])):
            seen_before_handler.extend(s.name for s in exporter.get_finished_spans())
            return _inner(event, context)

        fn = wrapped(handler, exporter)
        event = copy.deepcopy(inv["event"])
        c = ctx(inv["aws_request_id"], inv["function_name"])
        if inv["behaviour"]["kind"] == "killed":
            with pytest.raises(Killed):
                fn(event, c)
        elif inv["behaviour"]["kind"] == "raise":
            with pytest.raises(RuntimeError):
                fn(event, c)
        else:
            assert fn(event, c) == behave(inv["behaviour"])(event, c)
        finished = exporter.get_finished_spans()  # before shutdown: the wrapper must have flushed
        fn.trailbeam_tracer_provider.shutdown()
        deliveries += len(records)
        assert event == inv["event"]  # the wrapper does not modify the event

        # Receives were exported (not just buffered) before the handler ran.
        assert seen_before_handler == ["trailbeam receive"] * len(records)
        receives = {s.attributes["messaging.message.id"]: s for s in finished if s.name == "trailbeam receive"}
        outcomes = {s.attributes["messaging.message.id"]: s for s in finished if s.name == "trailbeam outcome"}
        assert len(finished) == len(records) + len(outcomes)
        assert len(records) == len(receives)
        for r in records:
            mid = r["messageId"]
            rcv = receives[mid].attributes
            assert rcv["trailbeam.receipt_handle.hmac"] == h(r["receiptHandle"])
            assert rcv["trailbeam.receive_count"] == int(r["attributes"]["ApproximateReceiveCount"])
            assert rcv["faas.invocation_id"] == inv["aws_request_id"]
            assert rcv["messaging.client.id"] == inv["log_stream_name"]
            want = inv["expected"][mid]
            if want is None:  # killed: no outcome; the store infers it from the receive
                assert mid not in outcomes
            else:
                assert outcomes[mid].attributes["trailbeam.outcome"] == want, (inv["aws_request_id"], mid)
                assert outcomes[mid].start_time >= receives[mid].end_time
        if inv["behaviour"]["kind"] != "killed":
            assert set(outcomes) == set(receives)
        else:
            assert outcomes == {}
    assert deliveries == 34


def test_recorded_qv9_events_cover_every_outcome():
    got = {v for inv in QV9["invocations"] for v in inv["expected"].values()}
    assert got == {"success", "failed_reported", "failed_invalid_response", "failed_exception", None}


# -- span contents ----------------------------------------------------------------------------

def test_receive_and_outcome_attributes_for_the_aws_docs_event(exporter, monkeypatch):
    monkeypatch.setenv("AWS_LAMBDA_LOG_STREAM_NAME", "2026/11/13/[$LATEST]abc")
    fn = wrapped(lambda e, c: None, exporter, batch=False)
    assert fn(copy.deepcopy(DOCS), ctx()) is None
    rcv = spans(exporter, "trailbeam receive")
    out = spans(exporter, "trailbeam outcome")
    assert len(rcv) == len(out) == 2
    a = dict(rcv[0].attributes)
    pid = a.pop("process.pid")
    assert pid == os.getpid() and a.pop("thread.name")
    returned = a.pop("trailbeam.order.returned")
    assert isinstance(returned, int) and returned > 0
    assert a == {
        "trailbeam.schema_version": 3, "trailbeam.order.scope": _order.scope, "trailbeam.kind": "sqs.receive", "trailbeam.source": "lambda",
        "trailbeam.start_observed": False,
        "messaging.system.name": "aws.sqs", "messaging.operation.type": "receive",
        "messaging.operation.name": "LambdaEvent", "messaging.client.id": "2026/11/13/[$LATEST]abc",
        "messaging.destination.name": "my-queue",
        "aws.sqs.queue.url": "https://sqs.us-east-2.amazonaws.com/123456789012/my-queue",
        "trailbeam.queue.arn": "arn:aws:sqs:us-east-2:123456789012:my-queue",
        "messaging.message.id": "059f36b4-87a3-44ab-83d2-661975830a7d",
        "trailbeam.receipt_handle.hmac": h("AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a..."),
        "trailbeam.receipt_handle.key_id": "05769f58",
        "trailbeam.receive_count": 1, "trailbeam.sent_timestamp_ms": 1545082649183,
        "trailbeam.first_receive_timestamp_ms": 1545082649185,
        "faas.name": "orders-fn", "faas.invocation_id": "req-1",
    }
    o = dict(out[0].attributes)
    assert "trailbeam.start_observed" not in o  # the outcome is decided by the wrapper: its time is known
    # Contract v2 section 2: Lambda's own ReceiveMessage is not seen. The receive span is zero-length
    # at its upper bound (before the handler ran), marked start-not-observed; no start is invented.
    assert rcv[0].end_time <= out[0].start_time
    assert o["trailbeam.kind"] == "sqs.outcome" and o["messaging.operation.type"] == "process"
    assert o["trailbeam.outcome"] == "success" and "trailbeam.error_type" not in o
    assert (o["faas.name"], o["faas.invocation_id"]) == ("orders-fn", "req-1")
    for s in rcv + out:
        assert s.instrumentation_scope.name == "trailbeam" and s.start_time == s.end_time
        assert s.name in ("trailbeam receive", "trailbeam outcome")
        for v in s.attributes.values():
            assert v != "" and "AQEB" not in str(v)


def test_dead_letter_queue_source_arn_is_copied_if_the_record_carries_it(exporter):
    """Schema 3.1. Not verified on AWS whether Lambda's records carry it; the wrapper copies it if they do."""
    fn = wrapped(lambda e, c: None, exporter, batch=False)
    ev = copy.deepcopy(DOCS)
    ev["Records"][0]["attributes"]["DeadLetterQueueSourceArn"] = "arn:aws:sqs:us-east-2:123456789012:orders"
    ev["Records"][1]["attributes"]["DeadLetterQueueSourceArn"] = ""
    fn(ev, ctx())
    got = [s.attributes.get("trailbeam.dead_letter_queue_source_arn") for s in spans(exporter, "trailbeam receive")]
    assert got == ["arn:aws:sqs:us-east-2:123456789012:orders", None]


def test_exception_records_class_name_only_and_reraises(exporter):
    class SecretError(ValueError):
        pass

    def handler(event, context):
        raise SecretError("contains customer data")

    fn = wrapped(handler, exporter, batch=False)
    with pytest.raises(SecretError):
        fn(copy.deepcopy(DOCS), ctx())
    out = spans(exporter, "trailbeam outcome")
    assert [s.attributes["trailbeam.outcome"] for s in out] == ["failed_exception"] * 2
    assert {s.attributes["trailbeam.error_type"] for s in out} == {"SecretError"}
    assert not any("customer" in str(v) for s in out for v in s.attributes.values())


def test_receive_span_parent_is_the_active_span(exporter):
    tp = TracerProvider()
    fn = wrapped(lambda e, c: None, exporter, batch=False)
    with tp.get_tracer("app").start_as_current_span("invocation") as parent:
        fn(copy.deepcopy(DOCS), ctx())
    for s in spans(exporter):
        assert s.parent.span_id == parent.get_span_context().span_id
    exporter.clear()
    fn(copy.deepcopy(DOCS), ctx())
    assert all(s.parent is None for s in spans(exporter))


def test_non_sqs_events_pass_through(exporter):
    fn = wrapped(lambda e, c: "ok", exporter, batch=False)
    assert fn({"Records": [{"eventSource": "aws:sns"}]}, ctx()) == "ok"
    assert fn({"hello": 1}, ctx()) == "ok"
    assert fn([1, 2], ctx()) == "ok"
    assert spans(exporter) == []


def test_decorator_with_arguments(exporter):
    @ql.wrap(receipt_hash_key=TEST_KEY, span_processor=SimpleSpanProcessor(exporter))
    def handler(event, context):
        return {"batchItemFailures": []}

    assert handler.__name__ == "handler"
    handler(copy.deepcopy(DOCS), ctx())
    assert [s.attributes["trailbeam.outcome"] for s in spans(exporter, "trailbeam outcome")] == ["success"] * 2


def test_without_log_stream_falls_back_to_instance_id(exporter, monkeypatch):
    monkeypatch.delenv("AWS_LAMBDA_LOG_STREAM_NAME", raising=False)
    fn = wrapped(lambda e, c: None, exporter, batch=False)
    fn(copy.deepcopy(DOCS), ctx())
    s = spans(exporter)[0]
    assert s.attributes["messaging.client.id"] == s.resource.attributes["service.instance.id"]


# -- outcome rules ------------------------------------------------------------------------------

IDS = ["a", "b", "c"]


@pytest.mark.parametrize("result,expected", [
    (None, ["success"] * 3),
    ({}, ["success"] * 3),
    ("ok", ["success"] * 3),
    ({"batchItemFailures": None}, ["success"] * 3),
    ({"batchItemFailures": []}, ["success"] * 3),
    ({"batchItemFailures": [{"itemIdentifier": "b"}]}, ["success", "failed_reported", "success"]),
    ({"batchItemFailures": [{"itemIdentifier": "a"}, {"itemIdentifier": "c"}]},
     ["failed_reported", "success", "failed_reported"]),
    ({"batchItemFailures": [{"itemIdentifier": "b"}, {"itemIdentifier": "zzz"}]}, ["failed_invalid_response"] * 3),
    ({"batchItemFailures": [{"itemIdentifier": ""}]}, ["failed_invalid_response"] * 3),
    ({"batchItemFailures": [{"itemIdentifier": None}]}, ["failed_invalid_response"] * 3),
    ({"batchItemFailures": [{"itemId": "a"}]}, ["failed_invalid_response"] * 3),
    ({"batchItemFailures": ["a"]}, ["failed_invalid_response"] * 3),
    ({"batchItemFailures": "a"}, ["failed_invalid_response"] * 3),
])
def test_decide_outcomes(result, expected):
    assert [ql.decide_outcomes(IDS, result)[i] for i in IDS] == expected


def test_batch_item_failures_ignored_when_not_reported():
    r = {"batchItemFailures": [{"itemIdentifier": "zzz"}]}
    assert set(ql.decide_outcomes(IDS, r, report_batch_item_failures=False).values()) == {"success"}


def test_queue_from_arn():
    assert ql.queue_from_arn("arn:aws:sqs:eu-west-1:111122223333:orders") == (
        "orders", "https://sqs.eu-west-1.amazonaws.com/111122223333/orders")
    assert ql.queue_from_arn("garbage") == ("garbage", "")


# -- call order inside one process (contract v3) ----------------------------------------------

def test_receive_has_returned_only_and_outcome_has_one_tick_after_it(exporter):
    seen = {}

    def handler(event, context):
        seen["during"] = _order.tick()

    fn = wrapped(handler, exporter, batch=False)
    fn(copy.deepcopy(DOCS), ctx())
    rcv = spans(exporter, "trailbeam receive")
    out = spans(exporter, "trailbeam outcome")
    assert len(rcv) == len(out) == 2
    for s in rcv + out:
        assert s.attributes["trailbeam.order.scope"] == _order.scope
    # Lambda's poller made the ReceiveMessage call: its issue is not seen, so there is no issued tick.
    assert all("trailbeam.order.issued" not in s.attributes for s in rcv)
    assert len({s.attributes["trailbeam.order.returned"] for s in rcv}) == 1
    assert len({(s.attributes["trailbeam.order.issued"], s.attributes["trailbeam.order.returned"]) for s in out}) == 1
    r_ret = rcv[0].attributes["trailbeam.order.returned"]
    o = out[0].attributes
    assert o["trailbeam.order.issued"] == o["trailbeam.order.returned"]
    assert r_ret < seen["during"] < o["trailbeam.order.issued"]


def test_outcome_after_an_exception_is_ordered_after_the_receive(exporter):
    def handler(event, context):
        raise RuntimeError("boom")

    fn = wrapped(handler, exporter, batch=False)
    with pytest.raises(RuntimeError):
        fn(copy.deepcopy(DOCS), ctx())
    [r0, _] = spans(exporter, "trailbeam receive")
    [o0, _] = spans(exporter, "trailbeam outcome")
    assert o0.attributes["trailbeam.outcome"] == "failed_exception"
    assert o0.attributes["trailbeam.order.issued"] == o0.attributes["trailbeam.order.returned"] > r0.attributes["trailbeam.order.returned"]
