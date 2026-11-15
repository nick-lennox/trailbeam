"""The recorded killed Lambda invocation (QV-9) through the real Trailbeam Lambda wrapper, exported over OTLP/HTTP
to a running store: the store must show the outcome the wrapper could not send as inferred.

Needs the `trailbeam` package from ../python (pip install -e '../python[test]'): skipped locally without it, and a
failure in CI (tests/needs.py). No AWS.
"""
import copy
import json
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from needs import need

need("trailbeam.aws_lambda")

from trailbeam_store import queries  # noqa: E402

FIXTURE = Path(__file__).resolve().parents[2] / "python" / "tests" / "fixtures" / "qv9_lambda_events.json"
KILLED_MID = "ed51d232-5860-4f3f-82d1-34073e1cb9a3"  # function timeout in the QV-9 prototype
KEY = "lambda-end-to-end-key-0123456789"
SEC = 1_000_000_000
LT = 900  # the store's Lambda timeout, seconds


class Killed(BaseException):
    """Stands in for the runtime killing the invocation (timeout, out of memory): nothing after it runs."""


def invocations_of(mid):
    invs = json.loads(FIXTURE.read_text("utf-8"))["invocations"]
    return [i for i in invs if any(r["messageId"] == mid for r in i["event"]["Records"])]


def run(server, inv, monkeypatch):
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    import trailbeam.aws_lambda as ql

    monkeypatch.setenv("AWS_LAMBDA_LOG_STREAM_NAME", inv["log_stream_name"])
    kind = inv["behaviour"]["kind"]

    def handler(event, context):
        if kind == "killed":
            raise Killed()
        assert kind == "return"
        return {"batchItemFailures": [{"itemIdentifier": i} for i in inv["behaviour"]["batchItemFailures"]]}

    # The production processor: what reaches the store is what the wrapper flushed, not what shutdown() adds.
    proc = BatchSpanProcessor(OTLPSpanExporter(endpoint=server + "/v1/traces"), schedule_delay_millis=60_000)
    fn = ql.wrap(handler, receipt_hash_key=KEY, span_processor=proc)
    ctx = SimpleNamespace(aws_request_id=inv["aws_request_id"], function_name=inv["function_name"])
    if kind == "killed":
        with pytest.raises(Killed):
            fn(copy.deepcopy(inv["event"]), ctx)
    else:
        fn(copy.deepcopy(inv["event"]), ctx)
    return fn


def wait_rows(store, mid, n):
    deadline = time.time() + 5
    while time.time() < deadline and len(store.message_rows(mid)) < n:
        time.sleep(0.05)
    return store.message_rows(mid)


def types(t):
    return [(e["type"], e["inferred"]) for e in t["events"]]


def test_recorded_killed_invocation_gets_an_inferred_outcome_in_the_store(server, store, monkeypatch):
    killed, redelivered = invocations_of(KILLED_MID)
    assert killed["behaviour"]["kind"] == "killed" and killed["expected"] == {KILLED_MID: None}
    assert redelivered["expected"] == {KILLED_MID: "success"}

    # The killed invocation: the provider is never shut down, as when the runtime kills the process.
    run(server, killed, monkeypatch)
    rows = wait_rows(store, KILLED_MID, 1)
    assert [r["kind"] for r in rows] == ["receive"]  # flushed before the handler; no outcome
    received_ns = rows[0]["time_ns"]

    during = queries.message(store, KILLED_MID, received_ns + 60 * SEC, LT)
    assert types(during) == [("received", False)] and during["state"] == "processing"

    after = queries.message(store, KILLED_MID, received_ns + (LT + 1) * SEC, LT)
    assert types(after) == [("received", False), ("lambda_invocation_ended_without_outcome", True)]
    inferred = after["events"][1]
    assert inferred["time"] is None and inferred["reason"] and "span" not in inferred
    assert inferred["faas_invocation_id"] == killed["aws_request_id"] and inferred["delivery"] == 1
    assert after["state"] == "not_deleted"

    # The recorded redelivery succeeds: the killed delivery keeps its inferred outcome.
    fn = run(server, redelivered, monkeypatch)
    rows = wait_rows(store, KILLED_MID, 3)
    fn.trailbeam_tracer_provider.shutdown()
    assert [r["kind"] for r in rows] == ["receive", "receive", "outcome"]
    # Both invocations ran milliseconds apart here; on SQS the redelivery comes only after the visibility
    # timeout. Look once the killed receive is older than the Lambda timeout, as it is by then on SQS.
    final = queries.message(store, KILLED_MID, received_ns + (LT + 1) * SEC, LT)
    got = types(final)
    assert ("lambda_invocation_ended_without_outcome", True) in got
    assert [t for t in got if not t[1]] == [("received", False), ("received", False), ("lambda_outcome", False)]
    assert final["state"] == "deleted_inferred" and final["deliveries"] == 2


# -- order (QV-31): what the records carry orders the send, the deliveries and each handler's run --------

def _post_send(server, mid, end_ns):
    import urllib.request

    from spans import Consumer, to_proto

    send = Consumer("producer", service="orders-api", queue="qv2-exp-lambda", key=KEY).send(
        end_ns, mid, start=end_ns - 5_000_000)
    req = urllib.request.Request(server + "/v1/traces", data=to_proto([send]).SerializeToString(),
                                 headers={"Content-Type": "application/x-protobuf"}, method="POST")
    urllib.request.urlopen(req, timeout=5).read()


def _edges(t):
    by_id = {e["id"]: e for e in t["events"]}
    return {(by_id[e["from"]]["type"], by_id[e["from"]].get("delivery"),
             by_id[e["to"]]["type"], by_id[e["to"]].get("delivery")): e["sources"] for e in t["order"]["edges"]}


def _with_first_receive_timestamp(inv, delta_ms):
    """The record as Lambda delivers it: SQS events carry ApproximateFirstReceiveTimestamp (AWS's sample
    event, python/tests/fixtures/aws_docs_sqs_event.json); the QV-9 logs did not record it for this one."""
    inv = copy.deepcopy(inv)
    for r in inv["event"]["Records"]:
        a = r["attributes"]
        a["ApproximateFirstReceiveTimestamp"] = str(int(a["SentTimestamp"]) + delta_ms)
    return inv


@pytest.mark.parametrize("first_receive_ms", [None, 1])
def test_recorded_timed_out_retry_is_ordered_by_what_the_records_carry(server, store, monkeypatch, first_receive_ms):
    """The recorded QV-9 timeout: send, receive 1 (the function then timed out), receive 2, success.
    Before QV-31 the send and both receives were one "concurrent, order unknown" box, because a Lambda
    receive's start is not observed. Now the receive count puts receive 1 before receive 2, the wrapper's
    call order puts each outcome after its own receive, and, when the record carries
    ApproximateFirstReceiveTimestamp, SQS's timestamps put the send before receive 1. What stays unknown
    is when the killed invocation ended, so its inferred end shares a box with what came after receive 1."""
    killed, redelivered = invocations_of(KILLED_MID)
    if first_receive_ms is not None:
        killed = _with_first_receive_timestamp(killed, first_receive_ms)
        redelivered = _with_first_receive_timestamp(redelivered, first_receive_ms)
    _post_send(server, KILLED_MID, time.time_ns() - SEC)
    run(server, killed, monkeypatch)
    received_ns = wait_rows(store, KILLED_MID, 2)[-1]["time_ns"]
    fn = run(server, redelivered, monkeypatch)
    wait_rows(store, KILLED_MID, 4)
    fn.trailbeam_tracer_provider.shutdown()

    t = queries.message(store, KILLED_MID, received_ns + (LT + 1) * SEC, LT)
    edges = _edges(t)
    assert edges[("received", 1, "received", 2)] == ["receive_count"]  # no start: only SQS's count orders them
    assert "process_order" in edges[("received", 2, "lambda_outcome", 2)]
    if first_receive_ms is None:
        # The QV-9 record has no ApproximateFirstReceiveTimestamp: nothing orders the send and receive 1.
        assert ("sent", None, "received", 1) not in edges
        assert [e["type"] for e in t["events"]][:2] in (["sent", "received"], ["received", "sent"])
        assert t["events"][0]["group"] == t["events"][1]["group"] == 0
    else:
        assert edges[("sent", None, "received", 1)] == ["sqs_timestamps"]
        g = [(e["type"], e["group"]) for e in t["events"]]
        assert g[:2] == [("sent", 0), ("received", 1)]
        box = [e["type"] for e in t["events"] if e["group"] == 2]
        assert box == ["lambda_invocation_ended_without_outcome", "visibility_timeout_expired", "received",
                       "lambda_outcome", "deleted_by_lambda"]
    assert t["state"] == "deleted_inferred" and t["deliveries"] == 2
