"""The recorded killed Lambda invocation (QV-9) through the real qv2 Lambda wrapper, exported over OTLP/HTTP
to a running store: the store must show the outcome the wrapper could not send as inferred.

Skipped unless the `qv2` package from ../python is installed (pip install -e '../python[test]'). No AWS.
"""
import copy
import json
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip("qv2.aws_lambda")

from qv2_store import queries  # noqa: E402

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

    import qv2.aws_lambda as ql

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
    fn.qv2_tracer_provider.shutdown()
    assert [r["kind"] for r in rows] == ["receive", "receive", "outcome"]
    # Both invocations ran milliseconds apart here; on SQS the redelivery comes only after the visibility
    # timeout. Look once the killed receive is older than the Lambda timeout, as it is by then on SQS.
    final = queries.message(store, KILLED_MID, received_ns + (LT + 1) * SEC, LT)
    got = types(final)
    assert ("lambda_invocation_ended_without_outcome", True) in got
    assert [t for t in got if not t[1]] == [("received", False), ("received", False), ("lambda_outcome", False)]
    assert final["state"] == "deleted_inferred" and final["deliveries"] == 2
