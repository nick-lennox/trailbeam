"""qv2 wrapper for AWS Lambda handlers invoked by an SQS event source mapping (contract 4.2, 4.4).

Usage::

    import qv2.aws_lambda

    @qv2.aws_lambda.wrap
    def handler(event, context):
        ...

For each SQS record the wrapper emits one ``qv2 receive`` and flushes it before the handler runs,
so a timeout or out-of-memory kill cannot lose it. After the handler returns or throws it emits one
``qv2 outcome`` per record. A killed invocation has no outcome; the store infers it.
"""
from __future__ import annotations

import functools
import logging
import os
import time
from typing import Any, Callable, Optional

from opentelemetry.sdk.trace import SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import SpanExporter

from ._contract import (
    OUTCOME_FAILED_EXCEPTION,
    OUTCOME_FAILED_INVALID_RESPONSE,
    OUTCOME_FAILED_REPORTED,
    OUTCOME_SUCCESS,
    SPAN_OUTCOME,
    SPAN_RECEIVE,
    int_or_none,
    put,
    resolve_hasher,
)
from . import _order
from ._emitter import Emitter, make_provider

log = logging.getLogger("qv2")
OPERATION = "LambdaEvent"
DEFAULT_FLUSH_TIMEOUT_MILLIS = 2000


def queue_from_arn(arn: str) -> tuple[str, str]:
    """``arn:aws:sqs:<region>:<account>:<name>`` -> (name, https://sqs.<region>.amazonaws.com/<account>/<name>)."""
    parts = arn.split(":")
    if len(parts) < 6 or not parts[5]:
        return (parts[-1] if parts else ""), ""
    region, account, name = parts[3], parts[4], ":".join(parts[5:])
    return name, f"https://sqs.{region}.amazonaws.com/{account}/{name}"


def decide_outcomes(record_ids: list[str], result: Any, report_batch_item_failures: bool = True) -> dict[str, str]:
    """Outcome per record after the handler returned ``result`` (contract 4.4).

    Follows Lambda's partial batch response rules: no ``batchItemFailures``, null or empty means
    success; an entry whose ``itemIdentifier`` is missing, empty or not in the batch fails the batch.
    """
    ids = set(record_ids)
    failed: set = set()
    if report_batch_item_failures and isinstance(result, dict):
        items = result.get("batchItemFailures")
        if items is not None:
            valid = isinstance(items, list)
            for item in items if valid else []:
                ident = item.get("itemIdentifier") if isinstance(item, dict) else None
                if isinstance(ident, str) and ident in ids:
                    failed.add(ident)
                else:
                    valid = False
            if not valid:
                return {i: OUTCOME_FAILED_INVALID_RESPONSE for i in record_ids}
    return {i: (OUTCOME_FAILED_REPORTED if i in failed else OUTCOME_SUCCESS) for i in record_ids}


class LambdaRecorder:
    """Emits the wrapper's spans. One per wrapped handler; holds the qv2 tracer provider."""

    def __init__(self, provider: TracerProvider, emitter: Emitter, flush_timeout_millis: int,
                 report_batch_item_failures: bool):
        self.provider = provider
        self.emitter = emitter
        self.flush_timeout_millis = flush_timeout_millis
        self.report_batch_item_failures = report_batch_item_failures

    def _flush(self) -> None:
        try:
            self.provider.force_flush(self.flush_timeout_millis)
        except Exception:  # pragma: no cover - never fail the invocation because export failed
            log.debug("qv2 flush failed", exc_info=True)

    @staticmethod
    def _faas(context) -> dict:
        attrs: dict = {}
        put(attrs, "faas.name", getattr(context, "function_name", None) or os.environ.get("AWS_LAMBDA_FUNCTION_NAME"))
        put(attrs, "faas.invocation_id", getattr(context, "aws_request_id", None))
        return attrs

    def _client_id(self) -> None:
        # One Lambda execution environment = one log stream (contract section 5).
        self.emitter.client_id = os.environ.get("AWS_LAMBDA_LOG_STREAM_NAME") or self.emitter.default_client_id

    def receives(self, records: list[dict], context) -> None:
        self._client_id()
        faas = self._faas(context)
        # Lambda's poller made the ReceiveMessage call, out of sight of the wrapper. All the wrapper
        # knows is that it happened no later than now (contract section 2): the span ends now, and its
        # start is marked as not observed rather than invented. Likewise the call-order tick (schema
        # version 3): one "returned" tick for the moment the event arrived, and no "issued" tick.
        t = time.time_ns()
        returned = _order.tick()
        for r in records:
            arn = r.get("eventSourceARN") or ""
            name, url = queue_from_arn(arn)
            a = r.get("attributes") or {}
            attrs: dict = {"messaging.message.id": r["messageId"]}
            rh = r.get("receiptHandle")
            if isinstance(rh, str) and rh:
                attrs.update(self.emitter.hash_attrs(rh))
            put(attrs, "qv2.receive_count", int_or_none(a.get("ApproximateReceiveCount")))
            put(attrs, "qv2.sent_timestamp_ms", int_or_none(a.get("SentTimestamp")))
            put(attrs, "qv2.first_receive_timestamp_ms", int_or_none(a.get("ApproximateFirstReceiveTimestamp")))
            put(attrs, "qv2.aws_trace_header", a.get("AWSTraceHeader"))
            put(attrs, "qv2.queue.arn", arn)
            attrs.update(faas)
            self.emitter.emit(SPAN_RECEIVE, OPERATION, name, url, attrs, end_ns=t, start_observed=False,
                              order=(None, returned))
        self._flush()

    def outcomes(self, records: list[dict], context, outcomes: dict[str, str], error_type: Optional[str]) -> None:
        faas = self._faas(context)
        # The outcome is decided now, after the handler returned or threw: one moment, one tick.
        t = time.time_ns()
        tick = _order.tick()
        for r in records:
            name, url = queue_from_arn(r.get("eventSourceARN") or "")
            mid = r["messageId"]
            attrs: dict = {"messaging.message.id": mid, "qv2.outcome": outcomes[mid]}
            if outcomes[mid] == OUTCOME_FAILED_EXCEPTION:
                put(attrs, "qv2.error_type", error_type)
            attrs.update(faas)
            self.emitter.emit(SPAN_OUTCOME, OPERATION, name, url, attrs, start_ns=t, end_ns=t, order=(tick, tick))
        self._flush()

    def invoke(self, handler: Callable, event, context, *args, **kwargs):
        records = sqs_records(event)
        if not records:
            return handler(event, context, *args, **kwargs)
        try:
            self.receives(records, context)
        except Exception:  # pragma: no cover - defensive
            log.debug("qv2 receive spans failed", exc_info=True)
        ids = [r["messageId"] for r in records]
        try:
            result = handler(event, context, *args, **kwargs)
        except Exception as e:
            # BaseException (SystemExit, KeyboardInterrupt) is not caught: the process is going away,
            # like a timeout, and the store infers that from the receive with no outcome.
            self._safe_outcomes(records, context, {i: OUTCOME_FAILED_EXCEPTION for i in ids}, type(e).__name__)
            raise
        self._safe_outcomes(records, context, decide_outcomes(ids, result, self.report_batch_item_failures), None)
        return result

    def _safe_outcomes(self, records, context, outcomes, error_type) -> None:
        try:
            self.outcomes(records, context, outcomes, error_type)
        except Exception:  # pragma: no cover - defensive
            log.debug("qv2 outcome spans failed", exc_info=True)


def sqs_records(event) -> list[dict]:
    if not isinstance(event, dict):
        return []
    recs = event.get("Records")
    if not isinstance(recs, list):
        return []
    return [r for r in recs if isinstance(r, dict) and r.get("eventSource") == "aws:sqs"
            and isinstance(r.get("messageId"), str) and r["messageId"]]


def wrap(
    handler: Optional[Callable] = None,
    *,
    receipt_hash_key: Optional[str] = None,
    span_exporter: Optional[SpanExporter] = None,
    span_processor: Optional[SpanProcessor] = None,
    flush_timeout_millis: int = DEFAULT_FLUSH_TIMEOUT_MILLIS,
    report_batch_item_failures: bool = True,
):
    """Wrap a Lambda handler. Use as ``@wrap`` or ``@wrap(receipt_hash_key=...)``.

    ``report_batch_item_failures`` should match the event source mapping's
    ``FunctionResponseTypes``: when it is off, Lambda ignores ``batchItemFailures``, and so does qv2.
    Events without SQS records are passed to the handler untouched.
    """
    def decorate(fn: Callable) -> Callable:
        provider = make_provider(span_exporter, span_processor)
        emitter = Emitter(provider, resolve_hasher(receipt_hash_key), source="lambda")
        emitter.default_client_id = emitter.client_id
        recorder = LambdaRecorder(provider, emitter, flush_timeout_millis, report_batch_item_failures)

        @functools.wraps(fn)
        def wrapped(event, context, *args, **kwargs):
            return recorder.invoke(fn, event, context, *args, **kwargs)

        wrapped.qv2_tracer_provider = provider  # type: ignore[attr-defined]
        return wrapped

    return decorate(handler) if handler is not None else decorate
