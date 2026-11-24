"""Trailbeam wrapper for AWS Lambda handlers invoked by an SQS event source mapping (contract 4.2, 4.4).

Usage::

    import trailbeam.aws_lambda

    @trailbeam.aws_lambda.wrap
    def handler(event, context):
        ...

For each SQS record the wrapper emits one ``trailbeam receive`` and flushes it before the handler runs,
so a timeout or out-of-memory kill cannot lose it. After the handler returns or throws it emits one
``trailbeam outcome`` per record. A killed invocation has no outcome; Trailbeam infers it.
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
from . import _capture, _order
from ._emitter import Emitter, make_provider

log = logging.getLogger("trailbeam")
OPERATION = "LambdaEvent"
DEFAULT_FLUSH_TIMEOUT_MILLIS = 2000


def queue_from_arn(arn: str) -> tuple[str, str]:
    """``arn:aws:sqs:<region>:<account>:<name>`` -> (name, https://sqs.<region>.amazonaws.com/<account>/<name>)."""
    parts = arn.split(":")
    if len(parts) < 6 or not parts[5]:
        return (parts[-1] if parts else ""), ""
    region, account, name = parts[3], parts[4], ":".join(parts[5:])
    return name, f"https://sqs.{region}.amazonaws.com/{account}/{name}"


def reported_failure_count(outcomes: dict[str, str]) -> Optional[int]:
    """3.2: records the handler listed in a valid ``batchItemFailures`` (0 if none); None if the list was invalid."""
    values = list(outcomes.values())
    if any(v == OUTCOME_FAILED_INVALID_RESPONSE for v in values):
        return None
    return sum(1 for v in values if v == OUTCOME_FAILED_REPORTED)


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
    """Emits the wrapper's spans. One per wrapped handler; holds the Trailbeam tracer provider."""

    def __init__(self, provider: TracerProvider, emitter: Emitter, flush_timeout_millis: int,
                 report_batch_item_failures: bool, capture: Optional[_capture.Config] = None):
        self.capture = capture if capture is not None else _capture.Config()
        self.provider = provider
        self.emitter = emitter
        self.flush_timeout_millis = flush_timeout_millis
        self.report_batch_item_failures = report_batch_item_failures

    def _flush(self) -> None:
        try:
            self.provider.force_flush(self.flush_timeout_millis)
        except Exception:  # pragma: no cover - never fail the invocation because export failed
            log.debug("trailbeam flush failed", exc_info=True)

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
        size = len(records)
        for index, r in enumerate(records):
            arn = r.get("eventSourceARN") or ""
            name, url = queue_from_arn(arn)
            a = r.get("attributes") or {}
            attrs: dict = {"messaging.message.id": r["messageId"]}
            rh = r.get("receiptHandle")
            if isinstance(rh, str) and rh:
                attrs.update(self.emitter.hash_attrs(rh))
            put(attrs, "trailbeam.receive_count", int_or_none(a.get("ApproximateReceiveCount")))
            put(attrs, "trailbeam.sent_timestamp_ms", int_or_none(a.get("SentTimestamp")))
            put(attrs, "trailbeam.first_receive_timestamp_ms", int_or_none(a.get("ApproximateFirstReceiveTimestamp")))
            put(attrs, "trailbeam.aws_trace_header", a.get("AWSTraceHeader"))
            put(attrs, "trailbeam.dead_letter_queue_source_arn", a.get("DeadLetterQueueSourceArn"))
            put(attrs, "trailbeam.queue.arn", arn)
            attrs.update(faas)
            attrs["trailbeam.batch.size"] = size
            attrs["trailbeam.batch.index"] = index
            attrs.update(_capture.fifo_attrs(a.get("MessageGroupId"), a.get("SequenceNumber")))
            try:
                attrs.update(_capture.content_attrs(self.capture, name, r.get("body"), r.get("messageAttributes"),
                                                    r.get("md5OfBody"), lambda_record=True))
            except Exception:  # pragma: no cover - defensive
                attrs["trailbeam.body.capture_status"] = _capture.STATUS_REDACTION_FAILED
            try:  # 3.4 (section 21.3): the SNS publish this record came from
                attrs.update(_capture.sns_receive_attrs(self.capture, name, r.get("body"), r.get("messageAttributes"),
                                                        lambda_record=True))
            except Exception:  # pragma: no cover - defensive
                pass
            self.emitter.emit(SPAN_RECEIVE, OPERATION, name, url, attrs, end_ns=t, start_observed=False,
                              order=(None, returned))
        self._flush()

    def outcomes(self, records: list[dict], context, outcomes: dict[str, str], error_type: Optional[str],
                 error: Optional[BaseException] = None) -> None:
        faas = self._faas(context)
        size = len(records)
        reported = reported_failure_count(outcomes) if error is None else None
        # The outcome is decided now, after the handler returned or threw: one moment, one tick.
        t = time.time_ns()
        tick = _order.tick()
        for index, r in enumerate(records):
            name, url = queue_from_arn(r.get("eventSourceARN") or "")
            mid = r["messageId"]
            attrs: dict = {"messaging.message.id": mid, "trailbeam.outcome": outcomes[mid]}
            if outcomes[mid] == OUTCOME_FAILED_EXCEPTION:
                put(attrs, "trailbeam.error_type", error_type)
                if error is not None:
                    put(attrs, "trailbeam.error_message", _capture.error_message(self.capture, name, error))
            attrs.update(faas)
            attrs["trailbeam.batch.size"] = size
            attrs["trailbeam.batch.index"] = index
            if reported is not None:
                attrs["trailbeam.batch.reported_failure_count"] = reported
            self.emitter.emit(SPAN_OUTCOME, OPERATION, name, url, attrs, start_ns=t, end_ns=t, order=(tick, tick))
        self._flush()

    def invoke(self, handler: Callable, event, context, *args, **kwargs):
        records = sqs_records(event)
        if not records:
            return handler(event, context, *args, **kwargs)
        try:
            self.receives(records, context)
        except Exception:  # pragma: no cover - defensive
            log.debug("trailbeam receive spans failed", exc_info=True)
        ids = [r["messageId"] for r in records]
        try:
            result = handler(event, context, *args, **kwargs)
        except Exception as e:
            # BaseException (SystemExit, KeyboardInterrupt) is not caught: the process is going away,
            # like a timeout, and Trailbeam infers that from the receive with no outcome.
            self._safe_outcomes(records, context, {i: OUTCOME_FAILED_EXCEPTION for i in ids}, type(e).__name__, e)
            raise
        self._safe_outcomes(records, context, decide_outcomes(ids, result, self.report_batch_item_failures), None)
        return result

    def _safe_outcomes(self, records, context, outcomes, error_type, error=None) -> None:
        try:
            self.outcomes(records, context, outcomes, error_type, error)
        except Exception:  # pragma: no cover - defensive
            log.debug("trailbeam outcome spans failed", exc_info=True)


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
    tenant: Optional[dict] = None,
    capture: Optional[dict] = None,
    sns: Optional[dict] = None,
):
    """Wrap a Lambda handler. Use as ``@wrap`` or ``@wrap(receipt_hash_key=...)``.

    ``tenant``, ``capture`` and ``sns`` are as in ``trailbeam.boto.install`` (contract sections 15 and 21). With the
    record's queue in ``capture["error_queues"]``, a thrown exception's message is recorded, redacted.

    ``report_batch_item_failures`` should match the event source mapping's
    ``FunctionResponseTypes``: when it is off, Lambda ignores ``batchItemFailures``, and so does Trailbeam.
    Events without SQS records are passed to the handler untouched.
    """
    def decorate(fn: Callable) -> Callable:
        provider = make_provider(span_exporter, span_processor)
        emitter = Emitter(provider, resolve_hasher(receipt_hash_key), source="lambda")
        emitter.default_client_id = emitter.client_id
        recorder = LambdaRecorder(provider, emitter, flush_timeout_millis, report_batch_item_failures,
                                  _capture.resolve_config(tenant, capture, sns))

        @functools.wraps(fn)
        def wrapped(event, context, *args, **kwargs):
            return recorder.invoke(fn, event, context, *args, **kwargs)

        wrapped.trailbeam_tracer_provider = provider  # type: ignore[attr-defined]
        return wrapped

    return decorate(handler) if handler is not None else decorate
