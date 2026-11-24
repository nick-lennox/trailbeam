"""Builders for contract v3 Trailbeam spans and OTLP requests, for Trailbeam tests."""
from __future__ import annotations

import hashlib
import hmac
import itertools
import os
from dataclasses import dataclass, field

from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, InstrumentationScope, KeyValue
from opentelemetry.proto.resource.v1.resource_pb2 import Resource
from opentelemetry.proto.trace.v1.trace_pb2 import ResourceSpans, ScopeSpans, Span

KEY = "db-test-key-0123456789"
T0 = 1_763_080_000_000_000_000  # ns
SEC = 1_000_000_000


def rh_hash(handle: str, key: str = KEY) -> str:
    return hmac.new(key.encode(), handle.encode(), hashlib.sha256).hexdigest()[:32]


def key_id(key: str = KEY) -> str:
    return hmac.new(key.encode(), b"trailbeam key id", hashlib.sha256).hexdigest()[:8]


_ids = itertools.count(1)


@dataclass
class S:
    """One Trailbeam span: end time ``t`` (ns), attributes, trace ID, and start time (default: ``t``)."""
    name: str
    t: int
    attrs: dict
    trace_id: bytes = field(default_factory=lambda: os.urandom(16))
    span_id: bytes = field(default_factory=lambda: next(_ids).to_bytes(8, "big"))
    service: str | None = None  # resource service.name; None = the request's default
    start: int | None = None
    version: str | None = None  # resource service.version; None: not set

    @property
    def start_time(self) -> int:
        return self.t if self.start is None else self.start


@dataclass
class Consumer:
    """Builds the spans one process emits, with the attributes of contract section 3."""
    client_id: str = "consumer-1"
    service: str = "orders-worker"
    source: str = "client"
    key: str = KEY
    queue: str = "orders"
    version: str | None = None  # the resource's service.version; None: not set

    scope: str | None = None  # trailbeam.order.scope; None: no call-order attributes (as a v2 span)

    def _s(self, name, t, a, trace_id=None, start=None, order=None) -> S:
        """``t`` is the span's end (the response); ``start`` its start (default: ``t``). ``order`` is
        (issued, returned), the call-order counter values (contract section 2a); either may be None."""
        if order is not None:
            assert self.scope, "set Consumer(scope=...) to emit call order"
            a["trailbeam.order.scope"] = self.scope
            if order[0] is not None:
                a["trailbeam.order.issued"] = order[0]
            if order[1] is not None:
                a["trailbeam.order.returned"] = order[1]
        sp = S(name, t, a, service=self.service, start=start, version=self.version)
        if trace_id:
            sp.trace_id = trace_id
        return sp

    def base(self, kind: str, op_type: str, op: str, queue: str | None) -> dict:
        q = queue or self.queue
        return {
            "trailbeam.schema_version": 3, "trailbeam.kind": kind, "trailbeam.source": self.source,
            "messaging.system.name": "aws.sqs", "messaging.operation.type": op_type,
            "messaging.operation.name": op, "messaging.destination.name": q,
            "aws.sqs.queue.url": f"http://localhost:9324/000000000000/{q}",
            "messaging.client.id": self.client_id, "process.pid": 4242, "thread.name": "worker-1",
        }

    def send(self, t, mid, queue=None, batch_entry=None, trace_id=None, start=None, order=None):
        a = self.base("sqs.send", "send", "SendMessageBatch" if batch_entry else "SendMessage", queue)
        a["messaging.message.id"] = mid
        if batch_entry:
            a["trailbeam.batch_entry_id"] = batch_entry
        return self._s("trailbeam send", t, a, trace_id, start, order)

    def receive(self, t, mid, handle, count=1, queue=None, trace_id=None, sent_ms=None, lambda_inv=None, start=None,
                first_ms=None, order=None, dlq_source=None):
        """``dlq_source``: the queue name SQS reports in DeadLetterQueueSourceArn (contract 3.1)."""
        a = self.base("sqs.receive", "receive", "LambdaEvent" if lambda_inv else "ReceiveMessage", queue)
        a.update({"messaging.message.id": mid, "trailbeam.receive_count": count,
                  "trailbeam.receipt_handle.hmac": rh_hash(handle, self.key),
                  "trailbeam.receipt_handle.key_id": key_id(self.key)})
        if sent_ms is not None:
            a["trailbeam.sent_timestamp_ms"] = sent_ms
        if first_ms is not None:
            a["trailbeam.first_receive_timestamp_ms"] = first_ms
        if dlq_source is not None:
            a["trailbeam.dead_letter_queue_source_arn"] = f"arn:aws:sqs:us-east-1:000000000000:{dlq_source}"
        if lambda_inv:
            # The Lambda wrapper only knows an upper bound (contract section 2).
            a.update({"faas.name": "orders-fn", "faas.invocation_id": lambda_inv, "trailbeam.start_observed": False})
        return self._s("trailbeam receive", t, a, trace_id, start, order)

    def delete(self, t, handle, mid=None, ok=True, batch_entry=None, queue=None, trace_id=None,
               error="ReceiptHandleIsInvalid", start=None, order=None):
        a = self.base("sqs.settle", "settle", "DeleteMessageBatch" if batch_entry else "DeleteMessage", queue)
        a.update({"trailbeam.receipt_handle.hmac": rh_hash(handle, self.key),
                  "trailbeam.receipt_handle.key_id": key_id(self.key), "trailbeam.result": "ok" if ok else "failed"})
        if not ok:
            a["trailbeam.error_code"] = error
        if mid:
            a["messaging.message.id"] = mid
        if batch_entry:
            a["trailbeam.batch_entry_id"] = batch_entry
        return self._s("trailbeam delete", t, a, trace_id, start, order)

    def change_visibility(self, t, handle, timeout, mid=None, ok=True, queue=None, start=None, order=None):
        a = self.base("sqs.settle", "settle", "ChangeMessageVisibility", queue)
        a.update({"trailbeam.receipt_handle.hmac": rh_hash(handle, self.key),
                  "trailbeam.receipt_handle.key_id": key_id(self.key), "trailbeam.result": "ok" if ok else "failed",
                  "trailbeam.visibility_timeout": timeout})
        if not ok:
            a["trailbeam.error_code"] = "MessageNotInflight"
        if mid:
            a["messaging.message.id"] = mid
        return self._s("trailbeam change_visibility", t, a, start=start, order=order)

    def outcome(self, t, mid, outcome, invocation, queue=None, error_type=None, order=None):
        a = self.base("sqs.outcome", "process", "LambdaEvent", queue)
        a.update({"messaging.message.id": mid, "trailbeam.outcome": outcome,
                  "faas.name": "orders-fn", "faas.invocation_id": invocation})
        if error_type:
            a["trailbeam.error_type"] = error_type
        return self._s("trailbeam outcome", t, a, order=order)

    def purge(self, t, queue=None, ok=True, start=None, order=None, error="PurgeQueueInProgress"):
        """A trailbeam purge (contract section 18): one PurgeQueue call, with no message ID."""
        a = self.base("sqs.purge", "purge", "PurgeQueue", queue)
        a["trailbeam.result"] = "ok" if ok else "failed"
        if not ok:
            a["trailbeam.error_code"] = error
        return self._s("trailbeam purge", t, a, start=start, order=order)


def _value(v) -> AnyValue:
    if isinstance(v, bool):
        return AnyValue(bool_value=v)
    if isinstance(v, int):
        return AnyValue(int_value=v)
    return AnyValue(string_value=str(v))


def to_proto(spans: list[S], service: str = "orders-worker", scope: str = "trailbeam") -> ExportTraceServiceRequest:
    """One ResourceSpans per service (and service.version), as if each service exported its own spans."""
    groups: dict[tuple, list[S]] = {}
    for sp in spans:
        groups.setdefault((sp.service or service, sp.version), []).append(sp)
    return ExportTraceServiceRequest(resource_spans=[_resource_spans(g, svc, scope, ver)
                                                     for (svc, ver), g in groups.items()])


def _resource_spans(spans: list[S], service: str, scope: str, version: str | None = None) -> ResourceSpans:
    pb = [Span(name=s.name, trace_id=s.trace_id, span_id=s.span_id, kind=Span.SPAN_KIND_INTERNAL,
               start_time_unix_nano=s.start_time, end_time_unix_nano=s.t,
               attributes=[KeyValue(key=k, value=_value(v)) for k, v in s.attrs.items()]) for s in spans]
    return ResourceSpans(
        resource=Resource(attributes=[KeyValue(key="service.name", value=AnyValue(string_value=service))]
                          + ([KeyValue(key="service.version", value=AnyValue(string_value=version))] if version else [])),
        scope_spans=[ScopeSpans(scope=InstrumentationScope(name=scope, version="0.1.0"), spans=pb)])


def _json_value(v) -> dict:
    if isinstance(v, bool):
        return {"boolValue": v}
    if isinstance(v, int):
        return {"intValue": str(v)}
    return {"stringValue": str(v)}


def to_json(spans: list[S], service: str = "orders-worker", scope: str = "trailbeam") -> dict:
    """OTLP/JSON, as the OTLP spec encodes it: hex IDs, 64-bit integers as strings."""
    return {"resourceSpans": [{
        "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": service}}]},
        "scopeSpans": [{"scope": {"name": scope, "version": "0.1.0"}, "spans": [
            {"traceId": s.trace_id.hex(), "spanId": s.span_id.hex(), "name": s.name, "kind": 1,
             "startTimeUnixNano": str(s.start_time), "endTimeUnixNano": str(s.t),
             "attributes": [{"key": k, "value": _json_value(v)} for k, v in s.attrs.items()]}
            for s in spans]}]}]}
