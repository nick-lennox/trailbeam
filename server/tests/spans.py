"""Builders for contract v1 qv2 spans and OTLP requests, for the store tests."""
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

KEY = "store-test-key-0123456789"
T0 = 1_763_080_000_000_000_000  # ns
SEC = 1_000_000_000


def rh_hash(handle: str, key: str = KEY) -> str:
    return hmac.new(key.encode(), handle.encode(), hashlib.sha256).hexdigest()[:32]


def key_id(key: str = KEY) -> str:
    return hmac.new(key.encode(), b"qv2 key id", hashlib.sha256).hexdigest()[:8]


_ids = itertools.count(1)


@dataclass
class S:
    """One qv2 span: start time (ns), attributes, trace ID."""
    name: str
    t: int
    attrs: dict
    trace_id: bytes = field(default_factory=lambda: os.urandom(16))
    span_id: bytes = field(default_factory=lambda: next(_ids).to_bytes(8, "big"))


@dataclass
class Consumer:
    """Builds the spans one process emits, with the attributes of contract section 3."""
    client_id: str = "consumer-1"
    service: str = "orders-worker"
    source: str = "client"
    key: str = KEY
    queue: str = "orders"

    def base(self, kind: str, op_type: str, op: str, queue: str | None) -> dict:
        q = queue or self.queue
        return {
            "qv2.schema_version": 1, "qv2.kind": kind, "qv2.source": self.source,
            "messaging.system.name": "aws.sqs", "messaging.operation.type": op_type,
            "messaging.operation.name": op, "messaging.destination.name": q,
            "aws.sqs.queue.url": f"http://localhost:9324/000000000000/{q}",
            "messaging.client.id": self.client_id, "process.pid": 4242, "thread.name": "worker-1",
        }

    def send(self, t, mid, queue=None, batch_entry=None, trace_id=None):
        a = self.base("sqs.send", "send", "SendMessageBatch" if batch_entry else "SendMessage", queue)
        a["messaging.message.id"] = mid
        if batch_entry:
            a["qv2.batch_entry_id"] = batch_entry
        return S("qv2 send", t, a, **({"trace_id": trace_id} if trace_id else {}))

    def receive(self, t, mid, handle, count=1, queue=None, trace_id=None, sent_ms=None, lambda_inv=None):
        a = self.base("sqs.receive", "receive", "LambdaEvent" if lambda_inv else "ReceiveMessage", queue)
        a.update({"messaging.message.id": mid, "qv2.receive_count": count,
                  "qv2.receipt_handle.hmac": rh_hash(handle, self.key),
                  "qv2.receipt_handle.key_id": key_id(self.key)})
        if sent_ms is not None:
            a["qv2.sent_timestamp_ms"] = sent_ms
        if lambda_inv:
            a.update({"faas.name": "orders-fn", "faas.invocation_id": lambda_inv})
        return S("qv2 receive", t, a, **({"trace_id": trace_id} if trace_id else {}))

    def delete(self, t, handle, mid=None, ok=True, batch_entry=None, queue=None, trace_id=None, error="ReceiptHandleIsInvalid"):
        a = self.base("sqs.settle", "settle", "DeleteMessageBatch" if batch_entry else "DeleteMessage", queue)
        a.update({"qv2.receipt_handle.hmac": rh_hash(handle, self.key),
                  "qv2.receipt_handle.key_id": key_id(self.key), "qv2.result": "ok" if ok else "failed"})
        if not ok:
            a["qv2.error_code"] = error
        if mid:
            a["messaging.message.id"] = mid
        if batch_entry:
            a["qv2.batch_entry_id"] = batch_entry
        return S("qv2 delete", t, a, **({"trace_id": trace_id} if trace_id else {}))

    def change_visibility(self, t, handle, timeout, mid=None, ok=True, queue=None):
        a = self.base("sqs.settle", "settle", "ChangeMessageVisibility", queue)
        a.update({"qv2.receipt_handle.hmac": rh_hash(handle, self.key),
                  "qv2.receipt_handle.key_id": key_id(self.key), "qv2.result": "ok" if ok else "failed",
                  "qv2.visibility_timeout": timeout})
        if not ok:
            a["qv2.error_code"] = "MessageNotInflight"
        if mid:
            a["messaging.message.id"] = mid
        return S("qv2 change_visibility", t, a)

    def outcome(self, t, mid, outcome, invocation, queue=None, error_type=None):
        a = self.base("sqs.outcome", "process", "LambdaEvent", queue)
        a.update({"messaging.message.id": mid, "qv2.outcome": outcome,
                  "faas.name": "orders-fn", "faas.invocation_id": invocation})
        if error_type:
            a["qv2.error_type"] = error_type
        return S("qv2 outcome", t, a)


def _value(v) -> AnyValue:
    if isinstance(v, bool):
        return AnyValue(bool_value=v)
    if isinstance(v, int):
        return AnyValue(int_value=v)
    return AnyValue(string_value=str(v))


def to_proto(spans: list[S], service: str = "orders-worker", scope: str = "qv2") -> ExportTraceServiceRequest:
    pb = [Span(name=s.name, trace_id=s.trace_id, span_id=s.span_id, kind=Span.SPAN_KIND_INTERNAL,
               start_time_unix_nano=s.t, end_time_unix_nano=s.t,
               attributes=[KeyValue(key=k, value=_value(v)) for k, v in s.attrs.items()]) for s in spans]
    return ExportTraceServiceRequest(resource_spans=[ResourceSpans(
        resource=Resource(attributes=[KeyValue(key="service.name", value=AnyValue(string_value=service))]),
        scope_spans=[ScopeSpans(scope=InstrumentationScope(name=scope, version="0.1.0"), spans=pb)])])


def _json_value(v) -> dict:
    if isinstance(v, bool):
        return {"boolValue": v}
    if isinstance(v, int):
        return {"intValue": str(v)}
    return {"stringValue": str(v)}


def to_json(spans: list[S], service: str = "orders-worker", scope: str = "qv2") -> dict:
    """OTLP/JSON, as the OTLP spec encodes it: hex IDs, 64-bit integers as strings."""
    return {"resourceSpans": [{
        "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": service}}]},
        "scopeSpans": [{"scope": {"name": scope, "version": "0.1.0"}, "spans": [
            {"traceId": s.trace_id.hex(), "spanId": s.span_id.hex(), "name": s.name, "kind": 1,
             "startTimeUnixNano": str(s.t), "endTimeUnixNano": str(s.t),
             "attributes": [{"key": k, "value": _json_value(v)} for k, v in s.attrs.items()]}
            for s in spans]}]}]}
