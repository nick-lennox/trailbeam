"""The Trailbeam tracer provider (section 2) and the code that builds and emits Trailbeam spans."""
from __future__ import annotations

import os
import threading
import time
import uuid
from typing import Optional

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import SpanExporter
from opentelemetry.sdk.trace.sampling import ALWAYS_ON

from . import __version__, _order
from ._export import CountingSpanProcessor
from ._contract import (
    DEFAULT_ENDPOINT,
    ENDPOINT_ENV,
    KINDS,
    SCHEMA_VERSION,
    SCOPE_NAME,
    SYSTEMS,
    ReceiptHasher,
)

SERVICE_INSTANCE_ID = "service.instance.id"


def make_provider(
    span_exporter: Optional[SpanExporter] = None,
    span_processor: Optional[SpanProcessor] = None,
) -> TracerProvider:
    """An always-on provider of our own, so the application's sampling never drops Trailbeam spans.

    Exports OTLP/HTTP to $TRAILBEAM_OTLP_TRACES_ENDPOINT unless an exporter or processor is given. Without
    a processor, spans go through a ``CountingSpanProcessor``: a batch queue of $TRAILBEAM_MAX_QUEUE_SIZE
    spans (default 16,384) that counts and logs every span it drops (``trailbeam.export_stats()``).
    ``service.instance.id`` comes from OTEL_RESOURCE_ATTRIBUTES if set, else a UUID v4 per provider.
    """
    resource = Resource.create({})  # reads OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES
    if str(resource.attributes.get("service.name", "")).startswith("unknown_service:"):
        # The SDK default is "unknown_service:<executable>"; the contract (section 5) says "unknown_service".
        resource = resource.merge(Resource({"service.name": "unknown_service"}))
    if not resource.attributes.get(SERVICE_INSTANCE_ID):
        resource = resource.merge(Resource({SERVICE_INSTANCE_ID: str(uuid.uuid4())}))
    provider = TracerProvider(resource=resource, sampler=ALWAYS_ON)
    if span_processor is None:
        if span_exporter is None:
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

            span_exporter = OTLPSpanExporter(endpoint=os.environ.get(ENDPOINT_ENV) or DEFAULT_ENDPOINT)
        span_processor = CountingSpanProcessor(span_exporter)
    provider.add_span_processor(span_processor)
    return provider


class Emitter:
    """Emits Trailbeam spans with the attributes every Trailbeam span carries (section 3).

    Times (section 2): ``start_ns`` and ``end_ns`` bound the SQS call (just before the request was sent,
    and when the response arrived). ``start_observed=False`` marks a span whose start is not known (a
    Lambda receive: only its end bound is); the span is then zero-length at ``end_ns``.

    Call order (schema version 3): ``order=(issued, returned)`` are ticks of this process's call-order
    counter (``_order.tick()``), taken at the span's start and end. ``issued`` is None when the start
    was not observed. With an order, the span also carries ``trailbeam.order.scope``.
    """

    def __init__(self, provider: TracerProvider, hasher: ReceiptHasher, source: str, client_id: Optional[str] = None):
        self.provider = provider
        self.hasher = hasher
        self.source = source
        self.client_id = client_id or str(provider.resource.attributes.get(SERVICE_INSTANCE_ID))
        self.tracer = provider.get_tracer(SCOPE_NAME, __version__)

    def hash_attrs(self, receipt_handle: str) -> dict:
        return {
            "trailbeam.receipt_handle.hmac": self.hasher(receipt_handle),
            "trailbeam.receipt_handle.key_id": self.hasher.key_id,
        }

    def emit(self, name: str, operation: str, queue_name: str, queue_url: str, attrs: dict,
             parent=None, start_ns: Optional[int] = None, end_ns: Optional[int] = None,
             start_observed: bool = True, order: Optional[tuple] = None) -> None:
        kind, op_type = KINDS[name]
        base = {
            "trailbeam.schema_version": SCHEMA_VERSION,
            "trailbeam.kind": kind,
            "trailbeam.source": self.source,
            "messaging.system.name": SYSTEMS.get(name, "aws.sqs"),
            "messaging.operation.type": op_type,
            "messaging.operation.name": operation,
            "messaging.client.id": self.client_id,
            "process.pid": os.getpid(),
            "thread.name": threading.current_thread().name,
        }
        if queue_name:
            base["messaging.destination.name"] = queue_name
        if queue_url:
            base["aws.sqs.queue.url"] = queue_url
        if not start_observed:
            base["trailbeam.start_observed"] = False
        if order is not None:
            issued, returned = order
            if issued is not None or returned is not None:
                base["trailbeam.order.scope"] = _order.scope
            if issued is not None:
                base["trailbeam.order.issued"] = issued
            if returned is not None:
                base["trailbeam.order.returned"] = returned
        base.update(attrs)
        ctx = trace.set_span_in_context(parent) if parent is not None else None
        end = end_ns if end_ns is not None else time.time_ns()
        start = start_ns if start_ns is not None and start_observed else end
        start = min(start, end)
        span = self.tracer.start_span(name, context=ctx, kind=trace.SpanKind.INTERNAL, attributes=base,
                                      start_time=start)
        span.end(end_time=end)
