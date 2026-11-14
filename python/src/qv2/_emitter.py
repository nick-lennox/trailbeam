"""The qv2 tracer provider (section 2) and the code that builds and emits qv2 spans."""
from __future__ import annotations

import os
import threading
import time
import uuid
from typing import Optional

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter
from opentelemetry.sdk.trace.sampling import ALWAYS_ON

from . import __version__
from ._contract import (
    DEFAULT_ENDPOINT,
    ENDPOINT_ENV,
    KINDS,
    SCHEMA_VERSION,
    SCOPE_NAME,
    ReceiptHasher,
)

SERVICE_INSTANCE_ID = "service.instance.id"


def make_provider(
    span_exporter: Optional[SpanExporter] = None,
    span_processor: Optional[SpanProcessor] = None,
) -> TracerProvider:
    """An always-on provider of our own, so the application's sampling never drops qv2 spans.

    Exports OTLP/HTTP to $QV2_OTLP_TRACES_ENDPOINT unless an exporter or processor is given.
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
        span_processor = BatchSpanProcessor(span_exporter)
    provider.add_span_processor(span_processor)
    return provider


class Emitter:
    """Emits zero-length qv2 spans with the attributes every qv2 span carries (section 3)."""

    def __init__(self, provider: TracerProvider, hasher: ReceiptHasher, source: str, client_id: Optional[str] = None):
        self.provider = provider
        self.hasher = hasher
        self.source = source
        self.client_id = client_id or str(provider.resource.attributes.get(SERVICE_INSTANCE_ID))
        self.tracer = provider.get_tracer(SCOPE_NAME, __version__)

    def hash_attrs(self, receipt_handle: str) -> dict:
        return {
            "qv2.receipt_handle.hmac": self.hasher(receipt_handle),
            "qv2.receipt_handle.key_id": self.hasher.key_id,
        }

    def emit(self, name: str, operation: str, queue_name: str, queue_url: str, attrs: dict,
             parent=None, start_ns: Optional[int] = None) -> None:
        kind, op_type = KINDS[name]
        base = {
            "qv2.schema_version": SCHEMA_VERSION,
            "qv2.kind": kind,
            "qv2.source": self.source,
            "messaging.system.name": "aws.sqs",
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
        base.update(attrs)
        ctx = trace.set_span_in_context(parent) if parent is not None else None
        t = start_ns if start_ns is not None else time.time_ns()
        span = self.tracer.start_span(name, context=ctx, kind=trace.SpanKind.INTERNAL, attributes=base, start_time=t)
        span.end(end_time=t)
