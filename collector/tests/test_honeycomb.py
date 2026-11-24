"""Runs the real Collector with trailbeam-collector.yaml and honeycomb.yaml between a fake Trailbeam server and a fake
Honeycomb OTLP endpoint. Nothing here calls Honeycomb or needs an account.

Needs an otelcol-contrib binary, like test_collector.py; skipped otherwise.
"""
from __future__ import annotations

import time

import pytest
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, InstrumentationScope
from opentelemetry.proto.logs.v1.logs_pb2 import LogRecord, ResourceLogs, ScopeLogs
from opentelemetry.proto.metrics.v1.metrics_pb2 import Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics
from opentelemetry.proto.trace.v1.trace_pb2 import ResourceSpans, ScopeSpans

from test_collector import (OTELCOL, OTHER, STOCK, TRAILBEAM, Sink, free_port, request, run_collector, span,
                            stop, wait_for)
# The New Relic stand-in records any OTLP/HTTP request's path, headers and names; it fits Honeycomb as it is.
from test_newrelic import NewRelic as OtlpEndpoint, send

pytestmark = pytest.mark.skipif(not OTELCOL, reason="no otelcol-contrib binary (set TRAILBEAM_OTELCOL_BIN)")

CONFIGS = ["trailbeam-collector.yaml", "honeycomb.yaml"]
EVERYTHING = sorted(TRAILBEAM + STOCK + OTHER)
KEY = "test-ingest-key-not-real"


@pytest.fixture
def sinks():
    s = {"trailbeam": Sink(), "honeycomb": OtlpEndpoint()}
    yield s
    for x in s.values():
        x.close()


def start(sinks, **env):
    return run_collector(CONFIGS, {"TRAILBEAM_ENDPOINT": sinks["trailbeam"].url,
                                   "HONEYCOMB_OTLP_ENDPOINT": sinks["honeycomb"].url,
                                   "HONEYCOMB_API_KEY": KEY, **env})


def headers(sinks, path: str) -> list[dict[str, str]]:
    """The requests' headers for one path, with lower-case names."""
    return [{k.lower(): v for k, v in h.items()} for p, h, _ in sinks["honeycomb"].requests if p == path]


def no_errors(log: str):
    assert "\terror\t" not in log, log


def test_server_gets_trailbeam_spans_and_honeycomb_gets_every_span(sinks):
    proc, url = start(sinks)
    try:
        send(url, request())
        wait_for(sinks["trailbeam"].snapshot, TRAILBEAM)
        wait_for(lambda: sinks["honeycomb"].names("/v1/traces"), EVERYTHING)
        time.sleep(1)  # nothing else arrives later
        assert sinks["trailbeam"].snapshot() == TRAILBEAM
        assert sinks["honeycomb"].names("/v1/traces") == EVERYTHING
    finally:
        log = stop(proc)
    no_errors(log)
    hs = headers(sinks, "/v1/traces")
    assert {h.get("x-honeycomb-team") for h in hs} == {KEY}
    assert {h.get("content-encoding") for h in hs} == {"gzip"}
    # traces go to the dataset named after service.name: no dataset header
    assert all("x-honeycomb-dataset" not in h for h in hs)
    # the API key goes to Honeycomb only
    assert all(KEY not in str(h) for h in sinks["trailbeam"].headers)


def metrics_and_logs():
    metrics = ExportMetricsServiceRequest(resource_metrics=[ResourceMetrics(scope_metrics=[ScopeMetrics(
        scope=InstrumentationScope(name="app"),
        metrics=[Metric(name="orders.queue.depth", gauge=Gauge(data_points=[
            NumberDataPoint(time_unix_nano=time.time_ns(), as_int=7)]))])])])
    logs = ExportLogsServiceRequest(resource_logs=[ResourceLogs(scope_logs=[ScopeLogs(
        scope=InstrumentationScope(name="app"),
        log_records=[LogRecord(time_unix_nano=time.time_ns(), body=AnyValue(string_value="order 42 processed"))])])])
    return metrics, logs


@pytest.mark.parametrize("dataset", [None, "sqs-metrics"])
def test_metrics_go_to_a_named_dataset_and_logs_to_honeycomb(sinks, dataset):
    metrics, logs = metrics_and_logs()
    proc, url = start(sinks, **({"HONEYCOMB_METRICS_DATASET": dataset} if dataset else {}))
    base = url.removesuffix("/v1/traces")
    try:
        send(base + "/v1/metrics", metrics)
        send(base + "/v1/logs", logs)
        wait_for(lambda: sinks["honeycomb"].names("/v1/metrics"), ["orders.queue.depth"])
        wait_for(lambda: sinks["honeycomb"].names("/v1/logs"), ["order 42 processed"])
    finally:
        log = stop(proc)
    no_errors(log)
    assert sinks["trailbeam"].snapshot() == []
    m = headers(sinks, "/v1/metrics")
    assert {h.get("x-honeycomb-dataset") for h in m} == {dataset or "metrics"}
    assert {h.get("x-honeycomb-team") for h in m} == {KEY}
    lg = headers(sinks, "/v1/logs")
    assert {h.get("x-honeycomb-team") for h in lg} == {KEY}
    assert all("x-honeycomb-dataset" not in h for h in lg)


def test_requests_to_honeycomb_hold_at_most_2000_spans(sinks):
    """Honeycomb rejects requests over 15 MB, so honeycomb.yaml caps a request at 2,000 spans."""
    tid = bytes(range(16))
    spans = [span(f"trailbeam receive {i}", tid, i + 1, trailbeam__schema_version=1) for i in range(5000)]
    big = ExportTraceServiceRequest(resource_spans=[ResourceSpans(scope_spans=[
        ScopeSpans(scope=InstrumentationScope(name="trailbeam"), spans=spans)])])
    proc, url = start(sinks)
    try:
        send(url, big)
        wait_for(lambda: len(sinks["honeycomb"].names("/v1/traces")), 5000)
        wait_for(lambda: len(sinks["trailbeam"].snapshot()), 5000)
    finally:
        log = stop(proc)
    no_errors(log)
    sizes = [len(ns) for p, _, ns in sinks["honeycomb"].requests if p == "/v1/traces"]
    assert max(sizes) <= 2000, sizes


def test_honeycomb_down_does_not_hold_up_the_server(sinks):
    """Nothing listens at the Honeycomb endpoint. The Trailbeam server still gets its spans."""
    proc, url = start(sinks, HONEYCOMB_OTLP_ENDPOINT=f"http://127.0.0.1:{free_port()}")
    try:
        for _ in range(3):
            send(url, request())
        wait_for(lambda: sinks["trailbeam"].snapshot(), sorted(TRAILBEAM * 3))
    finally:
        stop(proc)
