"""Runs the real Collector with trailbeam-collector.yaml and newrelic.yaml between a fake Trailbeam server and a fake
New Relic OTLP endpoint. Nothing here calls New Relic or needs an account.

Needs an otelcol-contrib binary, like test_collector.py; skipped otherwise.
"""
from __future__ import annotations

import gzip
import http.server
import threading
import time
import urllib.request

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

pytestmark = pytest.mark.skipif(not OTELCOL, reason="no otelcol-contrib binary (set TRAILBEAM_OTELCOL_BIN)")

CONFIGS = ["trailbeam-collector.yaml", "newrelic.yaml"]
EVERYTHING = sorted(TRAILBEAM + STOCK + OTHER)
KEY = "test-license-key-not-real"


class NewRelic:
    """A stand-in for otlp.nr-data.net: records each request's path, headers and span, metric or log names."""

    DECODE = {"/v1/traces": ExportTraceServiceRequest, "/v1/metrics": ExportMetricsServiceRequest,
              "/v1/logs": ExportLogsServiceRequest}

    def __init__(self):
        self.requests: list[tuple[str, dict, list[str]]] = []
        self.lock = threading.Lock()
        nr = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                if self.headers.get("Content-Encoding") == "gzip":
                    body = gzip.decompress(body)
                req = nr.DECODE[self.path].FromString(body)
                if self.path == "/v1/traces":
                    names = [s.name for rs in req.resource_spans for ss in rs.scope_spans for s in ss.spans]
                elif self.path == "/v1/metrics":
                    names = [m.name for rm in req.resource_metrics for sm in rm.scope_metrics for m in sm.metrics]
                else:
                    names = [r.body.string_value for rl in req.resource_logs for sl in rl.scope_logs
                             for r in sl.log_records]
                with nr.lock:
                    nr.requests.append((self.path, dict(self.headers.items()), names))
                self.send_response(200)
                self.send_header("Content-Type", "application/x-protobuf")
                self.end_headers()

            def log_message(self, *a):
                pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def names(self, path: str) -> list[str]:
        with self.lock:
            return sorted(n for p, _, ns in self.requests if p == path for n in ns)

    def close(self):
        self.server.shutdown()


@pytest.fixture
def sinks():
    s = {"trailbeam": Sink(), "newrelic": NewRelic()}
    yield s
    for x in s.values():
        x.close()


def send(url: str, req):
    r = urllib.request.Request(url, data=req.SerializeToString(), method="POST",
                               headers={"Content-Type": "application/x-protobuf"})
    with urllib.request.urlopen(r, timeout=5) as resp:
        assert resp.status == 200


def start(sinks, **env):
    return run_collector(CONFIGS, {"TRAILBEAM_ENDPOINT": sinks["trailbeam"].url,
                                   "NEW_RELIC_OTLP_ENDPOINT": sinks["newrelic"].url,
                                   "NEW_RELIC_LICENSE_KEY": KEY, **env})


def no_errors(log: str):
    assert "\terror\t" not in log, log


def test_server_gets_trailbeam_spans_and_new_relic_gets_every_span(sinks):
    proc, url = start(sinks)
    try:
        send(url, request())
        wait_for(sinks["trailbeam"].snapshot, TRAILBEAM)
        wait_for(lambda: sinks["newrelic"].names("/v1/traces"), EVERYTHING)
        time.sleep(1)  # nothing else arrives later
        assert sinks["trailbeam"].snapshot() == TRAILBEAM
        assert sinks["newrelic"].names("/v1/traces") == EVERYTHING
    finally:
        log = stop(proc)
    no_errors(log)
    headers = [h for _, h, _ in sinks["newrelic"].requests]
    assert {h.get("Api-Key") for h in headers} == {KEY}
    assert {h.get("Content-Encoding") for h in headers} == {"gzip"}
    # the license key goes to New Relic only
    assert all(KEY not in str(h) for h in sinks["trailbeam"].headers)


def test_metrics_and_logs_go_to_new_relic(sinks):
    metrics = ExportMetricsServiceRequest(resource_metrics=[ResourceMetrics(scope_metrics=[ScopeMetrics(
        scope=InstrumentationScope(name="app"),
        metrics=[Metric(name="orders.queue.depth", gauge=Gauge(data_points=[
            NumberDataPoint(time_unix_nano=time.time_ns(), as_int=7)]))])])])
    logs = ExportLogsServiceRequest(resource_logs=[ResourceLogs(scope_logs=[ScopeLogs(
        scope=InstrumentationScope(name="app"),
        log_records=[LogRecord(time_unix_nano=time.time_ns(), body=AnyValue(string_value="order 42 processed"))])])])
    proc, url = start(sinks)
    base = url.removesuffix("/v1/traces")
    try:
        send(base + "/v1/metrics", metrics)
        send(base + "/v1/logs", logs)
        wait_for(lambda: sinks["newrelic"].names("/v1/metrics"), ["orders.queue.depth"])
        wait_for(lambda: sinks["newrelic"].names("/v1/logs"), ["order 42 processed"])
    finally:
        log = stop(proc)
    no_errors(log)
    assert sinks["trailbeam"].snapshot() == []


def test_requests_to_new_relic_hold_at_most_1000_spans(sinks):
    """New Relic rejects requests over 1 MB, so newrelic.yaml caps a request at 1,000 spans."""
    tid = bytes(range(16))
    spans = [span(f"trailbeam receive {i}", tid, i + 1, trailbeam__schema_version=1) for i in range(2500)]
    big = ExportTraceServiceRequest(resource_spans=[ResourceSpans(scope_spans=[
        ScopeSpans(scope=InstrumentationScope(name="trailbeam"), spans=spans)])])
    proc, url = start(sinks)
    try:
        send(url, big)
        wait_for(lambda: len(sinks["newrelic"].names("/v1/traces")), 2500)
        wait_for(lambda: len(sinks["trailbeam"].snapshot()), 2500)
    finally:
        log = stop(proc)
    no_errors(log)
    sizes = [len(ns) for p, _, ns in sinks["newrelic"].requests if p == "/v1/traces"]
    assert max(sizes) <= 1000, sizes


def test_new_relic_down_does_not_hold_up_the_server(sinks):
    """Nothing listens at the New Relic endpoint. The Trailbeam server still gets its spans."""
    proc, url = start(sinks, NEW_RELIC_OTLP_ENDPOINT=f"http://127.0.0.1:{free_port()}")
    try:
        for _ in range(3):
            send(url, request())
        wait_for(lambda: sinks["trailbeam"].snapshot(), sorted(TRAILBEAM * 3))
    finally:
        stop(proc)
