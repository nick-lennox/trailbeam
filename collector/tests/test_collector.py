"""Runs the real Collector with trailbeam-collector.yaml (and backend.yaml) between fake OTLP/HTTP sinks.

Needs an otelcol-contrib binary: $TRAILBEAM_OTELCOL_BIN, or `otelcol-contrib` on PATH. Skipped otherwise.
The test sends one OTLP request holding Trailbeam spans, stock SQS spans of several SDKs, and other spans,
and checks which spans reach Trailbeam sink and the backend sink.
"""
from __future__ import annotations

import gzip
import http.server
import os
import shutil
import socket
import subprocess
import threading
import time
import urllib.request
from pathlib import Path

import pytest
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, InstrumentationScope, KeyValue
from opentelemetry.proto.trace.v1.trace_pb2 import ResourceSpans, ScopeSpans, Span

HERE = Path(__file__).resolve().parent.parent
OTELCOL = os.environ.get("TRAILBEAM_OTELCOL_BIN") or shutil.which("otelcol-contrib")
pytestmark = pytest.mark.skipif(not OTELCOL, reason="no otelcol-contrib binary (set TRAILBEAM_OTELCOL_BIN)")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Sink:
    """An OTLP/HTTP traces endpoint that records the names of the spans it receives, and the requests' headers."""

    def __init__(self, port: int = 0):
        self.names: list[str] = []
        self.headers: list[dict[str, str]] = []
        self.lock = threading.Lock()
        sink = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                if self.headers.get("Content-Encoding") == "gzip":
                    body = gzip.decompress(body)
                req = ExportTraceServiceRequest.FromString(body)
                with sink.lock:
                    sink.headers.append(dict(self.headers.items()))
                    for rs in req.resource_spans:
                        for ss in rs.scope_spans:
                            sink.names.extend(s.name for s in ss.spans)
                self.send_response(200)
                self.send_header("Content-Type", "application/x-protobuf")
                self.end_headers()

            def log_message(self, *a):
                pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", port), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def snapshot(self) -> list[str]:
        with self.lock:
            return sorted(self.names)

    def close(self):
        self.server.shutdown()


def attr(k, v) -> KeyValue:
    if isinstance(v, int):
        return KeyValue(key=k, value=AnyValue(int_value=v))
    return KeyValue(key=k, value=AnyValue(string_value=v))


def span(name: str, trace_id: bytes, n: int, **attrs) -> Span:
    t = time.time_ns()
    return Span(name=name, trace_id=trace_id, span_id=n.to_bytes(8, "big"), kind=Span.SPAN_KIND_INTERNAL,
                start_time_unix_nano=t, end_time_unix_nano=t,
                attributes=[attr(k.replace("__", "."), v) for k, v in attrs.items()])


def request() -> ExportTraceServiceRequest:
    """One trace with every kind of span, so tail sampling decides on the same trace the SQS spans are in."""
    tid = os.urandom(16)
    trailbeam = [span(f"trailbeam {k}", tid, i + 1, trailbeam__schema_version=1, trailbeam__kind=k)
           for i, k in enumerate(["send", "receive", "delete"])]
    stock = [
        span("SQS.ReceiveMessage", tid, 11, rpc__system="aws-api", rpc__service="SQS"),  # botocore
        span("SQS.DeleteMessage", tid, 12, rpc__system="aws-api", rpc__service="SQS",
             messaging__system="aws_sqs"),
        span("Sqs.SendMessage", tid, 13, rpc__system="aws-api", rpc__service="Sqs"),  # Java
        span("orders send", tid, 14, messaging__system="aws_sqs"),  # Node.js aws-sdk
        span("orders process", tid, 15, messaging__system__name="aws.sqs"),  # current semconv
    ]
    other = [span("GET /orders", tid, 21, http__request__method="GET"),
             span("SELECT orders", tid, 22, db__system="postgresql")]
    return ExportTraceServiceRequest(resource_spans=[ResourceSpans(scope_spans=[
        ScopeSpans(scope=InstrumentationScope(name="trailbeam", version="0.1.0"), spans=trailbeam),
        ScopeSpans(scope=InstrumentationScope(name="opentelemetry.instrumentation.botocore"), spans=stock),
        ScopeSpans(scope=InstrumentationScope(name="app"), spans=other),
    ])])


TRAILBEAM = sorted(["trailbeam send", "trailbeam receive", "trailbeam delete"])
STOCK = sorted(["SQS.ReceiveMessage", "SQS.DeleteMessage", "Sqs.SendMessage", "orders send", "orders process"])
OTHER = sorted(["GET /orders", "SELECT orders"])


def run_collector(configs: list[str], env: dict):
    http_port, grpc_port = free_port(), free_port()
    env = {**os.environ, **env, "TRAILBEAM_COLLECTOR_HTTP_ENDPOINT": f"127.0.0.1:{http_port}",
           "TRAILBEAM_COLLECTOR_GRPC_ENDPOINT": f"127.0.0.1:{grpc_port}"}
    args = [OTELCOL] + [a for c in configs for a in ("--config", str(HERE / c))]
    proc = subprocess.Popen(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    deadline = time.time() + 20
    while time.time() < deadline:
        try:
            socket.create_connection(("127.0.0.1", http_port), timeout=0.2).close()
            return proc, f"http://127.0.0.1:{http_port}/v1/traces"
        except OSError:
            if proc.poll() is not None:
                break
            time.sleep(0.1)
    proc.kill()
    pytest.fail("Collector did not start:\n" + proc.communicate()[0].decode(errors="replace"))


def post(url: str, req: ExportTraceServiceRequest):
    r = urllib.request.Request(url, data=req.SerializeToString(), method="POST",
                               headers={"Content-Type": "application/x-protobuf"})
    with urllib.request.urlopen(r, timeout=5) as resp:
        assert resp.status == 200


def wait_for(fn, want, timeout=15.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if fn() == want:
            return
        time.sleep(0.2)
    assert fn() == want


@pytest.fixture
def sinks():
    s = {"trailbeam": Sink(), "backend": Sink()}
    yield s
    for x in s.values():
        x.close()


def stop(proc):
    proc.terminate()
    try:
        out = proc.communicate(timeout=10)[0].decode(errors="replace")
    except subprocess.TimeoutExpired:
        proc.kill()
        out = proc.communicate()[0].decode(errors="replace")
    return out


def test_store_gets_every_trailbeam_span_and_nothing_else(sinks):
    proc, url = run_collector(["trailbeam-collector.yaml"], {"TRAILBEAM_ENDPOINT": sinks["trailbeam"].url})
    try:
        post(url, request())
        wait_for(sinks["trailbeam"].snapshot, TRAILBEAM)
        time.sleep(1)  # nothing else arrives later
        assert sinks["trailbeam"].snapshot() == TRAILBEAM
    finally:
        log = stop(proc)
    assert '"error"' not in log.lower() and "\terror\t" not in log, log


@pytest.mark.parametrize("percentage", [0, 100])
def test_backend_keeps_every_sqs_span_while_the_rest_is_sampled(sinks, percentage):
    proc, url = run_collector(["trailbeam-collector.yaml", "backend.yaml"], {
        "TRAILBEAM_ENDPOINT": sinks["trailbeam"].url,
        "TRAILBEAM_BACKEND_ENDPOINT": sinks["backend"].url,
        "TRAILBEAM_BACKEND_SAMPLING_PERCENTAGE": str(percentage),
    })
    try:
        post(url, request())
        want = sorted(TRAILBEAM + STOCK + (OTHER if percentage == 100 else []))
        wait_for(sinks["backend"].snapshot, want, timeout=20)  # tail sampling waits 5s per trace
        if percentage == 0:
            time.sleep(6)  # past decision_wait: the sampled-out spans must not arrive late
        assert sinks["backend"].snapshot() == want
        assert sinks["trailbeam"].snapshot() == TRAILBEAM
    finally:
        log = stop(proc)
    assert "\terror\t" not in log, log


def test_persistent_queue_keeps_spans_through_an_outage_and_a_crash(sinks, tmp_path):
    """Trailbeam is down. The Collector still accepts the spans, writes them to its queue on disk, and is
    killed (no clean shutdown). A new Collector on the same directory delivers all of them once Trailbeam
    is back."""
    env = {"TRAILBEAM_COLLECTOR_QUEUE_DIR": str(tmp_path / "queue")}
    configs = ["trailbeam-collector.yaml", "persistent-queue.yaml"]
    down = f"http://127.0.0.1:{free_port()}"  # nothing listens here
    proc, url = run_collector(configs, {**env, "TRAILBEAM_ENDPOINT": down})
    try:
        for _ in range(20):
            post(url, request())  # answered once on disk, although Trailbeam is down
        time.sleep(1)
    finally:
        proc.kill()  # a crash: nothing is flushed
        proc.communicate()
    assert sinks["trailbeam"].snapshot() == []

    proc, url = run_collector(configs, {**env, "TRAILBEAM_ENDPOINT": sinks["trailbeam"].url})
    try:
        wait_for(lambda: len(sinks["trailbeam"].snapshot()), 20 * len(TRAILBEAM), timeout=30)
        time.sleep(1)  # and nothing twice
        assert sinks["trailbeam"].snapshot() == sorted(TRAILBEAM * 20)
    finally:
        log = stop(proc)
    assert "\terror\t" not in log, log
