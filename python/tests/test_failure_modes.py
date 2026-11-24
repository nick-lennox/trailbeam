"""What happens to spans when the script ends or the Trailbeam server misbehaves (QV-169).

The same cases as node/test/failure-modes.test.js. In every one the application must not crash, its SQS
calls must not be slowed, and its exit must not wait longer than $TRAILBEAM_SHUTDOWN_TIMEOUT_MS (plus
interpreter start-up); every span is either exported or counted and logged as a drop.

The exit cases run the README snippet as a separate Python process, against the moto SQS emulator and a
local OTLP endpoint; nothing reaches AWS.
"""
import http.server
import os
import socket
import subprocess
import sys
import textwrap
import threading
import time

import pytest
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult

import trailbeam
import trailbeam.boto
from trailbeam import _export
from trailbeam._export import CountingSpanProcessor

SHUTDOWN_MS = 500
# Interpreter start, imports and the boto3 client take well under this on CI; without the shutdown bound
# the unreachable and slow cases take 6 to 10 s (the exporter's retries and request timeout).
EXIT_BUDGET_S = 5.0


class Otlp:
    """A local OTLP/HTTP endpoint. ``mode``: ``ok`` (200), ``401``, or ``slow`` (answers after 30 s)."""

    def __init__(self, mode="ok"):
        self.mode = mode
        self.requests = 0
        outer = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_POST(self):
                self.rfile.read(int(self.headers.get("content-length", 0)))
                if outer.mode == "slow":
                    time.sleep(30)
                if outer.mode == "401":
                    self.send_response(401)
                    self.end_headers()
                    return
                outer.requests += 1
                self.send_response(200)
                self.send_header("content-type", "application/x-protobuf")
                self.end_headers()

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}/v1/traces"

    def stop(self):
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def otlp():
    servers = []

    def make(mode="ok"):
        s = Otlp(mode)
        servers.append(s)
        return s

    yield make
    for s in servers:
        s.stop()


def refused_url():
    """An endpoint with nothing listening: a port that was free a moment ago."""
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return f"http://127.0.0.1:{port}/v1/traces"


SCRIPT = """
import os, sys, time, boto3
import trailbeam, trailbeam.boto
trailbeam.boto.install()
sqs = boto3.client("sqs", endpoint_url={endpoint!r}, region_name="us-east-1")
q = sqs.create_queue(QueueName="trailbeam-failure-modes")["QueueUrl"]
slowest = 0.0
for _ in range(3):
    t = time.monotonic()
    sqs.send_message(QueueUrl=q, MessageBody="x")
    slowest = max(slowest, time.monotonic() - t)
print(f"slowest_send_s={{slowest:.3f}}")
{ending}
"""


def run_script(sqs_endpoint, otlp_url, ending=""):
    env = {k: v for k, v in os.environ.items() if not k.startswith(("AWS_", "TRAILBEAM_", "OTEL_"))}
    env.update(
        AWS_ACCESS_KEY_ID="test", AWS_SECRET_ACCESS_KEY="test", AWS_CONFIG_FILE=os.devnull,
        AWS_SHARED_CREDENTIALS_FILE=os.devnull, AWS_EC2_METADATA_DISABLED="true",
        TRAILBEAM_OTLP_TRACES_ENDPOINT=otlp_url, TRAILBEAM_RECEIPT_HASH_KEY="failure-modes-key-0123456789",
        TRAILBEAM_SHUTDOWN_TIMEOUT_MS=str(SHUTDOWN_MS),
    )
    script = SCRIPT.format(endpoint=sqs_endpoint, ending=textwrap.dedent(ending))
    t = time.monotonic()
    r = subprocess.run([sys.executable, "-c", script], env=env, capture_output=True, text=True, timeout=60)
    r.wall = time.monotonic() - t
    r.slowest_send = float(r.stdout.split("slowest_send_s=")[1].split()[0]) if "slowest_send_s=" in r.stdout else None
    return r


def test_short_script_exports_its_spans_at_exit(sqs_endpoint, otlp):
    server = otlp()
    r = run_script(sqs_endpoint, server.url)
    assert r.returncode == 0, r.stderr
    assert server.requests >= 1, r.stderr
    assert "dropped" not in r.stderr
    assert r.wall < EXIT_BUDGET_S


def test_sys_exit_with_spans_queued_still_exports_them(sqs_endpoint, otlp):
    server = otlp()
    r = run_script(sqs_endpoint, server.url, "sys.exit(3)")
    assert r.returncode == 3, r.stderr
    assert server.requests >= 1, r.stderr
    assert "dropped" not in r.stderr


def test_server_unreachable_from_the_start(sqs_endpoint):
    r = run_script(sqs_endpoint, refused_url())
    assert r.returncode == 0, r.stderr
    assert r.slowest_send < 1.0
    assert r.wall < EXIT_BUDGET_S, r.stderr
    assert "trailbeam: dropped 7 spans" in r.stderr  # 3 sends, 3 stock spans, 1 CreateQueue span
    # The OTLP exporter retries a refused connection until shutdown gives up (a shutdown drop), except
    # 1.28 (the oldest supported), which fails the batch at once (an export_failed drop).
    if "(7 shutdown)" in r.stderr:
        assert "TRAILBEAM_SHUTDOWN_TIMEOUT_MS" in r.stderr
    else:
        assert "(7 export failed)" in r.stderr and "unreachable" in r.stderr


def test_wrong_token(sqs_endpoint, otlp):
    server = otlp("401")
    r = run_script(sqs_endpoint, server.url)
    assert r.returncode == 0, r.stderr
    assert r.wall < EXIT_BUDGET_S, r.stderr
    assert "401" in r.stderr
    assert "export failed" in r.stderr and "OTEL_EXPORTER_OTLP_HEADERS" in r.stderr


def test_slow_server(sqs_endpoint, otlp):
    server = otlp("slow")
    r = run_script(sqs_endpoint, server.url)
    assert r.returncode == 0, r.stderr
    assert r.slowest_send < 1.0
    assert r.wall < EXIT_BUDGET_S, r.stderr
    assert "trailbeam: dropped 7 spans" in r.stderr


def test_server_going_away_mid_run(monkeypatch, otlp, make_client, queue_url):
    monkeypatch.setenv("OTEL_BSP_SCHEDULE_DELAY", "100")
    monkeypatch.setenv(_export.SHUTDOWN_TIMEOUT_ENV, str(SHUTDOWN_MS))
    server = otlp()
    monkeypatch.setenv("TRAILBEAM_OTLP_TRACES_ENDPOINT", server.url)
    _export._stats._reset()
    trailbeam.boto.install("failure-modes-key-0123456789")
    try:
        sqs = make_client()
        sqs.send_message(QueueUrl=queue_url, MessageBody="before")
        deadline = time.monotonic() + 5
        while trailbeam.export_stats()["exported"] < 2 and time.monotonic() < deadline:
            time.sleep(0.05)
        assert trailbeam.export_stats()["exported"] >= 2
        server.stop()
        slowest = 0.0
        for _ in range(3):
            t = time.monotonic()
            sqs.send_message(QueueUrl=queue_url, MessageBody="after")
            slowest = max(slowest, time.monotonic() - t)
        assert slowest < 1.0
    finally:
        t = time.monotonic()
        trailbeam.boto.uninstall()
        took = time.monotonic() - t
    stats = trailbeam.export_stats()
    assert took < SHUTDOWN_MS / 1000 + 1.0
    assert stats["queued"] == 0
    assert stats["dropped"] == 6  # 3 sends after the stop: 3 Trailbeam spans and 3 stock spans
    _export._stats._reset()


class Stuck(SpanExporter):
    """Never finishes an export until shut down, like a request to a server that never answers."""

    def __init__(self):
        self.stop = threading.Event()
        self.shutdowns = 0

    def export(self, spans):
        self.stop.wait(30)
        return SpanExportResult.SUCCESS  # late: must not count as exported after shutdown gave up

    def shutdown(self):
        self.shutdowns += 1
        self.stop.set()


def _ended_span(provider_processor):
    from opentelemetry.sdk.trace import TracerProvider

    p = TracerProvider()
    p.add_span_processor(provider_processor)
    p.get_tracer("t").start_span("s").end()


def test_shutdown_keeps_to_its_timeout_and_counts_what_is_left():
    _export._stats._reset()
    exporter = Stuck()
    proc = CountingSpanProcessor(exporter, shutdown_timeout_millis=200, schedule_delay_millis=10)
    _ended_span(proc)
    time.sleep(0.1)  # the worker has taken the span and is stuck in export
    t = time.monotonic()
    proc.shutdown()
    assert time.monotonic() - t < 1.5
    assert exporter.shutdowns == 1  # stopped once, early, which ends the exporter's retries
    time.sleep(0.1)  # the late export result arrives
    s = trailbeam.export_stats()
    assert (s["exported"], s["queued"], s["dropped_by_reason"]["shutdown"]) == (0, 0, 1)
    _export._stats._reset()


def test_force_flush_keeps_to_its_timeout():
    _export._stats._reset()
    exporter = Stuck()
    proc = CountingSpanProcessor(exporter, shutdown_timeout_millis=200)
    _ended_span(proc)
    t = time.monotonic()
    assert proc.force_flush(200) is False
    assert time.monotonic() - t < 1.5
    proc.shutdown()
    _export._stats._reset()


def test_shutdown_timeout_from_the_environment(monkeypatch, caplog):
    monkeypatch.setenv(_export.SHUTDOWN_TIMEOUT_ENV, "750")
    assert _export.configured_shutdown_timeout_ms() == 750
    monkeypatch.setenv(_export.SHUTDOWN_TIMEOUT_ENV, "soon")
    assert _export.configured_shutdown_timeout_ms() == _export.DEFAULT_SHUTDOWN_TIMEOUT_MS
    assert "not a non-negative integer" in caplog.text
