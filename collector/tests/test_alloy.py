"""Runs Grafana Alloy with trailbeam.alloy between two fake OTLP/HTTP sinks: one for the Trailbeam server, one for
Tempo or Grafana Cloud.

Needs Docker; skipped without it. The image is $TRAILBEAM_ALLOY_IMAGE, by default the version README.md names. The
container uses host networking, so Alloy reaches the sinks on 127.0.0.1 and listens on free ports of its own.
"""
from __future__ import annotations

import base64
import os
import shutil
import socket
import subprocess
import time
import uuid
from pathlib import Path

import pytest

from test_collector import HERE, OTHER, STOCK, TRAILBEAM, Sink, free_port, post, request, wait_for

DOCKER = shutil.which("docker")
IMAGE = os.environ.get("TRAILBEAM_ALLOY_IMAGE", "grafana/alloy:v1.22.0")
pytestmark = pytest.mark.skipif(not DOCKER, reason="no docker")

EVERYTHING = sorted(TRAILBEAM + STOCK + OTHER)


# The Grafana Cloud change README.md describes: basic auth on the backend exporter.
TEMPO_EXPORTER = """otelcol.exporter.otlphttp "backend" {
  client {"""
GRAFANA_CLOUD_EXPORTER = """otelcol.auth.basic "grafana_cloud" {
  username = sys.env("GRAFANA_CLOUD_INSTANCE_ID")
  password = sys.env("GRAFANA_CLOUD_API_KEY")
}

otelcol.exporter.otlphttp "backend" {
  client {
    auth     = otelcol.auth.basic.grafana_cloud.handler"""


def run_alloy(env: dict, config: Path = HERE / "trailbeam.alloy") -> tuple[str, str]:
    """Starts Alloy on a configuration file. Returns the container's name and its OTLP/HTTP traces URL."""
    name = f"tb-alloy-test-{uuid.uuid4().hex[:8]}"
    http_port, grpc_port, ui_port = free_port(), free_port(), free_port()
    env = {**env, "TRAILBEAM_COLLECTOR_HTTP_ENDPOINT": f"127.0.0.1:{http_port}",
           "TRAILBEAM_COLLECTOR_GRPC_ENDPOINT": f"127.0.0.1:{grpc_port}"}
    args = [DOCKER, "run", "-d", "--name", name, "--network", "host",
            "-v", f"{config}:/etc/alloy/trailbeam.alloy:ro"]
    for k, v in env.items():
        args += ["-e", f"{k}={v}"]
    args += [IMAGE, "run", f"--server.http.listen-addr=127.0.0.1:{ui_port}", "--storage.path=/tmp/alloy",
             "/etc/alloy/trailbeam.alloy"]
    subprocess.run(args, check=True, capture_output=True)
    deadline = time.time() + 60  # the first run may pull the image
    while time.time() < deadline:
        try:
            socket.create_connection(("127.0.0.1", http_port), timeout=0.2).close()
            return name, f"http://127.0.0.1:{http_port}/v1/traces"
        except OSError:
            running = subprocess.run([DOCKER, "inspect", "-f", "{{.State.Running}}", name],
                                     capture_output=True, text=True).stdout.strip()
            if running == "false":
                break
            time.sleep(0.2)
    pytest.fail("Alloy did not start:\n" + stop(name))


def stop(name: str) -> str:
    """Stops and removes the container. Returns its log."""
    subprocess.run([DOCKER, "stop", "-t", "10", name], capture_output=True)
    log = subprocess.run([DOCKER, "logs", name], capture_output=True, text=True)
    subprocess.run([DOCKER, "rm", "-f", "-v", name], capture_output=True)
    return log.stdout + log.stderr


def assert_no_errors(log: str):
    assert "level=error" not in log, log


@pytest.fixture
def sinks():
    s = {"trailbeam": Sink(), "backend": Sink()}
    yield s
    for x in s.values():
        x.close()


def test_server_gets_trailbeam_spans_and_backend_gets_every_span(sinks):
    name, url = run_alloy({"TRAILBEAM_ENDPOINT": sinks["trailbeam"].url,
                           "TRAILBEAM_BACKEND_ENDPOINT": sinks["backend"].url})
    try:
        post(url, request())
        wait_for(sinks["trailbeam"].snapshot, TRAILBEAM)
        wait_for(sinks["backend"].snapshot, EVERYTHING)
        time.sleep(1)  # nothing else arrives later
        assert sinks["trailbeam"].snapshot() == TRAILBEAM
        assert sinks["backend"].snapshot() == EVERYTHING
    finally:
        log = stop(name)
    assert_no_errors(log)
    # gzip to the server, as trailbeam-collector.yaml does
    assert all(h.get("Content-Encoding") == "gzip" for h in sinks["trailbeam"].headers)


def test_access_token_and_grafana_cloud_credentials(sinks, tmp_path):
    source = (HERE / "trailbeam.alloy").read_text()
    assert source.count(TEMPO_EXPORTER) == 1
    config = tmp_path / "grafana-cloud.alloy"
    config.write_text(source.replace(TEMPO_EXPORTER, GRAFANA_CLOUD_EXPORTER))
    name, url = run_alloy({"TRAILBEAM_ENDPOINT": sinks["trailbeam"].url, "TRAILBEAM_TOKEN": "tb-secret",
                           "TRAILBEAM_BACKEND_ENDPOINT": sinks["backend"].url,
                           "GRAFANA_CLOUD_INSTANCE_ID": "123456", "GRAFANA_CLOUD_API_KEY": "glc_secret"}, config)
    try:
        post(url, request())
        wait_for(sinks["trailbeam"].snapshot, TRAILBEAM)
        wait_for(sinks["backend"].snapshot, EVERYTHING)
    finally:
        log = stop(name)
    assert_no_errors(log)
    assert {h.get("Authorization") for h in sinks["trailbeam"].headers} == {"Bearer tb-secret"}
    basic = "Basic " + base64.b64encode(b"123456:glc_secret").decode()
    assert {h.get("Authorization") for h in sinks["backend"].headers} == {basic}


def test_spans_wait_while_the_server_is_down(sinks):
    """The server is down when the spans arrive. Alloy keeps retrying and delivers them, once each, when it is
    back. The backend gets them meanwhile."""
    port = free_port()
    name, url = run_alloy({"TRAILBEAM_ENDPOINT": f"http://127.0.0.1:{port}",
                           "TRAILBEAM_BACKEND_ENDPOINT": sinks["backend"].url})
    server = None
    try:
        for _ in range(5):
            post(url, request())
        wait_for(lambda: len(sinks["backend"].snapshot()), 5 * len(EVERYTHING))
        time.sleep(3)  # a few failed exports
        server = Sink(port)
        wait_for(lambda: len(server.snapshot()), 5 * len(TRAILBEAM), timeout=60)  # retry backoff grows to 30s
        time.sleep(1)  # and nothing twice
        assert server.snapshot() == sorted(TRAILBEAM * 5)
    finally:
        stop(name)
        if server:
            server.close()
