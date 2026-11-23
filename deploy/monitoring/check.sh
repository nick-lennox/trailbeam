#!/usr/bin/env bash
# Check the monitoring files (QV-304) against real tools, with no Datadog account: a Trailbeam with one stuck
# message and one dead-letter move, then
#
#   1. the Datadog Agent's OpenMetrics check (datadog/openmetrics.yaml), run once by `agent check` in the Agent's
#      container: a dry run. Nothing is sent to Datadog, and the API key is a placeholder;
#   2. promtool on prometheus/prometheus.yml and the alert rules' unit tests;
#   3. a real Prometheus scraping Trailbeam, and every Grafana dashboard and alert expression run on it.
#
# CI runs it (.github/workflows/ci.yml, job "monitoring"). Locally, from the repository root, with the server
# installed (pip install -e 'python[server]'):
#
#   deploy/monitoring/check.sh
#
# Needs Docker, trailbeam on the PATH (or TRAILBEAM=/path/to/trailbeam) and a Python that has
# opentelemetry-proto (PYTHON, default python3). Images: DD_AGENT_IMAGE, PROMETHEUS_IMAGE.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
agent_image=${DD_AGENT_IMAGE:-gcr.io/datadoghq/agent:7.86.0}
prom_image=${PROMETHEUS_IMAGE:-prom/prometheus:v3.16.0}
trailbeam=${TRAILBEAM:-trailbeam}
python=${PYTHON:-python3}
work=$(mktemp -d)
prom_name=tb-monitoring-prom-$$
tb_pid=

cleanup() {
  [ -n "$tb_pid" ] && kill "$tb_pid" 2>/dev/null && wait "$tb_pid" 2>/dev/null
  docker rm -f "$prom_name" >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT

step() { printf '\n== %s\n' "$*"; }
free_port() { "$python" -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'; }

step "Trailbeam with one stuck message and one dead-letter move"
port=$(free_port)
"$trailbeam" --db "$work/trailbeam.sqlite" --port "$port" --stuck-after 300 >"$work/trailbeam.log" 2>&1 &
tb_pid=$!
for _ in $(seq 100); do curl -sf "http://127.0.0.1:$port/api/ready" >/dev/null && break; sleep 0.1; done
"$python" "$here/seed.py" "http://127.0.0.1:$port"

step "Datadog Agent: the OpenMetrics check, run once (dry run, nothing sent)"
sed "s#http://localhost:4318/metrics#http://127.0.0.1:$port/metrics#" "$here/datadog/openmetrics.yaml" >"$work/openmetrics.yaml"
# `agent check` normally runs next to a running Agent and reads its IPC token and certificate; in a container of
# its own there is none, so make throwaway ones first. --check-rate runs it twice: counters need two readings.
docker run --rm --network host -e DD_API_KEY=placeholder-not-a-key -e DD_HOSTNAME=trailbeam-check \
  -v "$work/openmetrics.yaml:/etc/datadog-agent/conf.d/openmetrics.d/conf.yaml:ro" \
  --entrypoint sh "$agent_image" -c '
    set -e; cd /etc/datadog-agent; cp datadog-docker.yaml datadog.yaml
    export OPENSSL_CONF=/dev/null
    openssl rand -hex 32 > auth_token
    openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -keyout /tmp/k -out /tmp/c \
      -subj /CN=localhost -days 1 2>/dev/null
    openssl ec -in /tmp/k -out /tmp/k1 2>/dev/null; cat /tmp/c /tmp/k1 > ipc_cert.pem
    agent version >&2
    agent check openmetrics --json --check-rate' >"$work/agent.json" 2>"$work/agent.err" \
  || { cat "$work/agent.err"; exit 1; }
head -1 "$work/agent.err"
"$python" "$here/verify.py" agent "$work/agent.json" "$here/datadog"

step "promtool: the Prometheus configuration and the alert rules' unit tests"
docker run --rm -v "$here/prometheus:/p:ro" -w /p --entrypoint promtool "$prom_image" check config prometheus.yml
docker run --rm -v "$here/prometheus:/p:ro" -w /p --entrypoint promtool "$prom_image" test rules trailbeam-alerts.test.yml

step "Prometheus scraping Trailbeam; the Grafana dashboard's and the alerts' expressions on it"
mkdir -p "$work/prom" && cp "$here/prometheus/trailbeam-alerts.yml" "$work/prom/"
sed -e "s#localhost:4318#127.0.0.1:$port#" -e "s#scrape_interval: 15s#scrape_interval: 1s#" \
  "$here/prometheus/prometheus.yml" >"$work/prom/prometheus.yml"
chmod -R a+rX "$work/prom"
prom_port=$(free_port)
docker run -d --name "$prom_name" --network host -v "$work/prom:/etc/prometheus:ro" "$prom_image" \
  --config.file=/etc/prometheus/prometheus.yml --web.listen-address="127.0.0.1:$prom_port" >/dev/null
for _ in $(seq 60); do
  curl -sf "http://127.0.0.1:$prom_port/api/v1/query?query=trailbeam_stuck_messages" | grep -q '"value"' && break
  sleep 1
done
sleep 3  # a few scrapes, for the rate() expressions
"$python" "$here/verify.py" prometheus "http://127.0.0.1:$prom_port" "$here/grafana/trailbeam-dashboard.json" \
  "$here/prometheus/trailbeam-alerts.yml"

step "all monitoring checks passed"
