#!/usr/bin/env bash
# Fetch the Java agents the jar must work beside into DIR (CI passes $RUNNER_TEMP/agents):
# DIR/otel.jar (OpenTelemetry Java agent) and DIR/dd.jar (dd-java-agent). Used by ci.yml's Java jobs.
#
# CI caches DIR keyed on this file's hash, so a run downloads only when a version below changes or the cache
# was evicted. A jar already in DIR with the pinned SHA-256 is kept; anything else is downloaded again.
#
# Maven Central answers bursts from shared runner IPs with 429 Too Many Requests, sometimes for minutes:
# QV-544's 6 tries 10 s apart (one minute) all got 429 on the nightly at 3b218ed (QV-560). curl's own backoff
# (no --retry-delay) doubles from 1 s and honours Retry-After; 10 retries span about 17 minutes at most.
set -euo pipefail

dir="${1:?usage: fetch-agents.sh DIR}"
mkdir -p "$dir"

# name  sha256  url
agents=(
  "otel.jar da44154cea128257779c8a782b9651fd7c0c6aa177cd3810f9a867ac8509555d https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v2.34.0/opentelemetry-javaagent.jar"
  "dd.jar 07416b4005ce560893561b6a03d97f0674a8c7e721ecb517e2baeae60e85f673 https://repo1.maven.org/maven2/com/datadoghq/dd-java-agent/1.69.1/dd-java-agent-1.69.1.jar"
)

ok() { [ -f "$1" ] && echo "$2  $1" | sha256sum --check --status; }

for entry in "${agents[@]}"; do
  read -r name sum url <<<"$entry"
  out="$dir/$name"
  if ok "$out" "$sum"; then echo "$name: cached ($out)"; continue; fi
  echo "$name: fetching $url"
  rm -f "$out" "$out.part"
  curl -fL --no-progress-meter --retry 10 --retry-max-time 1200 -o "$out.part" "$url" \
    || { echo "::error::could not fetch $name from $url"; exit 1; }
  if ! ok "$out.part" "$sum"; then
    echo "::error::$name from $url has SHA-256 $(sha256sum "$out.part" | cut -d' ' -f1), expected $sum"
    rm -f "$out.part"; exit 1
  fi
  mv "$out.part" "$out"
done
