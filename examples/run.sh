#!/usr/bin/env bash
# Run one example and check that its timelines reached Trailbeam, then remove everything it created
# (containers, network, images built for it). CI runs this for every example.
#
#   examples/run.sh python-boto3 | python-lambda | node-sdk-v3 | node-lambda
#
# COMPOSE_PROJECT_NAME and TRAILBEAM_EXAMPLE_PORT (default 4318) let it run next to another checkout.
set -euo pipefail
cd "$(dirname "$0")/.."
example=${1:?usage: examples/run.sh <example>}
case $example in
  python-boto3|node-sdk-v3) consumer=orders-worker ;;
  python-lambda|node-lambda) consumer=orders-function ;;
  *) echo "unknown example: $example" >&2; exit 2 ;;
esac
compose=(docker compose -f "examples/$example/compose.yaml")
port=${TRAILBEAM_EXAMPLE_PORT:-4318}
cleanup() { "${compose[@]}" down -v --rmi local --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT
"${compose[@]}" up -d --build
if ! python3 examples/check.py "http://127.0.0.1:$port" --consumer "$consumer" --timeout 120; then
  "${compose[@]}" logs --tail 60
  exit 1
fi
