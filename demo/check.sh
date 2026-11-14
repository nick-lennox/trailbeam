#!/bin/sh
# qv2 0.1 concurrency check: three processes of four consumers each, 200 messages, one language at a time.
#
#   demo/check.sh python [count]
#   demo/check.sh node [count]
#
# Starts the emulator, the Collector and the store (demo/compose.yaml), creates a fresh queue for this run,
# starts one deleter process and three consumer processes (four consumers each) and a producer, waits until
# every message is deleted, stops the processes (which flushes their spans), then compares every message's
# timeline in the store with what the processes really did (demo/check/check.py). Exit status 0 only if
# every message passes. The summary and the full report are saved in demo/results/.
#
# Set DOCKER to another command if `docker` isn't the Docker CLI (for example DOCKER=docker.exe in WSL).
set -eu

usage() { echo "usage: $0 python|node [count]" >&2; exit 2; }
lang=${1:-}
count=${2:-200}
case $lang in
  python) svc=consumer-python; per_process=--threads ;;
  node)   svc=consumer-node;   per_process=--consumers ;;
  *) usage ;;
esac
DOCKER=${DOCKER:-docker}
cd "$(dirname "$0")"
dc() { $DOCKER compose "$@"; }

run=$(date -u +%Y%m%d-%H%M%S)-$lang
queue=check-$run
truth=/truth/$run
# A fresh random receipt-hash key for this run, shared by its processes. Passed with -e, not stored anywhere.
key=$(od -An -tx1 -N32 /dev/urandom | tr -d ' \n')
deleter=qv2-check-$run-deleter
names=""
mkdir -p results

cleanup() {
  # shellcheck disable=SC2086
  $DOCKER rm -f $names >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "== run $run: $count messages on queue $queue"
dc build -q $svc checker
dc up -d --build --wait sqs collector qv2

start() {  # start NAME ARGS...: one driver process in its own container
  name=$1; shift
  names="$names $name"
  dc run -d --no-deps --name "$name" -e QV2_RECEIPT_HASH_KEY="$key" "$svc" "$@" --queue "$queue" --truth "$truth" >/dev/null
}
start "$deleter" deleter --port 8080
for i in 1 2 3; do
  start "qv2-check-$run-consumer-$i" consume "$per_process" 4 --deleter "http://$deleter:8080"
done
sleep 3

echo "== producing"
dc run --rm -T --no-deps -e QV2_RECEIPT_HASH_KEY="$key" "$svc" produce --queue "$queue" --truth "$truth" \
  --count "$count" --mix check

echo "== waiting until every message is deleted"
wait_status=0
dc run --rm -T --no-deps checker wait --truth "$truth" --count "$count" --timeout 180 || wait_status=$?

echo "== stopping the consumers and the deleter (flushes their spans)"
# shellcheck disable=SC2086
$DOCKER stop -t 20 $names >/dev/null
for n in $names; do
  code=$($DOCKER inspect -f '{{.State.ExitCode}}' "$n")
  [ "$code" = 0 ] || echo "warning: $n exited with status $code" >&2
done

echo "== comparing the store's timelines with what the processes did"
status=0
dc run --rm -T --no-deps checker verify --truth "$truth" --count "$count" --store http://qv2:4318 \
  --report "$truth/report.json" > "results/$run.txt" || status=$?
cat "results/$run.txt"
dc run --rm -T --no-deps --entrypoint cat checker "$truth/report.json" > "results/$run-report.json" || true
[ "$wait_status" = 0 ] || status=1
echo "== saved results/$run.txt and results/$run-report.json"
exit "$status"
