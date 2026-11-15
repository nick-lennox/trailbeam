#!/bin/sh
# Trailbeam 0.1 concurrency check: three processes of four consumers each, 200 messages, one language at a time.
#
#   demo/check.sh [--aws] [--dlq] python [count]
#   demo/check.sh [--aws] [--dlq] node [count]
#
# --aws runs the same workload against real SQS instead of the emulator (the Collector and the store stay
# local). It needs AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY (and AWS_SESSION_TOKEN if temporary) in the
# environment; they are passed to the containers by name only, never on a command line. AWS_REGION defaults
# to us-east-1. Queues are named qv2-measure-RUN and qv2-measure-RUN-dlq and are deleted when the run ends.
#
# --dlq adds a poison message every 20 (--mix check-dlq): it is released at once on every receive, so SQS
# moves it to the dead-letter queue after maxReceiveCount (3) receives, and the consumers read and delete it
# there. The check then needs the store to show each move as observed (DeadLetterQueueSourceArn, contract
# 3.1) and none inferred. CI uses it on the emulator only: on real SQS it is not approved yet (QV-33).
#
# Starts the emulator, the Collector and the store (demo/compose.yaml), creates a fresh queue for this run,
# starts one deleter process and three consumer processes (four consumers each) and a producer, waits until
# every message is deleted, stops the processes (which flushes their spans), then compares every message's
# timeline in the store with what the processes really did (demo/check/check.py). Exit status 0 only if
# every message passes. The summary and the full report are saved in demo/results/.
#
# Set DOCKER to another command if `docker` isn't the Docker CLI (for example DOCKER=docker.exe in WSL).
# COMPOSE_PROJECT_NAME and TRAILBEAM_DEMO_PORT, if set, choose the Compose project and the store's host port, as for
# `docker compose` (demo/README.md#more-than-one-checkout-on-the-same-machine). They are added to WSLENV, so
# they reach docker.exe too, and the project and port Compose resolved are printed before anything starts.
#
# The resolved-share floor (90% of send->receive and of receive->delete, demo/check/check.py) gates with
# --aws. On the emulator it is reported but does not fail the run, as in CI's ElasticMQ job: ElasticMQ
# stamps SentTimestamp and ApproximateFirstReceiveTimestamp in the same millisecond, so send->receive stays
# near 5% there for a reason that is the emulator's, not the store's (demo/README.md#the-floor-in-ci).
# Everything else gates in both. Set TRAILBEAM_CHECK_FLOOR to a share (e.g. 0.9) to make the floor gate at that
# share on the emulator too.
set -eu

usage() { echo "usage: $0 [--aws] [--dlq] python|node [count]" >&2; exit 2; }
aws=0
mix=check
dlq_arg=""
while :; do
  case ${1:-} in
    --aws) aws=1; shift ;;
    --dlq) mix=check-dlq; dlq_arg=--dlq; shift ;;
    *) break ;;
  esac
done
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
# With DOCKER=docker.exe in WSL, Windows processes only see the variables listed in WSLENV (QV-35 section 4).
# Without this, COMPOSE_PROJECT_NAME and TRAILBEAM_DEMO_PORT are dropped and the run uses the shared trailbeam-demo project.
export WSLENV="COMPOSE_PROJECT_NAME:TRAILBEAM_DEMO_PORT${WSLENV:+:$WSLENV}"
resolved=$(dc config)
project=$(printf '%s\n' "$resolved" | sed -n 's/^name: *//p' | tr -d '"\r')
port=$(printf '%s\n' "$resolved" | sed -n 's/^ *published: *//p' | tr -d '"\r' | head -n 1)
echo "== Compose project $project, store on 127.0.0.1:$port"
# Stop rather than run on another project's stack if the variables didn't reach Compose.
if [ "$project" != "${COMPOSE_PROJECT_NAME:-trailbeam-demo}" ] || [ "$port" != "${TRAILBEAM_DEMO_PORT:-4318}" ]; then
  echo "error: Compose resolved project $project and port $port, not ${COMPOSE_PROJECT_NAME:-trailbeam-demo} and ${TRAILBEAM_DEMO_PORT:-4318}" >&2
  exit 2
fi

run=$(date -u +%Y%m%d-%H%M%S)-$lang
queue=check-$run
aws_env=""
if [ "$aws" = 1 ]; then
  : "${AWS_ACCESS_KEY_ID:?--aws needs AWS_ACCESS_KEY_ID}" "${AWS_SECRET_ACCESS_KEY:?--aws needs AWS_SECRET_ACCESS_KEY}"
  export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_REGION="${AWS_REGION:-us-east-1}"
  queue=qv2-measure-$run
  # Empty endpoint: the SDK's default, real SQS. -e NAME without a value copies the variable from here.
  aws_env="-e TRAILBEAM_DEMO_SQS_ENDPOINT= -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_REGION"
  if [ -n "${AWS_SESSION_TOKEN:-}" ]; then export AWS_SESSION_TOKEN; aws_env="$aws_env -e AWS_SESSION_TOKEN"; fi
  # With DOCKER=docker.exe in WSL, Windows processes only see the variables listed in WSLENV.
  export WSLENV="AWS_ACCESS_KEY_ID:AWS_SECRET_ACCESS_KEY:AWS_SESSION_TOKEN:AWS_REGION${WSLENV:+:$WSLENV}"
fi
truth=/truth/$run
# A fresh random receipt-hash key for this run, shared by its processes. Passed with -e, not stored anywhere.
key=$(od -An -tx1 -N32 /dev/urandom | tr -d ' \n')
deleter=trailbeam-check-$run-deleter
names=""
mkdir -p results

delete_queues() {  # real SQS only: delete this run's two queues
  # shellcheck disable=SC2086
  dc run --rm -T --no-deps $aws_env --entrypoint python consumer-python -c '
import os, sys, boto3
sqs = boto3.client("sqs", region_name=os.environ["AWS_REGION"])
for name in sys.argv[1:]:
    try:
        sqs.delete_queue(QueueUrl=sqs.get_queue_url(QueueName=name)["QueueUrl"])
        print("deleted queue", name)
    except sqs.exceptions.QueueDoesNotExist:
        pass
' "$queue" "$queue-dlq" || echo "warning: could not delete queues $queue and $queue-dlq" >&2
}

cleanup() {
  # shellcheck disable=SC2086
  $DOCKER rm -f $names >/dev/null 2>&1 || true
  if [ "$aws" = 1 ]; then delete_queues; fi
}
trap cleanup EXIT

if [ "$aws" = 1 ]; then where="real SQS in $AWS_REGION"; else where="the emulator"; fi
echo "== run $run: $count messages on queue $queue ($where, mix $mix)"
dc build -q $svc checker consumer-python
if [ "$aws" = 1 ]; then dc up -d --build --wait collector trailbeam; else dc up -d --build --wait sqs collector trailbeam; fi

start() {  # start NAME ARGS...: one driver process in its own container
  name=$1; shift
  names="$names $name"
  # shellcheck disable=SC2086
  dc run -d --no-deps --name "$name" -e TRAILBEAM_RECEIPT_HASH_KEY="$key" $aws_env "$svc" "$@" --queue "$queue" --truth "$truth" >/dev/null
}
start "$deleter" deleter --port 8080
for i in 1 2 3; do
  # shellcheck disable=SC2086
  start "trailbeam-check-$run-consumer-$i" consume "$per_process" 4 --deleter "http://$deleter:8080" $dlq_arg
done
sleep 3

echo "== producing"
# shellcheck disable=SC2086
dc run --rm -T --no-deps -e TRAILBEAM_RECEIPT_HASH_KEY="$key" $aws_env "$svc" produce --queue "$queue" --truth "$truth" \
  --count "$count" --mix "$mix"

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
floor_arg=""
if [ -n "${TRAILBEAM_CHECK_FLOOR:-}" ]; then
  floor_arg="--floor $TRAILBEAM_CHECK_FLOOR"
elif [ "$aws" = 0 ]; then
  floor_arg="--floor-report-only"
  echo "== the floor is reported, not gated, on the emulator (TRAILBEAM_CHECK_FLOOR=0.9 to gate it)"
fi
# shellcheck disable=SC2086
dc run --rm -T --no-deps checker verify --truth "$truth" --count "$count" --store http://trailbeam:4318 \
  --report "$truth/report.json" $floor_arg > "results/$run.txt" || status=$?
cat "results/$run.txt"
dc run --rm -T --no-deps --entrypoint cat checker "$truth/report.json" > "results/$run-report.json" || true
[ "$wait_status" = 0 ] || status=1
echo "== saved results/$run.txt and results/$run-report.json"
exit "$status"
