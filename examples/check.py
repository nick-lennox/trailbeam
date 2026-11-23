"""Check that an example's three orders reached Trailbeam with the timelines its README promises.
Standard library only; CI runs it after `docker compose up` (see run.sh).

    python3 examples/check.py [http://127.0.0.1:4318] --consumer orders-worker [--timeout 120]

Passes once the queue `orders` has these three messages, and no span was rejected:
  ok           received once by the consumer and deleted
  retry        received, not deleted, received again after the visibility timeout, then deleted
  dead letter  received 3 times, then moved to `orders-dlq` (reported by SQS, not inferred) and received there
"""
import argparse
import json
import sys
import time
import urllib.parse
import urllib.request


def get(base, path):
    with urllib.request.urlopen(base.rstrip("/") + path, timeout=10) as r:
        return json.load(r)


def found(base, consumer):
    seen = {}
    for m in get(base, "/api/messages?limit=100&queue=orders")["messages"]:
        tl = get(base, "/api/messages/" + urllib.parse.quote(m["message_id"], safe=""))
        events = tl["events"]
        types = [e["type"] for e in events]
        received = [e for e in events if e["type"] == "received" and e.get("queue", "orders") == "orders"]
        by = {(e.get("consumer") or {}).get("service_name") for e in received}
        if consumer not in by:
            continue
        deleted = m["state"] in ("deleted", "deleted_inferred")  # inferred: Lambda's poller deletes, out of sight
        if deleted and len(received) == 1:
            seen["ok"] = m["message_id"]
        elif deleted and len(received) == 2 and "visibility_timeout_expired" in types:
            seen["retry"] = m["message_id"]
        moves = [e for e in events if e["type"] == "moved_to_dead_letter_queue"]
        if any(e["inferred"] for e in moves):
            sys.exit(f"FAIL: {m['message_id']} shows an inferred move to the dead-letter queue")
        if (len(received) == 3 and any(e.get("source") == "sqs_dlq_source" for e in moves)
                and any(e["type"] == "received" and e.get("queue") == "orders-dlq" for e in events)):
            seen["dead letter"] = m["message_id"]
    return seen


def main():
    p = argparse.ArgumentParser()
    p.add_argument("base", nargs="?", default="http://127.0.0.1:4318")
    p.add_argument("--consumer", required=True, help="OTEL_SERVICE_NAME of the service that receives from orders")
    p.add_argument("--timeout", type=float, default=120)
    a = p.parse_args()
    want = ["ok", "retry", "dead letter"]
    deadline, seen = time.time() + a.timeout, {}
    while time.time() < deadline:
        try:
            seen = found(a.base, a.consumer)
        except OSError as e:
            print(f"Trailbeam not ready: {e}", flush=True)
        if len(seen) == len(want):
            break
        time.sleep(3)
    try:
        rejected = get(a.base, "/api/health")["ingest"]["rejected"]
    except OSError as e:
        rejected = f"unknown, Trailbeam not reachable: {e}"
    for w in want:
        print(f"ok      {w}: {a.base}/message?id={urllib.parse.quote(seen[w])}" if w in seen else f"MISSING {w}")
    print(f"rejected spans: {rejected}")
    ok = len(seen) == len(want) and rejected == 0
    print("PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
