"""Smoke test for the running demo (docker compose -f demo/compose.yaml up -d). Standard library only.

    python3 demo/smoke_test.py [http://127.0.0.1:4318] [--timeout 150]

Waits until the store has timelines from the demo workload that show each part working, then exits 0:
messages consumed by the Python consumer and by the Node.js consumer, a delete from the separate deleter
process joined to its message, a redelivery after ChangeMessageVisibility to 0, and a poison message
moved to the dead-letter queue and received there: the move observed (reported by SQS with
DeadLetterQueueSourceArn, contract 3.1), never inferred. Also checks that no qv2 span was rejected.
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


def seen(base):
    found = set()
    for m in get(base, "/api/messages?limit=1000&queue=orders")["messages"]:
        services = {c.get("service_name") for c in m["consumers"]}
        if "qv2-demo-py-consumer" in services:
            found.add("python consumer")
        if "qv2-demo-node-consumer" in services:
            found.add("node consumer")
        if m["state"] != "deleted" and not m["in_dead_letter_queue"] and m["deliveries"] < 2:
            continue
        tl = get(base, "/api/messages/" + urllib.parse.quote(m["message_id"], safe=""))
        types = [e["type"] for e in tl["events"]]
        deletes = [e for e in tl["events"] if e["type"] == "deleted" and e.get("status") == "final"]
        if any(e.get("consumer", {}).get("service_name") == "qv2-demo-py-deleter" and e.get("delivery") for e in deletes):
            found.add("delete by the deleter process, joined")
        if "visibility_changed" in types and types.count("received") >= 2 and "visibility_timeout_expired" in types:
            found.add("redelivery after ChangeMessageVisibility")
        moves = [e for e in tl["events"] if e["type"] == "moved_to_dead_letter_queue"]
        if any(e["inferred"] for e in moves):
            sys.exit(f"FAIL: {m['message_id']} shows an inferred move to the dead-letter queue")
        if any(e.get("source") == "sqs_dlq_source" for e in moves) and any(
                e["type"] == "received" and e.get("queue") == "orders-dlq" for e in tl["events"]):
            found.add("poison message moved to the dead-letter queue")
    return found


WANT = {"python consumer", "node consumer", "delete by the deleter process, joined",
        "redelivery after ChangeMessageVisibility", "poison message moved to the dead-letter queue"}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("base", nargs="?", default="http://127.0.0.1:4318")
    p.add_argument("--timeout", type=float, default=150)
    a = p.parse_args()
    deadline, found = time.time() + a.timeout, set()
    while time.time() < deadline:
        try:
            found = seen(a.base)
        except OSError as e:
            print(f"store not ready: {e}")
        if found >= WANT:
            break
        time.sleep(5)
    health = get(a.base, "/api/health")
    print(json.dumps(health["store"]), json.dumps(health["ingest"]))
    for w in sorted(WANT):
        print(("ok      " if w in found else "MISSING ") + w)
    ok = found >= WANT and health["ingest"]["rejected"] == 0
    print("PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
