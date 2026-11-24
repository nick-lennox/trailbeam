"""Smoke test for the running demo (docker compose -f demo/compose.yaml up -d). Standard library only.

    python3 demo/smoke_test.py [http://127.0.0.1:4318] [--timeout 150]

Waits until Trailbeam has timelines from the demo workload that show each part working, then exits 0:
messages consumed by the Python consumer and by the Node.js consumer, a delete from the separate deleter
process joined to its message, a redelivery after ChangeMessageVisibility to 0, and a poison message
moved to the dead-letter queue and received there: the move observed (reported by SQS with
DeadLetterQueueSourceArn, contract 3.1), never inferred. And from RabbitMQ (the pika hooks, contract 4.0): a message
acked by the RabbitMQ consumer, one requeued with basic.nack and delivered again, one returned to the queue when the
consumer's connection closed, and one dead-lettered by basic.reject, the move read from x-death, never inferred.
Also checks that no Trailbeam span was rejected.
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
        if "trailbeam-demo-py-consumer" in services:
            found.add("python consumer")
        if "trailbeam-demo-node-consumer" in services:
            found.add("node consumer")
        if m["state"] != "deleted" and not m["in_dead_letter_queue"] and m["deliveries"] < 2:
            continue
        tl = get(base, "/api/messages/" + urllib.parse.quote(m["message_id"], safe=""))
        types = [e["type"] for e in tl["events"]]
        deletes = [e for e in tl["events"] if e["type"] == "deleted" and e.get("status") == "final"]
        if any(e.get("consumer", {}).get("service_name") == "trailbeam-demo-py-deleter" and e.get("delivery") for e in deletes):
            found.add("delete by the deleter process, joined")
        if "visibility_changed" in types and types.count("received") >= 2 and "visibility_timeout_expired" in types:
            found.add("redelivery after ChangeMessageVisibility")
        moves = [e for e in tl["events"] if e["type"] == "moved_to_dead_letter_queue"]
        if any(e["inferred"] for e in moves):
            sys.exit(f"FAIL: {m['message_id']} shows an inferred move to the dead-letter queue")
        if any(e.get("source") == "sqs_dlq_source" for e in moves) and any(
                e["type"] == "received" and e.get("queue") == "orders-dlq" for e in tl["events"]):
            found.add("poison message moved to the dead-letter queue")
    found |= seen_rabbitmq(base)
    return found


def seen_rabbitmq(base):
    found = set()
    for m in get(base, "/api/messages?limit=1000&queue=rmq-orders")["messages"]:
        if not m["message_id"].startswith("~rabbitmq~"):
            continue
        if m["state"] == "deleted" and "trailbeam-demo-rmq-consumer" in {c.get("service_name") for c in m["consumers"]}:
            found.add("rabbitmq: acked by the consumer")
        if m["deliveries"] < 2 and not m["in_dead_letter_queue"] and not m["dead_letter_queue_move"]:
            continue
        tl = get(base, "/api/messages/" + urllib.parse.quote(m["message_id"], safe=""))
        types = [e["type"] for e in tl["events"]]
        if "requeued" in types and "returned_to_queue" in types and types.count("received") >= 2:
            found.add("rabbitmq: requeued and delivered again")
        if "channel_closed" in types and "returned_to_queue" in types and types.count("received") >= 2:
            found.add("rabbitmq: returned to the queue when the connection closed")
        moves = [e for e in tl["events"] if e["type"] == "moved_to_dead_letter_queue"]
        if any(e["inferred"] for e in moves):
            sys.exit(f"FAIL: {m['message_id']} shows an inferred move to the dead-letter queue")
        if any(e.get("source") == "amqp_x_death" for e in moves) and any(
                e["type"] == "received" and e.get("queue") == "rmq-orders-dlq" for e in tl["events"]):
            found.add("rabbitmq: refused and dead-lettered (x-death)")
    return found


WANT = {"python consumer", "node consumer", "delete by the deleter process, joined",
        "redelivery after ChangeMessageVisibility", "poison message moved to the dead-letter queue",
        "rabbitmq: acked by the consumer", "rabbitmq: requeued and delivered again",
        "rabbitmq: returned to the queue when the connection closed", "rabbitmq: refused and dead-lettered (x-death)"}


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
            print(f"Trailbeam not ready: {e}")
        if found >= WANT:
            break
        time.sleep(5)
    health = get(a.base, "/api/health")
    print(json.dumps(get(a.base, "/api/stats")["data"]), json.dumps(health["ingest"]))
    for w in sorted(WANT):
        print(("ok      " if w in found else "MISSING ") + w)
    ok = found >= WANT and health["ingest"]["rejected"] == 0
    print("PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
