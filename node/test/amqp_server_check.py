# SPDX-License-Identifier: AGPL-3.0-only
# Imports the server, so it is under the server's licence (LICENSING.md).
"""What a Trailbeam server makes of the amqplib hooks' spans (test/amqplib-rabbitmq.test.js).

Reads {"otlp": <an OTLP/JSON export request>, "message_ids": [...]} on stdin and prints
{"rejected": [...], "timelines": {message ID: [event types] or null}}: the spans the ingest refused (A12) and each
message's RabbitMQ timeline, from a Trailbeam store holding the spans. Needs the trailbeam Python package
(python/ in this repository).
"""
import json
import sys
import time

from trailbeam.server import queries, systems
from trailbeam.server.db import SqliteStore
from trailbeam.server.otlp import decode


def main() -> None:
    req = json.load(sys.stdin)
    d = decode(json.dumps(req["otlp"]).encode(), "application/json")
    out = {"rejected": [repr(r) for r in d.rejected], "timelines": {}}
    store = SqliteStore()
    try:
        store.insert(d.records)
        for mid in req["message_ids"]:
            t = queries.message(store, systems.message_key(systems.RABBITMQ, mid), time.time_ns() + 10 ** 9, 900, None)
            out["timelines"][mid] = None if t is None else [e["type"] for e in t["events"]]
    finally:
        store.close()
    json.dump(out, sys.stdout)


if __name__ == "__main__":
    main()
