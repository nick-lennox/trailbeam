"""Trailbeam demo driver for RabbitMQ: a publisher and a consumer on a quorum queue, with the Trailbeam pika hooks.

    rabbitmqdemo.py produce --interval 1
    rabbitmqdemo.py consume

The publisher sends one message a second to the quorum queue ``rmq-orders`` through the default exchange, with
publisher confirms and a ``message_id``. The body is {"kind": ..., "n": ...}; what the consumer does depends on the
kind:

    ok          basic.ack
    retry_once  on the first delivery, basic.nack with requeue; basic.ack when it comes back
    crash       on the first delivery, close the connection without settling (the broker requeues it), reconnect;
                basic.ack when it comes back
    poison      basic.reject without requeue: the queue dead-letters it to ``rmq-orders-dlq`` (x-death reason
                ``rejected``), where the consumer acks it

Only message kinds and numbers go in the bodies.
"""

import argparse
import json
import os
import signal
import sys
import threading
import time
import uuid

import pika

import trailbeam.pika

URL = os.environ.get("TRAILBEAM_DEMO_AMQP_URL", "amqp://guest:guest@localhost:5672/%2F")
QUEUE, DLQ, DLX = "rmq-orders", "rmq-orders-dlq", "rmq-orders-dlx"
stop = threading.Event()


def kind_of(i: int) -> str:
    if i % 25 == 12:
        return "poison"
    return {3: "retry_once", 6: "crash"}.get(i % 10, "ok")


def connect() -> pika.BlockingConnection:
    for attempt in range(120):
        try:
            conn = pika.BlockingConnection(pika.URLParameters(URL))
            declare(conn.channel())
            return conn
        except pika.exceptions.AMQPConnectionError as e:  # the broker may still be starting
            if attempt == 119 or stop.is_set():
                raise
            print(f"waiting for RabbitMQ: {e!r}", file=sys.stderr)
            time.sleep(1)
    raise SystemExit("no RabbitMQ")


def declare(ch) -> None:
    """The queues, if they don't exist: rmq-orders dead-letters to rmq-orders-dlq through a fanout exchange."""
    ch.exchange_declare(DLX, "fanout", durable=True)
    ch.queue_declare(DLQ, durable=True, arguments={"x-queue-type": "quorum"})
    ch.queue_bind(DLQ, DLX)
    ch.queue_declare(QUEUE, durable=True, arguments={"x-queue-type": "quorum", "x-dead-letter-exchange": DLX,
                                                     "x-delivery-limit": 3})


def produce(args):
    conn = connect()
    ch = conn.channel()
    ch.confirm_delivery()
    run = uuid.uuid4().hex[:6]
    n = 0
    while not stop.is_set() and (args.count == 0 or n < args.count):
        body = json.dumps({"kind": kind_of(n), "n": n}).encode()
        ch.basic_publish("", QUEUE, body, properties=pika.BasicProperties(
            message_id=f"rmq-{run}-{n}", delivery_mode=2, content_type="application/json"))
        n += 1
        conn.sleep(args.interval)
    conn.close()


def consume(args):
    while not stop.is_set():
        conn = connect()
        ch = conn.channel()
        ch.basic_qos(prefetch_count=1)
        crash = []

        def on_message(ch, method, props, body):
            try:
                kind = json.loads(body).get("kind")
            except ValueError:
                kind = "ok"
            if kind == "ok" or method.redelivered:  # a retry_once or crash message that came back
                ch.basic_ack(method.delivery_tag)
            elif kind == "retry_once":
                ch.basic_nack(method.delivery_tag, requeue=True)
            elif kind == "poison":
                ch.basic_reject(method.delivery_tag, requeue=False)
            else:  # crash
                crash.append(method.delivery_tag)  # closed below, outside the callback, with this delivery open

        ch.basic_consume(QUEUE, on_message)
        ch.basic_consume(DLQ, lambda ch, m, p, b: ch.basic_ack(m.delivery_tag))
        while not stop.is_set() and not crash:
            conn.process_data_events(time_limit=0.5)
        conn.close()
        if crash:
            print(f"closing the connection with delivery {crash[0]} open, as a crash would", file=sys.stderr)
            time.sleep(1)


def on_sigterm(*_):
    stop.set()


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = p.add_subparsers(dest="role", required=True)
    s = sub.add_parser("produce")
    s.add_argument("--count", type=int, default=0, help="0: until stopped")
    s.add_argument("--interval", type=float, default=1.0, help="seconds between publishes")
    sub.add_parser("consume")
    args = p.parse_args(argv)
    signal.signal(signal.SIGTERM, on_sigterm)
    signal.signal(signal.SIGINT, on_sigterm)
    os.environ.setdefault("OTEL_SERVICE_NAME", f"trailbeam-demo-rmq-{'publisher' if args.role == 'produce' else 'consumer'}")
    trailbeam.pika.install()
    try:
        {"produce": produce, "consume": consume}[args.role](args)
    finally:
        trailbeam.pika.uninstall()  # flushes the Trailbeam spans


if __name__ == "__main__":
    main()
