"""Writes amqp-4.0-spans.json: the QV-303 proof's RabbitMQ scenarios as contract-4.0 spans.

These spans are written from contract/span-contract-amqp.md, not recorded: no package emits them yet. Replace
them with spans recorded from the Python package (QV-309) when it lands, keeping the scenarios.

    python contract/fixtures/make-amqp-spans.py
"""
import json
import uuid
from pathlib import Path

OUT = Path(__file__).resolve().parent / "amqp-4.0-spans.json"
T0 = 1763980000_000_000_000  # 2025-11-24T10:26:40Z, in nanoseconds
MS = 1_000_000


def _uuid(name: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"trailbeam-amqp-fixture:{name}"))


class Process:
    """One instrumented process: its identity, its call-order counter and its channels."""

    def __init__(self, service: str, pid: int):
        self.service, self.pid = service, pid
        self.client_id = _uuid(f"client:{service}")
        self.scope = _uuid(f"scope:{service}")
        self.ticks = 0
        self.channels = 0

    def tick(self) -> int:
        self.ticks += 1
        return self.ticks

    def channel(self) -> str:
        self.channels += 1
        return _uuid(f"channel:{self.service}:{self.channels}")


class Recorder:
    def __init__(self):
        self.now = T0
        self.spans = []
        self.scenario = ""

    def _common(self, proc, kind, op_type, op_name, dest):
        return {
            "trailbeam.schema_version": 4, "trailbeam.kind": kind, "trailbeam.source": "client",
            "messaging.system.name": "rabbitmq", "messaging.operation.type": op_type,
            "messaging.operation.name": op_name, "messaging.destination.name": dest,
            "trailbeam.amqp.vhost": "/", "server.address": "rabbitmq", "server.port": 5672,
            "messaging.client.id": proc.client_id, "process.pid": proc.pid, "trailbeam.order.scope": proc.scope,
        }

    def _emit(self, proc, name, attrs, start, end):
        self.spans.append({
            "scenario": self.scenario, "name": name,
            "resource": {"service.name": proc.service, "service.instance.id": proc.client_id},
            "start_time_unix_nano": start, "end_time_unix_nano": end,
            "attributes": {k: v for k, v in attrs.items() if v is not None},
        })

    def call(self, proc, name, kind, op_type, op_name, dest, ms=2, **attrs):
        """A call the application makes: an interval with both ticks."""
        a = self._common(proc, kind, op_type, op_name, dest)
        a["trailbeam.order.issued"] = proc.tick()
        start = self.now
        self.now += ms * MS
        a["trailbeam.order.returned"] = proc.tick()
        a.update(attrs)
        self._emit(proc, name, a, start, self.now)
        self.now += MS

    def event(self, proc, name, kind, op_type, op_name, dest, **attrs):
        """Something the broker sent, or a close: zero-length, start not observed, no issued tick."""
        a = self._common(proc, kind, op_type, op_name, dest)
        a["trailbeam.order.returned"] = proc.tick()
        a["trailbeam.start_observed"] = False
        a.update(attrs)
        self._emit(proc, name, a, self.now, self.now)
        self.now += MS

    # Helpers per span kind.

    def publish(self, proc, exchange, routing_key, message_id, mandatory=False, result="ok", **extra):
        self.call(proc, "trailbeam publish", "amqp.publish", "send", "basic.publish", exchange or "amq.default",
                  ms=3, **{"rabbitmq.destination.routing_key": routing_key or None,
                           "messaging.message.id": message_id,
                           "trailbeam.amqp.message_id.source": "application" if message_id else None,
                           "trailbeam.amqp.mandatory": mandatory, "trailbeam.amqp.confirmed": True,
                           "trailbeam.result": result, "trailbeam.amqp.delivery_mode": 2}, **extra)

    def deliver(self, proc, ch, queue, tag, message_id, redelivered=False, acquired=None, delivery=None,
                exchange="", routing_key=None, ctag="ctag-1", dead_letter=None):
        if acquired is not None:
            count, source = acquired + 1, "x-acquired-count"
        elif not redelivered:
            count, source = 1, "first_delivery"
        else:
            count, source = None, None
        attrs = {"messaging.message.id": message_id, "trailbeam.amqp.channel.id": ch,
                 "rabbitmq.message.delivery_tag": tag, "trailbeam.amqp.consumer_tag": ctag,
                 "trailbeam.amqp.exchange": exchange or "amq.default",
                 "rabbitmq.destination.routing_key": routing_key or (queue if not exchange else None),
                 "trailbeam.amqp.redelivered": redelivered, "trailbeam.receive_count": count,
                 "trailbeam.receive_count.source": source, "trailbeam.amqp.acquired_count": acquired,
                 "trailbeam.amqp.delivery_count": delivery}
        attrs.update(dead_letter or {})
        self.event(proc, "trailbeam deliver", "amqp.deliver", "receive", "basic.deliver", queue, **attrs)

    def settle(self, proc, ch, op, queue, tag, message_id, requeue=None, ctag="ctag-1"):
        kind = f"amqp.{op}"
        self.call(proc, f"trailbeam {op}", kind, "settle", f"basic.{op}", queue, ms=0,
                  **{"messaging.message.id": message_id, "trailbeam.amqp.channel.id": ch,
                     "rabbitmq.message.delivery_tag": tag, "trailbeam.amqp.consumer_tag": ctag,
                     "trailbeam.amqp.multiple": False if op != "reject" else None,
                     "trailbeam.amqp.requeue": requeue, "trailbeam.result": "ok"})

    def closed(self, proc, ch, queue, tag, message_id, initiator, op_name, code=None, text=None, ctag="ctag-1"):
        self.event(proc, "trailbeam channel_closed", "amqp.channel_closed", "settle", op_name, queue,
                   **{"messaging.message.id": message_id, "trailbeam.amqp.channel.id": ch,
                      "rabbitmq.message.delivery_tag": tag, "trailbeam.amqp.consumer_tag": ctag,
                      "trailbeam.amqp.close.initiator": initiator, "trailbeam.amqp.close.reply_code": code,
                      "trailbeam.amqp.close.reply_text": text})


def dead_letter(queue, reason, count, time_s):
    return {"trailbeam.dead_letter.source_queue": queue, "trailbeam.dead_letter.reason": reason,
            "trailbeam.dead_letter.count": count, "trailbeam.dead_letter.time_ms": time_s * 1000,
            "trailbeam.dead_letter.exchange": "amq.default", "trailbeam.dead_letter.first_queue": queue,
            "trailbeam.dead_letter.first_reason": reason}


SCENARIOS = {
    "happy": "publish, deliver, ack",
    "requeue-once": "nack with requeue, redelivered with x-acquired-count 1, acked",
    "nack-loop": "nacked with requeue 5 times on a queue with x-delivery-limit 2: never dead-lettered",
    "crash-loop": "the consumer's connection drops 3 times, the queue dead-letters it (delivery_limit), acked on the DLQ",
    "rejected-to-dlq": "reject without requeue, delivered on the DLQ with x-death reason rejected",
    "fanout": "one publish to a fanout exchange, a copy delivered and acked on each of two queues",
    "publisher-retry": "the same message_id published twice: two publishes, two deliveries, two acks",
    "unroutable": "a mandatory publish no queue takes: a return, then an ok publish",
    "classic": "a classic queue: nack with requeue, redelivered with no count, acked",
    "no-message-id": "published, delivered and acked without a message_id",
    "consumer-timeout": "the broker cancels the consumer (consumer timeout); redelivered and acked",
    "stuck": "delivered, never settled",
}


def main():
    r = Recorder()
    api = Process("checkout-api", 4101)
    worker = Process("orders-worker", 4202)
    dlq = Process("dlq-worker", 4303)

    r.scenario = "happy"
    ch = worker.channel()
    r.publish(api, "", "orders", "m-happy")
    r.deliver(worker, ch, "orders", 1, "m-happy")
    r.settle(worker, ch, "ack", "orders", 1, "m-happy")

    r.scenario = "requeue-once"
    r.publish(api, "", "orders", "m-requeue")
    r.deliver(worker, ch, "orders", 2, "m-requeue")
    r.settle(worker, ch, "nack", "orders", 2, "m-requeue", requeue=True)
    r.deliver(worker, ch, "orders", 3, "m-requeue", redelivered=True, acquired=1)
    r.settle(worker, ch, "ack", "orders", 3, "m-requeue")

    r.scenario = "nack-loop"
    r.publish(api, "", "orders", "m-loop")
    tag = 4
    for i in range(6):
        r.deliver(worker, ch, "orders", tag, "m-loop", redelivered=i > 0, acquired=i or None)
        r.settle(worker, ch, "nack" if i < 5 else "ack", "orders", tag, "m-loop", requeue=True if i < 5 else None)
        tag += 1

    r.scenario = "crash-loop"
    r.publish(api, "", "orders", "m-crash")
    for i in range(3):
        crash_ch = worker.channel()
        r.deliver(worker, crash_ch, "orders", 1, "m-crash", redelivered=i > 0, acquired=i or None,
                  delivery=i or None, ctag=f"ctag-crash-{i}")
        r.closed(worker, crash_ch, "orders", 1, "m-crash", "network", "connection.lost", ctag=f"ctag-crash-{i}")
    ch_dlq = dlq.channel()
    r.deliver(dlq, ch_dlq, "orders.dlq", 1, "m-crash", acquired=3, delivery=3, exchange="dlx", routing_key="orders",
              ctag="ctag-dlq", dead_letter=dead_letter("orders", "delivery_limit", 1, r.now // 1_000_000_000))
    r.settle(dlq, ch_dlq, "ack", "orders.dlq", 1, "m-crash", ctag="ctag-dlq")

    r.scenario = "rejected-to-dlq"
    r.publish(api, "", "orders", "m-reject")
    r.deliver(worker, ch, "orders", tag, "m-reject")
    r.settle(worker, ch, "reject", "orders", tag, "m-reject", requeue=False)
    tag += 1
    r.deliver(dlq, ch_dlq, "orders.dlq", 2, "m-reject", acquired=1, delivery=1, exchange="dlx", routing_key="orders",
              ctag="ctag-dlq", dead_letter=dead_letter("orders", "rejected", 1, r.now // 1_000_000_000))
    r.settle(dlq, ch_dlq, "ack", "orders.dlq", 2, "m-reject", ctag="ctag-dlq")

    r.scenario = "fanout"
    r.publish(api, "fan", "", "m-fan")
    for q in ("fan.a", "fan.b"):
        fch = worker.channel()
        r.deliver(worker, fch, q, 1, "m-fan", exchange="fan", ctag=f"ctag-{q}")
        r.settle(worker, fch, "ack", q, 1, "m-fan", ctag=f"ctag-{q}")

    r.scenario = "publisher-retry"
    r.publish(api, "", "orders", "m-retry")
    r.publish(api, "", "orders", "m-retry")
    for _ in range(2):
        r.deliver(worker, ch, "orders", tag, "m-retry")
        r.settle(worker, ch, "ack", "orders", tag, "m-retry")
        tag += 1

    r.scenario = "unroutable"
    r.event(api, "trailbeam return", "amqp.return", "send", "basic.return", "amq.default",
            **{"rabbitmq.destination.routing_key": "nobody", "messaging.message.id": "m-noroute",
               "trailbeam.amqp.message_id.source": "application", "trailbeam.amqp.reply_code": 312,
               "trailbeam.amqp.reply_text": "NO_ROUTE"})
    r.publish(api, "", "nobody", "m-noroute", mandatory=True)

    r.scenario = "classic"
    cch = worker.channel()
    r.publish(api, "", "classic", "m-classic")
    r.deliver(worker, cch, "classic", 1, "m-classic", ctag="ctag-classic")
    r.settle(worker, cch, "nack", "classic", 1, "m-classic", requeue=True, ctag="ctag-classic")
    r.deliver(worker, cch, "classic", 2, "m-classic", redelivered=True, ctag="ctag-classic")
    r.settle(worker, cch, "ack", "classic", 2, "m-classic", ctag="ctag-classic")

    r.scenario = "no-message-id"
    r.publish(api, "", "orders", None)
    r.deliver(worker, ch, "orders", tag, None)
    r.settle(worker, ch, "ack", "orders", tag, None)
    tag += 1

    r.scenario = "consumer-timeout"
    tch = worker.channel()
    r.publish(api, "", "orders", "m-timeout")
    r.deliver(worker, tch, "orders", 1, "m-timeout", ctag="ctag-slow")
    r.event(worker, "trailbeam cancel", "amqp.cancel", "cancel", "basic.cancel", "orders",
            **{"trailbeam.amqp.channel.id": tch, "trailbeam.amqp.consumer_tag": "ctag-slow",
               "trailbeam.amqp.cancelled_by": "broker", "trailbeam.amqp.unsettled_count": 1})
    r.deliver(worker, ch, "orders", tag, "m-timeout", redelivered=True, acquired=1)
    r.settle(worker, ch, "ack", "orders", tag, "m-timeout")
    tag += 1

    r.scenario = "stuck"
    r.publish(api, "", "orders", "m-stuck")
    r.deliver(worker, ch, "orders", tag, "m-stuck")

    doc = {
        "about": ("RabbitMQ spans of contract 4.0 (contract/span-contract-amqp.md), written by make-amqp-spans.py "
                  "from the contract, not recorded: no package emits them yet. Re-record from the Python package "
                  "(QV-309) and keep the scenarios. Do not edit by hand."),
        "scenarios": SCENARIOS,
        "spans": r.spans,
    }
    OUT.write_text(json.dumps(doc, indent=1, ensure_ascii=False) + "\n", "utf-8")
    print(f"wrote {len(r.spans)} spans to {OUT.name}")


if __name__ == "__main__":
    main()
