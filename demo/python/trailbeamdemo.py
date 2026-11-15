"""Trailbeam demo driver for Python: a producer, a consumer and a deleter on an SQS emulator, with the Trailbeam boto3 hooks.

    trailbeamdemo.py produce --queue orders --count 200 --mix check
    trailbeamdemo.py consume --queue orders --threads 4 --deleter http://deleter:8080
    trailbeamdemo.py deleter --queue orders --port 8080

Every message body is {"kind": ..., "n": ...}. What the consumer does depends on the kind:

    ok          delete it, from the thread that received it
    retry_once  on the first receive, ChangeMessageVisibility to 0; delete it on a later receive
    xproc       pass the receipt handle to the deleter, a separate process, which deletes it
    slow        (demo mix) on the first receive, work past the visibility timeout, so SQS delivers it
                again to another consumer; then delete it with the old handle
    poison      (demo mix) always fail, never delete; SQS moves it to the dead-letter queue
    poison_fast (check-dlq mix) ChangeMessageVisibility to 0 on every receive, never delete; after
                maxReceiveCount receives SQS moves it to <queue>-dlq, where consume --dlq receives and deletes it

With --truth DIR each process writes what it really did, one JSON line per SQS call, to its own file in DIR.
Each line brackets the call: ``t_start_ns`` just before it was made, ``t_end_ns`` just after it returned (one
clock, the host's: ``time.time_ns()``, which the Trailbeam hooks also read for the spans, so every line says
``same_clock_as_spans``). ``t_ns`` is kept as before: the start for a send or settle, the end for a receive.
A receive line also has SQS's own facts from the same response (QV-38): ``rc`` (ApproximateReceiveCount),
``sent_ms`` (SentTimestamp) and ``first_receive_ms`` (ApproximateFirstReceiveTimestamp), null if SQS left one out.
The concurrency check (demo/check/check.py) compares those files with the timelines in the Trailbeam store.
Only message kinds and numbers go in the bodies; nothing is read from AWS.
"""

import argparse
import hashlib
import json
import os
import queue
import signal
import socket
import sys
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import boto3

import trailbeam.boto

# Unset: the local emulator. Set but empty: the AWS SDK's default endpoint, that is real SQS (demo/check.sh --aws).
ENDPOINT = os.environ.get("TRAILBEAM_DEMO_SQS_ENDPOINT", "http://localhost:9324")
REGION = os.environ.get("AWS_REGION", "us-east-1")
stop = threading.Event()


def kind_of(i: int, mix: str) -> str:
    """The kind of message number i. ``check`` is the QV-11 recheck B2 mix; ``check-dlq`` adds a poison
    message every 20 (QV-33), so the check covers the move to the dead-letter queue."""
    if mix == "check-dlq" and i % 20 == 10:
        return "poison_fast"
    if mix in ("check", "check-dlq"):
        return "retry_once" if i % 7 == 3 else "xproc" if i % 7 == 5 else "ok"
    if i % 25 == 12:
        return "poison"
    return {3: "retry_once", 5: "xproc", 7: "slow"}.get(i % 10, "ok")


def handle_hash(receipt_handle: str) -> str:
    """A plain hash of a receipt handle for the truth files, independent of Trailbeam's keyed hash."""
    return hashlib.sha256(receipt_handle.encode()).hexdigest()[:16]


class Truth:
    """What this process really did, one JSON line per SQS call. A no-op without --truth."""

    def __init__(self, directory, role):
        self.f = None
        self.lock = threading.Lock()
        if directory:
            os.makedirs(directory, exist_ok=True)
            path = os.path.join(directory, f"py-{role}-{socket.gethostname()}-{os.getpid()}.jsonl")
            self.f = open(path, "a", encoding="utf-8")

    def write(self, **kw):
        if self.f is None:
            return
        kw.update(pid=os.getpid(), host=socket.gethostname(), thread=threading.current_thread().name,
                  same_clock_as_spans=True)
        with self.lock:
            self.f.write(json.dumps(kw) + "\n")
            self.f.flush()


# The driver asks for SQS's facts itself rather than relying on the Trailbeam hooks to add them, so the truth doesn't
# depend on the code under test.
RECEIVE_ATTRIBUTES = ["ApproximateReceiveCount", "SentTimestamp", "ApproximateFirstReceiveTimestamp"]


def sqs_facts(m) -> dict:
    """SQS's own facts about one received message, for its truth line."""
    a = m.get("Attributes", {})
    opt = lambda k: int(a[k]) if k in a else None  # noqa: E731
    return {"rc": int(a["ApproximateReceiveCount"]), "sent_ms": opt("SentTimestamp"),
            "first_receive_ms": opt("ApproximateFirstReceiveTimestamp")}


def sqs_client():
    return boto3.client("sqs", endpoint_url=ENDPOINT or None, region_name=REGION)


def ensure_queue(sqs, name: str, visibility_timeout: int, max_receives: int) -> str:
    """Create the queue and its dead-letter queue <name>-dlq if they don't exist. Returns the queue URL."""
    for attempt in range(60):
        try:
            dlq_url = sqs.create_queue(QueueName=f"{name}-dlq")["QueueUrl"]
            dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
            return sqs.create_queue(QueueName=name, Attributes={
                "VisibilityTimeout": str(visibility_timeout),
                "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": str(max_receives)}),
            })["QueueUrl"]
        except Exception as e:  # the emulator may still be starting
            if attempt == 59:
                raise
            print(f"waiting for SQS at {ENDPOINT}: {e}", file=sys.stderr)
            time.sleep(1)


def install_hooks(role: str):
    os.environ.setdefault("OTEL_SERVICE_NAME", f"trailbeam-demo-py-{role}")
    trailbeam.boto.install()


def on_sigterm(*_):
    stop.set()


def produce(args):
    install_hooks("producer")
    sqs = sqs_client()
    url = ensure_queue(sqs, args.queue, args.visibility_timeout, args.max_receives)
    truth = Truth(args.truth, "producer")
    i = 0
    while not stop.is_set() and (args.count == 0 or i < args.count):
        kind = kind_of(i, args.mix)
        t = time.time_ns()
        r = sqs.send_message(QueueUrl=url, MessageBody=json.dumps({"kind": kind, "n": i}))
        truth.write(op="send", id=r["MessageId"], kind=kind, n=i, t_ns=t, result="ok", t_start_ns=t,
                    t_end_ns=time.time_ns())
        i += 1
        if args.interval:
            stop.wait(args.interval)
    print(f"sent {i} messages to {args.queue}", flush=True)


def consume(args):
    install_hooks("consumer")
    sqs = sqs_client()
    url = ensure_queue(sqs, args.queue, args.visibility_timeout, args.max_receives)
    truth = Truth(args.truth, "consumer")

    def settle(op, call, m, **params):
        t = time.time_ns()
        try:
            call(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"], **params)
            result = "ok"
        except Exception as e:  # recorded, not fatal: a stale handle can be rejected
            result = "failed"
            print(f"{op} {m['MessageId']} failed: {e}", file=sys.stderr)
        t1 = time.time_ns()
        truth.write(op=op, id=m["MessageId"], rh=handle_hash(m["ReceiptHandle"]), t_ns=t, result=result,
                    t_start_ns=t, t_end_ns=t1)

    def handle(m):
        kind = json.loads(m["Body"])["kind"]
        rc = int(m["Attributes"]["ApproximateReceiveCount"])
        if kind == "retry_once" and rc == 1:
            settle("change_visibility", sqs.change_message_visibility, m, VisibilityTimeout=0)
        elif kind == "xproc":
            truth.write(op="handoff", id=m["MessageId"], rh=handle_hash(m["ReceiptHandle"]), t_ns=time.time_ns())
            body = json.dumps({"receipt_handle": m["ReceiptHandle"], "message_id": m["MessageId"]}).encode()
            urllib.request.urlopen(urllib.request.Request(args.deleter + "/delete", data=body, method="POST"), timeout=10)
        elif kind == "slow" and rc == 1:
            stop.wait(args.visibility_timeout + 3)
            settle("delete", sqs.delete_message, m)
        elif kind == "poison":
            print(f"poison message {m['MessageId']} failed (receive {rc})", file=sys.stderr)
        elif kind == "poison_fast":
            settle("change_visibility", sqs.change_message_visibility, m, VisibilityTimeout=0)
        else:
            settle("delete", sqs.delete_message, m)

    def loop():
        while not stop.is_set():
            t0 = time.time_ns()
            try:
                r = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=args.wait,
                                        MessageSystemAttributeNames=RECEIVE_ATTRIBUTES)
            except Exception as e:
                print(f"receive failed: {e}", file=sys.stderr)
                stop.wait(1)
                continue
            t = time.time_ns()
            for m in r.get("Messages", []):
                truth.write(op="receive", id=m["MessageId"], rh=handle_hash(m["ReceiptHandle"]), **sqs_facts(m),
                            t_ns=t, result="ok", queue=args.queue, t_start_ns=t0, t_end_ns=t)
            for m in r.get("Messages", []):
                try:
                    handle(m)
                except Exception as e:
                    print(f"handling {m['MessageId']} failed: {e}", file=sys.stderr)

    def dlq_loop():
        # Reads the dead-letter queue and deletes what it finds. It does not ask for DeadLetterQueueSourceArn:
        # the Trailbeam hooks do, and the store shows the move SQS reports with it (contract 3.1).
        dlq = f"{args.queue}-dlq"
        dlq_url = sqs.get_queue_url(QueueName=dlq)["QueueUrl"]
        while not stop.is_set():
            t0 = time.time_ns()
            try:
                r = sqs.receive_message(QueueUrl=dlq_url, MaxNumberOfMessages=10, WaitTimeSeconds=args.wait,
                                        MessageSystemAttributeNames=RECEIVE_ATTRIBUTES)
            except Exception as e:
                print(f"dead-letter receive failed: {e}", file=sys.stderr)
                stop.wait(1)
                continue
            t = time.time_ns()
            for m in r.get("Messages", []):
                truth.write(op="receive", id=m["MessageId"], rh=handle_hash(m["ReceiptHandle"]), **sqs_facts(m),
                            t_ns=t, result="ok", queue=dlq, t_start_ns=t0, t_end_ns=t)
            for m in r.get("Messages", []):
                print(f"dead-letter queue: {m['MessageId']}", flush=True)
                t = time.time_ns()
                try:
                    sqs.delete_message(QueueUrl=dlq_url, ReceiptHandle=m["ReceiptHandle"])
                    result = "ok"
                except Exception as e:
                    result = "failed"
                    print(f"dead-letter delete {m['MessageId']} failed: {e}", file=sys.stderr)
                truth.write(op="delete", id=m["MessageId"], rh=handle_hash(m["ReceiptHandle"]), t_ns=t,
                            result=result, queue=dlq, t_start_ns=t, t_end_ns=time.time_ns())

    threads = [threading.Thread(target=loop, name=f"consumer-{i}") for i in range(args.threads)]
    if args.dlq:
        threads.append(threading.Thread(target=dlq_loop, name="dlq"))
    for t in threads:
        t.start()
    print(f"consuming {args.queue} with {args.threads} threads", flush=True)
    for t in threads:
        t.join()


def deleter(args):
    install_hooks("deleter")
    sqs = sqs_client()
    url = ensure_queue(sqs, args.queue, args.visibility_timeout, args.max_receives)
    truth = Truth(args.truth, "deleter")
    work: queue.Queue = queue.Queue()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            work.put((body["receipt_handle"], body["message_id"]))
            self.send_response(202)
            self.end_headers()

        def log_message(self, *a):
            pass

    server = ThreadingHTTPServer(("0.0.0.0", args.port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    print(f"deleter for {args.queue} on port {args.port}", flush=True)
    while not stop.is_set():
        try:
            rh, mid = work.get(timeout=0.5)
        except queue.Empty:
            continue
        time.sleep(0.05)
        t = time.time_ns()
        try:
            sqs.delete_message(QueueUrl=url, ReceiptHandle=rh)
            result = "ok"
        except Exception as e:
            result = "failed"
            print(f"delete {mid} failed: {e}", file=sys.stderr)
        truth.write(op="delete", id=mid, rh=handle_hash(rh), t_ns=t, result=result, by="deleter", t_start_ns=t,
                    t_end_ns=time.time_ns())
    server.shutdown()


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = p.add_subparsers(dest="role", required=True)
    for role in ("produce", "consume", "deleter"):
        s = sub.add_parser(role)
        s.add_argument("--queue", default="orders")
        s.add_argument("--truth", help="directory for this process's truth file")
        s.add_argument("--visibility-timeout", type=int, default=30)
        s.add_argument("--max-receives", type=int, default=3, help="maxReceiveCount of the redrive policy")
    sub.choices["produce"].add_argument("--count", type=int, default=0, help="0: until stopped")
    sub.choices["produce"].add_argument("--interval", type=float, default=0.0, help="seconds between sends")
    sub.choices["produce"].add_argument("--mix", choices=("demo", "check", "check-dlq"), default="demo")
    sub.choices["consume"].add_argument("--threads", type=int, default=4)
    sub.choices["consume"].add_argument("--wait", type=int, default=1, help="WaitTimeSeconds")
    sub.choices["consume"].add_argument("--deleter", default="http://deleter:8080")
    sub.choices["consume"].add_argument("--dlq", action="store_true", help="also read and empty <queue>-dlq")
    sub.choices["deleter"].add_argument("--port", type=int, default=8080)
    args = p.parse_args(argv)
    signal.signal(signal.SIGTERM, on_sigterm)
    signal.signal(signal.SIGINT, on_sigterm)
    try:
        {"produce": produce, "consume": consume, "deleter": deleter}[args.role](args)
    finally:
        trailbeam.boto.uninstall()  # flushes the Trailbeam spans


if __name__ == "__main__":
    main()
