"""Trailbeam load generator: realistic OTLP/HTTP Trailbeam span traffic, sent to any OTLP traces URL.

    python bench/loadgen.py --url http://127.0.0.1:4318/v1/traces --rate 200 --duration 30
    python bench/loadgen.py --url ... --rate 200 --queues 500 --tenants 50 --body-size 64k --late-join 0.2
    python bench/loadgen.py --url ... --rate 200 --sdk          # through the OTel SDK's BatchSpanProcessor

The spans are the ones the packages emit (contract/span-contract.md, schema version 3): the same span
names, ``trailbeam.kind`` values, attributes and resource attributes as ``trailbeam._emitter`` and the
recorded fixtures in ``python/smoke`` and ``contract/fixtures``. Nothing calls SQS: each message's story
(send, deliveries, visibility changes, redeliveries, a dead-letter queue move, the final delete or a
Lambda outcome) is drawn at random from the profile, then written out as spans.

``--rate`` is messages per second. Each message is 2 to 12 spans depending on the profile; the result
reports spans per second as well. Without ``--sdk`` the generator builds the OTLP protobuf itself and
POSTs it from ``--concurrency`` threads per process, open loop: each export is due at a fixed time, and
if the server is slower than the rate, exports fall behind (``behind_s``) instead of the rate silently
dropping. With ``--sdk`` the spans go through ``BatchSpanProcessor`` and ``OTLPSpanExporter`` with their
default settings, as in an application; the result then counts the spans the SDK dropped (the server's
``/api/health`` counters before and after, so the URL must be a Trailbeam server for that count).

The same message stories feed the benchmark harness's seeding (``records``), so a seeded database and
live traffic have the same shape.
"""
from __future__ import annotations

import argparse
import http.client
import json
import math
import multiprocessing
import os
import random
import sys
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field, fields
from typing import Iterator, Optional
from urllib.parse import urlsplit

from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, KeyValue
from opentelemetry.proto.trace.v1.trace_pb2 import Span as PbSpan

SCHEMA_VERSION = 3
SCOPE = "trailbeam"
SCOPE_VERSION = "0.1.0.dev0"
KEY_ID = "b5e633f7"  # the published default key's ID (contract section 7)
ACCOUNT = "123456789012"
REGION = "us-east-1"
BODY_SIZES = {"none": 0, "0": 0, "1k": 1024, "64k": 64 * 1024, "256k": 256 * 1024}
NS = 1_000_000_000
MS = 1_000_000

SPAN_NAME = {"sqs.send": "trailbeam send", "sqs.receive": "trailbeam receive", "sqs.outcome": "trailbeam outcome",
             "delete": "trailbeam delete", "change_visibility": "trailbeam change_visibility"}


@dataclass
class Profile:
    """What the traffic looks like. Rates are per message unless they say otherwise."""
    queues: int = 10                 # 1 to 500; each has a dead-letter queue <queue>-dlq
    tenants: int = 20                # 0 = no trailbeam.tenant attribute; Zipf-like spread over the tenants
    services: int = 8                # producer/consumer processes (resource service.name, client IDs)
    body_size: int = 0               # bytes of trailbeam.body on send and receive spans; 0 = no body captured
    body_queues: int = 0             # bodies only on the first N queues (the ones a package captures); 0 = all
    redelivery: float = 0.05         # a delivery's visibility timeout expires and SQS delivers it again
    visibility_change: float = 0.05  # a delivery is released early with ChangeMessageVisibility 0
    dlq: float = 0.01                # the message fails max_receive_count deliveries and moves to the DLQ
    max_receive_count: int = 3
    stuck: float = 0.01              # never deleted (half: never received, half: received and abandoned)
    lambda_consumer: float = 0.2     # consumed by a Lambda (receive without a start, an outcome, no delete)
    late_join: float = 0.0           # the settle spans of a delivery are exported before its receive
    late_by_batches: int = 2         # how many export batches later those receives arrive
    batch_spans: int = 512           # spans per export: BatchSpanProcessor's max_export_batch_size

    def mean_spans_per_message(self, samples: int = 20000) -> float:
        f = MessageFactory(self, seed=12345)
        return sum(len(f.message(0)) for _ in range(samples)) / samples


@dataclass
class GenSpan:
    service: int
    trace_id: bytes
    span_id: bytes
    start_ns: int
    end_ns: int
    attrs: dict
    kind: str          # the record kind: send, receive, delete, change_visibility, outcome
    late: bool = False  # a receive held back so its settle spans arrive first


def queue_names(profile: Profile) -> list[str]:
    return [f"bench-q{i:03d}" for i in range(max(1, profile.queues))]


def body_queue_names(profile: Profile) -> list[str]:
    """The queues whose messages carry a body: give these to the server's TRAILBEAM_STORE_BODIES."""
    names = queue_names(profile)
    return names[:profile.body_queues] if profile.body_queues else names


def _arn(queue: str) -> str:
    return f"arn:aws:sqs:{REGION}:{ACCOUNT}:{queue}"


def _url(queue: str) -> str:
    return f"https://sqs.{REGION}.amazonaws.com/{ACCOUNT}/{queue}"


class MessageFactory:
    """Draws message stories from a profile and writes them out as Trailbeam spans."""

    def __init__(self, profile: Profile, seed: int = 0, worker: int = 0):
        self.p = profile
        self.rng = random.Random(seed)
        self.worker = worker
        self.queues = queue_names(profile)
        self.tenants = [f"tenant-{i:03d}" for i in range(profile.tenants)]
        # Zipf-like weights: a few big tenants and a long tail, as on a real multi-tenant queue
        self.tenant_weights = [1 / (i + 1) for i in range(profile.tenants)]
        self.client_ids = [f"bench-w{worker}-c{i}" for i in range(max(1, profile.services))]
        self.order_scopes = [f"bench-w{worker}-scope{i}" for i in range(max(1, profile.services))]
        self.ticks = [0] * max(1, profile.services)
        self.seq = 0

    # ---- ids --------------------------------------------------------------------------------

    def _hex(self, nbytes: int) -> str:
        return self.rng.getrandbits(nbytes * 8).to_bytes(nbytes, "big").hex()

    def _bytes(self, n: int) -> bytes:
        return self.rng.getrandbits(n * 8).to_bytes(n, "big")

    def _tick(self, svc: int) -> int:
        self.ticks[svc] += 1
        return self.ticks[svc]

    def body(self, tenant: Optional[str], n: int) -> str:
        status = "failed" if self.rng.random() < 0.02 else "paid"
        doc = {"order": {"id": f"o-{self.worker}-{self.seq}", "sku": f"SKU-{self.rng.randrange(1000)}",
                         "qty": self.rng.randrange(1, 9), "status": status},
               "customer": {"id": tenant or "anon", "email": "[REDACTED]"}, "pad": ""}
        base = len(json.dumps(doc, separators=(",", ":")))
        doc["pad"] = "x" * max(0, n - base)
        return json.dumps(doc, separators=(",", ":"))

    # ---- one message ------------------------------------------------------------------------

    def _base(self, kind: str, op_type: str, op: str, queue: str, svc: int, source: str = "client") -> dict:
        return {"trailbeam.schema_version": SCHEMA_VERSION, "trailbeam.kind": kind, "trailbeam.source": source,
                "messaging.system.name": "aws.sqs", "messaging.operation.type": op_type,
                "messaging.operation.name": op, "messaging.client.id": self.client_ids[svc],
                "process.pid": 1000 + svc, "thread.name": "MainThread",
                "messaging.destination.name": queue, "aws.sqs.queue.url": _url(queue)}

    def _order(self, a: dict, svc: int, issued: bool = True) -> None:
        a["trailbeam.order.scope"] = self.order_scopes[svc]
        if issued:
            a["trailbeam.order.issued"] = self._tick(svc)
        a["trailbeam.order.returned"] = self._tick(svc)

    def message(self, sent_ns: int) -> list[GenSpan]:
        """Every span of one message whose send ended at ``sent_ns``, in the order they happen."""
        p, rng = self.p, self.rng
        self.seq += 1
        queue = rng.choice(self.queues)
        tenant = rng.choices(self.tenants, self.tenant_weights)[0] if self.tenants else None
        mid = f"{self._hex(4)}-{self._hex(2)}-{self._hex(2)}-{self._hex(2)}-{self._hex(6)}"
        trace = self._bytes(16)
        captured = p.body_size and (not p.body_queues or self.queues.index(queue) < p.body_queues)
        body = self.body(tenant, p.body_size) if captured else None
        body_len = len(body) if body else 200 + rng.randrange(800)
        producer = rng.randrange(len(self.client_ids))
        spans: list[GenSpan] = []

        def content(a: dict) -> None:
            a["messaging.message.body.size"] = body_len
            if tenant:
                a["trailbeam.tenant"] = tenant
                a["trailbeam.tenant.source"] = "message_attribute"
            if body is not None:
                a.update({"trailbeam.body.capture_status": "captured", "trailbeam.body": body,
                          "trailbeam.body.format": "json", "trailbeam.body.redacted_count": 1})
            else:
                a["trailbeam.body.capture_status"] = "off"

        a = self._base("sqs.send", "send", "SendMessage", queue, producer)
        self._order(a, producer)
        a["messaging.message.id"] = mid
        content(a)
        spans.append(GenSpan(producer, trace, self._bytes(8), sent_ns - 8 * MS, sent_ns, a, "send"))

        r = rng.random()
        if r < p.stuck / 2:
            return spans  # never received
        abandoned = r < p.stuck
        to_dlq = not abandoned and rng.random() < p.dlq
        use_lambda = rng.random() < p.lambda_consumer
        consumer = rng.randrange(len(self.client_ids))
        t = sent_ns + int(rng.expovariate(1 / 0.05) * NS)  # time in queue, mean 50 ms
        first_receive_ms = None
        count = 0
        cur_queue = queue

        def delivery(final: bool, release: bool, dlq_move: bool) -> None:
            nonlocal t, count, first_receive_ms, cur_queue
            count += 1
            if dlq_move:
                cur_queue = queue + "-dlq"
            start = t
            t += int((rng.random() * 0.02 + 0.002) * NS)
            first_receive_ms = first_receive_ms or t // MS
            ra = self._base("sqs.receive", "receive", "ReceiveMessage", cur_queue, consumer,
                            "lambda" if use_lambda else "client")
            ra.update({"messaging.message.id": mid, "trailbeam.receive_count": count,
                       "trailbeam.sent_timestamp_ms": sent_ns // MS,
                       "trailbeam.first_receive_timestamp_ms": first_receive_ms})
            ra["trailbeam.receipt_handle.hmac"] = self._hex(32)  # a new handle per delivery, as SQS issues
            ra["trailbeam.receipt_handle.key_id"] = KEY_ID
            if use_lambda:  # the Lambda service made the ReceiveMessage call: no start, no issued tick
                ra["trailbeam.start_observed"] = False
                ra.update({"faas.name": f"bench-fn-{cur_queue}", "faas.invocation_id": self._hex(16)})
                self._order(ra, consumer, issued=False)
            else:
                self._order(ra, consumer)
            if dlq_move:
                ra["trailbeam.dead_letter_queue_source_arn"] = _arn(queue)
            content(ra)
            late = rng.random() < p.late_join
            spans.append(GenSpan(consumer, trace, self._bytes(8), start if not use_lambda else t, t, ra, "receive",
                                 late=late))
            t += int((rng.random() * 0.05 + 0.005) * NS)  # processing
            if use_lambda:
                oa = self._base("sqs.outcome", "process", "process", cur_queue, consumer, "lambda")
                oa.update({"messaging.message.id": mid, "faas.name": ra["faas.name"],
                           "faas.invocation_id": ra["faas.invocation_id"],
                           "trailbeam.outcome": "success" if final else "failed_exception"})
                if not final:
                    oa["trailbeam.error_type"] = "ValueError"
                tk = self._tick(consumer)
                oa.update({"trailbeam.order.scope": self.order_scopes[consumer], "trailbeam.order.issued": tk,
                           "trailbeam.order.returned": tk})
                spans.append(GenSpan(consumer, trace, self._bytes(8), t, t, oa, "outcome"))
                t += 30 * NS if not final else 0
                return

            def settle(kind: str, op: str, extra: dict) -> None:
                nonlocal t
                sa = self._base("sqs.settle", "settle", op, cur_queue, consumer)
                sa.update({"trailbeam.receipt_handle.hmac": ra["trailbeam.receipt_handle.hmac"],
                           "trailbeam.receipt_handle.key_id": KEY_ID, "trailbeam.result": "ok", **extra})
                if not late:  # the process that received it knows the message ID; a late join is another process
                    sa["messaging.message.id"] = mid
                self._order(sa, consumer)
                s = t
                t += int((rng.random() * 0.01 + 0.001) * NS)
                spans.append(GenSpan(consumer, trace, self._bytes(8), s, t, sa, kind))

            if release:
                settle("change_visibility", "ChangeMessageVisibility", {"trailbeam.visibility_timeout": 0})
                t += int(rng.random() * NS)
            elif final:
                settle("delete", "DeleteMessage", {})
            else:
                t += 30 * NS  # the visibility timeout expires

        if to_dlq:
            for _ in range(p.max_receive_count):
                delivery(final=False, release=False, dlq_move=False)
            delivery(final=True, release=False, dlq_move=True)
            return spans
        while True:
            again = rng.random()
            if again < p.visibility_change and count < 5:
                delivery(final=False, release=True, dlq_move=False)
            elif again < p.visibility_change + p.redelivery and count < 5:
                delivery(final=False, release=False, dlq_move=False)
            else:
                delivery(final=not abandoned, release=False, dlq_move=False)  # abandoned: no delete
                return spans


# ---- streams of export batches ------------------------------------------------------------

class Stream:
    """Export batches of ``profile.batch_spans`` spans, as a BatchSpanProcessor would make them.

    Late-join receives are held back ``late_by_batches`` batches, so their settle spans are stored first.
    ``clock`` gives each message's send time; the default puts the whole message in the recent past, so
    live traffic never has spans from the future.
    """

    def __init__(self, profile: Profile, seed: int = 0, worker: int = 0, clock=None):
        self.p = profile
        self.factory = MessageFactory(profile, seed, worker)
        self.clock = clock or (lambda: time.time_ns() - 2 * NS)
        self.held: deque = deque()  # (release at batch number, span)
        self.pending: list[GenSpan] = []
        self.batches = 0
        self.messages = 0

    def batch(self) -> list[GenSpan]:
        out: list[GenSpan] = []
        while self.held and self.held[0][0] <= self.batches:
            out.append(self.held.popleft()[1])
        while len(out) < self.p.batch_spans:
            if not self.pending:
                self.pending = self.factory.message(self.clock())
                self.messages += 1
            s = self.pending.pop(0)
            if s.late:
                self.held.append((self.batches + max(1, self.p.late_by_batches), s))
            else:
                out.append(s)
        self.batches += 1
        return out

    def batches_of_messages(self, n_messages: int) -> Iterator[list[GenSpan]]:
        """Exactly ``n_messages`` messages' spans, held receives included, in batches."""
        target = self.messages + n_messages
        while self.messages < target or self.pending or self.held:
            out: list[GenSpan] = []
            while self.held and self.held[0][0] <= self.batches:
                out.append(self.held.popleft()[1])
            while len(out) < self.p.batch_spans and (self.pending or self.messages < target):
                if not self.pending:
                    self.pending = self.factory.message(self.clock())
                    self.messages += 1
                s = self.pending.pop(0)
                if s.late:
                    self.held.append((self.batches + max(1, self.p.late_by_batches), s))
                else:
                    out.append(s)
            if not self.pending and self.messages >= target:
                # nothing left to make: release what is held, in order
                while self.held and len(out) < self.p.batch_spans:
                    out.append(self.held.popleft()[1])
            self.batches += 1
            if out:
                yield out


def _any(v) -> AnyValue:
    if isinstance(v, bool):
        return AnyValue(bool_value=v)
    if isinstance(v, int):
        return AnyValue(int_value=v)
    if isinstance(v, float):
        return AnyValue(double_value=v)
    return AnyValue(string_value=str(v))


def encode(batch: list[GenSpan], worker: int = 0) -> bytes:
    """One OTLP/HTTP protobuf request body: a resource per service, scope ``trailbeam``."""
    req = ExportTraceServiceRequest()
    by_service: dict[int, list[GenSpan]] = {}
    for s in batch:
        by_service.setdefault(s.service, []).append(s)
    for svc, spans in by_service.items():
        rs = req.resource_spans.add()
        rs.resource.attributes.extend([
            KeyValue(key="service.name", value=AnyValue(string_value=f"bench-svc-{svc}")),
            KeyValue(key="service.instance.id", value=AnyValue(string_value=f"bench-w{worker}-c{svc}"))])
        ss = rs.scope_spans.add()
        ss.scope.name = SCOPE
        ss.scope.version = SCOPE_VERSION
        for s in spans:
            ps = ss.spans.add()
            ps.trace_id = s.trace_id
            ps.span_id = s.span_id
            ps.name = SPAN_NAME.get(s.kind, SPAN_NAME.get(s.attrs["trailbeam.kind"]))
            ps.kind = PbSpan.SPAN_KIND_INTERNAL
            ps.start_time_unix_nano = s.start_ns
            ps.end_time_unix_nano = s.end_ns
            ps.attributes.extend([KeyValue(key=k, value=_any(v)) for k, v in s.attrs.items()])
    return req.SerializeToString()


def records(batch: list[GenSpan]) -> list[dict]:
    """The batch as the server's records (``otlp.to_record``), for seeding a database without HTTP."""
    from trailbeam.server.otlp import to_record

    out = []
    for s in batch:
        rec, reason = to_record({"service.name": f"bench-svc-{s.service}"}, s.trace_id.hex(), s.span_id.hex(),
                                s.start_ns, s.end_ns, s.attrs)
        if rec is None:
            raise ValueError(f"generated span rejected: {reason}")
        out.append(rec)
    return out


# ---- sending ------------------------------------------------------------------------------

def percentile(values: list, q: float) -> Optional[float]:
    if not values:
        return None
    s = sorted(values)
    return s[max(0, math.ceil(q * len(s)) - 1)]


class Sender:
    """One keep-alive HTTP connection to the OTLP URL."""

    def __init__(self, url: str, timeout: float):
        u = urlsplit(url)
        self.https = u.scheme == "https"
        self.host, self.port = u.hostname, u.port or (443 if self.https else 80)
        self.path = u.path or "/v1/traces"
        self.timeout = timeout
        self.headers = {"Content-Type": "application/x-protobuf"}
        for h in filter(None, os.environ.get("LOADGEN_HEADERS", "").split(",")):
            k, _, v = h.partition("=")
            self.headers[k.strip()] = v.strip()
        self.conn = None

    def post(self, body: bytes) -> int:
        for attempt in (0, 1):
            if self.conn is None:
                cls = http.client.HTTPSConnection if self.https else http.client.HTTPConnection
                self.conn = cls(self.host, self.port, timeout=self.timeout)
            try:
                self.conn.request("POST", self.path, body, {**self.headers, "Content-Length": str(len(body))})
                r = self.conn.getresponse()
                r.read()
                if r.getheader("Connection", "").lower() == "close":
                    self.conn.close()
                    self.conn = None
                return r.status
            except (ConnectionError, http.client.HTTPException) as e:
                self.conn.close()
                self.conn = None
                if attempt or isinstance(e, TimeoutError):
                    raise
        return 0


def _worker(args: dict, out) -> None:
    """One generator process: makes and sends its share of the rate, then reports raw samples."""
    p = Profile(**args["profile"])
    worker, rate, duration = args["worker"], args["rate"], args["duration"]
    stream = Stream(p, seed=args["seed"] * 1000 + worker, worker=worker)
    spm = args["spans_per_message"]
    interval = p.batch_spans / max(rate * spm, 1e-9)  # seconds between exports for this process
    todo: deque = deque()
    lock = threading.Condition()
    lat, statuses, behind, sent_spans, sent_bytes = [], {}, [], [0], [0]
    stop = threading.Event()
    t0 = time.monotonic() + 0.2
    n_exports = max(1, int(duration / interval))

    def produce():
        for i in range(n_exports):
            due = t0 + i * interval
            batch = stream.batch()
            body = encode(batch, worker)
            with lock:
                while len(todo) > 64 and not stop.is_set():  # do not build up gigabytes if the server stalls
                    lock.wait(0.1)
                todo.append((due, body, len(batch)))
                lock.notify()
        with lock:
            todo.append(None)
            lock.notify_all()

    def send():
        s = Sender(args["url"], args["timeout"])
        while True:
            with lock:
                while not todo:
                    lock.wait()
                item = todo[0] if todo[0] is None else todo.popleft()
                lock.notify_all()
            if item is None or stop.is_set():
                return
            due, body, n_spans = item
            wait = due - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            started = time.monotonic()
            try:
                status = s.post(body)
            except Exception as e:  # timeouts, resets: an error, like an exporter would see
                status = type(e).__name__
            took = time.monotonic() - started
            with lock:
                lat.append(took)
                behind.append(max(0.0, started - due))
                statuses[str(status)] = statuses.get(str(status), 0) + 1
                if status == 200:
                    sent_spans[0] += n_spans
                    sent_bytes[0] += len(body)
            if started - t0 > duration + args["grace"]:
                stop.set()

    threads = [threading.Thread(target=produce, daemon=True)] + [
        threading.Thread(target=send, daemon=True) for _ in range(args["concurrency"])]
    for th in threads:
        th.start()
    for th in threads[1:]:
        th.join()
    # The offered load covers n_exports intervals: the last export is due one interval before the window
    # ends, so measuring to the last completion alone would overstate the rate by an interval.
    out.put({"latencies": lat, "statuses": statuses, "behind": behind, "spans_ok": sent_spans[0],
             "bytes_ok": sent_bytes[0], "exports": len(lat),
             "elapsed": max(time.monotonic() - t0, n_exports * interval),
             "messages": stream.messages})


def health(url: str) -> Optional[dict]:
    """The Trailbeam server's /api/health next to an OTLP URL, or None if it is not a Trailbeam server."""
    u = urlsplit(url)
    try:
        c = http.client.HTTPConnection(u.hostname, u.port or 80, timeout=600)  # counts the whole table
        c.request("GET", "/api/health")
        r = c.getresponse()
        return json.loads(r.read()) if r.status == 200 else None
    except Exception:
        return None


def run(url: str, profile: Profile, rate: float, duration: float, *, concurrency: int = 4, processes: int = 1,
        timeout: float = 10.0, seed: int = 1, grace: float = 30.0, schedule_delay: float = 5.0) -> dict:
    """Send ``rate`` messages/s for ``duration`` s. Returns achieved rates and export latency percentiles.

    Like BatchSpanProcessor, an export is sent when it has ``batch_spans`` spans or every
    ``schedule_delay`` seconds, whichever comes first: at low rates the batches are smaller."""
    spm = profile.mean_spans_per_message()
    per_process = rate * spm / processes
    profile = Profile(**{**asdict(profile),
                         "batch_spans": max(1, min(profile.batch_spans, round(per_process * schedule_delay)))})
    ctx = multiprocessing.get_context("spawn")
    q = ctx.Queue()
    procs = [ctx.Process(target=_worker, args=({"profile": asdict(profile), "worker": w, "rate": rate / processes,
                                                "duration": duration, "url": url, "timeout": timeout, "seed": seed,
                                                "concurrency": concurrency, "spans_per_message": spm,
                                                "grace": grace}, q), daemon=True)
             for w in range(processes)]
    for pr in procs:
        pr.start()
    parts = [q.get() for _ in procs]
    for pr in procs:
        pr.join()
    lat = [x for p in parts for x in p["latencies"]]
    behind = [x for p in parts for x in p["behind"]]
    statuses: dict = {}
    for p in parts:
        for k, v in p["statuses"].items():
            statuses[k] = statuses.get(k, 0) + v
    elapsed = max(p["elapsed"] for p in parts)
    spans_ok = sum(p["spans_ok"] for p in parts)
    exports = sum(p["exports"] for p in parts)
    return {
        "target_msgs_per_s": rate, "target_spans_per_s": round(rate * spm, 1), "spans_per_message": round(spm, 3),
        "duration_s": duration, "elapsed_s": round(elapsed, 2),
        "achieved_spans_per_s": round(spans_ok / elapsed, 1) if elapsed else 0,
        "achieved_msgs_per_s": round(spans_ok / elapsed / spm, 1) if elapsed else 0,
        "exports": exports, "errors": exports - statuses.get("200", 0), "statuses": statuses,
        "export_p50_ms": _ms(percentile(lat, 0.50)), "export_p99_ms": _ms(percentile(lat, 0.99)),
        "export_max_ms": _ms(max(lat) if lat else None),
        "behind_p99_s": round(percentile(behind, 0.99) or 0, 3), "behind_max_s": round(max(behind or [0]), 3),
        "mb_per_s": round(sum(p["bytes_ok"] for p in parts) / elapsed / 1e6, 2) if elapsed else 0,
        "batch_spans": profile.batch_spans, "profile": asdict(profile), "concurrency": concurrency,
        "processes": processes,
    }


def _ms(s: Optional[float]) -> Optional[float]:
    return None if s is None else round(s * 1000, 1)


# ---- through the OpenTelemetry SDK --------------------------------------------------------

def run_sdk(url: str, profile: Profile, rate: float, duration: float, *, seed: int = 1, settle_s: float = 60.0,
            max_queue_size: Optional[int] = None) -> dict:
    """Emit the spans through a TracerProvider with a default BatchSpanProcessor and OTLPSpanExporter, as
    the packages do (trailbeam._emitter), and count what the server received. Spans lost in the SDK (its
    queue was full, or an export failed after its retries) are the difference."""
    from opentelemetry import trace as otrace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.trace.sampling import ALWAYS_ON

    import logging

    class Count(logging.Handler):
        def __init__(self):
            super().__init__(logging.WARNING)
            self.queue_full = self.export_failed = 0

        def emit(self, record):
            m = record.getMessage()
            if "Queue full" in m:
                self.queue_full += 1  # one warning per span the BatchSpanProcessor dropped
            elif "export" in m.lower() and ("fail" in m.lower() or "timeout" in m.lower()):
                self.export_failed += 1

    counter = Count()
    otel_log = logging.getLogger("opentelemetry")
    otel_log.addHandler(counter)
    before = health(url)
    if before is None:
        raise SystemExit("--sdk needs a Trailbeam server at the URL (it reads /api/health to count received spans)")
    provider = TracerProvider(resource=Resource.create({"service.name": "bench-sdk"}), sampler=ALWAYS_ON)
    kw = {"max_queue_size": max_queue_size} if max_queue_size else {}
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=url), **kw))
    tracer = provider.get_tracer(SCOPE, SCOPE_VERSION)
    stream = Stream(profile, seed=seed)
    spm = profile.mean_spans_per_message()
    emitted = 0
    t0 = time.monotonic()
    span_rate = rate * spm
    while (elapsed := time.monotonic() - t0) < duration:
        due = emitted / span_rate
        if due > elapsed:
            time.sleep(min(due - elapsed, 0.05))
            continue
        for s in stream.batch()[:64]:  # small steps keep the rate smooth
            sp = tracer.start_span(SPAN_NAME.get(s.kind), kind=otrace.SpanKind.INTERNAL, attributes=s.attrs,
                                   start_time=s.start_ns)
            sp.end(end_time=s.end_ns)
            emitted += 1
    emit_elapsed = time.monotonic() - t0
    flushed = provider.force_flush(int(settle_s * 1000))
    provider.shutdown()
    otel_log.removeHandler(counter)
    after = health(url)
    received = (after["ingest"]["inserted"] + after["ingest"]["duplicates"]) - (
        before["ingest"]["inserted"] + before["ingest"]["duplicates"])
    return {"mode": "sdk", "target_msgs_per_s": rate, "target_spans_per_s": round(span_rate, 1),
            "emitted_spans": emitted, "emit_spans_per_s": round(emitted / emit_elapsed, 1),
            "received_spans": received, "dropped_spans": emitted - received,
            "dropped_pct": round(100 * (emitted - received) / emitted, 2) if emitted else 0,
            "dropped_queue_full": counter.queue_full, "failed_export_warnings": counter.export_failed,
            "flushed_in_time": flushed, "max_queue_size": max_queue_size or 2048, "profile": asdict(profile)}


# ---- command line -------------------------------------------------------------------------

def body_size(text: str) -> int:
    t = text.strip().lower()
    if t in BODY_SIZES:
        return BODY_SIZES[t]
    if t.endswith("k") and t[:-1].isdigit():
        return int(t[:-1]) * 1024
    if t.isdigit():
        return int(t)
    raise argparse.ArgumentTypeError("none, 1k, 64k, 256k or a number of bytes")


def add_profile_args(p: argparse.ArgumentParser) -> None:
    d = Profile()
    p.add_argument("--queues", type=int, default=d.queues, help="number of queues, 1 to 500")
    p.add_argument("--tenants", type=int, default=d.tenants, help="number of tenants; 0 = no tenant attribute")
    p.add_argument("--services", type=int, default=d.services, help="producer/consumer processes per generator")
    p.add_argument("--body-size", type=body_size, default=0, help="none (default), 1k, 64k, 256k: body captured on "
                   "send and receive spans; the server keeps it only for queues in its TRAILBEAM_STORE_BODIES")
    for f in ("redelivery", "visibility_change", "dlq", "stuck", "lambda_consumer", "late_join"):
        p.add_argument("--" + f.replace("_", "-"), type=float, default=getattr(d, f),
                       help=f"fraction (default {getattr(d, f)})")
    p.add_argument("--body-queues", type=int, default=0, help="bodies only on the first N queues (default: all)")
    p.add_argument("--max-receive-count", type=int, default=d.max_receive_count)
    p.add_argument("--late-by-batches", type=int, default=d.late_by_batches)
    p.add_argument("--batch-spans", type=int, default=d.batch_spans, help="spans per export (default 512)")


def profile_from(a: argparse.Namespace) -> Profile:
    names = {f.name for f in fields(Profile)}
    prof = Profile(**{k: v for k, v in vars(a).items() if k in names})
    if not 1 <= prof.queues <= 500:
        raise SystemExit("--queues must be from 1 to 500")
    return prof


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--url", default="http://127.0.0.1:4318/v1/traces", help="OTLP/HTTP traces URL")
    p.add_argument("--rate", type=float, default=100, help="messages per second")
    p.add_argument("--duration", type=float, default=30, help="seconds")
    p.add_argument("--concurrency", type=int, default=4, help="concurrent exports per process")
    p.add_argument("--processes", type=int, default=1, help="generator processes")
    p.add_argument("--timeout", type=float, default=10, help="export timeout, seconds (the OTLP exporter's default)")
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--sdk", action="store_true", help="send through the OTel SDK and count dropped spans")
    p.add_argument("--sdk-max-queue-size", type=int, default=None)
    add_profile_args(p)
    a = p.parse_args(argv)
    prof = profile_from(a)
    if a.sdk:
        res = run_sdk(a.url, prof, a.rate, a.duration, seed=a.seed, max_queue_size=a.sdk_max_queue_size)
    else:
        res = run(a.url, prof, a.rate, a.duration, concurrency=a.concurrency, processes=a.processes,
                  timeout=a.timeout, seed=a.seed)
    json.dump(res, sys.stdout, indent=2)
    print()


if __name__ == "__main__":
    main()
