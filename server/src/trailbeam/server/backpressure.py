"""Ingest backpressure: answer an export Trailbeam cannot take now with 429 or 503 and ``Retry-After``,
instead of letting it hang until the exporter's timeout (QV-68).

Two limits, both checked before a span is written, so a rejected request has stored nothing and an
accepted one (200) has been committed. Trailbeam never answers 200 for a span it then drops.

- **Queue full: 429.** At most ``max_pending`` exports are in progress at once (decoding, waiting for the
  writer, or writing). One more is answered straight away, without being decoded.
- **Writer behind: 503.** An admitted export waits at most ``max_wait_s`` for the store's writer (SQLite:
  its single writer; Postgres: one of its pooled ingest connections). If the exports ahead of it take longer
  than that, it is answered without being written.

A third case is not overload: **store unreachable: 503** with ``Retry-After: 1`` (QV-81), when the store's
database can't be reached (``StoreUnavailable``), nothing written. One second, not the backlog estimate: the
exporter, or a load balancer sending it to another replica, should try again soon.

``Retry-After`` is the time the exports already waiting should take to write, from the recent write time,
from 1 to ``RETRY_AFTER_MAX_S`` whole seconds. OTLP/HTTP exporters retry 429 and 503
(https://opentelemetry.io/docs/specs/otlp/#otlphttp-throttling) and wait ``Retry-After`` when it is
given. The Python exporter (opentelemetry-exporter-otlp-proto-http 1.45) gives up when ``Retry-After``
is longer than what is left of its 10 s export timeout, so the value stays short: a few retries fit in
one timeout.
"""
from __future__ import annotations

import contextlib
import math
import threading
from typing import Iterator

DEFAULT_MAX_PENDING = 16
DEFAULT_MAX_WAIT_S = 2.0
RETRY_AFTER_MAX_S = 3
STORE_UNREACHABLE_RETRY_AFTER_S = 1
STORE_FULL_RETRY_AFTER_S = 3  # each try fails fast, but nothing passes until someone frees space
STORE_TIMEOUT_RETRY_AFTER_S = 3  # each try already held a connection for the whole timeout: give the database room
_EWMA = 0.2  # weight of the newest write time in the estimate


class Rejected(Exception):
    """An export Trailbeam did not take. ``status`` is 429 or 503; nothing in it was written."""

    def __init__(self, status: int, reason: str, retry_after_s: int, message: str):
        super().__init__(message)
        self.status = status
        self.reason = reason  # "queue_full" | "writer_behind" | "store_unreachable" | "store_full" | "store_timeout"
        self.retry_after_s = retry_after_s
        self.message = message


class IngestGate:
    def __init__(self, max_pending: int = DEFAULT_MAX_PENDING, max_wait_s: float = DEFAULT_MAX_WAIT_S):
        if max_pending < 1:
            raise ValueError("max_pending must be at least 1")
        if max_wait_s <= 0:
            raise ValueError("max_wait_s must be positive")
        self.max_pending = max_pending
        self.max_wait_s = max_wait_s
        self._lock = threading.Lock()
        self.pending = 0
        self._write_s = 0.0  # moving average of one export's write transaction, seconds

    @contextlib.contextmanager
    def admit(self) -> Iterator[None]:
        """Hold one of the ``max_pending`` places for the duration of the block; raise 429 if none is free."""
        with self._lock:
            if self.pending >= self.max_pending:
                ra = self._retry_after_locked()
                raise Rejected(429, "queue_full", ra,
                               f"Trailbeam is busy: {self.pending} exports in progress, the most it takes at "
                               f"once (TRAILBEAM_INGEST_MAX_PENDING). Retry after {ra} s.")
            self.pending += 1
        try:
            yield
        finally:
            with self._lock:
                self.pending -= 1

    def writer_behind(self, waited_s: float) -> Rejected:
        with self._lock:
            ra = self._retry_after_locked()
        return Rejected(503, "writer_behind", ra,
                        f"Trailbeam is behind: the exports ahead of this one did not finish writing in "
                        f"{waited_s:g} s (TRAILBEAM_INGEST_MAX_WAIT_S). Nothing in it was stored. "
                        f"Retry after {ra} s.")

    def store_unreachable(self) -> Rejected:
        return Rejected(503, "store_unreachable", STORE_UNREACHABLE_RETRY_AFTER_S,
                        "Trailbeam can't reach its store (the database is down or unreachable). Nothing in this "
                        f"export was stored. Retry after {STORE_UNREACHABLE_RETRY_AFTER_S} s.")

    def store_full(self) -> Rejected:
        return Rejected(503, "store_full", STORE_FULL_RETRY_AFTER_S,
                        "Trailbeam's store is out of disk space. Nothing in this export was stored. "
                        f"Retry after {STORE_FULL_RETRY_AFTER_S} s.")

    def store_timeout(self) -> Rejected:
        return Rejected(503, "store_timeout", STORE_TIMEOUT_RETRY_AFTER_S,
                        "Trailbeam's store took too long to write this export (a database statement timed out). "
                        f"Nothing in it was stored. Retry after {STORE_TIMEOUT_RETRY_AFTER_S} s.")

    def wrote(self, seconds: float) -> None:
        """Record how long one export's write transaction took."""
        with self._lock:
            self._write_s = seconds if self._write_s == 0 else (1 - _EWMA) * self._write_s + _EWMA * seconds

    def retry_after(self) -> int:
        with self._lock:
            return self._retry_after_locked()

    def _retry_after_locked(self) -> int:
        backlog = self._write_s * max(self.pending, 1)
        return int(min(max(math.ceil(backlog), 1), RETRY_AFTER_MAX_S))


def status_body(code: int, message: str, json_mode: bool) -> tuple[str, bytes]:
    """The OTLP error body (https://opentelemetry.io/docs/specs/otlp/#otlphttp-response, "Failures"): a
    ``google.rpc.Status`` in the request's encoding. Encoded by hand (fields 1 code, 2 message) so the server needs no
    googleapis-common-protos."""
    if json_mode:
        import json
        return "application/json", json.dumps({"code": code, "message": message}).encode()
    msg = message.encode()
    return "application/x-protobuf", b"\x08" + _varint(code) + b"\x12" + _varint(len(msg)) + msg


def _varint(n: int) -> bytes:
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


GRPC_RESOURCE_EXHAUSTED = 8  # 429
GRPC_UNAVAILABLE = 14  # 503
