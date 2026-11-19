"""Retention: how long spans are kept, and the background purge that deletes the rest.

Configuration (``trailbeam --retention``, ``--retention-queues``; README "Retention"):

- a global retention, ``7d`` by default: a duration (``90m``, ``36h``, ``7d``, ``3600s``) or ``forever``;
- per-queue retentions that override it: ``orders-dlq=30d,audit=forever``.

What is deleted (Store.purge_step): whole messages, once the newest span of the message is older than the
retention of every queue it has a span on. A message is never cut in half: one whose spans straddle the cutoff
keeps all of them, the old ones too, until its newest span expires; a message that moved to a dead-letter
queue kept longer is kept as long as that queue keeps it. Settle spans that no receive has claimed belong to
no message and go by their own time and queue. Ages are span time (when the SQS call ended, on the clock of
the process that made it) against the server's clock.

The purge (``Purger``) runs a pass at start and then every ``interval_s`` after the previous pass ended. A pass
is many small steps, each a short write transaction with a pause after it, so ingest and pages wait at most one
step and never the whole pass. The pause is at least three times the step (``duty``, 0.25): the purge holds the
writer lock at most a quarter of the time. Deleting costs about what inserting does (QV-72: about 0.9 ms per
message at 1M spans, nearly all of it the commit writing index pages), so a purge without that limit took two
thirds of the writer and ingest at 1,000 spans/s fell behind until exports timed out. After a pass it gives the
freed space back to the file system in small pieces too (``Store.reclaim_space``; only on files with
incremental vacuum, see db.py).
"""
from __future__ import annotations

import logging
import re
import threading
import time
from typing import Callable, Mapping, NamedTuple, Optional

from .store import Cutoffs, Store

log = logging.getLogger("trailbeam.server.retention")

DEFAULT = "7d"
FOREVER = "forever"
_UNITS = {"s": 1, "m": 60, "h": 3_600, "d": 86_400}
_DURATION = re.compile(r"^\s*(\d+)\s*([smhd])\s*$")
MAX_SECONDS = 100 * 365 * 86_400  # beyond that, say forever


def parse_duration(text: str) -> Optional[int]:
    """``7d`` -> 604800 seconds; ``forever`` -> None. Raises ValueError on anything else, and on 0."""
    t = (text or "").strip().lower()
    if t == FOREVER:
        return None
    m = _DURATION.match(t)
    if not m:
        raise ValueError(f"expected a duration such as 7d, 36h, 90m or 3600s, or {FOREVER}; got {text!r}")
    seconds = int(m.group(1)) * _UNITS[m.group(2)]
    if not 0 < seconds <= MAX_SECONDS:
        raise ValueError(f"a retention must be more than 0 and at most 100 years (else {FOREVER}); got {text!r}")
    return seconds


def parse_queue_retentions(text: str) -> dict:
    """``orders-dlq=30d,audit=forever`` -> {"orders-dlq": 2592000, "audit": None}."""
    out: dict = {}
    for part in filter(None, (p.strip() for p in (text or "").split(","))):
        name, sep, value = part.rpartition("=")
        if not sep or not name.strip():
            raise ValueError(f"expected QUEUE=DURATION, e.g. orders-dlq=30d; got {part!r}")
        if name.strip() in out:
            raise ValueError(f"queue {name.strip()!r} is given twice")
        out[name.strip()] = parse_duration(value)
    return out


def describe(seconds: Optional[int]) -> str:
    if seconds is None:
        return FOREVER
    for unit in ("d", "h", "m"):
        if seconds % _UNITS[unit] == 0:
            return f"{seconds // _UNITS[unit]}{unit}"
    return f"{seconds}s"


class Retention(NamedTuple):
    """Seconds to keep spans: ``default`` for every queue, ``queues`` overriding it. None: forever."""
    default: Optional[int] = parse_duration(DEFAULT)
    queues: Mapping[str, Optional[int]] = {}

    @classmethod
    def parse(cls, default: str = DEFAULT, queues: str = "") -> "Retention":
        return cls(parse_duration(default), parse_queue_retentions(queues))

    def cutoffs(self, now_ns: int) -> Cutoffs:
        def cut(s: Optional[int]) -> Optional[int]:
            return None if s is None else now_ns - s * 1_000_000_000
        return Cutoffs(cut(self.default), {q: cut(s) for q, s in self.queues.items()})

    def keeps_everything(self) -> bool:
        return self.default is None and all(s is None for s in self.queues.values())

    def config(self) -> dict:
        """For /api/health."""
        return {"default": describe(self.default), "queues": {q: describe(s) for q, s in sorted(self.queues.items())}}


class Purger:
    """Runs retention passes on a store, in a background thread (``start``) or one at a time (``run_pass``).

    ``batch``: messages looked at per step. After each step it waits ``pause_s`` or, if longer, as long as it
    takes for the steps to fill at most ``duty`` of the time, so writes waiting for the lock go first.
    ``reclaim_pages``: pages given back per reclaim step (4 KiB each)."""

    def __init__(self, store: Store, retention: Retention, *, interval_s: float = 600, batch: int = 50,
                 pause_s: float = 0.05, duty: float = 0.25, reclaim_pages: int = 256,
                 now: Callable[[], int] = time.time_ns):
        if batch < 1:
            raise ValueError(f"batch must be at least 1, got {batch}")
        if not 0 < duty <= 1:
            raise ValueError(f"duty must be in (0, 1], got {duty}")
        self.store = store
        self.retention = retention
        self.interval_s = interval_s
        self.batch = batch
        self.pause_s = pause_s
        self.duty = duty
        self.reclaim_pages = reclaim_pages
        self.now = now
        self._halt = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._running: Optional[dict] = None
        self.last_pass: Optional[dict] = None
        self.passes = 0

    # ---- one pass ------------------------------------------------------------------------------

    def run_pass(self) -> dict:
        """Delete everything expired now, then give the space back. Returns what it did; stops early, with
        ``"stopped": True``, if ``stop`` is called."""
        t0, started_ms = time.monotonic(), time.time_ns() // 1_000_000
        cutoffs = self.retention.cutoffs(self.now())
        res = {"started_ms": started_ms, "deleted_spans": 0, "examined": 0, "steps": 0, "reclaimed_pages": 0,
               "longest_step_ms": 0.0, "longest_write_ms": 0.0,
               "longest_reclaim_step_ms": 0.0, "stopped": False}
        with self._lock:
            self._running = res
        try:
            if not cutoffs.keeps_everything():
                resume = None
                while True:
                    s0 = time.monotonic()
                    step = self.store.purge_step(cutoffs, resume, self.batch)
                    took = self._step_done(res, s0, "longest_step_ms")
                    res["deleted_spans"] += step.deleted
                    res["examined"] += step.examined
                    res["longest_write_ms"] = max(res["longest_write_ms"], round(step.write_ms, 1))
                    resume = step.resume
                    if resume is None:
                        break
                    if self._halt.wait(self.pause(took)):
                        res["stopped"] = True
                        break
                while not res["stopped"]:
                    s0 = time.monotonic()
                    n = self.store.reclaim_space(self.reclaim_pages)
                    took = self._step_done(res, s0, "longest_reclaim_step_ms")
                    res["reclaimed_pages"] += n
                    if n < self.reclaim_pages:
                        break
                    if self._halt.wait(self.pause(took)):
                        res["stopped"] = True
        finally:
            res["seconds"] = round(time.monotonic() - t0, 3)
            with self._lock:
                self._running = None
                self.last_pass = res
                self.passes += 1
        return res

    def pause(self, step_s: float) -> float:
        """The wait after a step of ``step_s`` seconds."""
        return max(self.pause_s, step_s * (1 - self.duty) / self.duty)

    @staticmethod
    def _step_done(res: dict, s0: float, longest: str) -> float:
        took = time.monotonic() - s0
        res["steps"] += 1
        res[longest] = max(res[longest], round(took * 1000, 1))
        return took

    # ---- in the background ---------------------------------------------------------------------

    def start(self) -> None:
        if self.retention.keeps_everything():
            log.info("retention: every queue is kept forever, no purge")
            return
        self._thread = threading.Thread(target=self._loop, name="trailbeam-retention", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 30) -> None:
        """Stop after the current step (at most one short write)."""
        self._halt.set()
        if self._thread is not None:
            self._thread.join(timeout)

    def _loop(self) -> None:
        while not self._halt.is_set():
            try:
                r = self.run_pass()
                if r["deleted_spans"] or r["reclaimed_pages"]:
                    log.info("retention: deleted %d spans, gave back %d pages in %.1f s (%d steps, longest %.0f ms)",
                             r["deleted_spans"], r["reclaimed_pages"], r["seconds"], r["steps"], r["longest_step_ms"])
            except Exception:  # a failed pass is retried at the next interval; ingest goes on
                log.exception("retention pass failed")
            self._halt.wait(self.interval_s)

    def status(self) -> dict:
        """For /api/health: the configuration and the last pass."""
        with self._lock:
            running = dict(self._running) if self._running else None
            last = dict(self.last_pass) if self.last_pass else None
        lease = getattr(self.store, "retention_lease_held", None)  # several replicas on one store: who purges
        return self.retention.config() | {"purge_interval_s": self.interval_s, "purge_duty": self.duty,
                                          "purge_batch": self.batch,
                                          **({} if lease is None else {"runs_on_this_replica": lease}),
                                          "passes": self.passes,
                                          "running": running, "last_pass": last}
