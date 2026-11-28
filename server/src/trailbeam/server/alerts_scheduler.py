"""The scheduled alert check's timer (QV-639, part of QV-592: alerts in self-hosted Trailbeam).

The hosted app runs its check from outside (lib/alerts/scheduler.ts with EventBridge, or a cron calling
/api/cron/alerts). Self-hosted Trailbeam is one process, so a thread in the server does it instead: it runs
alerts_run.run_alerts every few minutes, one check at a time, and nothing else. No AWS.

- ``TRAILBEAM_ALERT_CHECK``: how often, in minutes (1 to 1440; ``on`` is every 5), or ``off`` (or 0). Off by default
  until the alert tables and API are in (QV-592), which then turn it on by default.
- When no workspace has a rule, a tick does nothing else: no lease, no data read.
- A check that raises is logged and the next tick runs as usual. A slow check delays the next tick rather than
  overlapping it; the leases (alerts_run.py) keep a second server process, if someone runs one, from checking a
  workspace twice.
- ``stop()`` starts no other check and waits up to STOP_WAIT_S for the one under way (QV-674). A check can take
  longer (its 60 s budget, then delivery: up to 20 channels at 10 s each), so it may be cut off when the process
  exits; the changes it was sending are in the outbox, and the next check sends them (alerts_run.py).
- No check can be stopped from outside (a Python thread can't be), so a check that hangs (a database call that never
  answers: Postgres connections have a 60 s statement_timeout, postgres.py, which ends most) holds up every later
  one. Each check runs on a thread of its own; one still going after DEADLINE_S is logged as an error, and
  ``health()`` (in /api/health) says ``stuck``, or ``late`` when no check has ended for LATE_AFTER intervals.
"""
from __future__ import annotations

import logging
import os
import threading
import time
from collections.abc import Callable, Mapping

log = logging.getLogger("trailbeam.server.alerts")

ENV = "TRAILBEAM_ALERT_CHECK"
DEFAULT_EVERY_MIN = 5
#: unset: off for now (QV-592 makes it DEFAULT_EVERY_MIN once rules can be stored)
UNSET: int | None = None
MAX_EVERY_MIN = 1440
#: a check running longer than this is reported stuck (health) and logged as an error. Above the run's 60 s budget
#: plus a slow delivery, and below alerts_run.LEASE_MS, so a stuck check is reported before its lease runs out (s)
DEADLINE_S = 300.0
#: /api/health reports the check late when none has ended for this many intervals
LATE_AFTER = 2
#: how long stop() waits for a check under way. Below the 10 s that ``docker stop`` (and Compose's
#: stop_grace_period, and Kubernetes' terminationGracePeriodSeconds is 30) gives before SIGKILL, leaving the rest of
#: the shutdown 2 s. A check longer than that is cut off; what it was sending stays in the outbox (s)
STOP_WAIT_S = 8.0


def check_every_min(env: Mapping[str, str] | None = None) -> int | None:
    """The check interval the setting asks for, in minutes; None: the check is off. Raises ValueError on a value it
    can't take, so a typo stops the server at start instead of leaving alerts silently off."""
    raw = (os.environ if env is None else env).get(ENV, "").strip().lower()
    if raw == "":
        return UNSET
    if raw in ("on", "true", "yes"):
        return DEFAULT_EVERY_MIN
    if raw in ("off", "false", "no", "0"):
        return None
    try:
        n = int(raw)
    except ValueError:
        raise ValueError(f"{ENV} must be a number of minutes (1 to {MAX_EVERY_MIN}), on or off, not {raw!r}") from None
    if not 1 <= n <= MAX_EVERY_MIN:
        raise ValueError(f"{ENV} must be between 1 and {MAX_EVERY_MIN} minutes, or off, not {n}")
    return n


class AlertScheduler:
    """Runs ``check`` every ``every_s`` seconds, starting at once: a daemon thread keeps the time and runs each check
    on a thread of its own, so it can tell when one runs past ``deadline_s``. ``has_rules`` is asked first; when it
    says no, the tick does nothing more. Both are usually alerts_run.run_alerts and alerts_run.has_rules over the same
    Deps."""

    def __init__(self, check: Callable[[], dict], every_s: float, has_rules: Callable[[], bool] | None = None,
                 clock: Callable[[], float] = time.time, deadline_s: float = DEADLINE_S):
        if every_s <= 0:
            raise ValueError("every_s must be positive")
        if deadline_s <= 0:
            raise ValueError("deadline_s must be positive")
        self.check, self.every_s, self.has_rules, self.clock = check, every_s, has_rules, clock
        self.deadline_s = deadline_s
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._worker: threading.Thread | None = None
        self._lock = threading.Lock()
        #: held while the loop starts a check's thread and while stop() asks for one: stop() then sees either no new
        #: thread (the loop sees _stop and starts none) or one already started, never one it can't join (QV-729)
        self._spawn = threading.Lock()
        #: for /api/health (health()): when the scheduler started, when the last tick began and ended (s since the
        #: epoch), what it found, and its error; running_since: the tick under way, if one is
        self.started_at: float | None = None
        self.last_at: float | None = None
        self.last_done_at: float | None = None
        self.running_since: float | None = None
        self.last_result: dict | None = None
        self.last_error: str | None = None
        self.ticks = 0
        self.idle = 0
        self.overdue = 0

    def tick(self) -> dict | None:
        """One tick: the check's result, or None when there was nothing to check or it raised."""
        with self._lock:
            self.ticks += 1
            self.last_at = self.running_since = self.clock()
            try:
                return self._tick()
            finally:
                self.last_done_at, self.running_since = self.clock(), None

    def _tick(self) -> dict | None:
        try:
            if self.has_rules is not None and not self.has_rules():
                self.idle += 1
                self.last_result, self.last_error = None, None
                return None
            r = self.check()
        except Exception as e:
            self.last_result, self.last_error = None, f"{type(e).__name__}: {e}"
            log.exception("alert check failed")
            return None
        self.last_result, self.last_error = r, None
        for f in r.get("failed") or []:
            log.warning("alert check: workspace %s failed: %s", f.get("ws"), f.get("error"))
        if (r.get("changes") or r.get("failed") or r.get("resent") or r.get("dropped")
                or (r.get("skipped") or {}).get("budget")):
            log.info("alert check: %d workspace(s), %d rule(s), %d change(s) sent, %d resent, %d dropped, %d failed, "
                     "skipped %s, %d ms", r.get("workspaces", 0), r.get("rules", 0), r.get("changes", 0),
                     r.get("resent", 0), r.get("dropped", 0), len(r.get("failed") or []), r.get("skipped"),
                     r.get("ms", 0))
        return r

    def _loop(self) -> None:
        while not self._stop.is_set():
            began = self.clock()
            w = threading.Thread(target=self.tick, name="trailbeam-alert-check-run", daemon=True)
            with self._spawn:
                if self._stop.is_set():
                    return
                # started before it is published: stop() can't join a thread that has not started (QV-729)
                w.start()
                self._worker = w
            w.join(self.deadline_s)
            if w.is_alive():
                # a thread can't be stopped from outside: say so, and start no other check until it returns
                self.overdue += 1
                log.error("alert check: a check has run for more than %d s and has not returned (a call that "
                          "never answers?); no alerts are checked until it does", self.deadline_s)
                while w.is_alive() and not self._stop.is_set():
                    w.join(1.0)
                if w.is_alive():
                    return
                log.warning("alert check: the late check returned after %d s", self.clock() - began)
            # the next tick every_s after this one began; a check longer than that is followed at once
            self._stop.wait(max(0.0, self.every_s - (self.clock() - began)))

    def start(self) -> AlertScheduler:
        if self._thread is None:
            self.started_at = self.clock()
            self._thread = threading.Thread(target=self._loop, name="trailbeam-alert-check", daemon=True)
            self._thread.start()
        return self

    def stop(self, timeout: float | None = STOP_WAIT_S) -> bool:
        """Starts no other check and waits up to ``timeout`` seconds (None: as long as it takes) for the one under
        way. True if it ended in time. A check still going when the process exits is cut off; the changes it was
        sending are in the outbox (alerts_run.py), and the next check sends them."""
        with self._spawn:
            self._stop.set()
            worker = self._worker
        end = None if timeout is None else time.monotonic() + timeout
        for t in (worker, self._thread):
            if t is not None:
                t.join(None if end is None else max(0.0, end - time.monotonic()))
        done = not any(t is not None and t.is_alive() for t in (worker, self._thread))
        if not done:
            log.warning("alert check: stopping with a check still under way after %s s; what it was sending is sent "
                        "by the next check", timeout)
        return done

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def health(self) -> dict:
        """The check as /api/health shows it. ``status``: ok; failing (the last check raised); late (no check has
        ended for LATE_AFTER intervals: the scheduler is not running, or a check runs that long); stuck (a check
        has run past ``deadline_s``). Times in ms since the epoch."""
        now = self.clock()
        def ms(t: float | None) -> int | None:
            return None if t is None else int(t * 1000)

        since = self.running_since
        ended = self.last_done_at if self.last_done_at is not None else self.started_at
        status, problem = "ok", None
        if since is not None and now - since > self.deadline_s:
            status = "stuck"
            problem = (f"A check has run for {int(now - since)} s, past its deadline of {int(self.deadline_s)} s; "
                       "no alerts are checked until it returns.")
        elif ended is not None and now - ended > LATE_AFTER * self.every_s:
            status = "late"
            problem = (f"No check has ended for {int((now - ended) // 60)} min; one should run every "
                       f"{self.every_s / 60:g} min.")
        elif not self.running and self.started_at is not None:
            status, problem = "late", "The check's thread is not running."
        elif self.last_error is not None:
            status, problem = "failing", f"The last check failed: {self.last_error}"
        r = self.last_result or {}
        return {"status": status, "problem": problem, "every_s": self.every_s, "deadline_s": self.deadline_s,
                "started_at_ms": ms(self.started_at), "last_at_ms": ms(self.last_at),
                "last_done_at_ms": ms(self.last_done_at), "running_since_ms": ms(since),
                "last_error": self.last_error, "ticks": self.ticks, "overdue": self.overdue,
                "last": {k: r.get(k) for k in ("workspaces", "changes", "resent", "dropped", "ms")} | {
                    "failed": len(r.get("failed") or [])} if self.last_result is not None else None}


def start_alert_scheduler(check: Callable[[], dict], has_rules: Callable[[], bool] | None = None,
                          env: Mapping[str, str] | None = None) -> AlertScheduler | None:
    """Starts the check at the interval TRAILBEAM_ALERT_CHECK sets; None, and no thread, when it is off."""
    every = check_every_min(env)
    if every is None:
        log.info("alert check off (set %s to turn it on)", ENV)
        return None
    log.info("alert check every %d min", every)
    return AlertScheduler(check, every * 60, has_rules).start()
