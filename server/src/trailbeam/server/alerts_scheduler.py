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
- ``stop()`` ends the thread at the next wait; a check under way finishes first (it is bounded by its budget).
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
    """Runs ``check`` every ``every_s`` seconds on a daemon thread, starting at once. ``has_rules`` is asked first;
    when it says no, the tick does nothing more. Both are usually alerts_run.run_alerts and alerts_run.has_rules over
    the same Deps."""

    def __init__(self, check: Callable[[], dict], every_s: float, has_rules: Callable[[], bool] | None = None,
                 clock: Callable[[], float] = time.time):
        if every_s <= 0:
            raise ValueError("every_s must be positive")
        self.check, self.every_s, self.has_rules, self.clock = check, every_s, has_rules, clock
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        #: for /api/health later: when the last tick ran (s since the epoch), what it found, and its error
        self.last_at: float | None = None
        self.last_result: dict | None = None
        self.last_error: str | None = None
        self.ticks = 0
        self.idle = 0

    def tick(self) -> dict | None:
        """One tick: the check's result, or None when there was nothing to check or it raised."""
        with self._lock:
            self.ticks += 1
            self.last_at = self.clock()
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
            if r.get("changes") or r.get("failed") or (r.get("skipped") or {}).get("budget"):
                log.info("alert check: %d workspace(s), %d rule(s), %d change(s) sent, %d failed, skipped %s, %d ms",
                         r.get("workspaces", 0), r.get("rules", 0), r.get("changes", 0), len(r.get("failed") or []),
                         r.get("skipped"), r.get("ms", 0))
            return r

    def _loop(self) -> None:
        while not self._stop.is_set():
            began = self.clock()
            self.tick()
            # the next tick every_s after this one began; a check longer than that is followed at once
            self._stop.wait(max(0.0, self.every_s - (self.clock() - began)))

    def start(self) -> AlertScheduler:
        if self._thread is None:
            self._thread = threading.Thread(target=self._loop, name="trailbeam-alert-check", daemon=True)
            self._thread.start()
        return self

    def stop(self, timeout: float | None = None) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout)

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()


def start_alert_scheduler(check: Callable[[], dict], has_rules: Callable[[], bool] | None = None,
                          env: Mapping[str, str] | None = None) -> AlertScheduler | None:
    """Starts the check at the interval TRAILBEAM_ALERT_CHECK sets; None, and no thread, when it is off."""
    every = check_every_min(env)
    if every is None:
        log.info("alert check off (set %s to turn it on)", ENV)
        return None
    log.info("alert check every %d min", every)
    return AlertScheduler(check, every * 60, has_rules).start()
