"""Call order inside one process (span contract schema version 3).

``scope`` names this process's counter; ``tick()`` returns a strictly increasing integer, starting
at 1. Two ticks with the same scope were taken in this order in one process, whatever the clock
said. Every Trailbeam hook and wrapper in the process shares this one counter, **across all its threads**:
the scope is the process, not a thread. That is sound because the lock makes the ticks one sequence in
real time, and an edge needs A's ``returned`` tick (after its response) before B's ``issued`` tick
(before its request), so two calls in flight together are never ordered, whichever threads made them
(docs/span-contract.md section 2a).

A forked child gets a new scope and a fresh counter: its ticks are not ordered against the
parent's after the fork, so they must not share a scope.
"""
from __future__ import annotations

import os
import threading
import uuid

scope: str = str(uuid.uuid4())
_lock = threading.Lock()
_last = 0


def tick() -> int:
    """The next value of this process's call-order counter (1, 2, 3, ...). Thread-safe."""
    global _last
    with _lock:
        _last += 1
        return _last


def _reset_in_child() -> None:
    global scope, _lock, _last
    # The parent's lock may have been held by another thread at fork time; never reuse it.
    _lock = threading.Lock()
    _last = 0
    scope = str(uuid.uuid4())


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_in_child)
