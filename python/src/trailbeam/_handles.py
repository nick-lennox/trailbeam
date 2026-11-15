"""Receipt handle -> message ID map for settle spans (contract section 8)."""
from __future__ import annotations

import threading
import time
from collections import OrderedDict
from typing import Callable, Optional

MAX_AGE_SECONDS = 12 * 60 * 60  # the longest SQS visibility timeout
DEFAULT_MAX_ENTRIES = 100_000


class HandleMap:
    """Thread-safe and bounded: oldest entries are dropped first, and entries expire after 12 hours."""

    def __init__(self, max_entries: int = DEFAULT_MAX_ENTRIES, max_age: float = MAX_AGE_SECONDS,
                 clock: Callable[[], float] = time.monotonic):
        self._d: "OrderedDict[str, tuple[str, float]]" = OrderedDict()
        self._lock = threading.Lock()
        self._max = max_entries
        self._age = max_age
        self._clock = clock

    def put(self, receipt_handle: str, message_id: str) -> None:
        now = self._clock()
        with self._lock:
            self._d.pop(receipt_handle, None)
            self._d[receipt_handle] = (message_id, now)
            while len(self._d) > self._max:
                self._d.popitem(last=False)
            # Insertion order is time order, so expired entries are at the front.
            while self._d:
                _, (_, t) = next(iter(self._d.items()))
                if now - t <= self._age:
                    break
                self._d.popitem(last=False)

    def get(self, receipt_handle: str) -> Optional[str]:
        with self._lock:
            v = self._d.get(receipt_handle)
            if v is None:
                return None
            if self._clock() - v[1] > self._age:
                del self._d[receipt_handle]
                return None
            return v[0]

    def discard(self, receipt_handle: str) -> None:
        with self._lock:
            self._d.pop(receipt_handle, None)

    def __len__(self) -> int:
        with self._lock:
            return len(self._d)
