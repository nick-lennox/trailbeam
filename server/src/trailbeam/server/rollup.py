"""The hourly tenant rollup behind the Tenants page (contract/tenants.md), the same for every backend (QV-128).

Until QV-128 the page added up every tenant event (summary.py ``tenant_events``) in its range on each request,
so its cost grew with the traffic in the range: 1.5 s at 4M spans and 2.8 s at 7M on Postgres, 0.5-0.76 s on a
t3.small with SQLite. Now each store also keeps, per hour and tenant, what the page adds up:

- ``sent``: send events; ``first_deliveries``: first deliveries with a time in queue; ``dead_letter_moves``:
  dead-letter moves, and ``repeat_moves``: moves that follow an earlier move of the same message (below);
- ``waits``: the first deliveries' times in queue, as a ``Sketch``.

A row's tenant is the message's tenant (``""`` for none: tenant names are never empty). Rows are the sum of what every stored message adds
(``contributions``), so a store keeps them exact by adding what a message adds now and taking away what it
added before, whenever it rewrites the message's summary, and taking it all away when retention removes the
message. Counts subtract exactly, and so does the sketch: it is a count per bucket.

**The page** (``tenant_page``) reads the rollup for the whole hours in its range. It reads a part of an hour at
either end (at most two) the cheaper way (``plan``): either its own tenant events, or the hour's rollup rows less
the events of the rest of that hour. Both give exact counts, and an exact sketch too, since sketches subtract
exactly. Until QV-224 it always read the part's events. The default range ends now, so the page read every event
of the current hour: 2.06M of 3.3M tenant events after a heavy ingest (QV-224's bench; 2.55M of 6.0M in QV-180),
and took 1.1-2.1 s. Now the current hour costs its rollup rows less the events newer than now (next to none), and
the hour at the start of the range at most half an hour of events. The events are added up in SQL, by (tenant,
kind, time in queue), instead of being fetched one by one.

**No queue in the key.** The plan (QV-67) had rows per hour, queue and tenant, for a queue filter the page
doesn't have. Measured at 1M spans (50 queues, 50 tenants, 7 days; QV-128 report), that made 154,390 rows for
627,241 tenant events: a tenant's traffic spreads over most queues, so the rows were only 4x fewer than the
events, and adding them up took as long as adding up the events had (last 7 days: 515 ms against 553 ms). Per
hour and tenant it is 8,150 rows. A queue filter would need a second rollup keyed by queue as well.

**Dead-lettered messages** must be distinct messages, which a sum per hour can't give: a message moved twice
(two dead-letter queues, or a chain of them) would count twice. Every move after a message's first is also a
``dead_lettered_again`` event (summary.py), at its own time and with the time since the move before it, which
count at most once. In a range, a message's moves in the range are a run of its moves in time order, so
distinct messages = moves in the range - repeats in the range whose previous move is also in the range. The
second term reads ``dead_lettered_again`` events only (rare: one per extra move), by an index of its own.

**p95** comes from the sketch, with a stated bound (contract/tenants.md): within 1 % of the nearest-rank p95
of the exact times in queue. ``Sketch`` keeps a count per bucket: times up to EXACT_MS ms each have their own
bucket and come back exact; above that, buckets grow by a factor GAMMA (relative accuracy ALPHA, DDSketch:
Masson, Rim and Lee, "DDSketch: a fast and fully-mergeable quantile sketch with relative-error guarantees",
VLDB 2019, https://arxiv.org/abs/1908.10693), and a bucket comes back as the integer nearest its centre. The
true p95 lies in the bucket the rank falls in, so the answer is within ALPHA x it + 0.5 ms of it, which is
under 1 % above EXACT_MS (at most 0.91 %). Buckets for times up to 400 days: about 2,070; a tenant-hour has
far fewer.
"""
from __future__ import annotations

import json
import math
import time
from typing import Iterable, Mapping, Optional

from .store import TenantActivity

HOUR_MS = 3_600_000
NO_TENANT = ""  # the rollup's tenant for messages with none

# the sketch: exact to EXACT_MS, then ALPHA relative; ALPHA + 0.5 / EXACT_MS is the stated 1 % bound
EXACT_MS = 100
ALPHA = 0.005
GAMMA = (1 + ALPHA) / (1 - ALPHA)
_LOG_GAMMA = math.log(GAMMA)
_FIRST_LOG = math.ceil(math.log(EXACT_MS) / _LOG_GAMMA)
BOUND = 0.01  # relative, as contract/tenants.md states it

SENT, FIRST_DELIVERY, DEAD_LETTERED, DEAD_LETTERED_AGAIN = "sent", "first_delivery", "dead_lettered", "dead_lettered_again"


def bucket(ms: int) -> int:
    """A time in queue's bucket: itself up to EXACT_MS, then EXACT_MS + 1 + its log-GAMMA bucket past EXACT_MS's."""
    if ms <= EXACT_MS:
        return max(0, int(ms))
    return EXACT_MS + 1 + math.ceil(math.log(ms) / _LOG_GAMMA) - _FIRST_LOG


def value(key: int) -> int:
    """What a bucket stands for: its time for an exact one, else the integer nearest the centre of
    (GAMMA^(i-1), GAMMA^i], 2 GAMMA^i / (GAMMA + 1), which is within ALPHA of every time in it."""
    if key <= EXACT_MS:
        return key
    i = key - EXACT_MS - 1 + _FIRST_LOG
    return int(round(2 * GAMMA ** i / (GAMMA + 1)))


class Sketch:
    """Counts per bucket of times in queue (ms). Adds, merges and subtracts exactly."""

    __slots__ = ("counts",)

    def __init__(self, counts: Optional[Mapping[int, int]] = None):
        self.counts: dict = {int(k): int(v) for k, v in (counts or {}).items() if v}

    @classmethod
    def of(cls, waits: Iterable[int]) -> "Sketch":
        s = cls()
        for w in waits:
            s.add(w)
        return s

    @classmethod
    def loads(cls, text: Optional[str]) -> "Sketch":
        return cls(json.loads(text)) if text else cls()

    def dumps(self) -> str:
        return json.dumps({str(k): v for k, v in sorted(self.counts.items())}, separators=(",", ":"))

    def add(self, ms: int, n: int = 1) -> None:
        k = bucket(ms)
        c = self.counts.get(k, 0) + n
        if c:
            self.counts[k] = c
        else:
            self.counts.pop(k, None)

    def merge(self, other: "Sketch", sign: int = 1) -> "Sketch":
        for k, v in other.counts.items():
            c = self.counts.get(k, 0) + sign * v
            if c:
                self.counts[k] = c
            else:
                self.counts.pop(k, None)
        return self

    @property
    def n(self) -> int:
        return sum(self.counts.values())

    def p95(self) -> Optional[int]:
        """Nearest rank, as queries.p95, from the buckets: None when empty."""
        n = self.n
        if n <= 0:
            return None
        rank, seen = max(1, math.ceil(0.95 * n)), 0
        for k in sorted(self.counts):
            seen += self.counts[k]
            if seen >= rank:
                return value(k)
        return value(max(self.counts))  # not reached: counts are never negative in a whole rollup

    def __bool__(self):
        return bool(self.counts)

    def __eq__(self, other):
        return isinstance(other, Sketch) and self.counts == other.counts

    def __repr__(self):
        return f"Sketch({self.counts})"


class Agg:
    """One rollup row's numbers: what some messages add to an (hour, tenant)."""

    __slots__ = ("sent", "first_deliveries", "dead_letter_moves", "waits")

    def __init__(self, sent: int = 0, first_deliveries: int = 0, dead_letter_moves: int = 0,
                 waits: Optional[Sketch] = None):
        self.sent, self.first_deliveries, self.dead_letter_moves = sent, first_deliveries, dead_letter_moves
        self.waits = waits if waits is not None else Sketch()

    def merge(self, other: "Agg", sign: int = 1) -> "Agg":
        self.sent += sign * other.sent
        self.first_deliveries += sign * other.first_deliveries
        self.dead_letter_moves += sign * other.dead_letter_moves
        self.waits.merge(other.waits, sign)
        return self

    def add_event(self, kind: str, wait: Optional[int], n: int = 1) -> None:
        """``n`` such events (negative: taken out)."""
        if kind == SENT:
            self.sent += n
        elif kind == FIRST_DELIVERY:
            self.first_deliveries += n
            self.waits.add(wait, n)
        elif kind == DEAD_LETTERED:
            self.dead_letter_moves += n

    def __bool__(self):
        return bool(self.sent or self.first_deliveries or self.dead_letter_moves or self.waits)

    def __eq__(self, other):
        return isinstance(other, Agg) and self.row() == other.row()

    def row(self) -> tuple:
        return (self.sent, self.first_deliveries, self.dead_letter_moves, self.waits.dumps())

    def __repr__(self):
        return f"Agg{self.row()}"


def hour(at_ms: int) -> int:
    return int(at_ms) // HOUR_MS


def contributions(tenant: Optional[str], events: Iterable) -> dict:
    """What one message adds to the rollup: {(hour, tenant): Agg}, from its summary's tenant and tenant_events
    ([at_ms, kind, wait_ms], as stored in events_json)."""
    out: dict = {}
    t = tenant if tenant is not None else NO_TENANT
    for at, kind, wait in events:
        if kind not in (SENT, FIRST_DELIVERY, DEAD_LETTERED):
            continue
        out.setdefault((hour(at), t), Agg()).add_event(kind, wait)
    return out


def of_summary(s) -> dict:
    """contributions() of a stored summary row (tenant, events_json) or a summarize() result."""
    events = s["tenant_events"] if "tenant_events" in s.keys() else json.loads(s["events_json"])
    return contributions(s["tenant"], events)


class Deltas:
    """What a transaction changes in the rollup, added up per (place, hour, tenant) so that each row is
    written once however many messages touched it. ``place`` is the store's: None for SQLite, (rclass, bucket)
    for Postgres."""

    def __init__(self):
        self.by: dict = {}

    def add(self, place, contrib: dict, sign: int = 1) -> None:
        for (h, t), a in contrib.items():
            self.by.setdefault((place, h, t), Agg()).merge(a, sign)

    def change(self, place_old, old: Optional[dict], place_new, new: Optional[dict]) -> None:
        """A message's contributions went from ``old`` (in ``place_old``) to ``new`` (in ``place_new``); None:
        it had or has none (not stored, or removed)."""
        if old and new and place_old == place_new and old == new:
            return
        if old:
            self.add(place_old, old, -1)
        if new:
            self.add(place_new, new, 1)

    def items(self):
        """((place, hour, tenant), Agg) for the rows that change, in key order (a fixed lock order)."""
        return sorted(((k, a) for k, a in self.by.items() if a), key=lambda kv: (repr(kv[0][0]),) + kv[0][1:])

    def __bool__(self):
        return any(a for a in self.by.values())


def plan(from_ms: int, to_ms: int, now_ms: Optional[int] = None) -> tuple:
    """How to read [from_ms, to_ms]: (the hours (first, last) whose rollup rows to add, or None; the
    [(from_ms, to_ms, sign)] whose tenant events to add (sign 1) or take out (sign -1)).

    The rollup hours are the whole hours, and each partial hour whose part in the range is longer than the rest of
    it, less the events of that rest. Only time up to ``now_ms`` (default: now) counts in the lengths, since later
    there are next to no events (only from clocks that run ahead). So the current hour is always its rollup rows
    less what is newer than ``to_ms``, and an earlier partial hour costs at most half an hour of events. The hours
    are consecutive: partial hours are only at the two ends."""
    if from_ms > to_ms:
        return None, []
    now_ms = int(time.time() * 1000) if now_ms is None else now_ms

    def past(x, y):  # how much of [x, y] is not in the future
        return max(0, min(y, now_ms) - x + 1)

    first, last = from_ms // HOUR_MS, to_ms // HOUR_MS
    hours, events = [], []
    for h in sorted({first, last}):
        lo, hi = h * HOUR_MS, (h + 1) * HOUR_MS - 1
        a, b = max(from_ms, lo), min(to_ms, hi)
        rest = [(x, y) for x, y in ((lo, a - 1), (b + 1, hi)) if x <= y]
        if rest and past(a, b) <= sum(past(x, y) for x, y in rest):
            events.append((a, b, 1))
            continue
        hours.append(h)
        events += [(x, y, -1) for x, y in rest]
    if last - first >= 2:
        hours += [first + 1, last - 1]  # the whole hours between
    return ((min(hours), max(hours)) if hours else None), events


def tenant_page(rollup_rows: Iterable, raw_events: Iterable, repeats: Iterable) -> list:
    """Store.tenant_activity's answer (TenantActivity, ``waits`` a Sketch) from its three reads, the same for every store:

    - ``rollup_rows``: (tenant, sent, first_deliveries, dead_letter_moves, waits: JSON or {bucket: count}) for
      the whole hours
      (any number of rows per tenant: hours, and on Postgres places and writers);
    - ``raw_events``: (tenant, kind, wait_ms, n) for the tenant events ``plan`` reads, added up: n of them, negative
      for those taken out (every kind may come; only sent, first_delivery and dead_lettered count);
    - ``repeats``: the tenant of each ``dead_lettered_again`` event in the range whose previous move is in it too.

    Tenants are the message's (None for none); a tenant with nothing in the range is left out."""
    by: dict = {}

    def g(t):
        t = None if t == NO_TENANT else t
        a = by.get(t)
        if a is None:
            a = by[t] = Agg()
        return a

    for t, sent, firsts, moves, waits in rollup_rows:
        g(t).merge(Agg(int(sent), int(firsts), int(moves), Sketch(waits) if isinstance(waits, dict) else Sketch.loads(waits)))
    for t, kind, wait, n in raw_events:
        g(t).add_event(kind, wait, int(n))
    again: dict = {}
    for t in repeats:
        t = None if t == NO_TENANT else t
        again[t] = again.get(t, 0) + 1
    out = []
    for t, a in by.items():
        if not a:
            continue
        out.append(TenantActivity(t, a.sent, a.dead_letter_moves - again.get(t, 0), a.waits))
    return out
