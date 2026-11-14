"""What order the observed steps of one message are proven to be in, and by which source (contract section 9a).

A step A is before a step B only when at least one **source** proves it. Each source is a fact that was
observed, never a rule about what causes what:

- ``clock``: A's interval ended before B's started (``A.end < B.start``), on the hosts' clocks.
- ``process_order``: A and B were made in the same process (same ``qv2.order.scope``), and A's call
  returned before B's call was issued (``A.order_returned < B.order_issued``, a per-process counter).
- ``receive_count``: A and B are receives on the same queue and SQS gave A a lower
  ``ApproximateReceiveCount`` than B: delivery n came before delivery n + 1.
- ``sqs_timestamps``: A is the send and B is the first receive (receive count 1) on the send's queue, and
  B's ``SentTimestamp`` is earlier than its ``ApproximateFirstReceiveTimestamp``. Both come from SQS's clock;
  it is never compared with a host clock. Equal timestamps (the same millisecond) prove nothing.

Order is transitive: A before B by one source and B before C by another puts A before C, and the edges
let a reader follow the chain. If the sources contradict each other (a cycle, e.g. a host clock that
jumped), no order is shown between the steps on the cycle; :func:`proven` reports them as conflicts.
"""
from __future__ import annotations

from typing import Optional

SOURCES = ("clock", "process_order", "receive_count", "sqs_timestamps")


def _edge_sources(a, b) -> list[str]:
    """The sources that prove row ``a`` before row ``b`` directly (in the order of SOURCES)."""
    out = []
    if b["start_ns"] is not None and a["time_ns"] < b["start_ns"]:
        out.append("clock")
    if (a["order_scope"] is not None and a["order_scope"] == b["order_scope"]
            and a["order_returned"] is not None and b["order_issued"] is not None
            and a["order_returned"] < b["order_issued"]):
        out.append("process_order")
    if (a["kind"] == "receive" and b["kind"] == "receive" and a["queue"] == b["queue"]
            and a["receive_count"] is not None and b["receive_count"] is not None
            and a["receive_count"] < b["receive_count"]):
        out.append("receive_count")
    if (a["kind"] == "send" and b["kind"] == "receive" and a["queue"] == b["queue"] and b["receive_count"] == 1
            and b["sent_ts_ms"] is not None and b["first_receive_ts_ms"] is not None
            and b["sent_ts_ms"] < b["first_receive_ts_ms"]):
        out.append("sqs_timestamps")
    return out


def _closure(n: int, edges) -> list[int]:
    """reach[i]: bit j set iff there is a path i -> j (Warshall, on int bitsets)."""
    reach = [0] * n
    for i, j in edges:
        reach[i] |= 1 << j
    for k in range(n):
        bk = 1 << k
        rk = reach[k]
        for i in range(n):
            if reach[i] & bk:
                reach[i] |= rk
    return reach


class Order:
    """The proven order between ``rows`` (observed steps, any order; referred to by list index)."""

    def __init__(self, rows: list):
        n = len(rows)
        self.rows = rows
        edges: dict[tuple[int, int], list[str]] = {}
        for i in range(n):
            for j in range(n):
                if i != j:
                    s = _edge_sources(rows[i], rows[j])
                    if s:
                        edges[(i, j)] = s
        reach = _closure(n, edges)
        # Steps on a cycle: the sources contradict each other, so no order between them is proven.
        cyclic = [i for i in range(n) if reach[i] >> i & 1]
        self.conflicts: list[tuple[int, int]] = []
        if cyclic:
            scc = {i: frozenset(j for j in cyclic if reach[i] >> j & 1 and reach[j] >> i & 1) for i in cyclic}
            self.conflicts = sorted(e for e in edges if e[0] in scc and e[1] in scc[e[0]])
            for e in self.conflicts:
                del edges[e]
            reach = _closure(n, edges)
        self.edges = edges
        self.reach = reach

    def before(self, i: int, j: int) -> bool:
        """Row index i is proven before row index j (directly or through a chain)."""
        return bool(self.reach[i] >> j & 1)

    def immediate(self, j: int) -> list[int]:
        """The proven predecessors of j that are not before another proven predecessor of j (its direct
        predecessors in the transitive reduction). Each of them is joined to j by a direct edge."""
        preds = [i for i in range(len(self.rows)) if self.before(i, j)]
        return [i for i in preds if not any(self.before(i, k) for k in preds if k != i)]


def layout(n_obs: int, order: Order, inferred: list[tuple[Optional[int], Optional[int]]],
           keys: list[tuple]) -> tuple[list[int], list[Optional[int]], list[bool]]:
    """Display order and groups for ``n_obs`` observed steps plus ``inferred`` steps.

    Nodes 0..n_obs-1 are the observed rows (indices into ``order.rows``); node n_obs + k is the k-th
    inferred step, given as (earlier anchor, later anchor) row indices, either None. An inferred step
    is after X exactly when X is its earlier anchor or before it, and before X exactly when X is its
    later anchor or after it: it is never ordered against a step except through what it was inferred
    from. ``keys[node]`` breaks ties (smallest first) among steps whose order is not proven.

    Returns (nodes in display order, group number per display position, concurrent flag per group).
    Groups are the maximal runs such that every step of an earlier run is proven before every step
    of a later one. A run of one inferred step alone gets group None (it sits between groups).
    """
    total = n_obs + len(inferred)

    def before(x: int, y: int) -> bool:
        if x < n_obs and y < n_obs:
            return order.before(x, y)
        if y >= n_obs:  # ... -> inferred y
            a, _ = inferred[y - n_obs]
            if a is None:
                return False
            if x < n_obs:
                return x == a or order.before(x, a)
            xb = inferred[x - n_obs][1]
            return xb is not None and (xb == a or order.before(xb, a))
        b = inferred[x - n_obs][1]  # inferred x -> observed y
        return b is not None and (y == b or order.before(b, y))

    succ = [[y for y in range(total) if y != x and before(x, y)] for x in range(total)]
    indeg = [0] * total
    for x in range(total):
        for y in succ[x]:
            indeg[y] += 1
    import heapq
    ready = [(keys[x], x) for x in range(total) if indeg[x] == 0]
    heapq.heapify(ready)
    seq: list[int] = []
    while ready:
        _, x = heapq.heappop(ready)
        seq.append(x)
        for y in succ[x]:
            indeg[y] -= 1
            if indeg[y] == 0:
                heapq.heappush(ready, (keys[y], y))
    if len(seq) != total:  # pragma: no cover - the relation is acyclic by construction
        raise AssertionError("order relation has a cycle")

    # Cut after position p iff every step at or before p is proven before every step after p.
    pos = {x: p for p, x in enumerate(seq)}
    pred_mask = [0] * total
    for x in range(total):
        for y in succ[x]:
            pred_mask[pos[y]] |= 1 << pos[x]
    full = (1 << total) - 1
    suffix = [full] * (total + 1)
    for p in range(total - 1, -1, -1):
        suffix[p] = suffix[p + 1] & pred_mask[p]
    runs: list[list[int]] = []
    start = 0
    for p in range(total):
        prefix = (1 << (p + 1)) - 1
        if p == total - 1 or suffix[p + 1] & prefix == prefix:
            runs.append(list(range(start, p + 1)))
            start = p + 1
    group: list[Optional[int]] = [None] * total
    concurrent: list[bool] = []
    for run in runs:
        if len(run) == 1 and seq[run[0]] >= n_obs:
            continue
        g = len(concurrent)
        concurrent.append(len(run) > 1)
        for p in run:
            group[p] = g
    return seq, group, concurrent
