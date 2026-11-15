"""qv2 0.1 concurrency check: compare the timelines in the qv2 store with what the demo drivers really did.

    check.py wait   --truth DIR --count 200 [--timeout 180]
    check.py verify --truth DIR --count 200 --store http://qv2:4318 [--timeout 60] [--report FILE]

The drivers (demo/python/qv2demo.py, demo/node/qv2demo.js) write one truth line per SQS call they make
(send, receive, change_visibility, delete) to their own file in DIR. ``wait`` returns once every sent
message has an accepted delete in the truth. ``verify`` reads each message's timeline from the store's
JSON API (GET /api/messages/{id}) and checks, per message:

- complete:      the observed events (sent, received, visibility_changed, deleted) are exactly the SQS
                 calls in the truth, no more and no fewer;
- in order:      the timeline never shows an effect definitely before its cause. Cause/effect pairs come
                 from the truth: send -> each receive; receive k -> receive k+1; receive k -> each settle
                 with delivery k's handle; an accepted change_visibility on delivery k -> receive k+1 (if
                 the truth has the change first). The store's proven order is the chain of its
                 ``order.edges`` (each with its sources: clock, process_order, receive_count,
                 sqs_timestamps); a timeline without ``order`` (before QV-31) is judged by its intervals.
                 With each event's interval (start, end) and group from the store, a pair is
                   violated   if the effect's interval ends before the cause's starts, or the store proves
                              the effect before the cause, or puts the effect in an earlier group than the
                              cause, or puts the cause in an earlier group although it does not prove it;
                   in order   ("resolved") if the store proves the cause before the effect;
                   concurrent otherwise (shown as "concurrent, order unknown").
                 A message is in order when it is complete and no pair is violated;
- edges verified: every edge in ``order.edges`` is checked, not trusted (QV-34): each source it claims
                 must recompute from the two events' facts (:func:`source_holds`), and no order the edges
                 imply may contradict the truth: the drivers bracket each call (``t_start_ns``,
                 ``t_end_ns``), and A before B is false if B returned before A was made (:func:`verify_edges`);
- joined:        every delete and visibility change is attached to the delivery whose receipt handle it
                 really used, and has the same result (ok / failed) as the real call;
- receive count: every receive (by delivery number) shows the real ApproximateReceiveCount;
- sqs facts:     every receive shows the SentTimestamp and ApproximateFirstReceiveTimestamp SQS really returned
                 for it (QV-38): the drivers write both into the truth (``sent_ms``, ``first_receive_ms``),
                 and the store's ``sent_timestamp_ms`` / ``first_receive_timestamp_ms`` must equal them, a
                 missing value included. The ``sqs_timestamps`` source is recomputed from these fields, so
                 a store that makes them up to resolve a send->receive pair fails here;
- in brackets:   where the truth says its brackets read the same clock as the spans (``same_clock_as_spans``:
                 both drivers; Python's truth and spans read ``time.time_ns()``, Node.js's both read
                 ``Date.now()``, whole milliseconds, node/src/emitter.js ``nowMs``), every observed event lies
                 within its call's truth bracket **and close to its ends** (QV-40): ``t_start_ns`` <= start
                 <= end <= ``t_end_ns``, and start - ``t_start_ns`` and ``t_end_ns`` - end within
                 :data:`BRACKET_BOUNDS_NS`: 1 ms and 1 ms for Python; for Node.js (``clock: "Date.now"``)
                 1 ms on the start, which in whole milliseconds reads "the same or the next millisecond", and
                 3 ms on the end, since its hook builds the span between the two stamps. The ``clock`` source
                 is recomputed from these times, so without the bound a store could move a receive's start to
                 just after its send's end, anywhere inside a long poll's bracket, and claim a false clock
                 order (QV-39 fake3). The summary reports the largest offsets seen
                 (``bracket_offsets_max_ns``). Truth without ``same_clock_as_spans`` (Node.js before QV-40)
                 is not checked this way, and ``events_checked_in_brackets`` says so;
- final:         the message's state is ``deleted``;
- dlq move:      the timeline shows a move to a dead-letter queue exactly when the truth has the message
                 received on a second queue (``--mix check-dlq``, ``demo/check.sh --dlq``): one observed
                 move (``inferred: false``, source ``sqs_dlq_source``, from the first queue to the second),
                 and never an inferred one. ``dlq_moves_observed`` and ``dlq_moves_inferred`` (which must
                 be 0) count them over the run.

So that "in order" cannot pass just by putting everything in one group, the summary also counts the
messages with at least one concurrent group and the pairs resolved in order vs. left concurrent, per
kind of pair. ``concurrent_via_chain`` (a diagnostic) counts the concurrent pairs whose own intervals do
not overlap: they share a group only through other overlapping steps. ``concurrent_tied`` counts those
where the cause ended at exactly the effect's start, a tie at the clock's resolution. Each check is evaluated on its own: joined and receive count compare the events regardless of the order
the store shows them in, so a message that fails only on order still reports its joins and receive counts.
The summary counts each check separately, plus the messages that fail on order alone. A message passes
only if all nine hold. The run passes only if every message passes **and** the floor holds (unless
--floor-report-only, which demo/check.sh uses on the emulator: the floor is then reported, not gated): at least 90%
(--floor) of the send->receive pairs and of the receive->delete pairs are resolved, so that the check
can't pass by leaving things concurrent. ``resolution`` in the summary gives, per kind (send->receive,
receive->delete, release->redelivery, receive n->receive n+1), the pairs resolved and left concurrent and
which sources resolved them; release->redelivery and receive n->receive n+1 are reported with no floor. Inferred events (such as the visibility-timeout expiry after a
ChangeMessageVisibility to 0) are not compared, but are counted in the report. ``verify`` polls the
store until every message passes or --timeout seconds pass (spans are exported in batches, every few
seconds), then reports what it has. It waits only for messages that still miss events; a message whose
events are all there but wrong fails at once. Exit status 0 only if all --count messages pass. Standard library only.
"""

import argparse
import collections
import glob
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

TRUTH_OPS = ("send", "receive", "change_visibility", "delete")
STORE_TYPES = {"sent": "send", "received": "receive", "visibility_changed": "change_visibility", "deleted": "delete"}
CHECKS = ("complete", "in_order", "edges_verified", "joined", "receive_count", "sqs_facts", "in_brackets", "final",
          "dlq_move")
# Contract 3.1, section 9b: the store shows a move to a dead-letter queue only when SQS reports it.
DLQ_MOVE = "moved_to_dead_letter_queue"
DLQ_SOURCE = "sqs_dlq_source"
PAIR_KINDS = ("send->receive", "receive->receive", "receive->settle", "change_visibility->receive")
PAIR_RESULTS = ("in_order", "concurrent", "violated")
# Of the concurrent pairs, those whose own intervals do not overlap (cause ended before effect started): the
# store's one-line grouping put them together only through a chain of other overlapping steps. A diagnostic.
CHAINED = "concurrent_via_chain"
# Of the concurrent pairs, those where the cause ended at exactly the time the effect started: a tie at the
# clock's resolution (1 ms in Node.js), not an overlap SQS could have acted in. A diagnostic.
TIED = "concurrent_tied"
DIAGNOSTICS = (CHAINED, TIED)
ORDER_SOURCES = ("clock", "process_order", "receive_count", "sqs_timestamps")
# Pair kinds reported as resolved vs. concurrent, with the sources that resolved them (QV-31). receive->delete
# is the receive->settle pairs whose effect is a delete; release->redelivery is change_visibility->receive.
RESOLUTION_KINDS = ("send->receive", "receive->delete", "release->redelivery", "receive n->receive n+1")
# CI fails if fewer than this share of these kinds' pairs is resolved, so that "in order" can't pass by
# leaving everything concurrent. release->redelivery has no floor: SQS gives no time for a redelivery.
FLOOR = 0.9
FLOOR_KINDS = ("send->receive", "receive->delete")
# How far an observed event's start may lie after its call's truth start, and its end before its truth end
# (QV-40), by the clock the truth line names (``clock``; Python's lines name none). The hooks stamp a span just
# inside the driver's own stamps. On the QV-40 runs: Python 0.04-0.38 ms on both sides. Node.js, in whole ms:
# starts 0 or 1 ms (3,948 events), ends 0, 1 or 2 ms (6 events at 2 ms), because the Node.js hook builds the
# span after stamping its end and before the driver's await resumes (node/src/sqs.js). A store whose times lie
# anywhere else in a wide bracket (QV-39's fake3) fails.
BRACKET_BOUND_NS = 1_000_000
BRACKET_BOUNDS_NS = {None: (BRACKET_BOUND_NS, BRACKET_BOUND_NS), "Date.now": (BRACKET_BOUND_NS, 3_000_000)}


def resolution_kind(kind: str, effect_op: str):
    if kind == "send->receive":
        return "send->receive"
    if kind == "receive->receive":
        return "receive n->receive n+1"
    if kind == "change_visibility->receive":
        return "release->redelivery"
    if kind == "receive->settle" and effect_op == "delete":
        return "receive->delete"
    return None


def load_truth(directory: str) -> dict:
    """Truth lines grouped by message ID, each message's calls in the order they happened."""
    by_id = collections.defaultdict(list)
    for path in sorted(glob.glob(os.path.join(directory, "*.jsonl"))):
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                if rec.get("op") in TRUTH_OPS:
                    by_id[rec["id"]].append(rec)
    for recs in by_id.values():
        recs.sort(key=_truth_order(recs))
    return dict(by_id)


def _truth_order(recs: list):
    """Sort key for one message's truth calls: by ``t_ns``. Node.js stamps whole milliseconds (the system
    clock, as its spans do), so two calls from different processes can tie; a tie is broken by the receive
    count of the delivery each call belongs to (a settle by its handle's receive), and a receive before the
    settles of its own delivery, which is the only order SQS allows. Within one process a tie keeps the
    file's order, which is the order the calls were made."""
    rc_of_handle = {r.get("rh"): r.get("rc") or 0 for r in recs if r["op"] == "receive"}

    def key(r):
        if r["op"] == "receive":
            return int(r["t_ns"]), r.get("rc") or 0, 0
        if r["op"] in ("delete", "change_visibility"):
            return int(r["t_ns"]), rc_of_handle.get(r.get("rh"), 0), 1
        return int(r["t_ns"]), 0, 0
    return key


def expected_events(truth: list) -> list:
    """The truth as (op, delivery, result, receive_count): delivery numbers count receives, as the store does,
    and a settle gets the number of the receive whose handle (``rh``) it used."""
    out, delivery_of_handle, n = [], {}, 0
    for r in truth:
        if r["op"] == "receive":
            n += 1
            delivery_of_handle[r["rh"]] = n
            out.append(("receive", n, r.get("result", "ok"), r.get("rc")))
        elif r["op"] in ("delete", "change_visibility"):
            out.append((r["op"], delivery_of_handle.get(r["rh"]), r.get("result", "ok"), None))
        else:
            out.append((r["op"], None, r.get("result", "ok"), None))
    return out


def _ns(v):
    return None if v is None else int(v)


def observed_details(timeline: dict) -> list:
    """The store's observed events as dicts: op, delivery, result, receive_count, start, end (ns; start None
    if not observed) and group, in the order the store shows them."""
    out = []
    for e in timeline.get("events", []):
        if e.get("inferred") or e.get("type") not in STORE_TYPES:
            continue
        op = STORE_TYPES[e["type"]]
        out.append({
            "id": e.get("id"),
            "op": op,
            "delivery": None if op == "send" else e.get("delivery"),
            "result": "ok" if op in ("send", "receive") else e.get("result"),
            "receive_count": e.get("receive_count") if op == "receive" else None,
            "start": _ns(e.get("start_unix_nano")),
            "end": _ns(e.get("end_unix_nano", e.get("time_unix_nano"))),
            "group": e.get("group"),
            "queue": e.get("queue"),
            "ticks": e.get("process_order"),
            "sent_ms": e.get("sent_timestamp_ms") if op == "receive" else None,
            "first_receive_ms": e.get("first_receive_timestamp_ms") if op == "receive" else None,
        })
    return out


def observed_events(timeline: dict) -> list:
    """The store's observed events as (op, delivery, result, receive_count), in the order the store shows them."""
    return [(d["op"], d["delivery"], d["result"], d["receive_count"]) for d in observed_details(timeline)]


def cause_effect_pairs(truth: list) -> list:
    """(kind, cause, effect) for one message, from the truth. Cause and effect are (op, delivery, n): the
    n-th (0, 1, ...) truth call with that op and delivery number."""
    want = expected_events(truth)
    seen = collections.Counter()
    keyed = []  # (key, op, delivery, result, t_ns)
    for (op, delivery, result, _), r in zip(want, truth):
        key = (op, delivery, seen[(op, delivery)])
        seen[(op, delivery)] += 1
        keyed.append((key, op, delivery, result, int(r["t_ns"])))
    sends = [k for k in keyed if k[1] == "send"]
    receives = {k[2]: k for k in keyed if k[1] == "receive"}
    pairs = []
    for rcv in receives.values():
        pairs += [("send->receive", s[0], rcv[0]) for s in sends]
        nxt = receives.get(rcv[2] + 1)
        if nxt is not None:
            pairs.append(("receive->receive", rcv[0], nxt[0]))
    for k in keyed:
        if k[1] in ("delete", "change_visibility") and k[2] in receives:
            pairs.append(("receive->settle", receives[k[2]][0], k[0]))
        if k[1] == "change_visibility" and k[3] == "ok" and k[2] is not None:
            nxt = receives.get(k[2] + 1)
            if nxt is not None and k[4] < nxt[4]:
                pairs.append(("change_visibility->receive", k[0], nxt[0]))
    return pairs


class Proof:
    """The order a timeline proves: the transitive closure of its ``order.edges`` (event ID -> event ID)."""

    def __init__(self, timeline: dict):
        self.direct: dict = {}
        self.succ: dict = collections.defaultdict(set)
        for e in (timeline.get("order") or {}).get("edges", []):
            self.direct[(e["from"], e["to"])] = list(e.get("sources", []))
            self.succ[e["from"]].add(e["to"])
        self._reach: dict = {}

    def path(self, a, b):
        """A shortest chain of edges a -> ... -> b as a list of (from, to), or None."""
        if a is None or b is None:
            return None
        prev, todo = {a: None}, collections.deque([a])
        while todo:
            x = todo.popleft()
            for y in self.succ.get(x, ()):
                if y not in prev:
                    prev[y] = x
                    if y == b:
                        out = []
                        while prev[y] is not None:
                            out.append((prev[y], y))
                            y = prev[y]
                        return out[::-1]
                    todo.append(y)
        return None

    def before(self, a, b) -> bool:
        key = (a, b)
        if key not in self._reach:
            self._reach[key] = self.path(a, b) is not None
        return self._reach[key]

    def attribution(self, a, b) -> dict:
        """How the store proved a before b: ``direct`` sources of the edge a -> b, or the sources along the
        chain if there is no direct edge."""
        if (a, b) in self.direct:
            return {"direct": self.direct[(a, b)], "chain": None}
        chain = self.path(a, b) or []
        return {"direct": None, "chain": sorted({s for e in chain for s in self.direct[e]})}


def source_holds(src: str, a: dict, b: dict) -> bool:
    """Whether the observed fact ``src`` puts event ``a`` before event ``b``, recomputed here from the facts
    the store reports for the two events (their times, call-order ticks, queues, receive counts and SQS
    timestamps), independently of the store's own code. The four sources are those of contract section 9a."""
    if src == "clock":
        return b["start"] is not None and a["end"] is not None and a["end"] < b["start"]
    if src == "process_order":
        ta, tb = a.get("ticks") or {}, b.get("ticks") or {}
        return (ta.get("scope") is not None and ta.get("scope") == tb.get("scope")
                and ta.get("returned") is not None and tb.get("issued") is not None
                and int(ta["returned"]) < int(tb["issued"]))
    if src == "receive_count":
        return (a["op"] == b["op"] == "receive" and a["queue"] == b["queue"]
                and a["receive_count"] is not None and b["receive_count"] is not None
                and int(a["receive_count"]) < int(b["receive_count"]))
    if src == "sqs_timestamps":
        return (a["op"] == "send" and b["op"] == "receive" and a["queue"] == b["queue"] and b["receive_count"] == 1
                and b["sent_ms"] is not None and b["first_receive_ms"] is not None
                and int(b["sent_ms"]) < int(b["first_receive_ms"]))
    return False  # not a source of the contract: a rule, a cause, or a typo, none of which proves an order


def truth_bounds(r: dict) -> tuple:
    """(start, end) of a truth call: just before it was made and just after it returned, on the host's clock.
    Truth files from before QV-34 have only ``t_ns``, which the drivers take before a send or settle and after
    a receive, so only that bound is known."""
    start = r.get("t_start_ns", None if r["op"] == "receive" else r["t_ns"])
    end = r.get("t_end_ns", r["t_ns"] if r["op"] == "receive" else None)
    return _ns(start), _ns(end)


def same_process(ta, tb) -> bool:
    """Whether the truth shows two calls made by one process. A call missing from the truth can't be shown in
    another process, so it doesn't fail here (completeness is its own check)."""
    if ta is None or tb is None:
        return True
    return (ta.get("host"), ta.get("pid")) == (tb.get("host"), tb.get("pid"))


def check_facts(matched: list) -> dict:
    """SQS's own facts and the events' times, compared with the truth (QV-38; see the module docstring).

    ``sqs_facts`` fails a receive whose store ``sent_timestamp_ms`` or ``first_receive_timestamp_ms`` differs
    from what the driver saw in the same response (``sent_ms``, ``first_receive_ms`` in its truth line). A truth
    line from before QV-38 has neither, and is not compared. ``in_brackets`` fails an event whose start or end
    lies outside its call's truth bracket, or starts or ends further inside it than :data:`BRACKET_BOUNDS_NS`
    allows for the truth's clock, where the truth reads the spans' clock (``same_clock_as_spans``).
    ``matched`` is the (store event, truth call) pairs, matched by key (op, delivery, n) as in check_order, so
    an event is compared whether or not the store gives it an ID."""
    facts_wrong, compared, outside, bracketed = [], 0, [], 0
    worst = {"start": None, "end": None}  # the largest start - t_start_ns and t_end_ns - end seen
    for d, t in matched:
        if d["op"] == "receive" and ("sent_ms" in t or "first_receive_ms" in t):
            compared += 1
            got = {"sent_ms": d["sent_ms"], "first_receive_ms": d["first_receive_ms"]}
            want = {"sent_ms": t.get("sent_ms"), "first_receive_ms": t.get("first_receive_ms")}
            if {k: _ns(v) for k, v in got.items()} != {k: _ns(v) for k, v in want.items()}:
                facts_wrong.append({"event": d["id"], "delivery": d["delivery"], "store": got, "truth": want})
        if t.get("same_clock_as_spans"):
            lo, hi = truth_bounds(t)
            if lo is None or hi is None:
                continue
            bracketed += 1
            start, end = d["start"], d["end"]
            if start is not None and end is not None:
                for k, off in (("start", start - lo), ("end", hi - end)):
                    if worst[k] is None or off > worst[k]:
                        worst[k] = off
            max_start, max_end = BRACKET_BOUNDS_NS.get(t.get("clock"), BRACKET_BOUNDS_NS[None])
            if (start is None or end is None or not lo <= start <= end <= hi
                    or start - lo > max_start or hi - end > max_end):
                outside.append({"event": d["id"], "op": d["op"], "store": [start, end], "truth": [lo, hi]})
    return {"receives_compared": compared, "sqs_facts_wrong": facts_wrong,
            "events_in_brackets_checked": bracketed, "outside_brackets": outside, "bracket_offsets_max_ns": worst}


def truth_keys(truth: list) -> dict:
    """The truth calls of one message by key (op, delivery, n), as in cause_effect_pairs."""
    seen, out = collections.Counter(), {}
    for (op, delivery, _, _), r in zip(expected_events(truth), truth):
        out[(op, delivery, seen[(op, delivery)])] = r
        seen[(op, delivery)] += 1
    return out


def verify_edges(timeline: dict, details: list, truth_of: dict) -> dict:
    """Check every edge in ``order.edges`` instead of trusting it (QV-32 section 5).

    - **source**: each source an edge claims must recompute (:func:`source_holds`) from the two events'
      facts. An edge with no source, or between events that are not observed steps, fails too.
    - **process**: a ``process_order`` source holds only between two calls the truth shows were made by one
      process (same ``host`` and ``pid``): the call-order scope is a process (Python) or a realm in one
      (Node.js), so equal scopes across two processes are made up (QV-38).
    - **truth**: every order the edges imply (their transitive closure) is compared with the truth files.
      A before B is contradicted when the truth shows B's call returned before A's call was made
      (``B.t_end_ns < A.t_start_ns``). That is the only thing one host's brackets can prove; it holds for any
      two calls, causally related or not, so an edge made up by a rule fails here when the rule is wrong.

    ``truth_of`` maps a store event ID to its truth call (or is missing it). Returns the counts and the failing
    edges and orders."""
    by_id = {d["id"]: d for d in details if d.get("id") is not None}
    edges = (timeline.get("order") or {}).get("edges", [])
    unverified, contradicted = [], []
    succ = collections.defaultdict(set)
    for e in edges:
        a, b, sources = by_id.get(e.get("from")), by_id.get(e.get("to")), list(e.get("sources") or [])
        bad = [s for s in sources if a is None or b is None or not source_holds(s, a, b)
               or (s == "process_order" and not same_process(truth_of.get(a["id"]), truth_of.get(b["id"])))]
        if not sources or bad:
            unverified.append({"from": e.get("from"), "to": e.get("to"), "sources": sources,
                               "not_recomputed": bad if sources else ["(no source)"]})
        if a is not None and b is not None:
            succ[a["id"]].add(b["id"])
    implied = 0
    for x in list(succ):
        seen, todo = set(), list(succ[x])
        while todo:
            y = todo.pop()
            if y in seen or y == x:
                continue
            seen.add(y)
            todo.extend(succ.get(y, ()))
        for y in seen:
            implied += 1
            tx, ty = truth_of.get(x), truth_of.get(y)
            if tx is None or ty is None:
                continue
            x_start, _ = truth_bounds(tx)
            _, y_end = truth_bounds(ty)
            if x_start is not None and y_end is not None and y_end < x_start:
                contradicted.append({"from": x, "to": y, "truth": [tx["op"], ty["op"]],
                                     "why": "the truth shows the later call returned before the earlier one was made"})
    return {"edges": len(edges), "unverified": unverified, "implied": implied, "contradicted": contradicted,
            "ok": not unverified and not contradicted}


def judge_pair(cause: dict, effect: dict, proof: "Proof | None" = None) -> str:
    """``in_order``, ``concurrent`` or ``violated``: see the module docstring. ``proof`` is the store's
    proven order; without it (a timeline from before QV-31), only the intervals prove an order."""
    if cause["start"] is not None and effect["end"] < cause["start"]:
        return "violated"  # the effect definitely happened before its cause, and the store has the times
    gc, ge = cause["group"], effect["group"]
    if gc is None or ge is None:
        return "violated"  # an observed event outside any group: the store gives no order
    if proof is not None and proof.before(effect["id"], cause["id"]):
        return "violated"  # the store claims a proof that the effect came first
    if ge < gc:
        return "violated"  # shown before its cause
    if proof is not None:
        proven = proof.before(cause["id"], effect["id"])
    else:
        proven = gc < ge and effect["start"] is not None and cause["end"] < effect["start"]
    if proven:
        return "in_order"
    if gc < ge:
        return "violated"  # ordered by the store, but not proven
    return "concurrent"


def check_order(truth: list, timeline: dict) -> dict:
    """Judge every cause/effect pair of one message. Pairs whose events are missing are not judged."""
    obs = collections.defaultdict(list)
    for d in sorted(observed_details(timeline), key=lambda d: (d["end"], -1 if d["start"] is None else d["start"])):
        obs[(d["op"], d["delivery"])].append(d)
    found = {}
    for (op, delivery), ds in obs.items():
        for n, d in enumerate(ds):
            found[(op, delivery, n)] = d
    counts = {k: collections.Counter() for k in PAIR_KINDS}
    resolution = {k: collections.Counter() for k in RESOLUTION_KINDS}
    proof = Proof(timeline) if "order" in timeline else None
    violations = []
    for kind, c, e in cause_effect_pairs(truth):
        if c not in found or e not in found:
            continue
        verdict = judge_pair(found[c], found[e], proof)
        counts[kind][verdict] += 1
        rk = resolution_kind(kind, e[0])
        if rk is not None:
            res = resolution[rk]
            res["pairs"] += 1
            res[{"in_order": "resolved", "concurrent": "concurrent", "violated": "violated"}[verdict]] += 1
            if verdict == "in_order" and proof is not None:
                how = proof.attribution(found[c]["id"], found[e]["id"])
                if how["direct"] is not None:
                    for src in how["direct"]:
                        res["direct:" + src] += 1
                    if len(how["direct"]) == 1:
                        res["only:" + how["direct"][0]] += 1
                else:
                    res["via_chain"] += 1
                    for src in how["chain"]:
                        res["chain:" + src] += 1
        if verdict == "concurrent" and found[e]["start"] is not None:
            if found[c]["end"] < found[e]["start"]:
                counts[kind][CHAINED] += 1
            elif found[c]["end"] == found[e]["start"]:
                counts[kind][TIED] += 1
        if verdict == "violated":
            violations.append({"pair": kind, "cause": list(c), "effect": list(e)})
    groups = collections.Counter(d["group"] for d in observed_details(timeline) if d["group"] is not None)
    keyed = truth_keys(truth)
    truth_of = {d["id"]: keyed[k] for k, d in found.items() if k in keyed and d.get("id") is not None}
    return {"pairs": {k: {r: v[r] for r in PAIR_RESULTS + DIAGNOSTICS} for k, v in counts.items()},
            "resolution": {k: dict(v) for k, v in resolution.items()},
            "violations": violations,
            "edges": verify_edges(timeline, observed_details(timeline), truth_of),
            "facts": check_facts([(d, keyed[k]) for k, d in found.items() if k in keyed]),
            "concurrent_groups": sum(n > 1 for n in groups.values())}


def dlq_moves(truth: list, timeline: dict) -> dict:
    """The moves the truth implies (the queues the message was received on, in order: a move between each
    two) against the moves the timeline shows, observed and inferred."""
    queues = []
    for r in truth:
        q = r.get("queue")
        if r["op"] == "receive" and q and q not in queues:
            queues.append(q)
    want = list(zip(queues, queues[1:]))
    moves = [e for e in timeline.get("events", []) if e.get("type") == DLQ_MOVE]
    observed = [(e.get("from_queue"), e.get("to_queue")) for e in moves
                if not e.get("inferred") and e.get("source") == DLQ_SOURCE]
    inferred = [e for e in moves if e.get("inferred")]
    return {"expected": want, "observed": observed, "inferred": len(inferred),
            "ok": not inferred and len(observed) == len(moves) and sorted(observed) == sorted(want)}


def evaluate(truth: list, timeline) -> dict:
    """Check one message. ``timeline`` is the store's JSON for it, or None if the store doesn't know it."""
    want = expected_events(truth)
    if timeline is None:
        return {"found": False, **{c: False for c in CHECKS}, "passed": False,
                "expected": [w[0] for w in want], "observed": []}
    got = observed_events(timeline)
    complete = collections.Counter(g[0] for g in got) == collections.Counter(w[0] for w in want)
    order = check_order(truth, timeline)
    in_order = complete and not order["violations"]
    edges_verified = order["edges"]["ok"]
    # Joins and receive counts are compared as multisets, so they are measured whatever the order is.
    settles = ("delete", "change_visibility")
    joined = (collections.Counter((e[0], e[1], e[2]) for e in got if e[0] in settles)
              == collections.Counter((e[0], e[1], e[2]) for e in want if e[0] in settles))
    receive_count = (collections.Counter((e[1], e[3]) for e in got if e[0] == "receive")
                     == collections.Counter((e[1], e[3]) for e in want if e[0] == "receive"))
    facts = order["facts"]
    final = timeline.get("state") == "deleted"
    moves = dlq_moves(truth, timeline)
    res = {"found": True, "complete": complete, "in_order": in_order, "edges_verified": edges_verified, "joined": joined,
           "receive_count": receive_count, "sqs_facts": not facts["sqs_facts_wrong"],
           "in_brackets": not facts["outside_brackets"], "final": final, "dlq_move": moves["ok"]}
    res["passed"] = all(res[c] for c in CHECKS)
    res["expected"] = [w[0] for w in want]
    res["observed"] = [g[0] for g in got]
    res["pairs"] = order["pairs"]
    res["resolution"] = order["resolution"]
    res["violations"] = order["violations"]
    res["edges"] = order["edges"]
    res["facts"] = facts
    res["concurrent_groups"] = order["concurrent_groups"]
    res["inferred"] = [e["type"] for e in timeline.get("events", []) if e.get("inferred")]
    res["notices"] = [n["code"] for n in timeline.get("notices", [])]
    res["state"] = timeline.get("state")
    res["dlq_moves"] = moves
    return res


def resolution_table(counters: dict) -> dict:
    """Per resolution kind: pairs, resolved, concurrent, violated, resolved share, and by source.

    ``by_source`` counts the resolved pairs with a direct edge, once for each source that proves that edge
    (a pair can count under more than one); ``only_source`` those whose edge has exactly that one source;
    ``via_chain`` the resolved pairs with no direct edge, proven through other steps, and ``chain_sources``
    the sources used along those chains."""
    out = {}
    for k in RESOLUTION_KINDS:
        c = counters.get(k, collections.Counter())
        n = c["pairs"]
        out[k] = {"pairs": n, "resolved": c["resolved"], "concurrent": c["concurrent"], "violated": c["violated"],
                  "resolved_share": round(c["resolved"] / n, 4) if n else None,
                  "by_source": {s: c["direct:" + s] for s in ORDER_SOURCES},
                  "only_source": {s: c["only:" + s] for s in ORDER_SOURCES},
                  "via_chain": c["via_chain"],
                  "chain_sources": {s: c["chain:" + s] for s in ORDER_SOURCES}}
    return out


def floor_check(table: dict, floor: float = FLOOR, gates: bool = True) -> dict:
    """The resolved-share floor: each kind in FLOOR_KINDS must have at least ``floor`` of its pairs resolved.
    A kind with no pairs fails too: there is nothing to show the floor holds. With ``gates`` false the result
    is still computed and reported, but the run's result doesn't depend on it (the emulator: see
    demo/README.md#the-floor-in-ci)."""
    failed = [k for k in FLOOR_KINDS if table[k]["pairs"] == 0 or table[k]["resolved"] < floor * table[k]["pairs"]]
    return {"min_resolved_share": floor, "kinds": list(FLOOR_KINDS), "failed": failed,
            "result": "FAIL" if failed else "PASS", "gates": gates}


def summarize(truth_by_id: dict, results: dict, count: int, floor: float = FLOOR, floor_gates: bool = True) -> dict:
    sent = [m for m, t in truth_by_id.items() if any(r["op"] == "send" for r in t)]
    s = collections.Counter()
    for mid in sent:
        r = results[mid]
        s["found"] += r["found"]
        for c in CHECKS + ("passed",):
            s[c] += r[c]
    procs = {(r.get("host"), r.get("pid")) for t in truth_by_id.values() for r in t if r["op"] in ("receive", "delete")}
    deleters = {(r.get("host"), r.get("pid")) for t in truth_by_id.values() for r in t if r.get("by") == "deleter"}
    inferred = collections.Counter(i for mid in sent for i in results[mid].get("inferred", []))
    pairs = {k: collections.Counter() for k in PAIR_KINDS}
    for mid in sent:
        for k, v in results[mid].get("pairs", {}).items():
            pairs[k].update(v)
    total = collections.Counter()
    for v in pairs.values():
        total.update(v)
    resolution = {k: collections.Counter() for k in RESOLUTION_KINDS}
    for mid in sent:
        for k, v in results[mid].get("resolution", {}).items():
            resolution[k].update(v)
    table = resolution_table(resolution)
    floor_result = floor_check(table, floor, floor_gates)
    offsets = {"start": [], "end": []}
    for mid in sent:
        for k, v in results[mid].get("facts", {}).get("bracket_offsets_max_ns", {}).items():
            if v is not None:
                offsets[k].append(v)
    return {
        "expected_messages": count,
        "messages_sent": len(sent),
        **{k: s[k] for k in ("found",) + CHECKS + ("passed",)},
        "failed_only_in_order": sum(not results[m]["in_order"] and all(results[m][c] for c in CHECKS if c != "in_order")
                                    for m in sent),
        # So that in_order cannot pass vacuously (everything in one concurrent group): how much was resolved.
        "with_concurrent_group": sum(results[m].get("concurrent_groups", 0) > 0 for m in sent),
        "pairs_in_order": total["in_order"],
        "pairs_concurrent": total["concurrent"],
        "pairs_violated": total["violated"],
        "pairs_concurrent_via_chain": total[CHAINED],
        "pairs_concurrent_tied": total[TIED],
        "pairs_by_kind": {k: {r: pairs[k][r] for r in PAIR_RESULTS + DIAGNOSTICS} for k in PAIR_KINDS},
        # Every edge the store claims, checked rather than trusted (QV-34): its sources recomputed from the
        # events' facts, and every order the edges imply compared with the truth files.
        "edges_checked": sum(results[m].get("edges", {}).get("edges", 0) for m in sent),
        "edges_unverified": sum(len(results[m].get("edges", {}).get("unverified", [])) for m in sent),
        "implied_orders_checked": sum(results[m].get("edges", {}).get("implied", 0) for m in sent),
        "implied_orders_contradicting_truth": sum(len(results[m].get("edges", {}).get("contradicted", []))
                                                  for m in sent),
        # SQS's own facts and the events' times against the truth (QV-38), with how much was compared, so a
        # pass with nothing compared shows as such.
        "receives_sqs_facts_compared": sum(results[m].get("facts", {}).get("receives_compared", 0) for m in sent),
        "receives_sqs_facts_wrong": sum(len(results[m].get("facts", {}).get("sqs_facts_wrong", [])) for m in sent),
        "events_checked_in_brackets": sum(results[m].get("facts", {}).get("events_in_brackets_checked", 0)
                                          for m in sent),
        "events_outside_brackets": sum(len(results[m].get("facts", {}).get("outside_brackets", [])) for m in sent),
        # The largest offsets of an event's start after its call's truth start, and of its end before its truth
        # end, over the run (ns; the bounds are BRACKET_BOUNDS_NS), so the margin an honest store leaves shows.
        "bracket_offsets_max_ns": {k: max(offsets[k], default=None) for k in ("start", "end")},
        "bracket_bounds_ns": {str(k): {"start": v[0], "end": v[1]} for k, v in BRACKET_BOUNDS_NS.items()},
        "with_change_visibility": sum(any(r["op"] == "change_visibility" for r in truth_by_id[m]) for m in sent),
        "deleted_by_another_process": sum(any(r.get("by") == "deleter" for r in truth_by_id[m]) for m in sent),
        "received_more_than_once": sum(sum(r["op"] == "receive" for r in truth_by_id[m]) > 1 for m in sent),
        "consumer_threads_seen": len({(r.get("host"), r.get("pid"), r.get("thread")) for t in truth_by_id.values()
                                      for r in t if r["op"] == "receive"}),
        "processes_seen": len(procs - deleters),
        "deleter_processes_seen": len(deleters),
        "inferred_events": dict(inferred),
        "dlq_messages": sum(bool(results[m].get("dlq_moves", {}).get("expected")) for m in sent),
        "dlq_moves_observed": sum(len(results[m].get("dlq_moves", {}).get("observed", [])) for m in sent),
        "dlq_moves_inferred": sum(results[m].get("dlq_moves", {}).get("inferred", 0) for m in sent),
        "resolution": table,
        "floor": floor_result,
        "messages_result": "PASS" if s["passed"] == count == len(sent) else "FAIL",
        "result": "PASS" if s["passed"] == count == len(sent)
                  and (floor_result["result"] == "PASS" or not floor_gates) else "FAIL",
    }


def fetch_timeline(store: str, mid: str):
    url = f"{store.rstrip('/')}/api/messages/{urllib.parse.quote(mid, safe='')}"
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


def cmd_wait(args) -> int:
    deadline = time.time() + args.timeout
    while True:
        truth = load_truth(args.truth)
        sent = [m for m, t in truth.items() if any(r["op"] == "send" for r in t)]
        done = [m for m in sent if any(r["op"] == "delete" and r.get("result") == "ok" for r in truth[m])]
        if len(sent) >= args.count and len(done) >= len(sent):
            print(f"all {len(sent)} sent messages have an accepted delete", flush=True)
            return 0
        if time.time() > deadline:
            print(f"timed out after {args.timeout} s: {len(done)} of {len(sent)} sent messages deleted", flush=True)
            return 1
        time.sleep(1)


def cmd_verify(args) -> int:
    truth = load_truth(args.truth)
    sent = [m for m, t in truth.items() if any(r["op"] == "send" for r in t)]
    deadline = time.time() + args.timeout
    results = {}
    while True:
        for mid in sent:
            if mid not in results or not results[mid]["passed"]:
                results[mid] = evaluate(truth[mid], fetch_timeline(args.store, mid))
        # Keep polling only while some message is still missing events (the export may be in flight).
        waiting = [m for m in sent if not results[m]["passed"]
                   and len(results[m]["observed"]) < len(results[m]["expected"])]
        if not waiting or time.time() > deadline:
            break
        time.sleep(2)
    summary = summarize(truth, results, args.count, args.floor, not args.floor_report_only)
    summary["seconds_waited_for_spans"] = round(args.timeout - max(0.0, deadline - time.time()), 1)
    failed = {m: r for m, r in results.items() if not r["passed"]}
    report = {"summary": summary, "failed": failed, "messages": results}
    if args.report:
        with open(args.report, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=1)
    print(json.dumps(summary, indent=1))
    worst = summary["bracket_offsets_max_ns"]
    ms = lambda v: "n/a" if v is None else f"{v / 1e6:.3f} ms"  # noqa: E731
    print(f"BRACKETS: {summary['events_checked_in_brackets']} events checked, "
          f"{summary['events_outside_brackets']} outside their call's truth bracket or too far inside it; largest "
          f"offsets from the truth: start {ms(worst['start'])}, end {ms(worst['end'])}", flush=True)
    if summary["floor"]["result"] != "PASS":
        print(("FLOOR NOT MET (reported, not gating: --floor-report-only)" if args.floor_report_only
               else "FLOOR FAILED") + ": fewer than {:.0%} of the pairs resolved for {}".format(
            summary["floor"]["min_resolved_share"], ", ".join(summary["floor"]["failed"])) + "; " + ", ".join(
            f"{k} {summary['resolution'][k]['resolved']}/{summary['resolution'][k]['pairs']}"
            for k in summary["floor"]["kinds"]), flush=True)
    for mid, r in list(failed.items())[:10]:
        print(f"FAILED {mid}: " + ", ".join(f"{c}={r[c]}" for c in ("found",) + CHECKS)
              + f"; expected {r['expected']}, store shows {r['observed']}"
              + (f"; violated pairs {r['violations']}" if r.get("violations") else "")
              + (f"; edges not recomputed {r['edges']['unverified'][:3]}" if r.get("edges", {}).get("unverified") else "")
              + (f"; edges against the truth {r['edges']['contradicted'][:3]}"
                 if r.get("edges", {}).get("contradicted") else "")
              + (f"; SQS facts against the truth {r['facts']['sqs_facts_wrong'][:3]}"
                 if r.get("facts", {}).get("sqs_facts_wrong") else "")
              + (f"; times outside the truth's brackets {r['facts']['outside_brackets'][:3]}"
                 if r.get("facts", {}).get("outside_brackets") else ""), flush=True)
    return 0 if summary["result"] == "PASS" else 1


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("wait", "verify"):
        s = sub.add_parser(name)
        s.add_argument("--truth", required=True, help="directory of the drivers' truth files")
        s.add_argument("--count", type=int, default=200, help="number of messages the producer sent")
    sub.choices["wait"].add_argument("--timeout", type=float, default=180)
    sub.choices["verify"].add_argument("--store", default="http://qv2:4318")
    sub.choices["verify"].add_argument("--timeout", type=float, default=60, help="seconds to wait for spans to arrive")
    sub.choices["verify"].add_argument("--report", help="write the full per-message report here (JSON)")
    sub.choices["verify"].add_argument("--floor", type=float, default=FLOOR,
                                       help="least share of send->receive and receive->delete pairs that must be "
                                            "resolved (default %(default)s)")
    sub.choices["verify"].add_argument("--floor-report-only", action="store_true",
                                       help="report the floor but don't fail the run on it (the emulator, where "
                                            "SQS's two timestamps tie: demo/README.md#the-floor-in-ci)")
    args = p.parse_args(argv)
    return {"wait": cmd_wait, "verify": cmd_verify}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
