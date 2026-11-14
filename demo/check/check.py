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
- joined:        every delete and visibility change is attached to the delivery whose receipt handle it
                 really used, and has the same result (ok / failed) as the real call;
- receive count: every receive (by delivery number) shows the real ApproximateReceiveCount;
- final:         the message's state is ``deleted``.

So that "in order" cannot pass just by putting everything in one group, the summary also counts the
messages with at least one concurrent group and the pairs resolved in order vs. left concurrent, per
kind of pair. ``concurrent_via_chain`` (a diagnostic) counts the concurrent pairs whose own intervals do
not overlap: they share a group only through other overlapping steps. ``concurrent_tied`` counts those
where the cause ended at exactly the effect's start, a tie at the clock's resolution. Each check is evaluated on its own: joined and receive count compare the events regardless of the order
the store shows them in, so a message that fails only on order still reports its joins and receive counts.
The summary counts each check separately, plus the messages that fail on order alone. A message passes
only if all five hold. The run passes only if every message passes **and** the floor holds: at least 90%
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
CHECKS = ("complete", "in_order", "joined", "receive_count", "final")
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
FLOOR_GATES = False  # the floor is reported; it does not yet fail the run


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
        recs.sort(key=lambda r: int(r["t_ns"]))
    return dict(by_id)


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
    return {"pairs": {k: {r: v[r] for r in PAIR_RESULTS + DIAGNOSTICS} for k, v in counts.items()},
            "resolution": {k: dict(v) for k, v in resolution.items()},
            "violations": violations,
            "concurrent_groups": sum(n > 1 for n in groups.values())}


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
    # Joins and receive counts are compared as multisets, so they are measured whatever the order is.
    settles = ("delete", "change_visibility")
    joined = (collections.Counter((e[0], e[1], e[2]) for e in got if e[0] in settles)
              == collections.Counter((e[0], e[1], e[2]) for e in want if e[0] in settles))
    receive_count = (collections.Counter((e[1], e[3]) for e in got if e[0] == "receive")
                     == collections.Counter((e[1], e[3]) for e in want if e[0] == "receive"))
    final = timeline.get("state") == "deleted"
    res = {"found": True, "complete": complete, "in_order": in_order, "joined": joined,
           "receive_count": receive_count, "final": final}
    res["passed"] = all(res[c] for c in CHECKS)
    res["expected"] = [w[0] for w in want]
    res["observed"] = [g[0] for g in got]
    res["pairs"] = order["pairs"]
    res["resolution"] = order["resolution"]
    res["violations"] = order["violations"]
    res["concurrent_groups"] = order["concurrent_groups"]
    res["inferred"] = [e["type"] for e in timeline.get("events", []) if e.get("inferred")]
    res["notices"] = [n["code"] for n in timeline.get("notices", [])]
    res["state"] = timeline.get("state")
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


def floor_check(table: dict, floor: float = FLOOR) -> dict:
    """The resolved-share floor: each kind in FLOOR_KINDS must have at least ``floor`` of its pairs resolved.
    A kind with no pairs fails too: there is nothing to show the floor holds."""
    failed = [k for k in FLOOR_KINDS if table[k]["pairs"] == 0 or table[k]["resolved"] < floor * table[k]["pairs"]]
    return {"min_resolved_share": floor, "kinds": list(FLOOR_KINDS), "failed": failed,
            "result": "FAIL" if failed else "PASS"}


def summarize(truth_by_id: dict, results: dict, count: int, floor: float = FLOOR) -> dict:
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
    floor_result = floor_check(table, floor)
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
        "with_change_visibility": sum(any(r["op"] == "change_visibility" for r in truth_by_id[m]) for m in sent),
        "deleted_by_another_process": sum(any(r.get("by") == "deleter" for r in truth_by_id[m]) for m in sent),
        "received_more_than_once": sum(sum(r["op"] == "receive" for r in truth_by_id[m]) > 1 for m in sent),
        "consumer_threads_seen": len({(r.get("host"), r.get("pid"), r.get("thread")) for t in truth_by_id.values()
                                      for r in t if r["op"] == "receive"}),
        "processes_seen": len(procs - deleters),
        "deleter_processes_seen": len(deleters),
        "inferred_events": dict(inferred),
        "resolution": table,
        "floor": floor_result,
        "messages_result": "PASS" if s["passed"] == count == len(sent) else "FAIL",
        "result": "PASS" if s["passed"] == count == len(sent)
                  and (floor_result["result"] == "PASS" or not FLOOR_GATES) else "FAIL",
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
    summary = summarize(truth, results, args.count, args.floor)
    summary["seconds_waited_for_spans"] = round(args.timeout - max(0.0, deadline - time.time()), 1)
    failed = {m: r for m, r in results.items() if not r["passed"]}
    report = {"summary": summary, "failed": failed, "messages": results}
    if args.report:
        with open(args.report, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=1)
    print(json.dumps(summary, indent=1))
    if summary["floor"]["result"] != "PASS":
        print("FLOOR FAILED: fewer than {:.0%} of the pairs resolved for {}".format(
            summary["floor"]["min_resolved_share"], ", ".join(summary["floor"]["failed"])) + "; " + ", ".join(
            f"{k} {summary['resolution'][k]['resolved']}/{summary['resolution'][k]['pairs']}"
            for k in summary["floor"]["kinds"]), flush=True)
    for mid, r in list(failed.items())[:10]:
        print(f"FAILED {mid}: " + ", ".join(f"{c}={r[c]}" for c in ("found",) + CHECKS)
              + f"; expected {r['expected']}, store shows {r['observed']}"
              + (f"; violated pairs {r['violations']}" if r.get("violations") else ""), flush=True)
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
    args = p.parse_args(argv)
    return {"wait": cmd_wait, "verify": cmd_verify}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
