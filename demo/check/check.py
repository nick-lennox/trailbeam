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
                 the truth has the change first). With each event's interval (start, end) and group from
                 the store, a pair is
                   violated   if the effect's interval ends before the cause's starts, or the store puts
                              the effect in an earlier group than the cause, or puts the cause in an
                              earlier group although the intervals do not prove it (cause end < effect
                              start);
                   concurrent if both are in one group ("concurrent, order unknown");
                   in order   otherwise.
                 A message is in order when it is complete and no pair is violated;
- joined:        every delete and visibility change is attached to the delivery whose receipt handle it
                 really used, and has the same result (ok / failed) as the real call;
- receive count: every receive (by delivery number) shows the real ApproximateReceiveCount;
- final:         the message's state is ``deleted``.

So that "in order" cannot pass just by putting everything in one group, the summary also counts the
messages with at least one concurrent group and the pairs resolved in order vs. left concurrent, per
kind of pair. Each check is evaluated on its own: joined and receive count compare the events regardless of the order
the store shows them in, so a message that fails only on order still reports its joins and receive counts.
The summary counts each check separately, plus the messages that fail on order alone. A message passes
only if all five hold. Inferred events (such as the visibility-timeout expiry after a
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


def judge_pair(cause: dict, effect: dict) -> str:
    """``in_order``, ``concurrent`` or ``violated``: see the module docstring."""
    if cause["start"] is not None and effect["end"] < cause["start"]:
        return "violated"  # the effect definitely happened before its cause, and the store has the times
    gc, ge = cause["group"], effect["group"]
    if gc is None or ge is None:
        return "violated"  # an observed event outside any group: the store gives no order
    if ge < gc:
        return "violated"  # shown before its cause
    if gc == ge:
        return "concurrent"
    if effect["start"] is None or not cause["end"] < effect["start"]:
        return "violated"  # ordered by the store, but the intervals do not prove it
    return "in_order"


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
    violations = []
    for kind, c, e in cause_effect_pairs(truth):
        if c not in found or e not in found:
            continue
        verdict = judge_pair(found[c], found[e])
        counts[kind][verdict] += 1
        if verdict == "violated":
            violations.append({"pair": kind, "cause": list(c), "effect": list(e)})
    groups = collections.Counter(d["group"] for d in observed_details(timeline) if d["group"] is not None)
    return {"pairs": {k: {r: v[r] for r in PAIR_RESULTS} for k, v in counts.items()},
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
    res["violations"] = order["violations"]
    res["concurrent_groups"] = order["concurrent_groups"]
    res["inferred"] = [e["type"] for e in timeline.get("events", []) if e.get("inferred")]
    res["notices"] = [n["code"] for n in timeline.get("notices", [])]
    res["state"] = timeline.get("state")
    return res


def summarize(truth_by_id: dict, results: dict, count: int) -> dict:
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
        "pairs_by_kind": {k: {r: pairs[k][r] for r in PAIR_RESULTS} for k in PAIR_KINDS},
        "with_change_visibility": sum(any(r["op"] == "change_visibility" for r in truth_by_id[m]) for m in sent),
        "deleted_by_another_process": sum(any(r.get("by") == "deleter" for r in truth_by_id[m]) for m in sent),
        "received_more_than_once": sum(sum(r["op"] == "receive" for r in truth_by_id[m]) > 1 for m in sent),
        "consumer_threads_seen": len({(r.get("host"), r.get("pid"), r.get("thread")) for t in truth_by_id.values()
                                      for r in t if r["op"] == "receive"}),
        "processes_seen": len(procs - deleters),
        "deleter_processes_seen": len(deleters),
        "inferred_events": dict(inferred),
        "result": "PASS" if s["passed"] == count == len(sent) else "FAIL",
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
    summary = summarize(truth, results, args.count)
    summary["seconds_waited_for_spans"] = round(args.timeout - max(0.0, deadline - time.time()), 1)
    failed = {m: r for m, r in results.items() if not r["passed"]}
    report = {"summary": summary, "failed": failed, "messages": results}
    if args.report:
        with open(args.report, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=1)
    print(json.dumps(summary, indent=1))
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
    args = p.parse_args(argv)
    return {"wait": cmd_wait, "verify": cmd_verify}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
