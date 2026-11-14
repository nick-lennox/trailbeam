"""qv2 0.1 concurrency check: compare the timelines in the qv2 store with what the demo drivers really did.

    check.py wait   --truth DIR --count 200 [--timeout 180]
    check.py verify --truth DIR --count 200 --store http://qv2:4318 [--timeout 60] [--report FILE]

The drivers (demo/python/qv2demo.py, demo/node/qv2demo.js) write one truth line per SQS call they make
(send, receive, change_visibility, delete) to their own file in DIR. ``wait`` returns once every sent
message has an accepted delete in the truth. ``verify`` reads each message's timeline from the store's
JSON API (GET /api/messages/{id}) and checks, per message:

- complete:      the observed events (sent, received, visibility_changed, deleted) are exactly the SQS
                 calls in the truth, no more and no fewer;
- in order:      in the order the store shows them, they are the truth's calls in the order they happened;
- joined:        every delete and visibility change is attached to the delivery whose receipt handle it
                 really used, and has the same result (ok / failed) as the real call;
- receive count: every receive shows the real ApproximateReceiveCount;
- final:         the message's state is ``deleted``.

A message passes only if all five hold. Inferred events (such as the visibility-timeout expiry after a
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


def observed_events(timeline: dict) -> list:
    """The store's observed events as (op, delivery, result, receive_count), in the order the store shows them."""
    out = []
    for e in timeline.get("events", []):
        if e.get("inferred") or e.get("type") not in STORE_TYPES:
            continue
        op = STORE_TYPES[e["type"]]
        if op == "receive":
            out.append((op, e.get("delivery"), "ok", e.get("receive_count")))
        elif op == "send":
            out.append((op, None, "ok", None))
        else:
            out.append((op, e.get("delivery"), e.get("result"), None))
    return out


def evaluate(truth: list, timeline) -> dict:
    """Check one message. ``timeline`` is the store's JSON for it, or None if the store doesn't know it."""
    want = expected_events(truth)
    if timeline is None:
        return {"found": False, **{c: False for c in CHECKS}, "passed": False,
                "expected": [w[0] for w in want], "observed": []}
    got = observed_events(timeline)
    complete = collections.Counter(g[0] for g in got) == collections.Counter(w[0] for w in want)
    in_order = complete and [g[0] for g in got] == [w[0] for w in want]
    joined = in_order and all(g[1] == w[1] and g[2] == w[2] for g, w in zip(got, want) if w[0] in ("delete", "change_visibility"))
    receive_count = in_order and all(g[3] == w[3] for g, w in zip(got, want) if w[0] == "receive")
    final = timeline.get("state") == "deleted"
    res = {"found": True, "complete": complete, "in_order": in_order, "joined": joined,
           "receive_count": receive_count, "final": final}
    res["passed"] = all(res[c] for c in CHECKS)
    res["expected"] = [w[0] for w in want]
    res["observed"] = [g[0] for g in got]
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
    return {
        "expected_messages": count,
        "messages_sent": len(sent),
        **{k: s[k] for k in ("found",) + CHECKS + ("passed",)},
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
              + f"; expected {r['expected']}, store shows {r['observed']}", flush=True)
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
