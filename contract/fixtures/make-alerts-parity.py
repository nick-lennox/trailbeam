"""Writes alerts-parity.json: the alert rule cases both engines must agree on (QV-627).

Self-hosted Trailbeam (server/src/trailbeam/server/alerts.py, python/tests/server/test_alerts.py) and the hosted app
(lib/alerts/evaluate.ts and cause-rule.ts, its test/alerts-parity.test.ts, reading @trailbeam/ui/fixtures/alerts-parity.json)
run every case and must give the expected result. The expected results are the Python engine's; the hosted app's
test is what holds evaluate.ts to them. Run from the repository root after changing a case or the engine:

    PYTHONPATH=python/src:server/src python contract/fixtures/make-alerts-parity.py

and check the hosted app's test still passes on the new file before merging.
"""
import copy
import json
from pathlib import Path

from trailbeam.server import alerts

OUT = Path(__file__).resolve().parent / "alerts-parity.json"
H = 3_600_000
MIN = 60_000
T0 = 1_764_000_000_000  # 2025-11-24T16:00:00Z, ms

TONE = [
    ("no rule", 5, None),
    ("a rule with no limit", 5, {}),
    ("a rule with only below set", 5, {"below": True}),
    ("warn only, under", 4, {"warn": 5}),
    ("warn only, at the limit: not over", 5, {"warn": 5}),
    ("warn only, over", 6, {"warn": 5}),
    ("crit only, over", 11, {"crit": 10}),
    ("crit only, under", 9, {"crit": 10}),
    ("both, under warn", 1, {"warn": 5, "crit": 10}),
    ("both, between", 7, {"warn": 5, "crit": 10}),
    ("both, at crit: only warning", 10, {"warn": 5, "crit": 10}),
    ("both, over crit", 10.5, {"warn": 5, "crit": 10}),
    ("crit under warn: crit wins when both are passed", 20, {"warn": 15, "crit": 10}),
    ("crit under warn, between them: critical", 12, {"warn": 15, "crit": 10}),
    ("a zero limit counts", 0.001, {"crit": 0}),
    ("negative figures", -3, {"warn": -5, "crit": -1}),
    ("below: warn and crit", 8, {"warn": 10, "crit": 5, "below": True}),
    ("below: under crit", 4, {"warn": 10, "crit": 5, "below": True}),
    ("below: at warn is not under", 10, {"warn": 10, "crit": 5, "below": True}),
    ("below: over both", 50, {"warn": 10, "crit": 5, "below": True}),
    ("below false is above", 50, {"warn": 10, "below": False}),
]


def obs(widget, tone, dashboard="d1", **kw):
    o = {"dashboardId": dashboard, "dashboardName": "Orders", "widgetId": widget, "title": widget.title(), "tone": tone,
         "value": "7", "limit": "above 5", "queue": "orders"}
    o.update(kw)
    return o


def stored(widget, tone, since, checked, **kw):
    return {**obs(widget, tone, **kw), "since": since, "checkedAt": checked}


DIFF = [
    ("nothing before, nothing now", [], [], T0),
    ("first seen OK: stored, nothing sent", [], [obs("w1", "good")], T0),
    ("first seen firing: fired from nothing", [], [obs("w1", "warning"), obs("w2", "critical")], T0),
    ("stays critical: not sent again, since kept", [stored("w1", "critical", T0 - H, T0 - MIN)], [obs("w1", "critical", value="9")], T0),
    ("OK to warning: fired", [stored("w1", "good", T0 - H, T0 - MIN)], [obs("w1", "warning")], T0),
    ("OK to critical: fired", [stored("w1", "good", T0 - H, T0 - MIN)], [obs("w1", "critical")], T0),
    ("warning to critical: changed", [stored("w1", "warning", T0 - H, T0 - MIN)], [obs("w1", "critical")], T0),
    ("critical to warning: changed", [stored("w1", "critical", T0 - H, T0 - MIN)], [obs("w1", "warning")], T0),
    ("critical to OK: resolved", [stored("w1", "critical", T0 - H, T0 - MIN)], [obs("w1", "good")], T0),
    ("a firing rule deleted: dropped, no longer checked", [stored("w1", "critical", T0 - H, T0 - MIN), stored("w2", "good", T0 - H, T0 - MIN)],
     [obs("w2", "good")], T0),
    ("an OK rule deleted: dropped without a message", [stored("w1", "good", T0 - H, T0 - MIN)], [], T0, []),
    ("a warning rule deleted: no longer checked", [stored("w1", "warning", T0 - H, T0 - MIN)], [], T0, []),
    ("no figure for one check: kept as it was, nothing sent", [stored("w1", "critical", T0 - H, T0 - MIN)], [], T0, ["d1/w1"]),
    ("the figure back, still critical: the same incident", [stored("w1", "critical", T0 - H, T0 - 10 * MIN)],
     [obs("w1", "critical")], T0, ["d1/w1"]),
    ("the figure back and OK: resolved", [stored("w1", "critical", T0 - H, T0 - 10 * MIN)], [obs("w1", "good")], T0, ["d1/w1"]),
    ("no figure, 1 ms short of the limit: still kept", [stored("w1", "critical", T0 - H, T0 - alerts.GONE_AFTER_MS + 1)], [], T0, ["d1/w1"]),
    ("no figure for the whole limit: dropped, no longer checked", [stored("w1", "critical", T0 - H, T0 - alerts.GONE_AFTER_MS)], [], T0,
     ["d1/w1"]),
    ("an Alert me rule gone: dropped without a message", [stored("r1", "critical", T0 - H, T0 - MIN, dashboard="rule")], [], T0, []),
    ("same widget ID on two dashboards", [stored("w1", "good", T0 - H, T0 - MIN, dashboard="d1")],
     [obs("w1", "critical", dashboard="d1"), obs("w1", "critical", dashboard="d2")], T0),
    ("the order of now is kept", [stored("b", "warning", T0 - H, T0 - MIN), stored("a", "good", T0 - H, T0 - MIN)],
     [obs("c", "critical"), obs("a", "good"), obs("b", "good")], T0),
]


def cause_obs(tone, mode, new_count):
    """A cause rule's figure, as check_cause_rule makes it, for the diff cases."""
    return obs("r1", tone, dashboard="rule", dashboardName="", value=str(new_count),
               rule={"kind": "cause", "ruleId": "r1", "mode": mode, "newCount": new_count})


def cause_stored(tone, mode, since, checked):
    return {**cause_obs(tone, mode, 1), "since": since, "checkedAt": checked}


DIFF += [
    ("every: new messages while already firing: fired again, since kept",
     [cause_stored("critical", "every", T0 - 10 * MIN, T0 - 5 * MIN)], [cause_obs("critical", "every", 2)], T0),
    ("every: still firing with no new messages is not sent",
     [cause_stored("critical", "every", T0 - 10 * MIN, T0 - 5 * MIN)], [cause_obs("critical", "every", 0)], T0),
    ("quiet: new messages while already firing: not sent again",
     [cause_stored("critical", "quiet", T0 - 10 * MIN, T0 - 5 * MIN)], [cause_obs("critical", "quiet", 2)], T0),
]


def rule(**kw):
    r = {"id": "r1", "kind": "cause", "causeKey": "v1|orders|handler|exc:TimeoutError:abc", "causeId": "c-abc",
         "label": "orders-handler raised TimeoutError", "queue": "orders", "channels": ["ch1"], "mode": "quiet",
         "quietS": 6 * 3600, "mutedUntil": None, "mutedBy": None, "mutedAt": None, "createdBy": "u1",
         "createdAt": T0 - 48 * H, "updatedAt": T0 - 48 * H}
    r.update(kw)
    return r


def group(gid="c-abc", key="v1|orders|handler|exc:TimeoutError:abc", count=2, **kw):
    g = {"id": gid, "key": key, "label": "orders-handler raised TimeoutError", "queue": "orders", "count": count,
         "basis": "exception", "inferred": False}
    g.update(kw)
    return g


def read(groups, cause_of, capped=False):
    return {"groups": groups, "causeOf": cause_of, "capped": capped}


def mem(seen, last_new_at, was_muted=False):
    return {"seen": seen, "lastNewAt": last_new_at, "wasMuted": was_muted}


FIRING = {"tone": "critical"}
OK = {"tone": "good"}
R2 = read([group()], {"m1": "c-abc", "m2": "c-abc", "x": "other"})
R3 = read([group(count=3)], {"m1": "c-abc", "m2": "c-abc", "m3": "c-abc"})
QUIET = 6 * 3600 * 1000

CAUSE = [
    ("the first check only notes what is there", rule(), mem(None, None), None, R2, T0, None),
    ("the first check of an every rule: nothing sent", rule(mode="every", quietS=0), mem(None, None), None, R2, T0, None),
    ("every: new messages fire", rule(mode="every", quietS=0), mem(["m1"], T0 - H), OK, R2, T0, T0 - 10 * MIN),
    ("every: none new resolves", rule(mode="every", quietS=0), mem(["m1", "m2"], T0 - H), FIRING, R2, T0, T0 - 10 * MIN),
    ("quiet: new after the quiet period fires", rule(), mem(["m1"], T0 - QUIET - H), OK, R2, T0, T0 - 10 * MIN),
    ("quiet: new inside the quiet period does not", rule(), mem(["m1"], T0 - H), OK, R2, T0, T0 - 10 * MIN),
    ("quiet: new while firing stays firing", rule(), mem(["m1"], T0 - H), FIRING, R2, T0, T0 - 10 * MIN),
    ("quiet: no new, inside the period, still firing", rule(), mem(["m1", "m2"], T0 - H), FIRING, R2, T0, T0 - 10 * MIN),
    ("quiet: no new, the period over, resolves", rule(), mem(["m1", "m2"], T0 - QUIET), FIRING, R2, T0, T0 - 10 * MIN),
    ("quiet: the period counts as over 5 min early", rule(), mem(["m1", "m2"], T0 - QUIET + 5 * MIN), FIRING, R2, T0, T0 - 10 * MIN),
    ("quiet: 1 ms short of the slack is not over", rule(), mem(["m1", "m2"], T0 - QUIET + 5 * MIN + 1), FIRING, R2, T0, T0 - 10 * MIN),
    ("quiet: new exactly at the early end fires", rule(), mem(["m1"], T0 - QUIET + 5 * MIN), OK, R2, T0, T0 - 10 * MIN),
    ("quiet: no new, never fired, stays OK", rule(), mem(["m1", "m2"], T0 - 30 * H), OK, R2, T0, T0 - 10 * MIN),
    ("a message that left the cause is not new", rule(), mem(["m1", "m2", "m0"], T0 - 30 * H), OK, R2, T0, T0 - 10 * MIN),
    ("seen but no lastNewAt: counts from now", rule(), mem(["m1"], None), OK, R2, T0, None),
    ("30 min quiet, in minutes", rule(quietS=1800), mem(["m1"], T0 - H), OK, R3, T0, T0 - 10 * MIN),
    ("7 days quiet", rule(quietS=7 * 86_400), mem([], T0 - 8 * 24 * H), OK, R3, T0, T0 - 10 * MIN),
    ("muted: worked out as usual, muted said", rule(mutedUntil=T0 + H, mutedBy="u1", mutedAt=T0 - H), mem(["m1"], T0 - QUIET - H),
     OK, R2, T0, T0 - 10 * MIN),
    ("muted forever", rule(mutedUntil=alerts.FOREVER, mutedBy="u1", mutedAt=T0 - H), mem(["m1"], T0 - QUIET - H), OK, R2, T0, None),
    ("a snooze that ends now is over", rule(mutedUntil=T0, mutedBy="u1", mutedAt=T0 - H), mem(["m1"], T0 - QUIET - H, True),
     FIRING, R2, T0, T0 - 10 * MIN),
    ("the cause gone: the rule's own label, nothing seen", rule(), mem(["m1"], T0 - H), FIRING, read([], {}), T0, T0 - 10 * MIN),
    ("error texts merged into one group: still the rule's cause",
     rule(), mem(["m1"], T0 - QUIET - H), OK,
     read([group("c-all", "v1|orders|handler|exc:TimeoutError:*", 5, label="orders-handler raised TimeoutError (5 texts)")],
          {"m1": "c-all", "m2": "c-all", "m9": "c-all"}), T0, T0 - 10 * MIN),
    ("a rule on the merged group matches the texts split again",
     rule(causeKey="v1|orders|handler|exc:TimeoutError:*", causeId="c-all"), mem(["m1"], T0 - QUIET - H), OK,
     read([group("c-a", "v1|orders|handler|exc:TimeoutError:a", 1, label="a"), group("c-b", "v1|orders|handler|exc:TimeoutError:b", 4, label="b", queue="orders-2")],
          {"m1": "c-a", "m2": "c-b"}), T0, T0 - 10 * MIN),
    ("two groups of equal size: the first names it",
     rule(causeKey="v1|orders|handler|exc:TimeoutError:*", causeId="c-all"), mem(None, None), None,
     read([group("c-a", "v1|orders|handler|exc:TimeoutError:a", 2, label="a"), group("c-b", "v1|orders|handler|exc:TimeoutError:b", 2, label="b")],
          {"m1": "c-a", "m2": "c-b"}), T0, None),
    ("a merge only joins exception groups",
     rule(causeKey="v1|orders|handler|status:500"), mem(["m1"], T0 - QUIET - H), OK,
     read([group("c-s", "v1|orders|handler|status:*", 3)], {"m1": "c-s", "m2": "c-s"}), T0, T0 - 10 * MIN),
    ("capped read, inferred group", rule(), mem(["m1"], T0 - QUIET - H), OK,
     read([group(inferred=True, basis="")], {"m1": "c-abc", "m2": "c-abc"}, capped=True), T0, T0 - 10 * MIN),
]

SAME = [
    ("v1|q|c|exc:E:abc", "v1|q|c|exc:E:abc", True),
    ("v1|q|c|exc:E:abc", "v1|q|c|exc:E:*", True),
    ("v1|q|c|exc:E:*", "v1|q|c|exc:E:abc", True),
    ("v1|q|c|exc:E:abc", "v1|q|c|exc:F:*", False),
    ("v1|q|c|exc:E:abc", "v1|q2|c|exc:E:*", False),
    ("v1|q|c|status:500", "v1|q|c|status:*", False),
    ("v1|q|c|exc:E:abc", "v1|q|c|exc:E:abd", False),
    ("v1|q|c|exc:E:*", "v1|q|c|exc:E:*", True),
]


def every_walk():
    """One "every" rule through checks 5 min apart, new messages at checks 1, 2 and 3 (QV-659): each of those sends."""
    r = rule(mode="every", quietS=0)
    arrivals = {1: ["m1"], 2: ["m2"], 3: ["m3"], 6: ["m4", "m5"]}
    checks = []
    msgs: list = []
    for i in range(8):
        msgs = msgs + arrivals.get(i, [])
        checks.append({"at": T0 + i * 5 * MIN, "causeOf": {m: "c-abc" for m in msgs}})
    return {"rule": r, "group": group(count=1), "checks": checks}


def walk():
    """One quiet rule through a day of checks every 30 min: what each check sends. Memory and stored state are carried
    from check to check, as the scheduled check carries them."""
    r = rule(quietS=2 * 3600)
    arrivals = {0: ["m1"], 2: ["m2"], 3: ["m3"], 9: ["m4"], 14: [], 20: ["m5", "m6"]}
    checks = []
    msgs: list = []
    for i in range(24):
        msgs = msgs + arrivals.get(i, [])
        checks.append({"at": T0 + i * 30 * MIN, "causeOf": {m: "c-abc" for m in msgs}})
    return {"rule": r, "group": group(count=1), "checks": checks}


def run_walk(w):
    memory = mem(None, None)
    state: list = []
    prev_at = None
    out = []
    for c in w["checks"]:
        rd = read([{**w["group"], "count": len(c["causeOf"])}], c["causeOf"])
        prev = next((s for s in state if s["widgetId"] == w["rule"]["id"]), None)
        got = alerts.check_cause_rule(w["rule"], memory, prev, rd, c["at"], prev_at)
        d = alerts.diff(state, [got["observed"]], c["at"], [alerts.key_of(got["observed"])])
        out.append({"at": c["at"], "tone": got["observed"]["tone"], "newCount": got["observed"]["rule"]["newCount"],
                    "changes": [x["kind"] for x in d["changes"]]})
        memory, state, prev_at = got["memory"], d["next"], c["at"]
    return out


def diff_case(n, p, o, at, present=None):
    """present: the keys of the rules that still exist (alerts.diff); left out, the rules in now."""
    there = present if present is not None else [alerts.key_of(x) for x in o]
    return {"name": n, "prev": p, "now": o, "at": at, "present": there,
            "expected": alerts.diff(copy.deepcopy(p), copy.deepcopy(o), at, there)}


def main():
    f = {
        "about": "Alert rule cases both engines must agree on (QV-627). Written by make-alerts-parity.py; see that file.",
        "version": 1,
        "toneOf": [{"name": n, "value": v, "rule": r, "expected": alerts.tone_of(v, r)} for n, v, r in TONE],
        "diff": [diff_case(*c) for c in DIFF],
        "causeRule": [{"name": n, "rule": r, "memory": m, "prev": p, "read": rd, "at": at, "prevCheckAt": pc,
                       "expected": alerts.check_cause_rule(r, m, p, rd, at, pc)} for n, r, m, p, rd, at, pc in CAUSE],
        "sameCause": [{"rule": r, "group": g, "expected": e} for r, g, e in SAME],
    }
    for c in f["sameCause"]:
        assert alerts.same_cause(c["rule"], c["group"]) is c["expected"], c
    w = walk()
    f["walk"] = {**w, "expected": run_walk(w)}
    ew = every_walk()
    f["everyWalk"] = {**ew, "expected": run_walk(ew)}
    assert [x["changes"] for x in f["everyWalk"]["expected"]] == [
        [], ["fired"], ["fired"], ["fired"], ["resolved"], [], ["fired"], ["resolved"]], f["everyWalk"]["expected"]
    OUT.write_text(json.dumps(f, indent=1) + "\n")


if __name__ == "__main__":
    main()
