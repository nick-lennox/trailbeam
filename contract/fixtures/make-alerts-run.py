# SPDX-License-Identifier: AGPL-3.0-only
# Imports the server, so it is under the server's licence (LICENSING.md).
"""Writes alerts-run.json: the scheduled alert check's decisions both engines must agree on (QV-639).

Self-hosted Trailbeam's check (server/src/trailbeam/server/alerts_run.py, python/tests/server/test_alerts_run.py) and
the hosted app's (lib/alerts/run.ts runAlerts) take the same inputs and must make the same decisions: which workspaces
are checked, in which order and under which time budget, which are skipped for a lease or a missing channel, what a
muted or snoozed rule records and sends, the one "still firing" change when it wakes, and a failing workspace that
doesn't stop the others. The expected results are the Python check's. Run from the repository root after changing a
case or the check:

    PYTHONPATH=python/src:server/src python contract/fixtures/make-alerts-run.py

A case is one check:
- ``start``: the clock when the check starts (ms). The clock moves only when a workspace's data or causes are read:
  by ``cost[ws]`` ms, once per workspace, so the time budget is decided the same way everywhere.
- ``budgetMs``, ``owner`` (the check's lease owner), ``only`` (check this workspace alone, or null).
- ``channels``: the workspaces with an alert channel, or null for "every workspace" (no channel list given).
- ``dashboards`` (ws -> [{id, name, def}]), ``data`` (ws -> measures' EventData), ``causes`` (ws -> Needs attention's
  read: {groups, causeOf, capped}), ``fail`` (ws -> the error its data or causes read raises).
- ``store``: what is stored before the check: ``rules`` (ws -> [{rule, memory}]), ``states`` (ws -> [state]),
  ``checks`` (ws -> {checkedAt, error}), ``leases`` (ws -> {owner, until}).
- ``expected``: ``result`` (run_alerts' result), ``delivered`` ([{ws, changes}], in order) and ``store`` after.
"""
import copy
import json
from pathlib import Path

from trailbeam.server import alerts_run as run

OUT = Path(__file__).resolve().parent / "alerts-run.json"
S = 1000
MIN = 60_000
H = 3_600_000
D = 86_400_000
T = 1_764_000_000_000  # 2025-11-24T16:00:00Z, ms

KEY = "v1|orders|worker|exc:TimeoutError"


def group(n: int, id_: str = "c1", key: str = KEY) -> dict:
    return {"id": id_, "key": key, "label": "TimeoutError: read timed out", "queue": "orders", "count": n,
            "basis": "exception", "inferred": False}


def read(*mids: str) -> dict:
    return {"groups": [group(len(mids))] if mids else [], "causeOf": {m: "c1" for m in mids}, "capped": False}


def rule(id_: str = "r1", **o) -> dict:
    return {"id": id_, "kind": "cause", "causeKey": KEY, "causeId": "c1", "label": "TimeoutError: read timed out",
            "queue": "orders", "channels": ["ch1"], "mode": "quiet", "quietS": 3600, "mutedUntil": None,
            "mutedBy": None, "mutedAt": None, "createdBy": "u1", "createdAt": T - D, "updatedAt": T - D, **o}


def memory(**o) -> dict:
    return {"seen": ["m1"], "lastNewAt": T - 2 * H, "wasMuted": False, "wasHeld": False, "regressedSeen": None, **o}


def firing(id_: str = "r1", since: int = T - 3 * H, checked: int = T - 5 * MIN) -> dict:
    return {"dashboardId": "rule", "widgetId": id_, "dashboardName": "", "title": "TimeoutError: read timed out",
            "tone": "critical", "value": "1", "limit": "new messages after 1 h quiet", "queue": "orders",
            "since": since, "checkedAt": checked}


BOARD = {"id": "d1", "name": "Orders", "def": {"variables": [], "widgets": [
    {"id": "w1", "type": "number", "title": "Dead-letter moves", "layout": {"x": 0, "y": 0, "w": 3, "h": 2},
     "queries": [{"measure": "dead_lettered"}], "rule": {"crit": 0}}]}}
NO_RULE = {"id": "d2", "name": "Plain", "def": {"variables": [], "widgets": [
    {"id": "w1", "type": "number", "title": "Sent", "layout": {"x": 0, "y": 0, "w": 3, "h": 2},
     "queries": [{"measure": "sent"}]}]}}
DLQ_DATA = {"now": T, "queues": ["orders"], "queueLive": {}, "events": [
    {"ms": T - 10 * MIN, "event": "dead_letter", "status": "error", "queue": "orders", "messageId": "m9"}]}


def case(name: str, *, rules=None, states=None, checks=None, leases=None, causes=None, dashboards=None, data=None,
         fail=None, cost=None, channels=None, budget=run.DEFAULT_BUDGET_MS, only=None) -> dict:
    return {"name": name, "start": T, "budgetMs": budget, "owner": "check-1", "only": only, "channels": channels,
            "cost": cost or {}, "dashboards": dashboards or {}, "data": data or {}, "causes": causes or {},
            "fail": fail or {}, "store": {"rules": rules or {}, "states": states or {}, "checks": checks or {},
                                          "leases": leases or {}}}


# three workspaces with a rule each, checked 10 min ago, never and 20 min ago; each read takes 25 s
_THREE = {"rules": {w: [{"rule": rule(), "memory": memory()}] for w in ("a", "b", "c")},
          "causes": {w: read("m1") for w in ("a", "b", "c")},
          "checks": {"a": {"checkedAt": T - 10 * MIN, "error": None}, "c": {"checkedAt": T - 20 * MIN, "error": None}},
          "cost": {"a": 25 * S, "b": 25 * S, "c": 25 * S}}

CASES = [
    case("oldest checked first: never, then 20 min ago, then 10 min ago", **_THREE),
    case("the budget: no workspace starts once it has passed; the rest wait", **{**_THREE, "budget": 50 * S}),
    case("a budget of 0: nothing is checked", **{**_THREE, "budget": 0}),
    case("leases: held by another check, or checked fine within the gap, is skipped; an ended lease or a failed "
         "check within the gap is not",
         rules={w: [{"rule": rule(), "memory": memory()}] for w in ("held", "fresh", "failed", "ended", "old")},
         causes={w: read("m1") for w in ("held", "fresh", "failed", "ended", "old")},
         leases={"held": {"owner": "other", "until": T + MIN}, "ended": {"owner": "other", "until": T - 1}},
         checks={"fresh": {"checkedAt": T - 30 * S, "error": None},
                 "failed": {"checkedAt": T - 30 * S, "error": "boom"},
                 "old": {"checkedAt": T - run.MIN_GAP_MS, "error": None}}),
    case("channels: a workspace with rules and no channel is skipped; one without rules is not counted",
         rules={"a": [{"rule": rule(), "memory": memory()}], "b": [{"rule": rule(), "memory": memory()}]},
         causes={"a": read("m1"), "b": read("m1")}, dashboards={"c": [NO_RULE], "d": [BOARD]}, data={"d": DLQ_DATA},
         channels=["a", "c"]),
    case("only: one workspace", rules={w: [{"rule": rule(), "memory": memory()}] for w in ("a", "b")},
         causes={"a": read("m1", "m2"), "b": read("m1", "m2")}, only="b"),
    case("a dashboard rule fires and is sent", dashboards={"a": [BOARD, NO_RULE]}, data={"a": DLQ_DATA}),
    case("a dashboard rule still critical: recorded, not sent again", dashboards={"a": [BOARD]}, data={"a": DLQ_DATA},
         states={"a": [{"dashboardId": "d1", "widgetId": "w1", "dashboardName": "Orders", "title": "Dead-letter moves",
                        "tone": "critical", "value": "1", "limit": "above 0", "queue": "", "since": T - H,
                        "checkedAt": T - 5 * MIN}]}),
    case("a rule fires on a new message after the quiet period",
         rules={"a": [{"rule": rule(), "memory": memory()}]}, causes={"a": read("m1", "m2")}),
    case("muted: it fires, the state is recorded, nothing is sent",
         rules={"a": [{"rule": rule(mutedUntil=run.alerts.FOREVER, mutedBy="u2", mutedAt=T - H), "memory": memory()}]},
         causes={"a": read("m1", "m2")}),
    case("snoozed: it resolves, the state is recorded, nothing is sent",
         rules={"a": [{"rule": rule(mutedUntil=T + H, mutedBy="u2", mutedAt=T - H), "memory": memory(wasMuted=True)}]},
         states={"a": [firing()]}, causes={"a": read("m1")}),
    case("a snooze that ended: still firing sends one change, since the rule began firing",
         rules={"a": [{"rule": rule(mutedUntil=T - MIN, mutedBy="u2", mutedAt=T - H),
                       "memory": memory(lastNewAt=T - 20 * MIN, wasMuted=True)}]},
         states={"a": [firing()]}, causes={"a": read("m1")}),
    case("a snooze that ended on a quiet rule: it resolves and nothing is sent",
         rules={"a": [{"rule": rule(mutedUntil=T - MIN), "memory": memory(wasMuted=True)}]},
         states={"a": [firing()]}, causes={"a": read("m1")}),
    case("a snooze that ended on a rule that fires again now: the usual change, no second one",
         rules={"a": [{"rule": rule(mutedUntil=T - MIN), "memory": memory(wasMuted=True)}]},
         causes={"a": read("m1", "m2")}),
    case("a failing workspace is recorded with its error and doesn't stop the others",
         rules={w: [{"rule": rule(), "memory": memory()}] for w in ("a", "b")},
         causes={"a": read("m1", "m2"), "b": read("m1", "m2")}, fail={"a": "the causes read failed"},
         checks={"a": {"checkedAt": T - 2 * H, "error": None}, "b": {"checkedAt": T - H, "error": None}}),
    case("no rules anywhere: nothing is read", dashboards={"a": [NO_RULE]}),
]


class _Clock:
    def __init__(self, at: int):
        self.at = at

    def __call__(self) -> int:
        return self.at


def run_case(c: dict) -> dict:
    """Runs one case with the in-memory store: {result, delivered, store} after."""
    c = copy.deepcopy(c)
    st = c["store"]
    store = run.MemoryAlertStore(st["rules"], st["states"], st["checks"], st["leases"])
    clock = _Clock(c["start"])
    charged: set = set()
    delivered: list = []

    def load(ws: str):
        if ws not in charged:
            charged.add(ws)
            clock.at += c["cost"].get(ws, 0)
        if ws in c["fail"]:
            raise RuntimeError(c["fail"][ws])

    def data(ws, boards):
        load(ws)
        return c["data"][ws]

    def causes(ws):
        load(ws)
        return c["causes"].get(ws) or {"groups": [], "causeOf": {}, "capped": False}

    def dashboards(only):
        return {ws: ds for ws, ds in c["dashboards"].items() if only is None or ws == only}

    channels = None if c["channels"] is None else (lambda: set(c["channels"]))
    deps = run.Deps(store=store, dashboards=dashboards, data=data, causes=causes, channels=channels,
                    deliver=lambda ws, changes: delivered.append({"ws": ws, "changes": copy.deepcopy(changes)}),
                    now=clock, budget_ms=c["budgetMs"], owner=c["owner"], only=c["only"])
    result = run.run_alerts(deps)
    return {"result": result, "delivered": delivered, "store": {
        "rules": store.rules,
        "states": {ws: sorted(s.values(), key=run.alerts.key_of) for ws, s in sorted(store.states.items()) if s},
        "checks": dict(sorted(store.checks.items())), "leases": dict(sorted(store.leases.items()))}}


def main():
    f = {"about": "The scheduled alert check's decisions both engines must agree on (QV-639). Written by "
                  "make-alerts-run.py; see that file.",
         "version": 1, "cases": [{**c, "expected": run_case(c)} for c in CASES]}
    OUT.write_text(json.dumps(f, indent=1) + "\n")


if __name__ == "__main__":
    main()
