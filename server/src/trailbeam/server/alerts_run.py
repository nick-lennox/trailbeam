"""The scheduled alert check (QV-639, the third part of QV-592: alerts in self-hosted Trailbeam).

The hosted app's lib/alerts/run.ts (runAlerts) in Python, with the parts of store.ts (record, lease), needs.ts
(alertNeeds), rules.ts and cause-rule.ts it calls, and the same behaviour. One check: for each workspace that has a
dashboard rule or an "Alert me" rule (and, when ``channels`` is given, somewhere to send alerts), load the data its
rules need, evaluate every rule with the QV-627 engine (alerts.py, measures.alert_of), record the state and hand the
changes to ``deliver`` (QV-636's alert_delivery.deliver, wired by the caller). A workspace that fails is recorded with
its error and does not stop the others.

It runs every few minutes (alerts_scheduler.py), so:

- each workspace is checked under its lease: a second check that starts while the first is still going, or within
  MIN_GAP_MS after it, skips the workspace;
- the workspaces checked longest ago go first, and no new one starts once ``budget_ms`` has passed. The rest wait for
  the next check, which starts with them.

A muted or snoozed "Alert me" rule's state is still recorded, but its changes are not sent; when it wakes up still
firing, one "still firing" change is (``afterSnooze``). While a rule's cause is ignored in triage the rule is held:
worked out as usual, sending nothing, its own mute left alone; when the ignore ends while it is firing, one "Back from
ignored" change is sent. A regression of a resolved cause sends one "Regressed" change at the check that first sees it.

Storage is not here: rules, alert state, checks and leases are read and written through ``AlertStore`` (a Protocol),
and what the rules read (dashboards, events and queue figures, causes, triage, channels) comes in through ``Deps``.
MemoryAlertStore is the store for tests; the SQLite and Postgres tables are a later part of QV-592. The records are
dicts with the hosted app's JSON field names, as in alerts.py.

contract/fixtures/alerts-run.json holds the run's decisions (which workspaces are checked in which order under a
budget, lease skipping, mute and snooze, still firing, a failing workspace) as shared cases; test_alerts_run.py runs
them here.
"""
from __future__ import annotations

import copy
import logging
import time
import uuid
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Protocol

from . import alerts
from . import measures as ms

log = logging.getLogger("trailbeam.server.alerts")

#: no workspace is started after this long into a check (ms)
DEFAULT_BUDGET_MS = 60_000
#: a lease outlives the longest a check may run, so it only runs out under a check that died (ms)
LEASE_MS = 6 * 60_000
#: a workspace checked without an error less than this ago is not checked again: two triggers close together do the
#: work once (ms)
MIN_GAP_MS = 45_000
#: what tb_alert_state keeps of a rule's figure; the cause rule's ``rule`` info travels with the change only
STATE_FIELDS = ("dashboardId", "widgetId", "dashboardName", "title", "tone", "value", "limit", "queue", "since",
                "checkedAt")


def span_key(ws: str) -> str:
    """The lease and check key of the hosted app's checks at ingest; the scheduled check reads its time as the
    previous check too, so a store shared with them gives the same "since the last check"."""
    return f"{ws}#spans"


def now_ms() -> int:
    return int(time.time() * 1000)


# ── storage ───────────────────────────────────────────────────────────────────────────────────────────────────────

class AlertStore(Protocol):
    """What the check reads and writes. Each write that decides whether a change is sent is conditional and says
    whether it took effect, so of two overlapping checks exactly one sends it (as the hosted app's SQL does)."""

    def last_checks(self) -> dict[str, int]:
        """When each workspace was last checked (ms), by workspace ID."""

    def last_check(self, key: str) -> int | None:
        """When this workspace (or span_key) was last checked, with or without an error; None: never."""

    def checked(self, key: str, at: int, error: str | None) -> None:
        """Notes a check of the workspace at ``at``, and its error (None: it went fine)."""

    def lease(self, key: str, owner: str, at: int, ttl_ms: int, min_gap_ms: int) -> bool:
        """Takes the workspace's lease for ``owner`` until ``at + ttl_ms``, if nobody holds it (its lease ended
        before ``at``) and it was not checked without an error after ``at - min_gap_ms``. Atomic."""

    def release(self, key: str, owner: str) -> None:
        """Gives the lease back, if ``owner`` still holds it."""

    def alert_states(self, ws: str) -> list[dict]:
        """The workspace's stored rule states (STATE_FIELDS)."""

    def write_state(self, ws: str, state: dict, prev_tone: str | None) -> bool:
        """Stores a rule's state. prev_tone None: insert it unless one is there; else update it only if its tone is
        still prev_tone. True if the write took effect."""

    def drop_state(self, ws: str, dashboard_id: str, widget_id: str) -> None:
        ...

    def workspaces_with_rules(self) -> set[str]:
        """The workspaces with at least one "Alert me" rule."""

    def rules_with_memory(self, ws: str) -> list[dict]:
        """The workspace's "Alert me" rules, oldest first: [{"rule", "memory"}], as alerts.check_cause_rule takes
        them; memory also has wasHeld and regressedSeen."""

    def remember(self, ws: str, rule_id: str, memory: dict, seen_as: dict | None) -> None:
        """Keeps what this check saw (seen, lastNewAt, wasMuted, wasHeld) and, with seen_as, the cause's label,
        queue and causeId as the check found them. Not regressedSeen: set_regressed_seen writes that."""

    def clear_was_muted(self, ws: str, rule_id: str) -> bool:
        """Clears the rule's wasMuted if it is set. True if it was: only that check sends "still firing"."""

    def set_regressed_seen(self, ws: str, rule_id: str, regressed_at: int) -> bool:
        """Sets the rule's regressedSeen to ``regressed_at`` if it isn't already. True if it changed."""


class MemoryAlertStore:
    """AlertStore in memory, for the tests and until the tables exist. One lock-free process: the check is the only
    writer, and the scheduler runs one check at a time."""

    def __init__(self, rules: dict | None = None, states: dict | None = None, checks: dict | None = None,
                 leases: dict | None = None):
        #: ws -> [{"rule", "memory"}]
        self.rules: dict[str, list[dict]] = copy.deepcopy(rules or {})
        #: ws -> {"dashboardId/widgetId": state}
        self.states: dict[str, dict[str, dict]] = {
            ws: {alerts.key_of(s): {k: s[k] for k in STATE_FIELDS} for s in ss} for ws, ss in (states or {}).items()}
        #: key -> {"checkedAt", "error"}
        self.checks: dict[str, dict] = copy.deepcopy(checks or {})
        #: key -> {"owner", "until"}
        self.leases: dict[str, dict] = copy.deepcopy(leases or {})

    def last_checks(self) -> dict[str, int]:
        return {k: c["checkedAt"] for k, c in self.checks.items()}

    def last_check(self, key: str) -> int | None:
        c = self.checks.get(key)
        return c["checkedAt"] if c else None

    def checked(self, key: str, at: int, error: str | None) -> None:
        self.checks[key] = {"checkedAt": at, "error": error}

    def lease(self, key: str, owner: str, at: int, ttl_ms: int, min_gap_ms: int) -> bool:
        held = self.leases.setdefault(key, {"owner": "", "until": 0})
        c = self.checks.get(key)
        if held["until"] >= at or (c is not None and c["error"] is None and c["checkedAt"] > at - min_gap_ms):
            return False
        self.leases[key] = {"owner": owner, "until": at + ttl_ms}
        return True

    def release(self, key: str, owner: str) -> None:
        held = self.leases.get(key)
        if held and held["owner"] == owner:
            held["until"] = 0

    def alert_states(self, ws: str) -> list[dict]:
        return [dict(s) for s in self.states.get(ws, {}).values()]

    def write_state(self, ws: str, state: dict, prev_tone: str | None) -> bool:
        mine = self.states.setdefault(ws, {})
        k = alerts.key_of(state)
        had = mine.get(k)
        if (had is not None) if prev_tone is None else (had is None or had["tone"] != prev_tone):
            return False
        mine[k] = {f: state[f] for f in STATE_FIELDS}
        return True

    def drop_state(self, ws: str, dashboard_id: str, widget_id: str) -> None:
        self.states.get(ws, {}).pop(f"{dashboard_id}/{widget_id}", None)

    def workspaces_with_rules(self) -> set[str]:
        return {ws for ws, rs in self.rules.items() if rs}

    def rules_with_memory(self, ws: str) -> list[dict]:
        rs = sorted(self.rules.get(ws, []), key=lambda x: (x["rule"]["createdAt"], x["rule"]["id"]))
        return copy.deepcopy(rs)

    def _rule(self, ws: str, rule_id: str) -> dict | None:
        return next((x for x in self.rules.get(ws, []) if x["rule"]["id"] == rule_id), None)

    def remember(self, ws: str, rule_id: str, memory: dict, seen_as: dict | None) -> None:
        x = self._rule(ws, rule_id)
        if x is None:
            return
        x["memory"] = {**x["memory"], "seen": list(memory.get("seen") or []), "lastNewAt": memory.get("lastNewAt"),
                       "wasMuted": bool(memory.get("wasMuted")), "wasHeld": bool(memory.get("wasHeld"))}
        if seen_as:
            x["rule"] = {**x["rule"], "label": seen_as["label"][:300], "queue": seen_as["queue"],
                         "causeId": seen_as["causeId"]}

    def clear_was_muted(self, ws: str, rule_id: str) -> bool:
        x = self._rule(ws, rule_id)
        if x is None or not x["memory"].get("wasMuted"):
            return False
        x["memory"] = {**x["memory"], "wasMuted": False}
        return True

    def set_regressed_seen(self, ws: str, rule_id: str, regressed_at: int) -> bool:
        x = self._rule(ws, rule_id)
        if x is None or x["memory"].get("regressedSeen") == regressed_at:
            return False
        x["memory"] = {**x["memory"], "regressedSeen": regressed_at}
        return True


# ── what the rules need ───────────────────────────────────────────────────────────────────────────────────────────

def has_rule(d: dict) -> bool:
    """A dashboard with at least one widget that sets a warn or crit limit."""
    return any((w.get("rule") or {}).get("warn") is not None or (w.get("rule") or {}).get("crit") is not None
               for w in (d.get("def") or {}).get("widgets") or [])


_FROM_EVENTS = {"visibility_headroom"}
_FROM_IN_QUEUE = {"stuck_in_flight", "near_dlq"}
#: the CloudWatch metrics each queue measure needs; a measure not here needs none
CLOUDWATCH_FOR = {"oldest_age": ["oldestS"], "retention_headroom": ["oldestS"], "time_to_drain": ["sent", "deleted"]}


def _range_ms(id_: str | None) -> int:
    p: dict = next((p for p in ms.PRESETS if p["id"] == (id_ or "1h")), ms.PRESETS[2])
    return int(p["ms"])


def alert_needs(dashboards: Iterable[dict]) -> dict:
    """What one check has to read for a workspace's dashboard rules (needs.ts alertNeeds), for the ``data`` loader:
    {"events": read events at all, "sinceMs": from how far back, "queues": read queue figures, "queuesOf": the queue
    names a queue-measure rule can match, out of the given ones, "inQueue": work out the messages in each queue,
    "metrics": the CloudWatch metrics to read, "rules": how many rules}."""
    events, since_ms, queues, in_queue, rules = False, 0, False, False, 0
    metrics: set[str] = set()
    filters: list[str] = []
    for d in dashboards:
        dfn = d.get("def") or {}
        for w in dfn.get("widgets") or []:
            qs = w.get("queries") or []
            m = ms.measure((qs[0] or {}).get("measure") if qs else "")
            rule = w.get("rule")
            if (not m or not rule or (rule.get("warn") is None and rule.get("crit") is None)
                    or w.get("type") not in ("number", "timeseries")):
                continue
            rules += 1
            if not m.queue or m.id in _FROM_EVENTS:
                events = True
                since_ms = max(since_ms, _range_ms(w.get("time")))
            if m.queue:
                queues = True
                filters.append(ms.effective_filter(w, dfn.get("variables") or []))
            if m.id in _FROM_IN_QUEUE:
                in_queue = True
            metrics.update(CLOUDWATCH_FOR.get(m.id, []))

    def queues_of(names: list) -> list:
        out: set = set()
        for f in filters:
            out.update(ms.queues_for(ms.parse(f), names))
        return [n for n in names if n in out]

    return {"events": events, "sinceMs": since_ms, "queues": queues, "queuesOf": queues_of, "inQueue": in_queue,
            "rules": rules, "metrics": sorted(metrics)}


def observe(dashboards: Iterable[dict], data: dict) -> list[dict]:
    """Every rule in these dashboards that has a figure now (evaluate.ts observe). A rule whose figure is missing
    (a queue measure with no figures) is left out."""
    out = []
    for d in dashboards:
        dfn = d.get("def") or {}
        for w in dfn.get("widgets") or []:
            a = ms.alert_of(w, dfn.get("variables") or [], data)
            if a:
                out.append({"dashboardId": d["id"], "dashboardName": d["name"], "widgetId": w["id"],
                            "title": w.get("title") or "Untitled", "tone": a["tone"], "value": a["value"],
                            "limit": a["limit"], "queue": a["queue"]})
    return out


def record(store: AlertStore, ws: str, now: list[dict], at: int) -> list[dict]:
    """Records what this check saw (store.ts record). Returns the changes this check won and must deliver: a change
    whose conditional write did not take effect was made by another check, which sends it."""
    prev = store.alert_states(ws)
    d = alerts.diff(prev, now, at)
    changed = {alerts.key_of(c["alert"]): c for c in d["changes"]}
    before = {alerts.key_of(s): s for s in prev}
    won = []
    for s in d["next"]:
        k = alerts.key_of(s)
        p = before.get(k)
        ok = store.write_state(ws, {f: s[f] for f in STATE_FIELDS}, p["tone"] if p else None)
        c = changed.get(k)
        if c and ok:
            won.append(c)
    for s in d["gone"]:
        store.drop_state(ws, s["dashboardId"], s["widgetId"])
    store.checked(ws, at, None)
    return won


# ── the check ─────────────────────────────────────────────────────────────────────────────────────────────────────

@dataclass
class Deps:
    """Everything the check reads and writes. Only ``store``, ``dashboards`` and ``data`` are needed."""

    store: AlertStore
    #: the workspaces' dashboards [{id, name, def}], by workspace ID; given a workspace ID, that workspace's alone
    dashboards: Callable[[str | None], dict]
    #: what the workspace's dashboard rules need (alert_needs), read for them: measures' EventData
    data: Callable[[str, list], dict]
    #: the workspace's causes as Needs attention reads them: {"groups", "causeOf", "capped"}. None: "Alert me" rules
    #: are not checked
    causes: Callable[[str], dict] | None = None
    #: the workspaces with at least one alert channel. None: every workspace is checked
    channels: Callable[[], set] | None = None
    #: sends a workspace's changes (alert_delivery.deliver with the workspace's channels)
    deliver: Callable[[str, list], None] | None = None
    #: the triage of the rules' causes now, worked out and its transitions written as a page read does (the hosted
    #: app's rulesTriage): (ws, cause keys, causes read, at) -> {cause key: {"triage": {status, regressed,
    #: regressedAt, regressedCount, regressedVersion, statusAt}, "info": the message's TriageInfo, "ignoredHow"}}.
    #: None: no triage, no rule is held
    triage: Callable[[str, list, dict, int], dict] | None = None
    now: Callable[[], int] = now_ms
    budget_ms: int = DEFAULT_BUDGET_MS
    #: names this check's leases; random by default
    owner: str | None = None
    #: check this workspace alone
    only: str | None = None


def _error(e: BaseException) -> str:
    return str(e) or type(e).__name__


def run_alerts(deps: Deps) -> dict:
    """One scheduled check. Returns {"workspaces": checked, "rules": figures recorded, "changes": changes sent,
    "failed": [{ws, error}], "skipped": {"noChannel", "leased", "budget"}: workspaces with rules left out, "ms"}."""
    clock = deps.now
    start = clock()
    owner = deps.owner or str(uuid.uuid4())
    store = deps.store
    every = deps.dashboards(deps.only)
    with_rules = store.workspaces_with_rules() if deps.causes else set()
    with_channel = deps.channels() if deps.channels else None
    last = store.last_checks()
    out: dict = {"workspaces": 0, "rules": 0, "changes": 0, "failed": [],
                 "skipped": {"noChannel": 0, "leased": 0, "budget": 0}, "ms": 0}
    due = []
    for ws in dict.fromkeys([*every.keys(), *sorted(with_rules)]):
        if deps.only is not None and ws != deps.only:
            continue
        if not any(has_rule(d) for d in every.get(ws, [])) and ws not in with_rules:
            continue
        if with_channel is not None and ws not in with_channel:
            out["skipped"]["noChannel"] += 1
            continue
        due.append(ws)
    due.sort(key=lambda ws: (last.get(ws, -1), ws))
    for ws in due:
        dashboards = every.get(ws, [])
        boards, rules = any(has_rule(d) for d in dashboards), ws in with_rules
        at = clock()
        if at - start >= deps.budget_ms:
            out["skipped"]["budget"] += 1
            continue
        if not store.lease(ws, owner, at, LEASE_MS, MIN_GAP_MS):
            out["skipped"]["leased"] += 1
            continue
        out["workspaces"] += 1
        try:
            seen = observe(dashboards, deps.data(ws, dashboards)) if boards else []
            ruled = _CauseRules(deps, ws, at) if rules else None
            if ruled:
                seen.extend(ruled.observed)
            out["rules"] += len(seen)
            changes = record(store, ws, seen, at)
            if ruled:
                changes = ruled.settle(changes)
            out["changes"] += len(changes)
            if changes and deps.deliver:
                deps.deliver(ws, changes)
        except Exception as e:  # noqa: BLE001 - a workspace that fails is recorded and doesn't stop the others
            out["failed"].append({"ws": ws, "error": _error(e)})
            try:
                store.checked(ws, at, _error(e))
            except Exception:  # the failure is in the result already
                log.warning("alert check: could not note the error of workspace %s", ws, exc_info=True)
        finally:
            try:
                store.release(ws, owner)
            except Exception:  # the lease runs out on its own (LEASE_MS)
                log.warning("alert check: could not release the lease of workspace %s", ws, exc_info=True)
    out["ms"] = clock() - start
    return out


def has_rules(deps: Deps) -> bool:
    """Whether any workspace has a rule to check: the scheduler does nothing else when none has."""
    if deps.causes and deps.store.workspaces_with_rules():
        return True
    return any(has_rule(d) for ds in deps.dashboards(deps.only).values() for d in ds)


class _CauseRules:
    """The workspace's "Alert me" rules at this check: their figures (observed), to record with the dashboard
    rules, and settle(), which takes the changes record() won and returns the ones to send. It leaves out a muted or
    held rule's changes, adds a "still firing" change for a rule that woke up firing, and keeps what each rule saw."""

    def __init__(self, deps: Deps, ws: str, at: int):
        assert deps.causes is not None
        self.deps, self.ws, self.at = deps, ws, at
        store = deps.store
        items = store.rules_with_memory(ws)
        self.prev = {s["widgetId"]: s for s in store.alert_states(ws) if s["dashboardId"] == alerts.RULE_DASHBOARD}
        # the check before this one: the scheduled one or one at ingest, whichever was later
        times = [t for t in (store.last_check(ws), store.last_check(span_key(ws))) if t is not None]
        prev_check_at = max(times) if times else None
        read = deps.causes(ws)
        keys = [x["rule"]["causeKey"] for x in items]
        triage = (deps.triage(ws, keys, read, at) or {}) if deps.triage and keys else {}
        self.checks = []
        for x in items:
            rule, memory = x["rule"], x["memory"]
            c = alerts.check_cause_rule(rule, memory, self.prev.get(rule["id"]), read, at, prev_check_at)
            t = triage.get(rule["causeKey"])
            # held while its cause is ignored: worked out as usual, nothing sent, and its own mute left alone
            held = t is not None and (t.get("triage") or {}).get("status") == "ignored"
            quiet = bool(c["muted"]) or held
            if t:
                c["observed"]["rule"]["triage"] = t.get("info")
            c = {**c, "muted": quiet, "memory": {**c["memory"], "wasMuted": quiet, "wasHeld": held,
                                                 "regressedSeen": memory.get("regressedSeen")}}
            self.checks.append({"rule": rule, "memory": memory, "t": t, "c": c})
        self.observed = [x["c"]["observed"] for x in self.checks]

    def settle(self, changes: list[dict]) -> list[dict]:
        store, ws, at, R = self.deps.store, self.ws, self.at, alerts.RULE_DASHBOARD
        # muted or held: nothing is sent. Just woken and found quiet: it stopped while muted, so nothing is sent either
        muted = {x["rule"]["id"] for x in self.checks if x["c"]["muted"]}
        woken = {x["rule"]["id"] for x in self.checks if x["memory"].get("wasMuted") and not x["c"]["muted"]}
        out = [x for x in changes if not (x["alert"]["dashboardId"] == R and (
            x["alert"]["widgetId"] in muted or (x["kind"] == "resolved" and x["alert"]["widgetId"] in woken)))]
        changed = {alerts.key_of(x["alert"]) for x in changes}
        for x in self.checks:
            rule, memory, c, t = x["rule"], x["memory"], x["c"], x["t"]
            rid = rule["id"]
            p = self.prev.get(rid)
            woke = p is not None and (memory.get("wasMuted") and not c["muted"] and p["tone"] == "critical"
                    and c["observed"]["tone"] == "critical" and f"{R}/{rid}" not in changed)
            # only the check that clears the flag sends it, so two overlapping checks send it once
            if p is not None and woke and store.clear_was_muted(ws, rid):
                back = ({"backFromIgnored": {"how": (t or {}).get("ignoredHow") or "ignored"}} if memory.get("wasHeld")
                        else {"afterSnooze": True})
                out.append({"kind": "fired", "from": None, "alert": {
                    **c["observed"], "since": p["since"], "checkedAt": at, "rule": {**c["observed"]["rule"], **back}}})
            # a regression: one message, at the first check that sees it, even inside the quiet period. One that
            # happened while the rule was muted, or before the rule was made, is let pass, so a later check doesn't
            # send it late
            tr = (t or {}).get("triage") or {}
            reg = tr.get("regressedAt") if tr.get("regressed") and tr.get("status") == "open" else None
            if (reg is not None and reg != memory.get("regressedSeen") and store.set_regressed_seen(ws, rid, reg)
                    and not c["muted"] and reg >= rule["createdAt"]):
                info = (t or {}).get("info") or {}
                regressed = {"count": tr.get("regressedCount") or 1, "version": tr.get("regressedVersion"),
                             "by": info.get("resolvedBy"), "resolvedAt": tr.get("statusAt")}
                i = next((n for n, y in enumerate(out) if y["alert"]["dashboardId"] == R
                          and y["alert"]["widgetId"] == rid and y["kind"] != "resolved"), -1)
                if i >= 0:
                    # the rule fired at this check too: one message, the Regressed one
                    r = {k: v for k, v in out[i]["alert"]["rule"].items()
                         if k not in ("afterSnooze", "backFromIgnored")}
                    out[i] = {**out[i], "alert": {**out[i]["alert"], "rule": {**r, "regressed": regressed}}}
                else:
                    out.append({"kind": "fired", "from": None, "alert": {
                        **c["observed"], "tone": "critical", "since": at, "checkedAt": at,
                        "rule": {**c["observed"]["rule"], "regressed": regressed}}})
            store.remember(ws, rid, c["memory"], c["seenAs"])
        return out
