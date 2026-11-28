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

**An interrupted check (QV-674, QV-757).** record() writes the new state before the changes are delivered, and a
later check does not send a change again once its state is written. So the changes are put in an outbox
(``AlertStore.hold``) as soon as record() returns, before any other store write, and taken out (``sent``) once
``deliver`` returns. A check stopped in between (a shutdown, a crash, the process killed, ``deliver`` raising, a store
write of the "Alert me" rules failing) leaves them there, and the next check of the workspace, under its lease, sends
them first. That makes delivery at least once: a check stopped after some channels were sent to sends to them again.
A held batch is tried MAX_TRIES times, and one held longer than MAX_HELD_MS is not sent any more; either is logged as
an error and counted in the result's ``dropped``.

The "Alert me" rules' conditional writes come after the hold (_CauseRules.settle: "still firing", "Regressed"). When
they add or change a message, the batch is held again with it and the first one taken out; a stop between those two
sends the first batch's changes twice, not never. One of those writes that raises is logged, sends nothing at this
check and fails the workspace once the rest is sent; since it did not take effect, the next check tries it again and
sends its message then. What memory each rule keeps (``remember``) is written after the hold. Still unguarded: a stop
inside record()'s own writes, or between record() returning and hold() taking effect, which runs no other code; a
hold() that fails is logged and the changes are delivered anyway.

A muted or snoozed "Alert me" rule's state is still recorded, but its changes are not sent; when it wakes up still
firing, one "still firing" change is (``afterSnooze``). While a rule's cause is ignored in triage the rule is held:
worked out as usual, sending nothing, its own mute left alone; when the ignore ends while it is firing, one "Back from
ignored" change is sent. A regression of a resolved cause sends one "Regressed" change at the check that first sees it.

Storage is not here: rules, alert state, checks and leases are read and written through ``AlertStore`` (a Protocol),
and what the rules read (dashboards, events and queue figures, causes, triage, channels) comes in through ``Deps``.
MemoryAlertStore is the store for tests; alerts_store.AlertTables is the one on the SQLite and Postgres tables (QV-650). The records are
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
#: a held batch of changes (the outbox) is delivered this many times at most, the first time included
MAX_TRIES = 3
#: a held batch older than this is not sent any more: what it says is that old, and later checks have sent what
#: followed (ms)
MAX_HELD_MS = 6 * 60 * 60_000
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

    def drop_state(self, ws: str, dashboard_id: str, widget_id: str) -> bool:
        """Deletes a rule's state. True if there was one to delete: of two overlapping checks, the one that deleted it
        sends its "no longer checked" change."""

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

    def hold(self, ws: str, id_: str, changes: list, at: int) -> None:
        """Puts a batch of changes this check won in the outbox before they are delivered, as tried once."""

    def held(self, ws: str) -> list[dict]:
        """The workspace's batches in the outbox, oldest first: [{"id", "changes", "heldAt", "tries"}]."""

    def retry(self, ws: str, id_: str) -> None:
        """Counts one more delivery of the batch."""

    def sent(self, ws: str, id_: str) -> bool:
        """Takes the batch out of the outbox: its delivery returned, or it is given up. True if it was there."""


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
        #: ws -> {batch ID: {"id", "changes", "heldAt", "tries"}}
        self.outbox: dict[str, dict[str, dict]] = {}

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

    def drop_state(self, ws: str, dashboard_id: str, widget_id: str) -> bool:
        return self.states.get(ws, {}).pop(f"{dashboard_id}/{widget_id}", None) is not None

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

    def hold(self, ws: str, id_: str, changes: list, at: int) -> None:
        self.outbox.setdefault(ws, {})[id_] = {"id": id_, "changes": copy.deepcopy(changes), "heldAt": at, "tries": 1}

    def held(self, ws: str) -> list[dict]:
        return copy.deepcopy(sorted(self.outbox.get(ws, {}).values(), key=lambda b: (b["heldAt"], b["id"])))

    def retry(self, ws: str, id_: str) -> None:
        b = self.outbox.get(ws, {}).get(id_)
        if b is not None:
            b["tries"] += 1

    def sent(self, ws: str, id_: str) -> bool:
        return self.outbox.get(ws, {}).pop(id_, None) is not None


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


def _is_rule(w: dict) -> bool:
    """A widget the check evaluates: a number or time series of a known measure with a warn or crit limit."""
    qs = w.get("queries") or []
    rule = w.get("rule") or {}
    return (w.get("type") in ("number", "timeseries") and ms.measure((qs[0] or {}).get("measure") if qs else "") is not None
            and (rule.get("warn") is not None or rule.get("crit") is not None))


def rule_keys(dashboards: Iterable[dict]) -> set[str]:
    """The keys (alerts.key_of) of every dashboard rule in these dashboards, with a figure now or not: a stored state
    whose key is here is kept while its figure is missing (alerts.diff ``present``)."""
    return {f"{d['id']}/{w['id']}" for d in dashboards for w in (d.get("def") or {}).get("widgets") or [] if _is_rule(w)}


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


def record(store: AlertStore, ws: str, now: list[dict], at: int, present: Iterable[str] | None = None) -> list[dict]:
    """Records what this check saw (store.ts record). ``present``: the keys of the rules that still exist, as
    alerts.diff takes them. Returns the changes this check won and must deliver: a change whose conditional write did
    not take effect was made by another check, which sends it. A firing rule dropped at this check sends its
    "no longer checked" change only if this check is the one that deleted its state."""
    prev = store.alert_states(ws)
    d = alerts.diff(prev, now, at, present)
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
        ok = store.drop_state(ws, s["dashboardId"], s["widgetId"])
        c = changed.get(alerts.key_of(s))
        if c and ok:
            won.append(c)
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
    "failed": [{ws, error}], "skipped": {"noChannel", "leased", "budget"}: workspaces with rules left out, "ms"}, and,
    only when not 0 (the hosted app's runAlerts has no outbox, and the shared cases hold none), "resent": changes an
    interrupted check left in the outbox, sent now, and "dropped": held changes given up."""
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
            if deps.deliver:
                _send_held(deps, ws, at, out)
            seen = observe(dashboards, deps.data(ws, dashboards)) if boards else []
            ruled = _CauseRules(deps, ws, at) if rules else None
            if ruled:
                seen.extend(ruled.observed)
            out["rules"] += len(seen)
            changes = record(store, ws, seen, at, rule_keys(dashboards))
            if ruled:
                changes = ruled.quiet(changes)
            # into the outbox before any other write: whatever fails or stops the check from here, the next one sends
            # what record() won (QV-757)
            held = _hold(store, ws, changes, at) if changes and deps.deliver else None
            if ruled:
                settled = ruled.settle(changes)
                if settled != changes and deps.deliver:
                    again = _hold(store, ws, settled, at)
                    if again is not None:
                        if held is not None:
                            _take_out(store, ws, held, len(changes))
                        held = again
                changes = settled
                ruled.keep()
            out["changes"] += len(changes)
            if changes and deps.deliver:
                deps.deliver(ws, changes)
                if held is not None:
                    _take_out(store, ws, held, len(changes))
            if ruled and ruled.failed:
                # the changes went out; the workspace is still noted as failed, with the first error
                raise ruled.failed[0]
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


def _hold(store: AlertStore, ws: str, changes: list, at: int) -> str | None:
    """Puts the changes this check won in the outbox until ``deliver`` returns, and returns the batch's ID. If they
    can't be held, logs it and returns None: they are sent all the same, and only a stop before then loses them."""
    id_ = str(uuid.uuid4())
    try:
        store.hold(ws, id_, changes, at)
    except Exception:  # noqa: BLE001
        log.exception("alert check: could not hold %d change(s) of workspace %s before sending them; a stop now "
                      "would lose them", len(changes), ws)
        return None
    return id_


def _take_out(store: AlertStore, ws: str, id_: str, n: int) -> None:
    """Takes a batch out of the outbox: it was sent, or held again with more."""
    try:
        store.sent(ws, id_)
    except Exception:  # noqa: BLE001 - the next check sends them again: twice rather than never
        log.warning("alert check: could not take %d change(s) of workspace %s out of the outbox; the next check "
                    "sends them again", n, ws, exc_info=True)


def _send_held(deps: Deps, ws: str, at: int, out: dict) -> None:
    """Sends what an interrupted check of the workspace left in the outbox, oldest first, before this check's own
    changes. Raises what ``deliver`` raises: the batch stays, with one more try counted."""
    assert deps.deliver is not None
    store = deps.store
    for b in store.held(ws):
        n = len(b["changes"])
        if b["tries"] >= MAX_TRIES or at - b["heldAt"] > MAX_HELD_MS:
            store.sent(ws, b["id"])
            out["dropped"] = out.get("dropped", 0) + n
            log.error("alert check: gave up on %d change(s) of workspace %s won at %d and never known to be sent "
                      "(%d tries)", n, ws, b["heldAt"], b["tries"])
            continue
        store.retry(ws, b["id"])
        log.warning("alert check: sending %d change(s) of workspace %s that an interrupted check left (won at %d, "
                    "try %d)", n, ws, b["heldAt"], b["tries"] + 1)
        deps.deliver(ws, b["changes"])
        store.sent(ws, b["id"])
        out["resent"] = out.get("resent", 0) + n


def has_rules(deps: Deps) -> bool:
    """Whether any workspace has a rule to check: the scheduler does nothing else when none has."""
    if deps.causes and deps.store.workspaces_with_rules():
        return True
    return any(has_rule(d) for ds in deps.dashboards(deps.only).values() for d in ds)


class _CauseRules:
    """The workspace's "Alert me" rules at this check: their figures (observed), to record with the dashboard
    rules; quiet(), which leaves out a muted or held rule's changes from the ones record() won and writes nothing;
    settle(), which adds a "still firing" change for a rule that woke up firing and a "Regressed" one; and keep(),
    which writes what each rule saw. run_alerts holds the changes in the outbox between quiet() and settle()."""

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
        #: the store writes that failed in settle() and keep(); run_alerts raises the first after the changes are sent
        self.failed: list[Exception] = []
        #: the keys of the changes record() won, before quiet()
        self.changed: set[str] = set()

    def quiet(self, changes: list[dict]) -> list[dict]:
        """The changes record() won less a muted or held rule's. Writes nothing."""
        R = alerts.RULE_DASHBOARD
        self.changed = {alerts.key_of(x["alert"]) for x in changes}
        # muted or held: nothing is sent. Just woken and found quiet: it stopped while muted, so nothing is sent either
        muted = {x["rule"]["id"] for x in self.checks if x["c"]["muted"]}
        woken = {x["rule"]["id"] for x in self.checks if x["memory"].get("wasMuted") and not x["c"]["muted"]}
        return [x for x in changes if not (x["alert"]["dashboardId"] == R and (
            x["alert"]["widgetId"] in muted or (x["kind"] == "resolved" and x["alert"]["widgetId"] in woken)))]

    def _try(self, what: str, rid: str, write: Callable[[], bool]) -> bool | None:
        """A conditional write of settle(): whether it took effect. One that raises didn't: logged, None, and the
        workspace fails once the changes are sent; the next check tries it again."""
        try:
            return write()
        except Exception as e:  # noqa: BLE001
            log.warning("alert check: could not %s for rule %s of workspace %s; the next check tries again", what,
                        rid, self.ws, exc_info=True)
            self.failed.append(e)
            return None

    def settle(self, changes: list[dict]) -> list[dict]:
        """quiet()'s changes with the "still firing" and "Regressed" ones this check wins (conditional writes)."""
        store, ws, at, R = self.deps.store, self.ws, self.at, alerts.RULE_DASHBOARD
        out = list(changes)
        changed = self.changed
        for x in self.checks:
            rule, memory, c, t = x["rule"], x["memory"], x["c"], x["t"]
            rid = rule["id"]
            p = self.prev.get(rid)
            woke = p is not None and (memory.get("wasMuted") and not c["muted"] and p["tone"] == "critical"
                    and c["observed"]["tone"] == "critical" and f"{R}/{rid}" not in changed)
            # only the check that clears the flag sends it, so two overlapping checks send it once
            cleared = self._try("clear wasMuted", rid, lambda: store.clear_was_muted(ws, rid)) if woke else False
            if cleared is None:
                # the write failed, so nothing is sent: the flag is kept, and the next check that finds the rule
                # firing sends it
                x["c"] = c = {**c, "memory": {**c["memory"], "wasMuted": True}}
            elif p is not None and cleared:
                back = ({"backFromIgnored": {"how": (t or {}).get("ignoredHow") or "ignored"}} if memory.get("wasHeld")
                        else {"afterSnooze": True})
                out.append({"kind": "fired", "from": None, "alert": {
                    **c["observed"], "since": p["since"], "checkedAt": at, "rule": {**c["observed"]["rule"], **back}}})
            # a regression: one message, at the first check that sees it, even inside the quiet period. One that
            # happened while the rule was muted, or before the rule was made, is let pass, so a later check doesn't
            # send it late
            tr = (t or {}).get("triage") or {}
            reg = tr.get("regressedAt") if tr.get("regressed") and tr.get("status") == "open" else None
            if (reg is not None and reg != memory.get("regressedSeen")
                    and self._try("note the regression", rid, lambda: store.set_regressed_seen(ws, rid, reg))
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
        return out

    def keep(self) -> None:
        """Writes what each rule saw. record() has already won the changes: a memory that can't be written must not
        stop them being sent, or every alert of the workspace stops while the rule's cause exists (QV-658)."""
        store, ws = self.deps.store, self.ws
        for x in self.checks:
            rid, c = x["rule"]["id"], x["c"]
            try:
                store.remember(ws, rid, c["memory"], c["seenAs"])
            except Exception as e:  # noqa: BLE001 - raised by run_alerts once the changes are sent
                log.warning("alert check: could not keep what rule %s of workspace %s saw", rid, ws, exc_info=True)
                self.failed.append(e)
