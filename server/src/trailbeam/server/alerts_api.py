"""The alerts JSON API (QV-642, the fourth part of QV-592: alerts in self-hosted Trailbeam).

The hosted app's alert server actions in Python: lib/alerts/rule-actions.ts and rules.ts ("Alert me" rules: make,
edit, mute or snooze, delete), lib/alerts/actions.ts and channels.ts (alert channels: add, remove, send a test), with
the same checks and the same error texts, for the shared alert views to read. Plus what the hosted app does in its
pages rather than in actions: the lists, the current alert state, and changing a channel's name, target or tone.

Routes (``ROUTES``), all under ``/api/alerts``, JSON in and out, records with the hosted app's field names:

- ``GET  /rules``                    the rules, oldest first, each with its state (firing or not) and whether muted
- ``POST /rules``                    {causeId, channels, mode, quietS}: a rule on one of Needs attention's causes;
                                     a cause that has one already gets that one back (``existed``)
- ``PATCH /rules/{id}``              {channels, mode, quietS}
- ``POST /rules/{id}/mute``          {forS}: null mutes until unmuted, 3600, 14400 or 86400 snoozes, 0 unmutes
- ``DELETE /rules/{id}``             the rule and its state
- ``GET  /channels``                 the channels, oldest first
- ``POST /channels``                 {kind, name, target, minTone}; a webhook's signing secret is in this answer only
- ``PATCH /channels/{id}``           any of {name, target, minTone}
- ``DELETE /channels/{id}``
- ``POST /channels/{id}/test``       a sample alert through delivery (alert_delivery.send_to), noted on the channel
- ``GET  /state``                    every rule's state now (dashboard rules too), and when the last check ran

**Who may do what** (as the hosted app): anyone who may read the API reads everything here, and makes, edits, mutes
and deletes "Alert me" rules, which are the team's and often an on-call member's (QV-358 design section 10.3). Only an
admin adds, changes, tests or removes a channel, once the server has accounts (access.py); before an admin exists,
whoever is let in is the operator and may.

**Secrets.** A channel's target is never in an answer, only ``shown`` (alert_delivery.shown_target): a Slack URL up to
its team, a webhook up to its host, an email channel's addresses. A webhook's signing secret is answered once, when
the channel is added. The SMTP server and its password are the operator's settings, never the API's.

**Storage.** Rules, channels and alert state are read and written through ``AlertConfigStore``, which adds the
API's reads and writes to alerts_run.AlertStore, so the check and the API share one store: in the server, the SQLite
or Postgres tables (alerts_store.AlertTables, for_app); MemoryAlertConfig in tests. The routes answer only when
the scheduled check is on (``TRAILBEAM_ALERT_CHECK``, alerts_scheduler.py), so nothing half done shows by default.

contract/fixtures/alerts-api.json holds the checks' results (accepted and refused rules, mutes and channels, with
the error texts) as shared cases; test_alerts_api.py runs them here.
"""
from __future__ import annotations

import copy
import dataclasses
import logging
import os
import re
import secrets
import threading
import uuid
from collections.abc import Callable
from typing import Any, Protocol

from . import alert_delivery, alert_messages, alerts
from .alerts_run import AlertStore, MemoryAlertStore, now_ms

log = logging.getLogger("trailbeam.server.alerts")

# ── the hosted app's limits and choices (rules.ts, channels.ts, schedule.ts) ─────────────────────────────────────

MAX_RULES = 100
MAX_CHANNELS = 20
#: the quiet periods offered (design section 5.1), s; only those at least as long as the check interval are
QUIET_PERIODS_S = (1800, 3600, 6 * 3600, 24 * 3600, 7 * 86_400)
SNOOZES_S = (3600, 4 * 3600, 24 * 3600)
#: the hosted app's check interval when nothing says otherwise (schedule.ts CHECK_EVERY_MIN)
CHECK_EVERY_MIN = 1440
MIN_TONES = ("warning", "critical")
DEFAULT_NAMES = {"webhook": "Webhook", "slack": "Slack", "email": "Email"}

RULE_DELETED = "That alert was deleted."
CHANNEL_REMOVED = "That channel was removed."
CAUSE_GONE = "That cause has no unfinished messages now. Reload the page."
CAUSE_UNKNOWN = "That cause can't be found any more. Reload the page."
TIMED_OUT = "The endpoint did not answer in time."


def check_every(min_: int = CHECK_EVERY_MIN) -> str:
    """How often, in words (schedule.ts checkEvery)."""
    if min_ == 1440:
        return "once a day, at 13:00 UTC"
    if min_ == 1:
        return "every minute"
    if min_ % 60 == 0:
        return f"every {'hour' if min_ // 60 == 1 else f'{min_ // 60} hours'}"
    return f"every {min_} minutes"


def cadence_note(min_: int = CHECK_EVERY_MIN) -> str:
    """What a rule's form and list say about timing (rules.ts cadenceNote)."""
    late = "a day" if min_ == 1440 else "a minute" if min_ == 1 else re.sub(r"^every ", "", check_every(min_))
    return f"Trailbeam checks {check_every(min_)}, so an alert can arrive up to {late} after it happens."


def quiet_choices(min_: int = CHECK_EVERY_MIN) -> list[int]:
    return [s for s in QUIET_PERIODS_S if s >= min_ * 60]


def default_quiet(min_: int = CHECK_EVERY_MIN) -> int:
    """6 h, or the shortest quiet period the check interval allows."""
    c = quiet_choices(min_)
    return next((s for s in c if s >= 6 * 3600), c[0] if c else QUIET_PERIODS_S[-1])


# ── the checks (pure, the shared cases run them) ─────────────────────────────────────────────────────────────────

def _fail(error: str) -> dict:
    return {"ok": False, "error": error}


def _js_number(v: Any) -> float | None:
    """Number(v) as JavaScript reads a form value, None for NaN. Only what JSON can send."""
    if isinstance(v, bool):
        return float(v)
    if isinstance(v, (int, float)):
        return float(v)
    if v is None:
        return 0.0
    if isinstance(v, str):
        t = v.strip()
        if t == "":
            return 0.0
        try:
            return float(t) if re.fullmatch(r"[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?", t) else None
        except ValueError:
            return None
    return None


def check_rule_form(f: dict, channel_ids: list, every_min: int = CHECK_EVERY_MIN) -> dict:
    """A rule's form checked against the channels and the check interval (rules.ts checkForm): {"ok": True,
    "channels", "mode", "quietS"} to store, or {"ok": False, "error"}."""
    raw = f.get("channels")
    picked = raw if isinstance(raw, list) else []
    channels = [c for c in dict.fromkeys(x for x in picked if isinstance(x, str)) if c in channel_ids]
    if not channels:
        return _fail("Pick at least one channel." if channel_ids else "Add an alert channel in Settings first.")
    mode = f.get("mode")
    if mode not in ("quiet", "every"):
        return _fail("Pick when to send.")
    quiet = 0.0 if mode == "every" else _js_number(f.get("quietS"))
    if mode == "quiet" and quiet not in [float(s) for s in quiet_choices(every_min)]:
        return _fail(f"Pick a quiet period of {', '.join(alerts.quiet_text(s) for s in quiet_choices(every_min))}. "
                     f"{cadence_note(every_min)}")
    return {"ok": True, "channels": channels, "mode": mode, "quietS": int(quiet or 0)}


def check_mute(for_s: Any, at: int) -> dict:
    """A mute (rules.ts muteRule): {"ok": True, "mutedUntil"} (None: unmuted), or {"ok": False, "error"}. ``for_s``
    None mutes until someone unmutes, 0 unmutes or wakes, one of SNOOZES_S snoozes."""
    if for_s is not None and not (isinstance(for_s, (int, float)) and not isinstance(for_s, bool)
                                  and (for_s == 0 or for_s in SNOOZES_S)):
        return _fail("Snooze for 1, 4 or 24 hours.")
    until = alerts.FOREVER if for_s is None else None if for_s == 0 else at + int(for_s) * 1000
    return {"ok": True, "mutedUntil": until}


def check_channel_form(f: dict, allow_local: bool = False, allow_private: bool = False) -> dict:
    """A new channel (channels.ts addChannel, before it is stored): {"ok": True, "kind", "name", "target",
    "minTone"}, or {"ok": False, "error"}."""
    kind = f.get("kind")
    if kind not in alert_delivery.CHANNEL_KINDS:
        return _fail("Pick webhook, Slack or email.")
    min_tone = "critical" if f.get("minTone") == "critical" else "warning"
    t = alert_delivery.check_target(kind, _text(f.get("target")), allow_local, allow_private)
    if not t["ok"]:
        return t
    return {"ok": True, "kind": kind, "name": _name(kind, f.get("name")), "target": t["target"], "minTone": min_tone}


def _text(v: Any) -> str:
    return v if isinstance(v, str) else "" if v is None else str(v)


def _name(kind: str, v: Any) -> str:
    return _text(v).strip()[:60] or DEFAULT_NAMES[kind]


# ── storage ──────────────────────────────────────────────────────────────────────────────────────────────────────

class AlertConfigStore(AlertStore, Protocol):
    """alerts_run.AlertStore and what the API reads and writes. Rules are the hosted app's Rule records; the check's
    memory of each rule is the store's (alerts_run.AlertStore.rules_with_memory). Channels are alert_delivery's
    channel dicts with createdBy, createdAt, lastAt, lastOk and lastDetail."""

    def list_rules(self, ws: str) -> list[dict]:
        """Oldest first."""

    def get_rule(self, ws: str, rule_id: str) -> dict | None:
        ...

    def rule_for_cause(self, ws: str, cause_key: str) -> dict | None:
        ...

    def add_rule(self, ws: str, rule: dict) -> bool:
        """Stores a new rule, with an empty memory, unless the workspace has one for its cause already (one rule per
        cause, design section 5.2). True if it was stored."""

    def update_rule(self, ws: str, rule_id: str, fields: dict) -> dict | None:
        """Sets these fields of the rule; the rule after, or None when there is no such rule."""

    def remove_rule(self, ws: str, rule_id: str) -> bool:
        """Deletes the rule and its state. True if there was one."""

    def list_channels(self, ws: str) -> list[dict]:
        """Oldest first."""

    def get_channel(self, ws: str, channel_id: str) -> dict | None:
        ...

    def add_channel(self, ws: str, channel: dict) -> None:
        ...

    def update_channel(self, ws: str, channel_id: str, fields: dict) -> dict | None:
        ...

    def remove_channel(self, ws: str, channel_id: str) -> bool:
        ...

    def note_delivery(self, ws: str, channel_id: str, at: int, ok: bool, detail: str | None) -> None:
        """The outcome of the channel's last send, for Settings."""

    def workspaces_with_channels(self) -> set[str]:
        """For the check (alerts_run.Deps.channels): it checks only workspaces with somewhere to send."""


def _locked(fn):
    def call(self, *a, **k):
        with self._lock:
            return fn(self, *a, **k)
    call.__name__, call.__doc__ = fn.__name__, fn.__doc__
    return call


class MemoryAlertConfig(MemoryAlertStore):
    """AlertConfigStore in memory: the tests, and a server whose store has no alert tables (a test double; what it
    holds is lost when the server stops). The API runs on request threads and the check on its own, so every method holds one lock."""

    def __init__(self, rules: dict | None = None, states: dict | None = None, checks: dict | None = None,
                 leases: dict | None = None, channels: dict | None = None):
        super().__init__(rules, states, checks, leases)
        #: ws -> [channel]
        self.channels: dict[str, list[dict]] = copy.deepcopy(channels or {})
        self._lock = threading.RLock()

    # the check's reads and writes, under the same lock
    last_checks = _locked(MemoryAlertStore.last_checks)
    last_check = _locked(MemoryAlertStore.last_check)
    checked = _locked(MemoryAlertStore.checked)
    lease = _locked(MemoryAlertStore.lease)
    release = _locked(MemoryAlertStore.release)
    alert_states = _locked(MemoryAlertStore.alert_states)
    write_state = _locked(MemoryAlertStore.write_state)
    drop_state = _locked(MemoryAlertStore.drop_state)
    workspaces_with_rules = _locked(MemoryAlertStore.workspaces_with_rules)
    rules_with_memory = _locked(MemoryAlertStore.rules_with_memory)
    remember = _locked(MemoryAlertStore.remember)
    clear_was_muted = _locked(MemoryAlertStore.clear_was_muted)
    set_regressed_seen = _locked(MemoryAlertStore.set_regressed_seen)
    hold = _locked(MemoryAlertStore.hold)
    held = _locked(MemoryAlertStore.held)
    retry = _locked(MemoryAlertStore.retry)
    sent = _locked(MemoryAlertStore.sent)

    @_locked
    def list_rules(self, ws: str) -> list[dict]:
        return [x["rule"] for x in super().rules_with_memory(ws)]

    @_locked
    def get_rule(self, ws: str, rule_id: str) -> dict | None:
        x = self._rule(ws, rule_id)
        return copy.deepcopy(x["rule"]) if x else None

    @_locked
    def rule_for_cause(self, ws: str, cause_key: str) -> dict | None:
        x = next((x for x in self.rules.get(ws, []) if x["rule"]["kind"] == "cause"
                  and x["rule"]["causeKey"] == cause_key), None)
        return copy.deepcopy(x["rule"]) if x else None

    @_locked
    def add_rule(self, ws: str, rule: dict) -> bool:
        if self.rule_for_cause(ws, rule["causeKey"]) is not None:
            return False
        self.rules.setdefault(ws, []).append({"rule": copy.deepcopy(rule), "memory": {
            "seen": None, "lastNewAt": None, "wasMuted": False, "wasHeld": False, "regressedSeen": None}})
        return True

    @_locked
    def update_rule(self, ws: str, rule_id: str, fields: dict) -> dict | None:
        x = self._rule(ws, rule_id)
        if x is None:
            return None
        x["rule"] = {**x["rule"], **copy.deepcopy(fields)}
        return copy.deepcopy(x["rule"])

    @_locked
    def remove_rule(self, ws: str, rule_id: str) -> bool:
        rs = self.rules.get(ws, [])
        kept = [x for x in rs if x["rule"]["id"] != rule_id]
        self.rules[ws] = kept
        super().drop_state(ws, alerts.RULE_DASHBOARD, rule_id)
        return len(kept) < len(rs)

    @_locked
    def list_channels(self, ws: str) -> list[dict]:
        return copy.deepcopy(sorted(self.channels.get(ws, []), key=lambda c: c["createdAt"]))

    def _channel(self, ws: str, channel_id: str) -> dict | None:
        return next((c for c in self.channels.get(ws, []) if c["id"] == channel_id), None)

    @_locked
    def get_channel(self, ws: str, channel_id: str) -> dict | None:
        return copy.deepcopy(self._channel(ws, channel_id))

    @_locked
    def add_channel(self, ws: str, channel: dict) -> None:
        self.channels.setdefault(ws, []).append(copy.deepcopy(channel))

    @_locked
    def update_channel(self, ws: str, channel_id: str, fields: dict) -> dict | None:
        c = self._channel(ws, channel_id)
        if c is None:
            return None
        c.update(copy.deepcopy(fields))
        return copy.deepcopy(c)

    @_locked
    def remove_channel(self, ws: str, channel_id: str) -> bool:
        cs = self.channels.get(ws, [])
        self.channels[ws] = [c for c in cs if c["id"] != channel_id]
        return len(self.channels[ws]) < len(cs)

    @_locked
    def note_delivery(self, ws: str, channel_id: str, at: int, ok: bool, detail: str | None) -> None:
        c = self._channel(ws, channel_id)
        if c is not None:
            c.update({"lastAt": at, "lastOk": ok, "lastDetail": detail[:300] if detail is not None else None})

    @_locked
    def workspaces_with_channels(self) -> set[str]:
        return {ws for ws, cs in self.channels.items() if cs}


# ── the API ──────────────────────────────────────────────────────────────────────────────────────────────────────

class ApiProblem(Exception):
    """An answer other than 200: its status and the text in ``{"error"}``."""

    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status, self.message = status, message


#: half of a UTF-16 pair, as a truncated emoji leaves: JSON can carry one ("\\ud800") but neither database can store
#: it, nor can it be written as UTF-8. JSON joins a whole pair into one character, so any surrogate in a str is lone.
_LONE_SURROGATE = re.compile("[\ud800-\udfff]")
HIDDEN_CHARACTERS = ("The request has text that can't be saved (half of an emoji or another broken character), often "
                     "from pasted text. Retype the pasted part and try again.")


def check_text(v: Any) -> None:
    """Raises ApiProblem(400) when a JSON value has a lone surrogate in any string, a key or a value, at any depth
    (QV-687); before, the store's UTF-8 encode raised and the request was a 500."""
    stack = [v]
    while stack:
        x = stack.pop()
        if isinstance(x, str):
            if _LONE_SURROGATE.search(x):
                raise ApiProblem(400, HIDDEN_CHARACTERS)
        elif isinstance(x, dict):
            stack.extend(x.keys())
            stack.extend(x.values())
        elif isinstance(x, list):
            stack.extend(x)


#: (method, path template, handler, who may: "member" or "manager"). ``{id}`` is a rule's or channel's ID.
ROUTES: tuple[tuple[str, str, str, str], ...] = (
    ("GET", "/api/alerts/rules", "list_rules", "member"),
    ("POST", "/api/alerts/rules", "add_rule", "member"),
    ("PATCH", "/api/alerts/rules/{id}", "edit_rule", "member"),
    ("POST", "/api/alerts/rules/{id}/mute", "mute_rule", "member"),
    ("DELETE", "/api/alerts/rules/{id}", "remove_rule", "member"),
    ("GET", "/api/alerts/channels", "list_channels", "member"),
    ("POST", "/api/alerts/channels", "add_channel", "manager"),
    ("PATCH", "/api/alerts/channels/{id}", "change_channel", "manager"),
    ("DELETE", "/api/alerts/channels/{id}", "remove_channel", "manager"),
    ("POST", "/api/alerts/channels/{id}/test", "test_channel", "manager"),
    ("GET", "/api/alerts/state", "list_state", "member"),
)
_ID = "([A-Za-z0-9][A-Za-z0-9_-]{0,63})"
_RX = tuple((m, re.compile("^" + re.escape(t).replace(re.escape("{id}"), _ID) + "$"), h, w) for m, t, h, w in ROUTES)


def route(method: str, path: str) -> tuple[str, str | None, str] | None:
    """The alerts route of a request: (handler, the ``{id}`` or None, "member" or "manager"), or None."""
    for m, rx, h, w in _RX:
        if m == method:
            hit = rx.match(path)
            if hit:
                return h, (hit.group(1) if rx.groups else None), w
    return None


def is_alerts_path(path: str) -> bool:
    return path == "/api/alerts" or path.startswith("/api/alerts/")


def _by(who) -> str:
    """Who made or muted something, as stored: the person's ID, or how the request was let in."""
    if getattr(who, "user_id", None) is not None:
        return str(who.user_id)
    return getattr(who, "via", None) or "none"


def channel_view(c: dict) -> dict:
    """A channel as answered: never its target or secret."""
    return {"id": c["id"], "kind": c["kind"], "name": c["name"], "shown": alert_delivery.shown_target(c),
            "minTone": c["minTone"], "signed": bool(c.get("secret")), "createdBy": c.get("createdBy"),
            "createdAt": c.get("createdAt"), "lastAt": c.get("lastAt"), "lastOk": c.get("lastOk"),
            "lastDetail": c.get("lastDetail")}


class AlertsApi:
    """The handlers over one store, for one workspace (self-hosted Trailbeam has one).

    ``causes``: the workspace's causes as Needs attention groups them now ([{id, key, label, queue}]), for a new
    rule. ``delivery``: how a test is sent (alert_delivery.Delivery); its ``allow_local`` is also what a channel's
    target is checked with. ``every_min``: the scheduled check's interval, which bounds the quiet periods.
    ``is_manager``: whether a request's Who may change channels; the default lets anyone in."""

    def __init__(self, store: AlertConfigStore, workspace: dict, causes: Callable[[], list],
                 delivery: alert_delivery.Delivery, every_min: int = CHECK_EVERY_MIN,
                 is_manager: Callable[[Any], bool] = lambda who: True, now: Callable[[], int] = now_ms):
        self.store, self.ws, self.causes, self.delivery = store, workspace, causes, delivery
        self.every_min, self.is_manager, self.now = every_min, is_manager, now

    @property
    def ws_id(self) -> str:
        return str(self.ws["id"])

    def call(self, method: str, path: str, body: dict | None, who, base: str | None = None) -> tuple[int, dict]:
        """(status, JSON body) for an alerts route. ``base``: the URL the app's pages hang off, as this request
        reached the server, for the links in a test alert (else the delivery's). Raises ApiProblem (404 for no such
        route, 403 when a channel change isn't this person's to make)."""
        r = route(method, path)
        if r is None:
            raise ApiProblem(404, f"no such endpoint: {method} {path}")
        check_text(body)
        handler, rid, needs = r
        if needs == "manager" and not self.is_manager(who):
            raise ApiProblem(403, "this needs an admin")
        if handler == "test_channel":
            return self.test_channel(rid, body or {}, who, base)
        return getattr(self, handler)(rid, body or {}, who)

    # ---- rules ----------------------------------------------------------------------------------------------

    def _rule_view(self, rule: dict, states: dict, at: int) -> dict:
        return {**rule, "muted": alerts.is_muted(rule, at), "state": states.get(rule["id"])}

    def _rule_states(self) -> dict:
        return {s["widgetId"]: s for s in self.store.alert_states(self.ws_id) if s["dashboardId"] == alerts.RULE_DASHBOARD}

    def _channel_ids(self) -> list:
        return [c["id"] for c in self.store.list_channels(self.ws_id)]

    def list_rules(self, rid, body, who) -> tuple[int, dict]:
        at, states = self.now(), self._rule_states()
        return 200, {"rules": [self._rule_view(r, states, at) for r in self.store.list_rules(self.ws_id)],
                     "checkEvery": check_every(self.every_min), "everyMin": self.every_min, "cadence": cadence_note(self.every_min),
                     "quietChoices": quiet_choices(self.every_min), "defaultQuiet": default_quiet(self.every_min),
                     "snoozes": list(SNOOZES_S), "max": MAX_RULES}

    def add_rule(self, rid, body, who) -> tuple[int, dict]:
        cause_id = body.get("causeId")
        g = next((x for x in self.causes() if x.get("id") == cause_id), None) if isinstance(cause_id, str) else None
        if g is None:
            raise ApiProblem(404, CAUSE_GONE)
        if not str(g.get("key") or "").startswith("v1|") or not g.get("id"):
            raise ApiProblem(404, CAUSE_UNKNOWN)
        c = check_rule_form(body, self._channel_ids(), self.every_min)
        if not c["ok"]:
            raise ApiProblem(400, c["error"])
        ws = self.ws_id
        had = self.store.rule_for_cause(ws, g["key"])
        if had is not None:
            return 200, {"rule": had, "existed": True}
        if len(self.store.list_rules(ws)) >= MAX_RULES:
            raise ApiProblem(400, f"A workspace can have up to {MAX_RULES} alert rules.")
        at = self.now()
        rule = {"id": str(uuid.uuid4()), "kind": "cause", "causeKey": g["key"], "causeId": g["id"],
                "label": str(g.get("label") or "")[:300], "queue": str(g.get("queue") or ""),
                "channels": c["channels"], "mode": c["mode"], "quietS": c["quietS"], "mutedUntil": None,
                "mutedBy": None, "mutedAt": None, "createdBy": _by(who), "createdAt": at, "updatedAt": at}
        if self.store.add_rule(ws, rule):
            return 201, {"rule": rule, "existed": False}
        # two people made it at the same moment: theirs stands
        return 200, {"rule": self.store.rule_for_cause(ws, g["key"]), "existed": True}

    def edit_rule(self, rid, body, who) -> tuple[int, dict]:
        c = check_rule_form(body, self._channel_ids(), self.every_min)
        if not c["ok"]:
            raise ApiProblem(400, c["error"])
        r = self.store.update_rule(self.ws_id, rid, {"channels": c["channels"], "mode": c["mode"],
                                                     "quietS": c["quietS"], "updatedAt": self.now()})
        if r is None:
            raise ApiProblem(404, RULE_DELETED)
        return 200, {"rule": r}

    def mute_rule(self, rid, body, who) -> tuple[int, dict]:
        at = self.now()
        if "forS" not in body:
            raise ApiProblem(400, "Snooze for 1, 4 or 24 hours.")
        m = check_mute(body["forS"], at)
        if not m["ok"]:
            raise ApiProblem(400, m["error"])
        until = m["mutedUntil"]
        r = self.store.update_rule(self.ws_id, rid, {"mutedUntil": until, "mutedBy": None if until is None else _by(who),
                                                     "mutedAt": None if until is None else at, "updatedAt": at})
        if r is None:
            raise ApiProblem(404, RULE_DELETED)
        return 200, {"rule": self._rule_view(r, self._rule_states(), at)}

    def remove_rule(self, rid, body, who) -> tuple[int, dict]:
        return 200, {"removed": self.store.remove_rule(self.ws_id, rid)}

    # ---- channels -------------------------------------------------------------------------------------------

    def list_channels(self, rid, body, who) -> tuple[int, dict]:
        return 200, {"channels": [channel_view(c) for c in self.store.list_channels(self.ws_id)],
                     "max": MAX_CHANNELS, "canManage": self.is_manager(who)}

    def add_channel(self, rid, body, who) -> tuple[int, dict]:
        f = check_channel_form(body, self.delivery.allow_local, self.delivery.allow_private)
        if not f["ok"]:
            raise ApiProblem(400, f["error"])
        ws = self.ws_id
        if len(self.store.list_channels(ws)) >= MAX_CHANNELS:
            raise ApiProblem(400, f"A workspace can have up to {MAX_CHANNELS} alert channels.")
        c = {"id": str(uuid.uuid4()), "kind": f["kind"], "name": f["name"], "target": f["target"],
             "secret": f"whsec_{secrets.token_hex(24)}" if f["kind"] == "webhook" else None, "minTone": f["minTone"],
             "createdBy": _by(who), "createdAt": self.now(), "lastAt": None, "lastOk": None, "lastDetail": None}
        self.store.add_channel(ws, c)
        return 201, {"channel": channel_view(c), "secret": c["secret"]}

    def change_channel(self, rid, body, who) -> tuple[int, dict]:
        c = self.store.get_channel(self.ws_id, rid)
        if c is None:
            raise ApiProblem(404, CHANNEL_REMOVED)
        fields: dict = {}
        if "name" in body:
            fields["name"] = _name(c["kind"], body["name"])
        if "minTone" in body:
            fields["minTone"] = "critical" if body["minTone"] == "critical" else "warning"
        if "target" in body:
            t = alert_delivery.check_target(c["kind"], _text(body["target"]), self.delivery.allow_local,
                                            self.delivery.allow_private)
            if not t["ok"]:
                raise ApiProblem(400, t["error"])
            fields["target"] = t["target"]
        if "kind" in body and body["kind"] != c["kind"]:
            raise ApiProblem(400, "A channel's kind can't change: add a new channel instead.")
        after = self.store.update_channel(self.ws_id, rid, fields) if fields else c
        if after is None:
            raise ApiProblem(404, CHANNEL_REMOVED)
        return 200, {"channel": channel_view(after)}

    def remove_channel(self, rid, body, who) -> tuple[int, dict]:
        return 200, {"removed": self.store.remove_channel(self.ws_id, rid)}

    def test_channel(self, rid, body, who, base: str | None = None) -> tuple[int, dict]:
        """Sends a sample alert (actions.ts testChannelAction). A failed send is a 200 with ok false and the reason,
        as Settings shows it; it is noted on the channel either way."""
        c = self.store.get_channel(self.ws_id, rid)
        if c is None:
            raise ApiProblem(404, CHANNEL_REMOVED)
        at = self.now()
        out: dict = {"ok": True}
        try:
            d = dataclasses.replace(self.delivery, base=base) if base else self.delivery
            alert_delivery.send_to(c, self.ws, [alert_messages.test_change(at)], d)
        except alert_delivery.DeliveryError as e:
            out = {"ok": False, "error": alert_delivery._scrub(str(e), c, self.delivery)}
        except TimeoutError:
            out = {"ok": False, "error": TIMED_OUT}
        except Exception as e:  # noqa: BLE001 - nor a secret through a library's message
            out = {"ok": False, "error": f"Sending failed: {type(e).__name__}."}
        self.store.note_delivery(self.ws_id, rid, at, out["ok"], "Test sent." if out["ok"] else out["error"])
        return 200, out

    # ---- state ----------------------------------------------------------------------------------------------

    def list_state(self, rid, body, who) -> tuple[int, dict]:
        """Every rule's state as the last check left it: dashboard rules under their dashboard and widget, "Alert
        me" rules under dashboardId "rule" and the rule's ID."""
        ws = self.ws_id
        states = sorted(self.store.alert_states(ws), key=lambda s: (s["dashboardId"], s["widgetId"]))
        return 200, {"alerts": states, "checkedAt": self.store.last_check(ws),
                     "hasChannel": bool(self.store.list_channels(ws)), "checkEvery": check_every(self.every_min),
                     "everyMin": self.every_min}


# ── in the server ────────────────────────────────────────────────────────────────────────────────────────────────

#: self-hosted Trailbeam's one workspace, as the check, the store and the messages name it
WORKSPACE = {"id": "local", "name": "Trailbeam", "slug": "local"}


def for_app(app, env=None) -> AlertsApi | None:
    """The API for server.App, or None (the routes are 404) while the scheduled check is off. On the store's alert
    tables (alerts_store.AlertTables), so channels and their secrets outlive a restart and every server process on one
    database shares them; a store without them (a test double) keeps them in memory. Links point to --public-url and
    email goes through the operator's mail server (TRAILBEAM_SMTP_HOST, app.mailer), as for invites (QV-674).
    Webhooks may go to this server's own network when TRAILBEAM_ALERT_ALLOW_PRIVATE_WEBHOOKS is on (QV-764,
    allow_private_webhooks). ValueError on a setting the check can't take (alerts_scheduler.check_every_min, and
    that switch)."""
    from . import alerts_scheduler, alerts_store

    env = os.environ if env is None else env
    every = alerts_scheduler.check_every_min(env)
    if every is None:
        return None
    allow_private = allow_private_webhooks(env)

    def is_manager(who) -> bool:
        return app.access.mode() != "accounts" or getattr(who, "role", None) == "admin"

    def causes() -> list:
        return list(app.causes({}, [])["groups"])

    db = app.db
    store = alerts_store.AlertTables(db) if hasattr(db, "reader") or hasattr(db, "reads") else MemoryAlertConfig()
    if app.public_url is None:
        log.warning("alerts: --public-url is not set, so the links in scheduled alerts point to %s", LOCAL_BASE)
    mailer = getattr(app, "mailer", None)
    delivery = alert_delivery.Delivery(base=app.public_url or LOCAL_BASE,
                                       smtp=getattr(mailer, "smtp", None), allow_private=allow_private)
    if allow_private:
        log.info("alerts: %s is on, so webhooks may go to private and plain-http addresses on this network",
                 PRIVATE_WEBHOOKS_ENV)
    return AlertsApi(store, WORKSPACE, causes, delivery, every_min=every, is_manager=is_manager)


#: the operator's switch for alert webhooks on this server's own network (alert_delivery.Delivery.allow_private)
PRIVATE_WEBHOOKS_ENV = "TRAILBEAM_ALERT_ALLOW_PRIVATE_WEBHOOKS"


def allow_private_webhooks(env) -> bool:
    """TRAILBEAM_ALERT_ALLOW_PRIVATE_WEBHOOKS: 1, true, yes or on lets a webhook or Slack channel point at a private
    or local address, over https or plain http (plain http only to private addresses); off by default. Cloud
    metadata addresses stay refused. ValueError on anything else, so a typo doesn't silently leave it off."""
    raw = str(env.get(PRIVATE_WEBHOOKS_ENV, "")).strip().lower()
    if raw in ("1", "true", "yes", "on"):
        return True
    if raw in ("", "0", "false", "no", "off"):
        return False
    raise ValueError(f"{PRIVATE_WEBHOOKS_ENV} is 1 (on) or 0 (off), not {raw!r}")


#: where scheduled alerts link without --public-url (a test send links to the address it was asked at)
LOCAL_BASE = "http://localhost"


def check_deps(app, api: AlertsApi):
    """What the scheduled check (alerts_run.run_alerts) reads and writes in the server, on the API's store, so the
    check and the API share rules, channels and state:

    - ``dashboards``: the dashboards' rules, when TRAILBEAM_DASHBOARDS is on (app.dashboards), else none;
    - ``data``: what those rules need (alerts_run.alert_needs), read as the dashboards' measures read it
      (dashboards_api.event_data), from as far back as the longest rule's range;
    - ``causes``: the stuck messages grouped as Stuck (``/api/causes``) groups them, with each message's cause;
    - ``channels`` and ``deliver``: the workspaces with a channel, and each change sent to every channel that wants
      it (alert_delivery.deliver), its outcome noted on the channel as a test send's is. A channel that fails is
      noted, not retried: as the hosted app's deliverChanges, the next change goes out at the next check.

    No triage yet: no rule is held while its cause is ignored."""
    from . import alerts_run, causes as causes_mod, dashboards_api

    store, ws = api.store, api.ws
    ns = 1_000_000

    def dashboards(only):
        board = app.dashboards
        if board is None or (only is not None and only != api.ws_id):
            return {}
        return {api.ws_id: board.store.list_dashboards(board.ws)}

    def data(ws_id, boards):
        now = app.now() // ns
        need = alerts_run.alert_needs(boards)
        since = now - int(need.get("sinceMs") or 0)
        return dashboards_api.event_data(app.db, now, since, app.lambda_timeout_s, app.max_receive_counts)

    def causes(ws_id):
        read = causes_mod.read_unfinished(app.db, app.now(), app.lambda_timeout_s, app.max_receive_counts,
                                          app.stuck_after_s)
        assign: dict = {}
        groups = causes_mod.in_range(read["classified"], read["first_seen"], None, None, assign)
        return {"groups": [causes_mod.public(g) for g in groups], "causeOf": assign, "capped": read["capped"]}

    def deliver(ws_id, changes):
        for o in alert_delivery.deliver(store.list_channels(ws_id), ws, changes, api.delivery):
            try:
                store.note_delivery(ws_id, o["channel"], o["at"], o["ok"], o["detail"])
            except Exception:  # noqa: BLE001 - the changes went out; only the note on the channel is lost
                log.warning("alert check: could not note a delivery on channel %s", o["channel"], exc_info=True)

    return alerts_run.Deps(store=store, dashboards=dashboards, data=data, causes=causes,
                           channels=store.workspaces_with_channels, deliver=deliver,
                           now=lambda: app.now() // ns)


def start_check(app):
    """Starts the scheduled check in this server process (alerts_scheduler.AlertScheduler), on the alerts API's
    store, when the check is on (app.alerts); None otherwise. Every replica of one store runs it: each workspace's
    lease (alerts_run.LEASE_MS) lets one check it at a time."""
    from . import alerts_run, alerts_scheduler

    if app.alerts is None:
        return None
    deps = check_deps(app, app.alerts)
    return alerts_scheduler.AlertScheduler(lambda: alerts_run.run_alerts(deps), app.alerts.every_min * 60,
                                           lambda: alerts_run.has_rules(deps)).start()
