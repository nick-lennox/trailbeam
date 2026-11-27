"""Alert rules, evaluated (QV-627, the first part of QV-592: alerts in self-hosted Trailbeam).

The hosted app's engine in Python: lib/dashboards/measures.ts (toneOf and the limit alertOf names), lib/alerts/evaluate.ts
(diff) and lib/alerts/cause-rule.ts (sameCause, checkCauseRule), with the same behaviour. Pure functions over plain
inputs: no store, no clock, no delivery. The storage and the scheduled check that call them are later parts of QV-592.

The two engines must not drift: contract/fixtures/alerts-parity.json holds cases (a rule, its inputs, the expected
result) that python/tests/server/test_alerts.py runs here and the hosted app's test/alerts-parity.test.ts runs
against evaluate.ts. The file reaches the app inside @trailbeam/ui (@trailbeam/ui/fixtures/alerts-parity.json).

The records are dicts with the hosted app's JSON field names (dashboardId, widgetId, since, checkedAt, ...), so the
fixture, the stored state and, later, the API carry them unchanged. There are two kinds of rule:

- a dashboard rule: warn and crit limits on a number or time series widget. Its figure is worked out by the caller,
  and tone_of() says how it stands: "critical" past crit, "warning" past warn, else "good". "below" flips the test.
- a cause rule ("Alert me" on Needs attention): tell me when new messages hit this cause after it has been quiet for a
  while ("quiet"), or at every check that finds new ones ("every"). check_cause_rule() works it out at one check.

diff() compares what every rule is now with what was stored at the last check and says what changed. Only changes
are delivered; a rule that stays critical is not sent again.
"""
from __future__ import annotations

from typing import Iterable, Optional

Tone = str  # "good" | "warning" | "critical"

#: a cause rule's state is kept with the dashboard rules', under this dashboard ID and the rule's ID as widget ID
RULE_DASHBOARD = "rule"
#: checks don't start to the second: a quiet period counts as over this much early (ms)
SLACK_MS = 5 * 60_000
#: muted with no end (ms)
FOREVER = 8_640_000_000_000_000


# ── dashboard rules ───────────────────────────────────────────────────────────────────────────────────────────────

def _has(rule: Optional[dict], k: str) -> bool:
    return bool(rule) and rule.get(k) is not None


def tone_of(value: float, rule: Optional[dict]) -> Optional[Tone]:
    """How a figure stands against a rule {warn?, crit?, below?}: None when the rule sets no limit."""
    if not rule or (not _has(rule, "warn") and not _has(rule, "crit")):
        return None

    def over(k: str) -> bool:
        if not _has(rule, k):
            return False
        limit = rule[k]
        return value < limit if rule.get("below") else value > limit

    return "critical" if over("crit") else "warning" if over("warn") else "good"


def limit_of(rule: dict, tone: Tone) -> dict:
    """The limit an alert names: crit when critical or when there is no warn limit, else warn. As {"below", "limit"};
    the caller formats it in the figure's unit ("above 5", "below 10 %")."""
    limit = rule.get("crit") if tone == "critical" or not _has(rule, "warn") else rule.get("warn")
    return {"below": bool(rule.get("below")), "limit": 0 if limit is None else limit}


# ── state between checks ──────────────────────────────────────────────────────────────────────────────────────────

def key_of(a: dict) -> str:
    return f"{a['dashboardId']}/{a['widgetId']}"


def diff(prev: Iterable[dict], now: Iterable[dict], at: int) -> dict:
    """What the stored state becomes, and the changes to deliver: {"next", "changes", "gone"}.

    ``prev`` is the stored state (each rule as last seen, with ``since``: when its tone began, and ``checkedAt``);
    ``now`` is every rule that has a figure at this check. A rule seen for the first time only notifies if it is
    already firing. A rule that disappears (widget, rule or dashboard deleted, or its figure is gone) is dropped
    without a notification, and comes back in ``gone``. A change is {"kind": "fired" | "changed" | "resolved",
    "from": the tone before or None, "alert": the new stored record}."""
    before = {key_of(s): s for s in prev}
    nxt: list = []
    changes: list = []
    for o in now:
        k = key_of(o)
        p = before.pop(k, None)
        same = p is not None and p["tone"] == o["tone"]
        s = {**o, "since": p["since"] if same else at, "checkedAt": at}
        nxt.append(s)
        if same:
            continue
        if p is None:
            if o["tone"] != "good":
                changes.append({"kind": "fired", "from": None, "alert": s})
            continue
        kind = "resolved" if o["tone"] == "good" else "fired" if p["tone"] == "good" else "changed"
        changes.append({"kind": kind, "from": p["tone"], "alert": s})
    return {"next": nxt, "changes": changes, "gone": list(before.values())}


# ── cause rules ───────────────────────────────────────────────────────────────────────────────────────────────────

def _num(n: float) -> str:
    """A number as JavaScript's String() writes it, for the texts the hosted app makes."""
    return str(int(n)) if float(n).is_integer() else repr(float(n))


def quiet_text(s: int) -> str:
    """A quiet period as the forms and messages say it: "30 min", "6 h", "24 h", "7 days"."""
    if s % 86_400 == 0:
        return "24 h" if s == 86_400 else f"{_num(s / 86_400)} days"
    if s % 3600 == 0:
        return f"{_num(s / 3600)} h"
    return f"{_num(s / 60)} min"


def is_muted(rule: dict, at: int) -> bool:
    """Muted or snoozed at ``at`` (ms): ``mutedUntil`` is set and later."""
    return rule.get("mutedUntil") is not None and rule["mutedUntil"] > at


def same_cause(r: str, g: str) -> bool:
    """Whether a group with key g is the cause a rule was made on with key r. An exception class with too many error
    texts is merged into one group, "...|exc:<class>:*"; a rule matches across that merge in both directions."""
    if r == g:
        return True
    if g.endswith(":*") and "|exc:" in g and r.startswith(g[:-1]):
        return True
    if r.endswith(":*") and "|exc:" in r and g.startswith(r[:-1]):
        return True
    return False


def check_cause_rule(rule: dict, memory: dict, prev: Optional[dict], read: dict, at: int,
                     prev_check_at: Optional[int]) -> dict:
    """One cause rule at one check: its figure for diff(), and what to remember for the next check.

    ``rule`` has id, causeKey, causeId, label, queue, channels, mode ("quiet" or "every"), quietS and mutedUntil.
    ``memory`` is what the last check kept: seen (the cause's message IDs then, None: never checked), lastNewAt (the
    last check that found a new message, ms) and wasMuted. ``prev`` is the rule's stored state, if any. ``read`` is
    Needs attention's causes: {"groups": [{id, key, label, queue, count, basis, inferred}], "causeOf": {message ID:
    group ID}, "capped": bool}. ``at`` and ``prev_check_at`` are ms on the server's clock.

    A message is new when its ID was not in the cause at the last check, so no host's clock is trusted. A quiet rule
    fires when a check finds new messages and the last check that found any was at least the quiet period before; it
    resolves at the first check a quiet period after the last new message. The first check only notes what is there.
    Returns {"observed", "memory", "seenAs", "muted"}."""
    groups = [g for g in read["groups"] if same_cause(rule["causeKey"], g["key"])]
    ids = {g["id"] for g in groups}
    cur = sorted(mid for mid, gid in read["causeOf"].items() if gid in ids)
    baseline = memory.get("seen") is None
    seen = set(memory.get("seen") or [])
    new_count = 0 if baseline else sum(1 for m in cur if m not in seen)
    was_firing = prev is not None and prev.get("tone") == "critical"
    last_new = memory["lastNewAt"] if memory.get("lastNewAt") is not None else at
    quiet_over = at - last_new >= rule["quietS"] * 1000 - SLACK_MS

    if rule["mode"] == "every":
        firing = new_count > 0
        last_new_at = at if new_count > 0 or baseline else last_new
    elif baseline:
        firing, last_new_at = False, at
    elif new_count > 0:
        firing, last_new_at = was_firing or quiet_over, at
    else:
        firing, last_new_at = was_firing and not quiet_over, last_new

    # the biggest of the matching groups names the cause now (the first of equals); with none, what the rule kept
    main = sorted(groups, key=lambda g: -g["count"])[0] if groups else None
    label = main["label"] if main else rule["label"]
    queue = main["queue"] if main else rule["queue"]
    cause_id = main["id"] if main else rule["causeId"]
    info = {
        "kind": "cause", "ruleId": rule["id"], "channels": rule["channels"], "mode": rule["mode"], "quietS": rule["quietS"],
        "causeId": cause_id, "label": label, "queue": queue,
        "basis": (main or {}).get("basis") or "", "inferred": bool((main or {}).get("inferred")),
        "newCount": new_count, "notFinished": len(cur), "prevCheckAt": prev_check_at,
        "lastNewAt": memory.get("lastNewAt"), "capped": bool(read.get("capped")),
    }
    muted = is_muted(rule, at)
    limit = ("new messages at a check" if rule["mode"] == "every"
             else f"new messages after {quiet_text(rule['quietS'])} quiet")
    return {
        "observed": {
            "dashboardId": RULE_DASHBOARD, "dashboardName": "", "widgetId": rule["id"], "title": label,
            "tone": "critical" if firing else "good", "value": str(new_count), "limit": limit, "queue": queue, "rule": info,
        },
        "memory": {"seen": cur, "lastNewAt": last_new_at, "wasMuted": muted},
        "seenAs": {"label": label, "queue": queue, "causeId": cause_id} if main else None,
        "muted": muted,
    }

