"""What an alert says (QV-636, the second part of QV-592: alerts in self-hosted Trailbeam).

The hosted app's message texts and payloads in Python: lib/alerts/deliver.ts (the webhook body and its signature, the
Slack text, the email) and the line functions of lib/alerts/cause-rule.ts (causeHead, causeLine), with the same output
byte for byte, so a rule reads the same in both products. Pure functions: no network, no clock. Sending them is
alert_delivery.py.

contract/fixtures/alerts-delivery.json holds cases (a channel, a check's changes, what is sent) that
python/tests/server/test_alert_delivery.py runs here and the hosted app's test/alerts-delivery.test.ts runs against
sendTo(). Changes are the dicts alerts.diff() returns, with the hosted app's JSON field names.

Links. The hosted app links to <origin>/app/<workspace slug>/...; self-hosted Trailbeam has no workspace in its paths.
So every function here takes ``base``, the URL the app's pages hang off ("https://trailbeam.example.com", or for the
hosted app "https://trailbeam.app/app/acme"), and links to <base>/dashboards/<id>, <base>/stuck?cause=<id> and
<base>/settings#alerts, as the hosted app does under its base.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import re
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import quote

from .alerts import quiet_text

TONE_WORD = {"good": "OK", "warning": "Warning", "critical": "Critical"}
MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


# ── JavaScript's ways, for the same text ──────────────────────────────────────────────────────────────────────────

def iso(ms: int) -> str:
    """new Date(ms).toISOString(): "2025-11-24T16:00:00.000Z"."""
    s, m = divmod(int(ms), 1000)
    return datetime.fromtimestamp(s, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S") + f".{m:03d}Z"


def _js_round(x: float) -> int:
    """Math.round: halves go up, not to even."""
    return int((x + 0.5) // 1)


def _js_len(s: str) -> int:
    """String length as JavaScript counts it, in UTF-16 code units."""
    return len(s.encode("utf-16-le")) // 2


def _js_slice(s: str, n: int) -> str:
    """The first n UTF-16 code units. Where that would split a character in two (JavaScript keeps the half), the
    whole character is left out."""
    units = 0
    for i, ch in enumerate(s):
        units += 2 if ord(ch) > 0xFFFF else 1
        if units > n:
            return s[:i]
    return s


def _cut(s: str, n: int = 120) -> str:
    return f"{_js_slice(s, n - 1)}…" if _js_len(s) > n else s


def _plural(n: int, one: str, many: Optional[str] = None) -> str:
    return f"{n} {one if n == 1 else (many or one + 's')}"


def _p2(n: int) -> str:
    return f"{n:02d}"


def utc(ms: int, now: int) -> str:
    """ "13:58 UTC", or "Nov 22, 13:58 UTC" when it isn't the day of ``now``."""
    d = datetime.fromtimestamp(int(ms) // 1000, timezone.utc)
    day = iso(ms)[:10] == iso(now)[:10]
    return f"{'' if day else f'{MONTHS[d.month - 1]} {d.day}, '}{_p2(d.hour)}:{_p2(d.minute)} UTC"


def _before(ms: int, at: int) -> str:
    """ "18 h ago", "4 min ago", "3 days ago": how long before ``at``."""
    s = max(0, (at - ms) / 1000)
    if s < 3600:
        return f"{max(1, _js_round(s / 60))} min ago"
    if s < 2 * 86_400:
        return f"{_js_round(s / 3600)} h ago"
    return f"{_js_round(s / 86_400)} days ago"


# ── a cause rule's change (cause-rule.ts) ─────────────────────────────────────────────────────────────────────────

def cause_name(c: dict) -> str:
    """The cause as a message names it: its label, cut at 120, with "(inferred)" when Trailbeam inferred it."""
    label = c["label"]
    return _cut(f"{label} (inferred)" if c.get("inferred") and not re.search(r"\(inferred\)", label, re.I) else label)


def cause_head(kind: str, c: dict) -> str:
    """The head of a change, for a subject line: "Back after 24 h quiet: orders-handler raised TimeoutError…"."""
    if c.get("regressed"):
        return f"Regressed: {cause_name(c)}"
    if c.get("backFromIgnored"):
        return f"Back from ignored: {cause_name(c)}"
    every = c["mode"] == "every"
    if kind == "resolved":
        what = "No new messages" if every else f"Quiet for {quiet_text(c['quietS'])}"
    else:
        what = "New messages" if every else f"Back after {quiet_text(c['quietS'])} quiet"
    return f"{'Still firing after a snooze: ' if c.get('afterSnooze') else ''}{what}: {cause_name(c)}"


def triage_suffix(c: dict) -> str:
    """ "Assigned to Sam. 3 notes.", after a space; empty with no triage to tell. Note text is never sent."""
    t = c.get("triage")
    if not t:
        return ""
    parts = [f"Assigned to {t['assignee']}." if t.get("assignee") else "",
             f"Regressed after {t.get('resolvedBy') or 'someone'} resolved it." if t.get("regressed") and not c.get("regressed") else "",
             f"{_plural(t['notes'], 'note')}." if t.get("notes") else ""]
    parts = [p for p in parts if p]
    return f" {' '.join(parts)}" if parts else ""


def cause_line(kind: str, c: dict, since: int, at: int) -> str:
    """One change as a line, with the numbers in it. ``since`` is when the rule started firing."""
    on = f", on {c['queue']}" if c.get("queue") else ""
    capped = (" Trailbeam reads the 5,000 most recently active unfinished messages; this cause may have more."
              if c.get("capped") else "")
    tail = triage_suffix(c)
    if c.get("regressed"):
        r = c["regressed"]
        when = f" {_before(r['resolvedAt'], at)}" if r.get("resolvedAt") is not None else ""
        seen = f", seen in {r['version']}" if r.get("version") else ""
        return (f"{cause_head(kind, c)}{on}. {_plural(r['count'], 'new failure')} since {r.get('by') or 'someone'} resolved it"
                f"{when}{seen}. {c['notFinished']} not finished.{tail}{capped}")
    if kind == "resolved":
        if c["mode"] == "every":
            quiet_since = "since the last check"
        elif c.get("lastNewAt") is not None:
            quiet_since = f"since the check at {utc(c['lastNewAt'], at)}"
        else:
            quiet_since = ""
        return (f"{cause_head(kind, c)}{on}. No new messages {quiet_since}; {_plural(c['notFinished'], 'message')} still not"
                f" finished.{tail}{capped}").replace(" ;", ";", 1)
    if c["newCount"]:
        if c["mode"] == "every" or c.get("prevCheckAt") is None:
            found = f"{_plural(c['newCount'], 'new message')} since the last check"
        else:
            found = f"{_plural(c['newCount'], 'new message')} since the check at {utc(c['prevCheckAt'], at)}"
    else:
        found = "no new messages at this check"
    back = c.get("backFromIgnored")
    firing = f", firing since {utc(since, at)}" if c.get("afterSnooze") or back else ""
    ignored = f" It was {back['how']}." if back else ""
    return f"{cause_head(kind, c)}{on}. {found[0].upper()}{found[1:]}; {c['notFinished']} not finished{firing}.{ignored}{tail}{capped}"


# ── a check's changes (deliver.ts) ────────────────────────────────────────────────────────────────────────────────

def _base(base: str) -> str:
    return base[:-1] if base.endswith("/") else base


def dashboard_url(base: str, dashboard_id: str) -> str:
    """A dashboard's page. A test change has no dashboard, so its link is the Dashboards page."""
    return f"{_base(base)}/dashboards{f'/{dashboard_id}' if dashboard_id else ''}"


def cause_url(base: str, cause_id: str) -> str:
    """Needs attention, narrowed to the cause."""
    return f"{_base(base)}/stuck?cause={quote(cause_id, safe=_URI_SAFE)}"


def settings_url(base: str) -> str:
    return f"{_base(base)}/settings#alerts"


# what encodeURIComponent leaves as it is
_URI_SAFE = "-_.!~*'()"


def href_of(base: str, x: dict) -> str:
    """Where a change links: what a rule is about, or the dashboard."""
    r = x["alert"].get("rule")
    return cause_url(base, r["causeId"]) if r else dashboard_url(base, x["alert"]["dashboardId"])


def head(x: dict) -> str:
    a = x["alert"]
    if a.get("rule"):
        return cause_head(x["kind"], a["rule"])
    return f"Resolved: {a['title']}" if x["kind"] == "resolved" else f"{TONE_WORD[a['tone']]}: {a['title']}"


def _dash_line(x: dict) -> str:
    a = x["alert"]
    limit = "" if x["kind"] == "resolved" else f", {a['limit']}"
    queue = f" (worst queue: {a['queue']})" if a.get("queue") else ""
    was = f", was {TONE_WORD[x['from']].lower()}" if x["kind"] == "changed" else ""
    return f"{head(x)} is {a['value']}{limit}{queue}{was} on {a['dashboardName']}"


def line(x: dict, at: int) -> str:
    a = x["alert"]
    return cause_line(x["kind"], a["rule"], a["since"], at) if a.get("rule") else _dash_line(x)


def subject_of(ws: dict, changes: list) -> str:
    return f"[{ws['name']}] {head(changes[0])}" if len(changes) == 1 else f"[{ws['name']}] {len(changes)} alert changes"


def wants(c: dict, x: dict) -> bool:
    """Whether a channel gets a change. An "Alert me" rule's change goes only to the channels ticked on the rule,
    whatever their "Send" setting; a dashboard change by the channel's lowest tone ("warning": every change,
    "critical": only changes into or out of critical)."""
    r = x["alert"].get("rule")
    if r:
        return bool(c.get("id")) and c["id"] in r["channels"]
    return c.get("minTone") == "warning" or x["alert"]["tone"] == "critical" or x.get("from") == "critical"


def _js_json(v) -> str:
    """JSON.stringify(v): no spaces, characters as they are."""
    return json.dumps(v, ensure_ascii=False, separators=(",", ":"))


def webhook_payload(ws: dict, changes: list, base: str, at: int) -> dict:
    """The webhook's JSON, as a dict in the hosted app's field order."""
    out = []
    for x in changes:
        a, r = x["alert"], x["alert"].get("rule")
        item = {
            "kind": x["kind"], "from": x.get("from"), "tone": a["tone"], "title": cause_head(x["kind"], r) if r else a["title"],
            "value": a["value"], "limit": a["limit"], "queue": a.get("queue") or None, "since": iso(a["since"]),
            "url": href_of(base, x),
        }
        if r:
            # a rule's change has no dashboard: receivers read url. Note text is never sent, only the count
            reg, t = r.get("regressed"), r.get("triage")
            item.update({
                "rule": {"id": r["ruleId"], "kind": r["kind"]},
                "cause": {"id": r["causeId"], "label": r["label"], "queue": r.get("queue") or None, "basis": r.get("basis") or None},
                "counts": {"new": r["newCount"], "not_finished": r["notFinished"]},
                "after_snooze": bool(r.get("afterSnooze")),
                "regressed": {"new_failures": reg["count"], "version": reg.get("version"), "resolved_by": reg.get("by")} if reg else None,
                "back_from_ignored": bool(r.get("backFromIgnored")),
                "triage": {"status": t["status"], "regressed": t["regressed"], "assignee": t.get("assignee"), "notes": t["notes"]} if t else None,
                "dashboard": None, "widget_id": None,
            })
        else:
            item.update({
                "rule": {"kind": "dashboard"},
                "dashboard": {"id": a["dashboardId"], "name": a["dashboardName"], "url": dashboard_url(base, a["dashboardId"])},
                "widget_id": a["widgetId"],
            })
        out.append(item)
    return {"type": "trailbeam.alerts", "version": 1, "sent_at": iso(at),
            "workspace": {"id": ws["id"], "name": ws["name"], "slug": ws["slug"]}, "changes": out}


def webhook_body(ws: dict, changes: list, base: str, at: int) -> str:
    """The webhook's body, the bytes the signature is over (as text)."""
    return _js_json(webhook_payload(ws, changes, base, at))


def sign(secret: str, timestamp: str, body: str) -> str:
    """x-trailbeam-signature: sha256=<hex HMAC-SHA256 of "<timestamp>.<body>"> with the channel's secret."""
    mac = hmac.new(secret.encode(), f"{timestamp}.{body}".encode(), hashlib.sha256)
    return f"sha256={mac.hexdigest()}"


_HTML = {"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;"}


def _esc(t: str) -> str:
    return re.sub(r'[&<>"]', lambda m: _HTML[m.group()], t)


def _slack_esc(t: str) -> str:
    return re.sub(r"[&<>]", lambda m: _HTML[m.group()], t)


def slack_text(ws: dict, changes: list, base: str, at: int) -> str:
    return "\n".join([f"*{_slack_esc(subject_of(ws, changes))}*",
                      *(f"• <{href_of(base, x)}|{_slack_esc(line(x, at))}>" for x in changes)])


def slack_body(ws: dict, changes: list, base: str, at: int) -> str:
    return _js_json({"text": slack_text(ws, changes, base, at)})


def email_of(channel: dict, ws: dict, changes: list, base: str, at: int) -> dict:
    """The email a channel's addresses get: {"subject", "text", "html"}. A rule's email says where to mute it; there is
    no one-click mute, as a forwarded link could mute it."""
    settings = settings_url(base)
    ruled = any(x["alert"].get("rule") for x in changes)
    text = "\n\n".join([*(f"{line(x, at)}\n{href_of(base, x)}" for x in changes), "",
                        f'Sent by Trailbeam to the "{channel["name"]}" alert channel of {ws["name"]}.',
                        *([f"Mute this alert: {settings}"] if ruled else [])])
    html = ('<div style="font-family:system-ui,sans-serif;font-size:14px;line-height:1.5">'
            + "".join(f'<p><a href="{_esc(href_of(base, x))}">{_esc(line(x, at))}</a></p>' for x in changes)
            + f'<p style="color:#666">Sent by Trailbeam to the &ldquo;{_esc(channel["name"])}&rdquo; alert channel of {_esc(ws["name"])}.</p>'
            + (f'<p><a href="{_esc(settings)}">Mute this alert</a></p>' if ruled else "") + "</div>")
    return {"subject": subject_of(ws, changes), "text": text, "html": html}


def recipients(channel: dict) -> list:
    """An email channel's addresses, as stored: "a@x.io, b@y.io"."""
    return re.split(r",\s*", channel["target"])


def test_change(now: int) -> dict:
    """A sample change, for a channel's "Send test" button."""
    return {"kind": "fired", "from": None, "alert": {
        "dashboardId": "", "dashboardName": "A test from Settings", "widgetId": "test", "title": "Test alert", "tone": "warning",
        "value": "1", "limit": "above 0", "queue": "", "since": now, "checkedAt": now}}


test_change.__test__ = False  # not a pytest test
