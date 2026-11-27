"""Writes alerts-delivery.json: what an alert sends, which both products must agree on (QV-636).

Self-hosted Trailbeam (server/src/trailbeam/server/alert_messages.py and alert_delivery.py, tested by
python/tests/server/test_alert_delivery.py) and the hosted app (lib/alerts/deliver.ts and channels.ts, its
test/alerts-delivery.test.ts, reading @trailbeam/ui/fixtures/alerts-delivery.json) run every case and must give the
expected result: the webhook request (URL, headers with the signature, body), the Slack request and the emails a check's
changes make, which channels want which change, and how a channel's target is checked and shown. The expected results
are the Python side's; the hosted app's test is what holds deliver.ts to them. Links in the cases are the hosted app's,
<origin>/app/<workspace slug>/...; self-hosted Trailbeam makes the same with its own base. Run from the repository root
after changing a case or the code:

    PYTHONPATH=python/src:server/src python contract/fixtures/make-alerts-delivery.py

and check the hosted app's test still passes on the new file before merging.
"""
import json
from pathlib import Path

from trailbeam.server import alert_delivery as d
from trailbeam.server import alert_messages as m

OUT = Path(__file__).resolve().parent / "alerts-delivery.json"
H = 3_600_000
MIN = 60_000
T0 = 1_764_000_000_000  # 2025-11-24T16:00:00Z, ms
ORIGIN = "https://trailbeam.app"
WS = {"id": "ws_1", "name": "Acme", "slug": "acme"}
WS_ODD = {"id": "ws_2", "name": 'Bits & "Bobs" <ltd>', "slug": "bits-bobs"}

HOOK = {"id": "ch_hook", "kind": "webhook", "name": "Ops hook", "target": "https://hooks.example.com/alerts?token=abc",
        "secret": "whsec_0123456789abcdef0123456789abcdef0123456789abcdef", "minTone": "warning"}
SLACK = {"id": "ch_slack", "kind": "slack", "name": "#ops", "target": "https://hooks.slack.com/services/T000/B000/XXXXSECRET",
         "secret": None, "minTone": "warning"}
MAIL = {"id": "ch_mail", "kind": "email", "name": "On call", "target": "oncall@acme.io, lead@acme.io", "secret": None,
        "minTone": "critical"}


def dash(kind, tone, frm=None, **kw):
    a = {"dashboardId": "d1", "dashboardName": "Orders", "widgetId": "w1", "title": "Queue depth", "tone": tone,
         "value": "1,240", "limit": "above 1,000", "queue": "orders", "since": T0 - 5 * MIN, "checkedAt": T0}
    a.update(kw)
    return {"kind": kind, "from": frm, "alert": a}


def cause(kind, tone="critical", frm=None, since=T0, **kw):
    r = {"kind": "cause", "ruleId": "r1", "channels": ["ch_hook", "ch_slack", "ch_mail"], "mode": "quiet", "quietS": 86_400,
         "causeId": "c_9f2", "label": "orders-handler raised TimeoutError: upstream timed out", "queue": "orders",
         "basis": "exception", "inferred": False, "newCount": 3, "notFinished": 7, "prevCheckAt": T0 - 15 * MIN,
         "lastNewAt": T0 - 26 * H, "capped": False}
    r.update(kw)
    a = {"dashboardId": "rule", "dashboardName": "", "widgetId": r["ruleId"], "title": r["label"], "tone": tone,
         "value": str(r["newCount"]), "limit": "new messages after 24 h quiet" if r["mode"] == "quiet" else "new messages at a check",
         "queue": r["queue"], "rule": r, "since": since, "checkedAt": T0}
    return {"kind": kind, "from": frm, "alert": a}


FIRED = dash("fired", "warning")
CHANGED = dash("changed", "critical", "warning", value="5,100", limit="above 5,000")
DOWN = dash("changed", "warning", "critical")
RESOLVED = dash("resolved", "good", "critical", value="12", queue="")
NO_QUEUE = dash("fired", "critical", queue="", title="Error rate", value="7.5 %", limit="above 5 %", dashboardId="d2",
                dashboardName="Payments & <refunds>", widgetId="w9")
RULE_FIRED = cause("fired", frm="good")
RULE_EVERY = cause("fired", frm="good", mode="every", quietS=0, newCount=1, notFinished=1, prevCheckAt=None, lastNewAt=None)
RULE_RESOLVED = cause("resolved", tone="good", frm="critical", newCount=0, lastNewAt=T0 - 25 * H)
RULE_RESOLVED_EVERY = cause("resolved", tone="good", frm="critical", mode="every", newCount=0, notFinished=0)
RULE_RESOLVED_NEVER = cause("resolved", tone="good", frm="critical", newCount=0, lastNewAt=None, queue="")
RULE_FIRST = cause("fired", frm="good", prevCheckAt=None)
RULE_SNOOZE = cause("fired", afterSnooze=True, since=T0 - 30 * H, newCount=0)
RULE_REGRESSED = cause("fired", regressed={"count": 2, "version": "v1.4.2", "by": "Sam", "resolvedAt": T0 - 18 * H},
                       triage={"status": "open", "regressed": True, "assignee": "Sam", "notes": 3, "resolvedBy": "Sam"})
RULE_REGRESSED_BARE = cause("fired", regressed={"count": 1, "version": None, "by": None, "resolvedAt": None}, queue="")
RULE_REGRESSED_MIN = cause("fired", regressed={"count": 5, "version": None, "by": "Kim", "resolvedAt": T0 - 90 * 1000})
RULE_REGRESSED_DAYS = cause("fired", regressed={"count": 5, "version": "2", "by": "Kim", "resolvedAt": T0 - 4 * 86_400_000})
RULE_IGNORED = cause("fired", backFromIgnored={"how": "ignored until 10 more"}, since=T0 - 3 * 86_400_000,
                     triage={"status": "ignored", "regressed": False, "assignee": None, "notes": 1, "resolvedBy": None})
RULE_TRIAGE = cause("fired", triage={"status": "resolved", "regressed": True, "assignee": None, "notes": 0, "resolvedBy": None})
RULE_ODD = cause("fired", label="Rejected <order> & \"refund\" — " + "x" * 120, inferred=True, capped=True, basis="",
                 queue="orders.fifo", causeId="exc:Timeout/É ?&=", quietS=1800)
# a label cut where 120 UTF-16 units splits an emoji: the hosted app sends half of it, self-hosted leaves it out, so it
# is not a shared case (test_alert_delivery.py covers this side)
RULE_EMOJI = cause("fired", label="🚀" * 59 + "ab" + "🚀" * 10)
RULE_INFERRED = cause("fired", label="Rejected by consumer (inferred)", inferred=True, mode="every", quietS=0)
RULE_OTHER = cause("fired", channels=["ch_other"])
TEST = m.test_change(T0)


def send(name, channel, changes, ws=WS, at=T0):
    base = f"{ORIGIN}/app/{ws['slug']}"
    if channel["kind"] == "email":
        mail = m.email_of(channel, ws, changes, base, at)
        expected = {"emails": [{"to": to, **mail} for to in m.recipients(channel)]}
    else:
        if channel["kind"] == "webhook":
            body = m.webhook_body(ws, changes, base, at)
            headers = {"content-type": "application/json", "user-agent": d.USER_AGENT, "x-trailbeam-timestamp": str(at),
                       "x-trailbeam-signature": m.sign(channel["secret"] or "", str(at), body)}
        else:
            body = m.slack_body(ws, changes, base, at)
            headers = {"content-type": "application/json", "user-agent": d.USER_AGENT}
        expected = {"posts": [{"url": channel["target"], "headers": headers, "body": body}]}
    return {"name": name, "channel": channel, "workspace": ws, "origin": ORIGIN, "at": at, "changes": changes, "expected": expected}


SENDS = []
for ch in (HOOK, SLACK, MAIL):
    k = ch["kind"]
    SENDS += [
        send(f"{k}: a dashboard rule fires", ch, [FIRED]),
        send(f"{k}: warning to critical", ch, [CHANGED]),
        send(f"{k}: critical back to warning", ch, [DOWN]),
        send(f"{k}: resolved, no queue", ch, [RESOLVED]),
        send(f"{k}: escaped names, no queue", ch, [NO_QUEUE], ws=WS_ODD),
        send(f"{k}: several changes", ch, [FIRED, CHANGED, RESOLVED]),
        send(f"{k}: the Send test change", ch, [TEST]),
        send(f"{k}: a quiet cause rule fires", ch, [RULE_FIRED]),
        send(f"{k}: an every-check rule fires", ch, [RULE_EVERY]),
        send(f"{k}: a quiet rule resolves", ch, [RULE_RESOLVED]),
        send(f"{k}: an every-check rule resolves", ch, [RULE_RESOLVED_EVERY]),
        send(f"{k}: resolved with no last new check", ch, [RULE_RESOLVED_NEVER]),
        send(f"{k}: fired at the rule's first check with a previous check unknown", ch, [RULE_FIRST]),
        send(f"{k}: still firing after a snooze", ch, [RULE_SNOOZE]),
        send(f"{k}: regressed, with triage", ch, [RULE_REGRESSED]),
        send(f"{k}: regressed, nothing known", ch, [RULE_REGRESSED_BARE]),
        send(f"{k}: regressed minutes after", ch, [RULE_REGRESSED_MIN]),
        send(f"{k}: regressed days after", ch, [RULE_REGRESSED_DAYS]),
        send(f"{k}: back from ignored", ch, [RULE_IGNORED]),
        send(f"{k}: triage regressed, no one named", ch, [RULE_TRIAGE]),
        send(f"{k}: long inferred label, capped read, odd cause ID", ch, [RULE_ODD], ws=WS_ODD),
        send(f"{k}: a label cut between emoji", ch, [RULE_EMOJI]),
        send(f"{k}: a label that already says inferred", ch, [RULE_INFERRED]),
        send(f"{k}: a rule and a dashboard change together", ch, [RULE_FIRED, CHANGED]),
        send(f"{k}: on another day", ch, [RULE_SNOOZE, RULE_RESOLVED], at=T0 + 9 * H),
    ]
SENDS.append(send("webhook: no secret signs with an empty key", {**HOOK, "secret": None}, [FIRED]))
SENDS.append(send("email: one address", {**MAIL, "target": "solo@acme.io"}, [FIRED]))


def want(name, channel, change):
    return {"name": name, "channel": channel, "change": change, "expected": m.wants(channel, change)}


WANTS = [
    want("warning channel, warning change", {"id": "a", "minTone": "warning"}, FIRED),
    want("critical channel, warning change", {"id": "a", "minTone": "critical"}, FIRED),
    want("critical channel, into critical", {"id": "a", "minTone": "critical"}, CHANGED),
    want("critical channel, out of critical to warning", {"id": "a", "minTone": "critical"}, DOWN),
    want("critical channel, resolved from critical", {"id": "a", "minTone": "critical"}, RESOLVED),
    want("critical channel, resolved from warning", {"id": "a", "minTone": "critical"}, dash("resolved", "good", "warning")),
    want("a rule's change, channel ticked", {"id": "ch_mail", "minTone": "critical"}, RULE_RESOLVED),
    want("a rule's change, channel not ticked", {"id": "ch_mail", "minTone": "warning"}, RULE_OTHER),
    want("a rule's change, a channel with no ID", {"minTone": "warning"}, RULE_FIRED),
]


def target(name, kind, raw, allow_local=False):
    return {"name": name, "kind": kind, "raw": raw, "allowLocal": allow_local, "expected": d.check_target(kind, raw, allow_local)}


TARGETS = [
    target("email: one", "email", "oncall@acme.io"),
    target("email: a list, cased, repeated, mixed separators", "email", " A@x.io; b@y.io,\nA@X.io  c@z.io "),
    target("email: none", "email", "  , ; "),
    target("email: not an address", "email", "a@x.io, bob"),
    target("email: no dot in the domain", "email", "bob@localhost"),
    target("email: ten is the most", "email", ", ".join(f"u{i}@x.io" for i in range(10))),
    target("email: eleven", "email", ", ".join(f"u{i}@x.io" for i in range(11))),
    target("webhook: https", "webhook", "https://hooks.example.com/alerts?token=abc"),
    target("webhook: upper case host and default port", "webhook", "  HTTPS://Hooks.Example.COM:443  "),
    target("webhook: another port", "webhook", "https://hooks.example.com:8443/x"),
    target("webhook: not a URL", "webhook", "hooks.example.com/alerts"),
    target("webhook: no host", "webhook", "https://"),
    target("webhook: http", "webhook", "http://hooks.example.com/alerts"),
    target("webhook: ftp", "webhook", "ftp://hooks.example.com/"),
    target("webhook: javascript", "webhook", "javascript:alert(1)"),
    target("webhook: credentials before the host", "webhook", "https://user:pw@hooks.example.com/"),
    target("webhook: only a user", "webhook", "https://user@hooks.example.com/"),
    target("webhook: localhost", "webhook", "https://localhost/"),
    target("webhook: a .localhost name", "webhook", "https://app.localhost/"),
    target("webhook: a .local name", "webhook", "https://printer.local/"),
    target("webhook: a .internal name", "webhook", "https://metadata.google.internal/"),
    target("webhook: loopback", "webhook", "https://127.0.0.1/"),
    target("webhook: loopback, short form", "webhook", "https://127.1/"),
    target("webhook: loopback, hex", "webhook", "https://0x7f.0.0.1/"),
    target("webhook: loopback, one number", "webhook", "https://2130706433/"),
    target("webhook: 0.0.0.0", "webhook", "https://0/"),
    target("webhook: 10/8", "webhook", "https://10.1.2.3/"),
    target("webhook: 172.16/12", "webhook", "https://172.20.0.1/"),
    target("webhook: 172.32 is public", "webhook", "https://172.32.0.1/"),
    target("webhook: 192.168/16", "webhook", "https://192.168.1.1/"),
    target("webhook: cloud metadata", "webhook", "https://169.254.169.254/latest/meta-data"),
    target("webhook: carrier-grade NAT", "webhook", "https://100.64.0.1/"),
    target("webhook: 100.128 is public", "webhook", "https://100.128.0.1/"),
    target("webhook: a bad address", "webhook", "https://1.2.3.256/"),
    target("webhook: IPv6 loopback", "webhook", "https://[::1]/"),
    target("webhook: IPv6 unspecified", "webhook", "https://[::]/"),
    target("webhook: IPv6 unique local", "webhook", "https://[fd00::1]/"),
    target("webhook: IPv6 link local", "webhook", "https://[fe80::1]/"),
    target("webhook: IPv4-mapped IPv6", "webhook", "https://[::ffff:127.0.0.1]/"),
    target("webhook: public IPv6", "webhook", "https://[2001:db8:0:0:0:0:0:1]/hook"),
    target("webhook: path normalised", "webhook", "https://hooks.example.com/a/./b/../c d?x=1 2#frag"),
    target("webhook: non-ASCII host and path", "webhook", "https://bücher.example/ü"),
    target("webhook: allowLocal, http on localhost", "webhook", "http://localhost:3000/hook", True),
    target("webhook: allowLocal, http on a private address", "webhook", "http://10.0.0.5:8080/hook", True),
    target("webhook: allowLocal, https on localhost is still refused", "webhook", "https://localhost/hook", True),
    target("webhook: allowLocal, http on a public host is refused", "webhook", "http://hooks.example.com/", True),
    target("slack: an incoming webhook", "slack", "https://hooks.slack.com/services/T000/B000/XXXX"),
    target("slack: another Slack URL", "slack", "https://hooks.slack.com/workflows/T000/A000"),
    target("slack: not Slack", "slack", "https://hooks.example.com/services/T000/B000/XXXX"),
    target("slack: http", "slack", "http://hooks.slack.com/services/T000/B000/XXXX"),
    target("slack: allowLocal, a local stand-in", "slack", "http://127.0.0.1:9000/slack", True),
    target("slack: allowLocal, https on localhost is still refused", "slack", "https://127.0.0.1/services/x", True),
]


def shown(name, kind, t):
    return {"name": name, "channel": {"kind": kind, "target": t}, "expected": d.shown_target({"kind": kind, "target": t})}


SHOWN = [
    shown("slack: up to the team", "slack", "https://hooks.slack.com/services/T000/B000/XXXXSECRET"),
    shown("slack: not a services URL", "slack", "http://127.0.0.1:9000/slack"),
    shown("webhook: just a host", "webhook", "https://hooks.example.com/"),
    shown("webhook: a path", "webhook", "https://hooks.example.com/alerts"),
    shown("webhook: a query", "webhook", "https://hooks.example.com/?token=abc"),
    shown("webhook: a port", "webhook", "https://hooks.example.com:8443/a"),
    shown("webhook: not a URL", "webhook", "not a url"),
    shown("email: as it is", "email", "a@x.io, b@y.io"),
]


def main():
    out = {
        "about": "What an alert sends, which self-hosted Trailbeam and the hosted app must agree on. Written by "
                 "make-alerts-delivery.py; don't edit by hand. Links are the hosted app's: <origin>/app/<slug>/...",
        "send": SENDS, "wants": WANTS, "checkTarget": TARGETS, "shownTarget": SHOWN,
    }
    OUT.write_text(json.dumps(out, indent=1, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
