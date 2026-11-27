"""Writes alerts-api.json: how an alert rule, a mute and a channel are checked, which both products must agree on
(QV-642).

Self-hosted Trailbeam (server/src/trailbeam/server/alerts_api.py, tested by python/tests/server/test_alerts_api.py)
and the hosted app (lib/alerts/rules.ts checkForm and muteRule, lib/alerts/channels.ts addChannel) take the same
forms and must accept the same ones, store the same values and refuse the rest with the same text. The expected
results are the Python side's. Run from the repository root after changing a case or the code:

    PYTHONPATH=python/src:server/src python contract/fixtures/make-alerts-api.py
"""
import json
from pathlib import Path

from trailbeam.server import alerts_api as api

OUT = Path(__file__).resolve().parent / "alerts-api.json"
T0 = 1_764_000_000_000  # 2025-11-24T16:00:00Z, ms
CH = ["ch1", "ch2"]

# (name, form, the workspace's channel IDs, the check interval in minutes)
RULES = [
    ("refused: quiet 6 h, daily check", {"channels": ["ch1"], "mode": "quiet", "quietS": 21600}, CH, 1440),
    ("quiet 24 h, daily check", {"channels": ["ch1", "ch2"], "mode": "quiet", "quietS": 86400}, CH, 1440),
    ("quiet 7 days", {"channels": ["ch2"], "mode": "quiet", "quietS": 604800}, CH, 1440),
    ("every check: quietS is stored as 0", {"channels": ["ch1"], "mode": "every", "quietS": 3600}, CH, 1440),
    ("a channel twice is kept once", {"channels": ["ch1", "ch1", "ch2"], "mode": "every", "quietS": 0}, CH, 5),
    ("an unknown channel is dropped", {"channels": ["ch9", "ch2"], "mode": "every", "quietS": 0}, CH, 5),
    ("quiet 30 min, every 5 minutes", {"channels": ["ch1"], "mode": "quiet", "quietS": 1800}, CH, 5),
    ("quiet 1 h, every 5 minutes", {"channels": ["ch1"], "mode": "quiet", "quietS": 3600}, CH, 5),
    ("quietS as text", {"channels": ["ch1"], "mode": "quiet", "quietS": "3600"}, CH, 5),
    ("refused: no channel ticked", {"channels": [], "mode": "every", "quietS": 0}, CH, 1440),
    ("refused: only unknown channels", {"channels": ["ch9"], "mode": "every", "quietS": 0}, CH, 1440),
    ("refused: the workspace has no channel", {"channels": ["ch1"], "mode": "every", "quietS": 0}, [], 1440),
    ("refused: no mode", {"channels": ["ch1"], "mode": "sometimes", "quietS": 3600}, CH, 1440),
    ("refused: quiet 1 h, daily check", {"channels": ["ch1"], "mode": "quiet", "quietS": 3600}, CH, 1440),
    ("refused: quiet 30 min, hourly check", {"channels": ["ch1"], "mode": "quiet", "quietS": 1800}, CH, 60),
    ("refused: a quiet period not offered", {"channels": ["ch1"], "mode": "quiet", "quietS": 7200}, CH, 5),
    ("refused: quiet 0", {"channels": ["ch1"], "mode": "quiet", "quietS": 0}, CH, 5),
    ("refused: quiet period not a number", {"channels": ["ch1"], "mode": "quiet", "quietS": "soon"}, CH, 5),
    ("refused: quiet 30 min, every 2 hours", {"channels": ["ch1"], "mode": "quiet", "quietS": 1800}, CH, 120),
]

# (name, forS)
MUTES = [
    ("mute until unmuted", None),
    ("unmute", 0),
    ("snooze 1 h", 3600),
    ("snooze 4 h", 14400),
    ("snooze 24 h", 86400),
    ("refused: snooze 2 h", 7200),
    ("refused: snooze for a negative time", -3600),
    ("refused: snooze as text", "3600"),
]

# (name, form, allowLocal)
CHANNELS = [
    ("webhook", {"kind": "webhook", "name": " Ops hook ", "target": " https://hooks.example.com/alerts?token=abc ",
                 "minTone": "warning"}, False),
    ("webhook, critical only", {"kind": "webhook", "name": "", "target": "https://Example.COM", "minTone": "critical"},
     False),
    ("any other tone is warning", {"kind": "webhook", "name": "x", "target": "https://example.com/a",
                                   "minTone": "good"}, False),
    ("a long name is cut to 60", {"kind": "slack", "name": "n" * 70,
                                  "target": "https://hooks.slack.com/services/T000/B000/XXXX", "minTone": "warning"},
     False),
    ("slack, no name", {"kind": "slack", "name": "  ", "target": "https://hooks.slack.com/services/T000/B000/XXXX",
                        "minTone": "critical"}, False),
    ("email list", {"kind": "email", "name": "On call", "target": "Oncall@Acme.io; lead@acme.io,oncall@acme.io",
                    "minTone": "critical"}, False),
    ("webhook on this machine, allowed locally", {"kind": "webhook", "name": "dev",
                                                  "target": "http://127.0.0.1:8080/hook", "minTone": "warning"}, True),
    ("refused: unknown kind", {"kind": "pager", "name": "x", "target": "https://example.com", "minTone": "warning"},
     False),
    ("refused: http webhook", {"kind": "webhook", "name": "x", "target": "http://example.com/hook",
                               "minTone": "warning"}, False),
    ("refused: not a URL", {"kind": "webhook", "name": "x", "target": "example.com/hook", "minTone": "warning"}, False),
    ("refused: credentials before the host", {"kind": "webhook", "name": "x",
                                              "target": "https://user:pw@example.com/hook", "minTone": "warning"},
     False),
    ("refused: private webhook", {"kind": "webhook", "name": "x", "target": "https://10.0.0.5/hook",
                                  "minTone": "warning"}, False),
    ("refused: webhook on this machine", {"kind": "webhook", "name": "x", "target": "http://127.0.0.1:8080/hook",
                                          "minTone": "warning"}, False),
    ("refused: slack URL not Slack's", {"kind": "slack", "name": "x", "target": "https://example.com/services/T/B/X",
                                        "minTone": "warning"}, False),
    ("refused: no email address", {"kind": "email", "name": "x", "target": " , ", "minTone": "warning"}, False),
    ("refused: not an email address", {"kind": "email", "name": "x", "target": "oncall@acme.io, nobody",
                                       "minTone": "warning"}, False),
    ("refused: 11 email addresses", {"kind": "email", "name": "x",
                                     "target": ", ".join(f"p{i}@acme.io" for i in range(11)), "minTone": "warning"},
     False),
]


def main():
    out = {
        "about": "How an alert rule, a mute and a channel are checked, which self-hosted Trailbeam and the hosted app "
                 "must agree on. Written by make-alerts-api.py; don't edit by hand. mutedUntil is from at.",
        "at": T0,
        "rules": [{"name": n, "form": f, "channelIds": c, "everyMin": e, "expected": api.check_rule_form(f, c, e)}
                  for n, f, c, e in RULES],
        "mutes": [{"name": n, "forS": s, "expected": api.check_mute(s, T0)} for n, s in MUTES],
        "channels": [{"name": n, "form": f, "allowLocal": a, "expected": api.check_channel_form(f, a)}
                     for n, f, a in CHANNELS],
    }
    OUT.write_text(json.dumps(out, indent=1, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
