"""The alert rule engine (alerts.py, QV-627): parity with the hosted app's lib/alerts/evaluate.ts on the shared cases in
contract/fixtures/alerts-parity.json (the app's test/alerts-parity.test.ts runs the same file), and what the cases
don't cover."""
import copy
import importlib.util
import json
from pathlib import Path

import pytest

from trailbeam.server import alerts

FIXTURES = Path(__file__).resolve().parents[3] / "contract" / "fixtures"
F = json.loads((FIXTURES / "alerts-parity.json").read_text())


def test_the_fixture_is_what_its_script_writes(tmp_path, monkeypatch):
    """alerts-parity.json is make-alerts-parity.py's output from this engine: a change to either shows here."""
    spec = importlib.util.spec_from_file_location("make_alerts_parity", FIXTURES / "make-alerts-parity.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    monkeypatch.setattr(mod, "OUT", tmp_path / "alerts-parity.json")
    mod.main()
    assert json.loads(mod.OUT.read_text()) == F, "run contract/fixtures/make-alerts-parity.py and check the app's test"


@pytest.mark.parametrize("c", F["toneOf"], ids=lambda c: c["name"])
def test_tone(c):
    assert alerts.tone_of(c["value"], c["rule"]) == c["expected"]


@pytest.mark.parametrize("c", F["diff"], ids=lambda c: c["name"])
def test_diff(c):
    prev, now = copy.deepcopy(c["prev"]), copy.deepcopy(c["now"])
    assert alerts.diff(prev, now, c["at"], c["present"]) == c["expected"]
    assert prev == c["prev"] and now == c["now"], "diff() changes neither input"


@pytest.mark.parametrize("c", F["causeRule"], ids=lambda c: c["name"])
def test_cause_rule(c):
    assert alerts.check_cause_rule(c["rule"], c["memory"], c["prev"], c["read"], c["at"], c["prevCheckAt"]) == c["expected"]


@pytest.mark.parametrize("c", F["sameCause"], ids=lambda c: f"{c['rule']} / {c['group']}")
def test_same_cause(c):
    assert alerts.same_cause(c["rule"], c["group"]) is c["expected"]


def _walk(w):
    """The walk's checks, memory and stored state carried from check to check as the scheduled check carries them."""
    memory = {"seen": None, "lastNewAt": None, "wasMuted": False}
    state: list = []
    prev_at = None
    out = []
    for c in w["checks"]:
        rd = {"groups": [{**w["group"], "count": len(c["causeOf"])}], "causeOf": c["causeOf"], "capped": False}
        prev = next((s for s in state if s["widgetId"] == w["rule"]["id"]), None)
        got = alerts.check_cause_rule(w["rule"], memory, prev, rd, c["at"], prev_at)
        d = alerts.diff(state, [got["observed"]], c["at"], [f"rule/{w['rule']['id']}"])
        out.append({"at": c["at"], "tone": got["observed"]["tone"], "newCount": got["observed"]["rule"]["newCount"],
                    "changes": [x["kind"] for x in d["changes"]]})
        memory, state, prev_at = got["memory"], d["next"], c["at"]
    assert out == w["expected"]
    return [(i, k) for i, x in enumerate(out) for k in x["changes"]]


def test_walk():
    """A quiet rule (2 h) through a day of checks every 30 min: quiet at first, fires on a message after 2 h quiet,
    resolves 2 h after the last one, fires again."""
    assert _walk(F["walk"]) == [(9, "fired"), (13, "resolved"), (20, "fired")]


def test_every_walk():
    """An "every" rule sends at each check that finds new messages, also while already firing (QV-659)."""
    assert _walk(F["everyWalk"]) == [(1, "fired"), (2, "fired"), (3, "fired"), (4, "resolved"), (6, "fired"),
                                     (7, "resolved")]


def test_the_cases_cover_every_change_and_mode():
    kinds = {x["kind"] for c in F["diff"] for x in c["expected"]["changes"]}
    assert kinds == {"fired", "changed", "resolved"}
    again = [c for c in F["diff"] if any(x["kind"] == "fired" and x["from"] == "critical" for x in c["expected"]["changes"])]
    assert [c["name"] for c in again] == ["every: new messages while already firing: fired again, since kept"]
    assert any(x["alert"].get("noLongerChecked") for c in F["diff"] for x in c["expected"]["changes"])
    assert any(not c["now"] and c["expected"]["next"] for c in F["diff"]), "a rule with no figure kept"
    assert {c["rule"]["mode"] for c in F["causeRule"]} == {"quiet", "every"}
    assert {c["expected"] for c in F["toneOf"]} == {None, "good", "warning", "critical"}
    assert any(c["expected"]["muted"] for c in F["causeRule"])
    assert any(c["expected"]["seenAs"] is None for c in F["causeRule"])


@pytest.mark.parametrize("rule, tone, want", [
    ({"warn": 5, "crit": 10}, "critical", {"below": False, "limit": 10}),
    ({"warn": 5, "crit": 10}, "warning", {"below": False, "limit": 5}),
    ({"warn": 5, "crit": 10}, "good", {"below": False, "limit": 5}),
    ({"crit": 10}, "good", {"below": False, "limit": 10}),
    ({"warn": 3, "below": True}, "warning", {"below": True, "limit": 3}),
])
def test_limit_of(rule, tone, want):
    """The limit alertOf names in a message (measures.ts): crit when critical or with no warn limit, else warn."""
    assert alerts.limit_of(rule, tone) == want


@pytest.mark.parametrize("s, want", [(1800, "30 min"), (3600, "1 h"), (6 * 3600, "6 h"), (86_400, "24 h"),
                                     (7 * 86_400, "7 days"), (2 * 86_400, "2 days"), (90, "1.5 min")])
def test_quiet_text(s, want):
    assert alerts.quiet_text(s) == want


def test_muted():
    assert not alerts.is_muted({"mutedUntil": None}, 5)
    assert alerts.is_muted({"mutedUntil": 6}, 5)
    assert not alerts.is_muted({"mutedUntil": 5}, 5)
    assert alerts.is_muted({"mutedUntil": alerts.FOREVER}, 5)
