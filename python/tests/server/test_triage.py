"""The triage rules (triage.py, QV-593): parity with @trailbeam/ui's model/triage.ts and arrivals.ts on the shared
cases in contract/fixtures/triage-parity.json (ui/test/triage.test.ts runs the same file, and the hosted app's
test/triage-parity.test.ts runs a copy of it on the app's own copies of the rules), and what the cases don't cover."""
import copy
import importlib.util
import json
from pathlib import Path

import pytest

from trailbeam.server import triage

FIXTURES = Path(__file__).resolve().parents[3] / "contract" / "fixtures"
F = json.loads((FIXTURES / "triage-parity.json").read_text())


def test_the_fixture_is_what_its_script_writes(tmp_path, monkeypatch):
    """triage-parity.json is make-triage-parity.py's output from these rules: a change to either shows here."""
    spec = importlib.util.spec_from_file_location("make_triage_parity", FIXTURES / "make-triage-parity.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    monkeypatch.setattr(mod, "OUT", tmp_path / "triage-parity.json")
    mod.main()
    assert json.loads(mod.OUT.read_text()) == F, "run contract/fixtures/make-triage-parity.py and check ui's test"


@pytest.mark.parametrize("c", F["triageOf"], ids=lambda c: c["name"])
def test_triage_of(c):
    row, group = copy.deepcopy(c["row"]), copy.deepcopy(c["group"])
    assert triage.public(triage.triage_of(row, group, c["now"])) == c["expected"]
    assert row == c["row"] and group == c["group"], "triage_of() changes neither input"


@pytest.mark.parametrize("c", F["newSince"], ids=lambda c: c["name"])
def test_new_since(c):
    got = triage.new_since(c["arrivals"], c["sinceSeq"], c["atMs"], set(c["except"]) if c["except"] else None)
    assert [a["mid"] for a in got] == c["expected"]


@pytest.mark.parametrize("c", F["rowFor"], ids=lambda c: c["name"])
def test_row_for(c):
    r = triage.row_for(c["rows"], c["key"])
    assert (r["causeKey"] if r else None) == c["expected"]


def test_same_cause():
    for c in F["sameCause"]:
        assert triage.same_cause(c["r"], c["g"]) is c["expected"], c


@pytest.mark.parametrize("c", F["causeSignature"], ids=lambda c: f"{c['errorType']} / {c['text']}")
def test_cause_signature(c):
    """The exception part of a cause key (QV-702): the class escaped, and no text written as the merged form."""
    from trailbeam.server import causes
    assert causes.exc_signature(c["errorType"], c["text"]) == c["expected"]
    assert causes.merged_signature(c["errorType"]) == c["merged"]
    assert not triage.is_merged(f"v1|q|c|{c['expected']}")
    assert triage.is_merged(f"v1|q|c|{c['merged']}")


def test_in_filter_and_filter_of():
    for c in F["inFilter"]:
        assert triage.in_filter(c["triage"], c["filter"], c["me"], set(c["members"])) is c["expected"], c
    for c in F["filterOf"]:
        assert triage.filter_of(c["raw"]) == c["expected"]


@pytest.mark.parametrize("c", F["noteProblem"], ids=lambda c: c["name"])
def test_note_problem(c):
    assert triage.note_problem(c["body"]) == c["expected"]


def test_same_cause_is_the_alert_engines():
    """Triage and the alert check match a cause across the merge of error texts the same way."""
    from trailbeam.server import alerts
    for c in F["sameCause"]:
        assert alerts.same_cause(c["r"], c["g"]) is c["expected"]


def test_every_status_and_kind_in_the_cases():
    """The cases reach every outcome the rules have: each status, an ignore ended by time and by count, a regression
    found now and one stored before."""
    out = [c["expected"] for c in F["triageOf"]]
    assert {t["status"] for t in out} == set(triage.STATUSES)
    assert {t["ended"]["why"] for t in out if t["ended"]} == {"time", "count"}
    assert any(t["regression"] for t in out) and any(t["regressed"] and not t["regression"] for t in out)
