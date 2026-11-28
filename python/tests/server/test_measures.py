"""The dashboard widget measures engine (measures.py, QV-632): parity with the hosted app's lib/dashboards/measures.ts on
the shared cases in contract/fixtures/measures-parity.json (the app's test/measures-parity.test.ts runs the same file),
and what the cases don't cover."""
import copy
import importlib.util
import json
import math
from pathlib import Path

import pytest

from trailbeam.server import measures as M

FIXTURES = Path(__file__).resolve().parents[3] / "contract" / "fixtures"
F = json.loads((FIXTURES / "measures-parity.json").read_text())
D = F["datasets"]


def _script():
    spec = importlib.util.spec_from_file_location("make_measures_parity", FIXTURES / "make-measures-parity.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


MK = _script()
plain, unplain = MK.plain, MK.unplain


def test_the_fixture_is_what_its_script_writes(tmp_path, monkeypatch):
    """measures-parity.json is make-measures-parity.py's output from this engine: a change to either shows here."""
    monkeypatch.setattr(MK, "OUT", tmp_path / "measures-parity.json")
    MK.main()
    assert json.loads(MK.OUT.read_text()) == F, "run contract/fixtures/make-measures-parity.py and check the app's test"


def test_the_measures():
    got = [{"id": m.id, "label": m.label, "col": m.col, "ask": m.ask, "unit": m.unit, "agg": m.agg, "queue": m.queue,
            "spans": m.spans, "fold": (m.ev or {}).get("fold")} for m in M.MEASURES]
    assert got == F["measures"]
    assert M.GROUP_FIELDS == F["groupFields"] and M.NUMBER_FIELDS == F["numberFields"]


def _ids(section):
    return lambda c: c.get("name") or json.dumps({k: v for k, v in c.items() if k in ("measure", "queue", "data", "filter", "query", "value", "unit")})


@pytest.mark.parametrize("c", F["series"], ids=_ids("series"))
def test_series(c):
    data = copy.deepcopy(D[c["data"]])
    assert plain(M.series_for(M.measure(c["measure"]), c["filter"], c["groupBy"], c["time"], c["n"], data)) == c["expected"]
    assert data == D[c["data"]], "series_for() changes nothing it is given"


@pytest.mark.parametrize("top", [1, 3, 10])
@pytest.mark.parametrize("c", [c for c in F["series"] if len(c["expected"]) > 1], ids=_ids("series"))
def test_series_top_is_the_largest_lines_and_the_rest_as_one(c, top):
    """series_for(top=k) (QV-684): the first k lines and the rest folded as the hosted dashboard folds them, the
    same to the last bit as working out every line, though it never works out the rest's points one by one."""
    m = M.measure(c["measure"])
    full = M.series_for(m, c["filter"], c["groupBy"], c["time"], c["n"], D[c["data"]])
    want = full if len(full) <= top else full[:top] + [M.other_line(m, full[top:], c["n"])]
    assert plain(M.series_for(m, c["filter"], c["groupBy"], c["time"], c["n"], D[c["data"]], top=top)) == plain(want)


def test_other_line_adds_up_or_takes_the_largest():
    lines = [{"name": "a", "now": 3, "points": [1, 2]}, {"name": "b", "now": 5, "points": [4, 0]}]
    assert M.other_line(M.measure("sent"), lines, 2) == {"name": "Other (2)", "now": 8, "points": [5, 2], "others": 2}
    assert M.other_line(M.measure("processing_time"), lines, 2) == \
        {"name": "Other (2)", "now": 5, "points": [4, 2], "others": 2}


@pytest.mark.parametrize("c", F["queueValue"], ids=_ids("queueValue"))
def test_queue_value(c):
    assert plain(M.queue_value(M.measure(c["measure"]), c["queue"], D[c["data"]], c["time"])) == c["expected"]


@pytest.mark.parametrize("c", F["depthMissing"], ids=_ids("depthMissing"))
def test_depth_missing(c):
    assert M.depth_missing(M.measure(c["measure"]), D[c["data"]]) is c["expected"]


@pytest.mark.parametrize("c", F["noHistory"], ids=_ids("noHistory"))
def test_no_history(c):
    assert M.no_history(M.measure(c["measure"]), D[c["data"]], c["time"]) is c["expected"]


@pytest.mark.parametrize("c", F["noDepth"], ids=_ids("noDepth"))
def test_no_depth(c):
    assert M.no_depth(D[c["data"]]) == c["expected"]


@pytest.mark.parametrize("c", F["queueTone"], ids=_ids("queueTone"))
def test_queue_tone(c):
    assert M.queue_tone(c["queue"], D[c["data"]]) == c["expected"]


@pytest.mark.parametrize("c", F["spanQueues"], ids=_ids("spanQueues"))
def test_span_queues(c):
    assert M.span_queues(c["def"], c["names"]) == c["expected"]


@pytest.mark.parametrize("c", F["queuesFor"], ids=_ids("queuesFor"))
def test_queues_for(c):
    assert M.queues_for(M.parse(c["filter"]), c["names"]) == c["expected"]


@pytest.mark.parametrize("c", F["queueOnlyTerms"], ids=_ids("queueOnlyTerms"))
def test_queue_only_terms(c):
    assert M.queue_only_terms(c["filter"]) == c["expected"]


@pytest.mark.parametrize("c", F["effectiveFilter"], ids=_ids("effectiveFilter"))
def test_effective_filter(c):
    assert M.effective_filter(c["widget"], c["variables"]) == c["expected"]


@pytest.mark.parametrize("c", F["parse"], ids=_ids("parse"))
def test_parse(c):
    assert M.parse(c["query"]) == c["expected"]


@pytest.mark.parametrize("c", F["fmt"], ids=_ids("fmt"))
def test_fmt(c):
    assert M.fmt(unplain(c["value"]), c["unit"]) == c["expected"]


@pytest.mark.parametrize("c", F["p95Of"], ids=lambda c: json.dumps(c["values"])[:40])
def test_p95(c):
    assert M.p95_of(c["values"]) == c["expected"]


@pytest.mark.parametrize("c", F["eventBuckets"], ids=_ids("eventBuckets"))
def test_event_buckets(c):
    assert M.event_buckets(c["filter"], c["time"], c["n"], D[c["data"]]) == c["expected"]


@pytest.mark.parametrize("c", F["eventsFor"], ids=_ids("eventsFor"))
def test_events_for(c):
    assert [e["key"] for e in M.events_for(c["filter"], c["time"], D[c["data"]])] == c["expected"]


@pytest.mark.parametrize("c", F["alertOf"], ids=_ids("alertOf"))
def test_alert_of(c):
    assert M.alert_of(c["widget"], c["variables"], D[c["data"]]) == c["expected"]


@pytest.mark.parametrize("c", F["problem"], ids=_ids("problem"))
def test_problem(c):
    assert M.problem(c["widget"]) == c["expected"]


@pytest.mark.parametrize("c", F["checkDef"], ids=_ids("checkDef"))
def test_check_def(c):
    assert M.check_def(c["def"]) == c["expected"]


def test_the_cases_cover_every_measure_unit_and_outcome():
    by = {}
    for c in F["series"]:
        by.setdefault(c["measure"], []).append(c["expected"])
    assert set(by) == {m.id for m in M.MEASURES}
    for m in M.MEASURES:
        assert any(e and any(s["now"] not in (0, "Infinity") for s in e) for e in by[m.id]), f"{m.id} never has a figure"
    queue = [m.id for m in M.MEASURES if m.queue]
    assert all(any(e == [] for e in by[i]) for i in queue), "every queue measure has a case with no figure"
    assert any(s["now"] == "Infinity" for e in by["time_to_drain"] for s in e), "a backlog that never drains"
    assert {c["unit"] for c in F["fmt"]} == {"count", "rate", "s", "ms", "pct", "bytes"}
    assert {c["expected"] for c in F["queueTone"]} >= {"warning", "good", None}
    assert {(c["expected"] or {}).get("tone") for c in F["alertOf"]} == {None, "good", "warning", "critical"}
    assert {c["expected"] for c in F["depthMissing"]} == {True, False}
    assert {c["expected"] for c in F["noHistory"]} == {True, False}
    assert any(c["expected"] is None for c in F["problem"]) and any("def" in c["expected"] for c in F["checkDef"])
    assert any("error" in t for c in F["parse"] for t in c["expected"])


# ── what the shared cases don't cover ───────────────────────────────────────────────────────────────────────────

def test_a_definition_over_the_size_limit():
    big = {"widgets": [{"id": "w", "type": "note", "title": "x", "text": "a" * M.MAX_DEF_BYTES, "layout": {"x": 0, "y": 0, "w": 1, "h": 1}}]}
    assert M.check_def(big) == {"error": "That dashboard is 257 KB. A dashboard can be at most 256 KB."}
    under = {"widgets": [], "variables": [], "sections": [{"id": "s", "title": "é" * 1000}]}
    assert "def" in M.check_def(under)


def test_the_size_is_counted_as_javascript_writes_the_json():
    assert M.js_json({"a": [1.0, 2.5, None, True, "é\n"], "b": {"c": 1e21, "d": 1e-7}}) == '{"a":[1,2.5,null,true,"é\\n"],"b":{"c":1e+21,"d":1e-7}}'


@pytest.mark.parametrize("x, want", [(2.5, 3), (-2.5, -2), (0.49999999999999994, 0), (-0.5, 0), (1.5, 2), (7, 7)])
def test_round_as_javascript(x, want):
    assert M.js_round(x) == want


@pytest.mark.parametrize("x, d, want", [(0.25, 1, "0.3"), (0.35, 1, "0.3"), (1.005, 2, "1.00"), (2.5, 0, "3"), (-0.25, 1, "-0.3"), (5.55, 1, "5.5")])
def test_to_fixed_as_javascript(x, d, want):
    """toFixed rounds the exact binary value half up: 0.35 is a little under, 0.25 is exact."""
    assert M.to_fixed(x, d) == want


def test_sums_add_left_to_right():
    """reduce((a, b) => a + b), not Python 3.12's compensated sum()."""
    xs = [0.1] * 10
    assert M._total(xs) == 0.9999999999999999


def test_drain_with_no_period():
    """A zero period divides as JavaScript does: no rate, so it never drains."""
    assert M.drain_s(5, [[0, 1]], [[0, 1]], 0) == math.inf
    assert M.drain_s(0, [], [], 0) == 0
    assert M.drain_s(None, [], [], 60) is None


def test_numbers_against_text_values():
    """A number written as text compares as JavaScript's < and > do, but is never equal to a number."""
    assert M.num_hit("3", ">=2") is True
    assert M.num_hit("3", "3") is False
    assert M.num_hit("x", ">=2") is False
    assert M.num_hit(3, "abc") is None
    assert M.num_hit(1500, "1s..2s") is True


def test_inputs_are_not_changed():
    data = copy.deepcopy(D["own"])
    w = {"id": "w", "type": "number", "title": "t", "queries": [{"measure": "messages_visible"}], "rule": {"crit": 1},
         "filter": "queue:$q", "layout": {"x": 0, "y": 0, "w": 1, "h": 1}}
    v = [{"name": "q", "field": "queue", "value": "orders"}]
    before = copy.deepcopy((w, v))
    assert M.alert_of(w, v, data)["tone"] == "critical"
    assert (w, v) == before and data == D["own"]
