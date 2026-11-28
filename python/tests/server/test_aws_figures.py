"""The figures from AWS reads (aws_figures.py, QV-589): parity with @trailbeam/ui's model/aws.ts on the shared cases in
contract/fixtures/aws-figures-parity.json (ui/test/aws.test.ts runs the same file), and what the cases don't cover."""
import copy
import importlib.util
import json
from pathlib import Path

import pytest

from trailbeam.server import aws_figures as af

FIXTURES = Path(__file__).resolve().parents[3] / "contract" / "fixtures"
F = json.loads((FIXTURES / "aws-figures-parity.json").read_text())


def test_the_fixture_is_what_its_script_writes(tmp_path, monkeypatch):
    """aws-figures-parity.json is make-aws-figures-parity.py's output from these figures: a change to either shows."""
    spec = importlib.util.spec_from_file_location("make_aws_figures_parity", FIXTURES / "make-aws-figures-parity.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    monkeypatch.setattr(mod, "OUT", tmp_path / "aws-figures-parity.json")
    mod.main()
    assert json.loads(mod.OUT.read_text()) == F, "run contract/fixtures/make-aws-figures-parity.py and check ui's test"


def j(v):
    """The value as JSON writes it, as the fixture has it."""
    return json.loads(json.dumps(v))


@pytest.mark.parametrize("c", F["settingsFrom"], ids=lambda c: c["name"])
def test_settings_from(c):
    assert j(af.settings_from(c["attributes"])) == c["expected"]


@pytest.mark.parametrize("c", F["parseQueueArn"], ids=lambda c: str(c["arn"]))
def test_parse_queue_arn(c):
    assert af.parse_queue_arn(c["arn"]) == c["expected"]


@pytest.mark.parametrize("c", F["periodFor"], ids=lambda c: c["name"])
def test_period_for(c):
    args = (c["fromMs"], c["toMs"], c["nowMs"]) + ((c["maxPoints"],) if c["maxPoints"] is not None else ())
    assert af.period_for(*args) == c["expected"]


@pytest.mark.parametrize("c", F["nameMatches"], ids=lambda c: f'{c["pattern"]} {c["name"]}')
def test_name_matches(c):
    assert af.name_matches(c["pattern"], c["name"]) == c["expected"]


@pytest.mark.parametrize("c", F["nowFigures"], ids=lambda c: f'{c["aws"]} {c["windowId"]}')
def test_now_figures(c):
    assert j(af.now_figures(F["aws"][c["aws"]] if c["aws"] else None, c["windowId"])) == c["expected"]


@pytest.mark.parametrize("c", F["liveOf"], ids=lambda c: c["name"])
def test_live_of(c):
    aws = copy.deepcopy(F["aws"][c["aws"]])
    view = F["views"][c["view"]] if c["view"] else None
    spans = F["spans"][c["spans"]] if c["spans"] else None
    assert j(af.live_of(aws, view, c["windows"], c["now"], spans)) == c["expected"]
    assert aws == F["aws"][c["aws"]], "live_of changes nothing it is given"


@pytest.mark.parametrize("c", F["queueAwsView"], ids=lambda c: c["aws"])
def test_queue_aws_view(c):
    assert j(af.queue_aws_view(F["aws"][c["aws"]], c["windowId"])) == c["expected"]


def test_numbers_as_javascript_reads_them():
    """Number() on an attribute value: what the cases don't spell out."""
    n = af._num
    assert [n(None), n(""), n("  "), n("7"), n(" 7 "), n("1.5"), n("1e3"), n(3), n(2.5)] == [None, None, 0, 7, 7, 1.5, 1000, 3, 2.5]
    assert [n("NaN"), n("Infinity"), n("-inf"), n("1_000"), n("abc"), n(float("nan"))] == [None] * 6
    assert isinstance(n("30"), int) and isinstance(n("30.0"), int)


def test_metrics_are_the_hosted_ones():
    assert [(m["key"], m["name"], m["stat"]) for m in af.METRICS] == [
        ("visible", "ApproximateNumberOfMessagesVisible", "Maximum"),
        ("inFlight", "ApproximateNumberOfMessagesNotVisible", "Maximum"),
        ("oldestS", "ApproximateAgeOfOldestMessage", "Maximum"),
        ("sent", "NumberOfMessagesSent", "Sum"),
        ("deleted", "NumberOfMessagesDeleted", "Sum"),
    ]
