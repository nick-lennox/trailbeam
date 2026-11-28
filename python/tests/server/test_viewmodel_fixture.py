"""The server's answers in ui/test/fixtures/viewmodel-parity.json are still what it answers (One UI row 0d, QV-578).

The shared view-model functions (@trailbeam/ui/model) must make the same Event explorer events, Overview and cause
rows of the server's answers and of the hosted app's for the same spans; ui/test/viewmodel.test.ts checks that on the
recorded answers. This checks the recording: GET /api/messages/{id}, /api/flows, /api/causes and each queue's
/api/queues/{queue}/now (QV-579) for the contract 3.2 spans still give answers.python. After a deliberate change, run ``python ui/test/make-viewmodel-fixture.py``, then
``node --import tsx test/make-viewmodel-expected.ts`` in ui/, and check the hosted app still matches.
"""
import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
FIXTURE = ROOT / "ui" / "test" / "fixtures" / "viewmodel-parity.json"


def _maker():
    spec = importlib.util.spec_from_file_location("make_viewmodel_fixture", ROOT / "ui" / "test" / "make-viewmodel-fixture.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_the_recorded_answers_are_the_servers():
    fixture = json.loads(FIXTURE.read_text())
    now_ms, from_ms, answers = _maker().record()
    assert (now_ms, from_ms) == (fixture["nowMs"], fixture["fromMs"])
    # through JSON, as the file holds them
    assert json.loads(json.dumps(answers)) == fixture["answers"]["python"]


def test_the_hosted_answers_are_recorded_too():
    fixture = json.loads(FIXTURE.read_text())
    assert set(fixture["answers"]) == {"python", "hosted"}
    assert len(fixture["answers"]["hosted"]["timelines"]) == len(fixture["answers"]["python"]["timelines"])
