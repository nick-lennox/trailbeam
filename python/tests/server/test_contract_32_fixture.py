"""Contract revision 3.2 on real spans: contract/fixtures/contract-3.2-spans.json holds 32 spans the Node.js
package made against canned SQS responses (four services, as protobuf OTLP exports). The hosted app tests the
same file, so both implementations read the same inputs.

Here: every span is accepted, and the Lambda error text (contract 15.5) is on the timeline, through the API
and on the server-rendered page. Errors grouped by cause are drawn by @trailbeam/ui from these events
(ui/src/errors.ts); its tests run on the timelines this server returns for the same spans.
"""
import base64
import json
import urllib.request
from pathlib import Path

import pytest

from trailbeam.server.otlp import decode

FIXTURE = Path(__file__).resolve().parents[3] / "contract" / "fixtures" / "contract-3.2-spans.json"
EXPORTS = json.loads(FIXTURE.read_text())["exports"]


def get_json(url, path):
    with urllib.request.urlopen(url + path, timeout=5) as r:
        return json.loads(r.read())


@pytest.fixture
def loaded(db, server):
    for e in EXPORTS:
        d = decode(base64.b64decode(e["base64"]), e["contentType"])
        assert d.rejected == [], e["service"]
        db.insert(d.records)
    return server


def test_every_recorded_span_is_accepted(db):
    total = 0
    for e in EXPORTS:
        d = decode(base64.b64decode(e["base64"]), e["contentType"])
        assert d.rejected == [] and len(d.records) == e["spans"], e["service"]
        total += len(d.records)
    assert total == 32


def test_lambda_error_text_is_on_each_failed_outcome(loaded):
    t = get_json(loaded, "/api/messages/m-bill-1")
    outcomes = [(e["delivery"], e["outcome"], e.get("error_type"), e.get("error_message"))
                for e in t["events"] if e["type"] == "lambda_outcome"]
    assert outcomes == [
        (1, "failed_exception", "TypeError", "card declined for [REDACTED]"),
        (2, "failed_exception", "Error", "Stripe timed out after 10000 ms"),
        (3, "failed_exception", "TypeError", "card declined for [REDACTED]"),
    ]


def test_no_error_text_where_none_was_recorded(loaded):
    for mid in ("m-orders-1", "m-pay-2", "m-legacy"):
        t = get_json(loaded, f"/api/messages/{mid}")
        assert not any("error_message" in e for e in t["events"]), mid


def test_error_text_on_the_server_rendered_page(loaded):
    with urllib.request.urlopen(loaded + "/message?id=m-bill-1", timeout=5) as r:
        html = r.read().decode()
    assert "card declined for [REDACTED]" in html
    assert "Stripe timed out after 10000 ms" in html
