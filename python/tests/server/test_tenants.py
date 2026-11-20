"""Tenants page, tenant filter and tenant search (contract/tenants.md).

The shared fixtures are contract/fixtures: 32 spans recorded from the Node.js package at contract
revision 3.2, and the expected outputs. The hosted app checks the same expected outputs against its own
store, so the two give the same numbers on the same spans.
"""
import base64
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from trailbeam.server import queries
from trailbeam.server.db import Database
from trailbeam.server.otlp import decode
from trailbeam.server.server import App, ApiError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "conformance"))
from cases import rows_match  # noqa: E402

FIXTURES = Path(__file__).resolve().parents[3] / "contract" / "fixtures"
SPANS = json.loads((FIXTURES / "contract-3.2-spans.json").read_text())
EXPECTED = json.loads((FIXTURES / "tenants-expected.json").read_text())
NOW = int(EXPECTED["now_ns"])
BOUND = EXPECTED["p95_relative_error"]



@pytest.fixture
def fixture_db():
    db = Database(":memory:")
    for e in SPANS["exports"]:
        d = decode(base64.b64decode(e["base64"]), e["contentType"])
        assert d.rejected == []
        db.insert(d.records)
    yield db
    db.close()


def test_the_fixture(fixture_db):
    assert fixture_db.query("SELECT COUNT(*) AS n FROM spans")[0]["n"] == 32
    last = fixture_db.query("SELECT MAX(time_ns) AS t FROM spans")[0]["t"]
    assert last + 60 * 10**9 == NOW


@pytest.mark.parametrize("case", EXPECTED["stats"], ids=lambda c: c["name"])
def test_stats_match_the_shared_expected_outputs(fixture_db, case):
    got = queries.tenants(fixture_db, NOW, EXPECTED["lambda_timeout_s"], case["from_ms"], case["to_ms"])
    assert {k: v for k, v in got.items() if k != "rows"} == {
        "from_ms": case["from_ms"], "to_ms": case["to_ms"], "capped": False,
        **{k: case[k] for k in ("total_sent", "untagged_sent")}}
    assert rows_match(got["rows"], case["rows"], BOUND), (got["rows"], case["rows"])


@pytest.mark.parametrize("case", EXPECTED["filter"], ids=lambda c: c["tenant"])
def test_filter_matches_the_shared_expected_outputs(fixture_db, case):
    got = queries.recent(fixture_db, NOW, 900, 100, tenant=case["tenant"])
    assert sorted(m["message_id"] for m in got) == case["message_ids"]


@pytest.mark.parametrize("case", EXPECTED["search"], ids=lambda c: repr(c["text"]))
def test_search_matches_the_shared_expected_outputs(fixture_db, case):
    assert queries.find_tenants(fixture_db, case["text"]) == case["tenants"]


def test_whole_fixture_by_hand(fixture_db):
    """The numbers the app's QV-53 Tenants page showed on these spans, worked out from the spans."""
    s = queries.tenants(fixture_db, NOW, 900, 0, NOW // 10**6)
    by = {r["tenant"]: r for r in s["rows"]}
    assert (s["total_sent"], s["untagged_sent"]) == (7, 1)  # legacy-svc's package predates tenants
    assert [r["tenant"] for r in s["rows"]] == ["globex", "acme", "initech", "hooli"]  # 2, 2 (1 DLQ first), 2, 0
    assert by["acme"]["share"] == 2 / 7
    assert by["globex"]["dead_lettered"] == 1  # m-pay-2, moved to payments-dlq
    assert by["hooli"]["sent"] == 0 and by["hooli"]["first_deliveries"] == 1  # m-orphan: no instrumented producer
    # acme's first deliveries: m-orders-1 and m-pay-1 from their sends, m-bill-1 (Lambda, no send) from SentTimestamp
    waits = []
    for mid in ("m-orders-1", "m-pay-1", "m-bill-1"):
        rows = fixture_db.message_rows(mid)
        waits += queries.tenant_counts(queries.timeline.build(rows, NOW, 900)["events"], "acme", 0, NOW)["waits"]
    assert len(waits) == 3 and queries.p95(waits) == max(waits) == 430
    assert abs(by["acme"]["p95_in_queue_ms"] - 430) <= BOUND * 430  # from the sketch (rollup.py)


def test_filter_is_most_recently_active_first(fixture_db):
    got = [m["message_id"] for m in queries.recent(fixture_db, NOW, 900, 100, tenant="acme")]
    last = {m: max(r["time_ns"] for r in fixture_db.message_rows(m)) for m in got}
    assert got == sorted(got, key=lambda m: -last[m])


def test_p95_is_nearest_rank():
    assert queries.p95([]) is None
    assert queries.p95([5]) == 5
    assert queries.p95(list(range(1, 101))) == 95
    assert queries.p95(list(range(1, 21))) == 19
    assert queries.p95([3, 1, 2]) == 3


def _row(kind, t, tenant=None, trace="t1", span="s1", **attrs):
    if tenant is not None:
        attrs["trailbeam.tenant"] = tenant
    return {"kind": kind, "time_ns": t, "trace_id": trace, "span_id": span, "attributes": json.dumps(attrs)}


def test_a_messages_tenant():
    # the earliest send's wins, even over an earlier receive's
    assert queries.message_tenant([_row("receive", 1, "r"), _row("send", 5, "late"), _row("send", 2, "early")]) == "early"
    # with no send naming one: the earliest receive's or outcome's; ties by trace then span ID
    assert queries.message_tenant([_row("send", 1), _row("outcome", 3, "o"), _row("receive", 3, "r", span="s0")]) == "r"
    # deletes don't name tenants; empty and non-string values don't count; cut to 256 characters
    assert queries.message_tenant([_row("delete", 1, "d")]) is None
    assert queries.message_tenant([_row("send", 1, ""), _row("receive", 2, 7), _row("receive", 3, "x" * 300)]) == "x" * 256


def test_rows_only_for_tenants_with_something_to_count():
    m = lambda tenant, sent=0, waits=(), dlq=False: {"tenant": tenant, "sent": sent, "waits": list(waits), "dead_lettered": dlq}
    s = queries.tenant_stats([m("only-deletes"), m("b", 1), m("a", 1), m("c", 1, dlq=True), m(None, 1), m("w", waits=[4])])
    assert [r["tenant"] for r in s["rows"]] == ["c", "a", "b", "w"]  # sent, then dead-lettered, then code points
    assert (s["total_sent"], s["untagged_sent"]) == (4, 1)
    assert s["rows"][-1] == {"tenant": "w", "sent": 0, "share": 0, "p95_in_queue_ms": 4, "first_deliveries": 1, "dead_lettered": 0}
    assert queries.tenant_stats([m("a")])["rows"] == []
    assert queries.tenant_stats([])["total_sent"] == 0


def _record(mid, kind, t, tenant, n):
    return {"trace_id": f"{n:032x}", "span_id": f"{n:016x}", "kind": kind, "time_ns": t, "queue": "q", "message_id": mid,
            "receive_count": 1 if kind == "receive" else None, "receipt_hmac": f"h{n}", "key_id": "k",
            "attributes": {"trailbeam.tenant": tenant}}


def test_search_orders_exact_then_recent_and_ignores_ascii_case_only(db):
    db.insert([_record("m1", "send", 100, "acme-old", 1), _record("m2", "send", 300, "acme-new", 2),
               _record("m3", "send", 200, "Acme", 3), _record("m4", "send", 50, "Ärger", 4),
               _record("m5", "send", 300, "acme-b", 5), _record("m6", "send", 400, "50%_off", 6)])
    assert queries.find_tenants(db, "ACME") == ["Acme", "acme-b", "acme-new", "acme-old"]
    assert queries.find_tenants(db, "acme", limit=2) == ["Acme", "acme-b"]
    assert queries.find_tenants(db, "Är") == ["Ärger"] and queries.find_tenants(db, "är") == []
    assert queries.find_tenants(db, "50%_") == ["50%_off"] and queries.find_tenants(db, "5_") == []


def test_every_message_counts_with_no_cap(db):
    """The page used to read at most 2,000 messages (QV-73): now every message in the range counts."""
    db.insert([_record(f"m{i}", "send", 1_000_000 * (i + 1), "t" if i % 2 else "u", i + 1) for i in range(2500)])
    s = queries.tenants(db, 10**12, 900, 0, 10**12)
    assert s["capped"] is False and s["total_sent"] == 2500
    assert [(r["tenant"], r["sent"]) for r in s["rows"]] == [("t", 1250), ("u", 1250)]
    assert queries.tenants(db, 10**12, 900, 0, 1)["total_sent"] == 1  # still only the range


# ---- the API ---------------------------------------------------------------------------------------

def test_api(fixture_db):
    app = App(fixture_db, now=lambda: NOW, ui_dir=None)
    whole = EXPECTED["stats"][0]
    got = app.api("tenants", {"from_ms": str(whole["from_ms"]), "to_ms": str(whole["to_ms"])})
    assert rows_match(got["rows"], whole["rows"], BOUND)
    # default: the last day up to now
    day = app.api("tenants", {})
    assert (day["from_ms"], day["to_ms"]) == (NOW // 10**6 - 86_400_000, NOW // 10**6) and day["rows"] == got["rows"]
    assert app.api("tenants", {"since_s": "1"})["rows"] == []  # the last second: 60 s after the spans
    assert app.api("tenants/search", {"q": " gl "})["tenants"] == ["globex"]
    assert {m["message_id"] for m in app.api("messages", {"tenant": "hooli"})["messages"]} == {"m-orphan"}
    assert len(app.api("messages", {"tenant": ""})["messages"]) == 10  # empty: no filter


@pytest.mark.parametrize("bad", ["0", "x", "100000"])
def test_api_ignores_lambda_timeout_s(fixture_db, bad):
    """QV-96: the Lambda timeout doesn't change what the page counts, so the parameter, even a bad one, is unread."""
    app = App(fixture_db, now=lambda: NOW, ui_dir=None)
    assert app.api("tenants", {"lambda_timeout_s": bad}) == app.api("tenants", {})


@pytest.mark.parametrize("path,q", [("tenants", {"from_ms": "x"}), ("tenants", {"from_ms": "5", "to_ms": "4"}),
                                    ("tenants", {"since_s": "0"}), ("tenants/search", {"limit": "0"}),
                                    ("tenants/nope", {})])
def test_api_bad_parameters(fixture_db, path, q):
    with pytest.raises(ApiError):
        App(fixture_db, now=lambda: NOW, ui_dir=None).api(path, q)


def test_tenants_page_without_the_shell(fixture_db, server):
    """Without the shared UI's build, /tenants is a server-rendered table from the same API."""
    with urllib.request.urlopen(server + "/tenants", timeout=5) as r:  # the server fixture's own, empty db
        assert r.status == 200 and "No tenants in this range" in r.read().decode()
    from trailbeam.server import web
    app = App(fixture_db, now=lambda: NOW, ui_dir=None)
    status, _, body = web.handle(app.api, ApiError, "/tenants", {"since_s": "3600"})
    html = body.decode()
    assert status == 200
    for t, sent, share, dlq in [("globex", 2, 29, 1), ("acme", 2, 29, 0), ("initech", 2, 29, 0), ("hooli", 0, 0, 0)]:
        assert f'href="/?tenant={t}">{t}</a></td><td class="mono">{sent}</td><td class="mono">{share}%</td>' in html
    assert "1 of them without a tenant" in html and "TRAILBEAM_TENANT_ATTRIBUTE" in html
    assert html.index(">globex<") < html.index(">acme<") < html.index(">initech<") < html.index(">hooli<")
    status, _, body = web.handle(app.api, ApiError, "/", {"tenant": "initech"})
    assert "m-fifo-1" in body.decode() and "m-orders-1" not in body.decode()
