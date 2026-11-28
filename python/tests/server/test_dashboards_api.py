"""The dashboards JSON API (dashboards_api.py, QV-652): the hosted app's checks and error texts (lib/dashboards/
actions.ts), locks and who may lift them, one widget at a time, the measures endpoint held to the shared cases in
contract/fixtures/measures-parity.json, the EventData read from stored spans, and the routes over HTTP with every
status contract/openapi.yaml lists for them (test_openapi.py leaves the Dashboards tag to this file: the routes
are off by default)."""
from __future__ import annotations

import copy
import itertools
import json
import re
import time
from pathlib import Path

import pytest
from openapi_contract import contract
from spans import SEC, Consumer, to_proto
from test_openapi import request, serve
from trailbeam.server import access as access_mod
from trailbeam.server import dashboards_api as api
from trailbeam.server import measures as M
from trailbeam.server import proxy_auth, server
from trailbeam.server.alerts_api import HIDDEN_CHARACTERS, ApiProblem
from trailbeam.server.dashboards_store import DashboardTables, MemoryDashboards
from trailbeam.server.db import Database
from trailbeam.server.passwords import Hasher
from trailbeam.server.proxy import TrustedProxies, parse_networks
from trailbeam.server.tokens import Tokens

FIXTURES = Path(__file__).resolve().parents[3] / "contract" / "fixtures"
F = json.loads((FIXTURES / "measures-parity.json").read_text())
D = F["datasets"]
LAYOUT = {"x": 0, "y": 0, "w": 3, "h": 2}
QUICK = Hasher(log2_n=10, r=8, p=1)
J = {"Content-Type": "application/json"}


class Who:
    def __init__(self, user_id=None, name=None, role=None, via="session"):
        self.user_id, self.name, self.role, self.via, self.email = user_id, name, role, via, None


ADA, BEN = Who(1, "Ada", "admin"), Who(2, "Ben", "member")


def widget(id_="w1", **over):
    return {"id": id_, "type": "number", "title": "DLQ depth", "queries": [{"measure": "dlq_depth"}],
            "layout": LAYOUT, **over}


def make(dataset="own", can_unlock=lambda who: who.role == "admin", store=None):
    data = D[dataset]
    return api.DashboardsApi(store or MemoryDashboards(), "local", lambda since, now, to: copy.deepcopy(data),
                             can_unlock=can_unlock, now=lambda: data["now"])


def refused(fn, *a) -> tuple[int, str]:
    with pytest.raises(ApiProblem) as e:
        fn(*a)
    return e.value.status, e.value.message


# ── dashboards ───────────────────────────────────────────────────────────────────────────────────────────────────

def test_make_list_get_and_the_hosted_apps_record():
    a = make()
    st, out = a.call("POST", "/api/dashboards", {"name": "  Orders  ", "def": {"widgets": [widget()]}}, ADA)
    d = out["dashboard"]
    assert st == 201 and len(d["id"]) == 8
    assert d == {"id": d["id"], "name": "Orders", "def": {"variables": [], "sections": [], "widgets": [widget()]},
                 "time": None, "starred": False, "lockedBy": None, "lockedById": None, "createdBy": "Ada",
                 "createdById": "1", "updatedBy": "Ada", "createdAt": D["own"]["now"], "updatedAt": D["own"]["now"],
                 "version": 1}
    assert a.call("GET", f"/api/dashboards/{d['id']}", {}, BEN) == (200, {"dashboard": d})
    assert a.call("GET", "/api/dashboards", {}, BEN) == (200, {"dashboards": [d], "canUnlock": False})
    assert a.call("POST", "/api/dashboards", {"name": "Empty"}, BEN)[1]["dashboard"]["def"]["widgets"] == []


@pytest.mark.parametrize("body, error", [
    ({}, api.NO_NAME), ({"name": "   "}, api.NO_NAME), ({"name": 7}, api.NO_NAME),
    ({"name": "x", "imported": "{nope"}, api.NOT_JSON),
    ({"name": "x", "imported": "x" * (api.MAX_IMPORT_CHARS + 1)}, api.TOO_BIG),
    ({"name": "x", "imported": "[]"}, "A dashboard is a JSON object."),
    ({"name": "x", "def": {"widgets": [widget(), {**widget("w2"), "queries": [{"measure": "depth"}]}]}},
     'Widget 2: queries has an unknown measure "depth".'),
    ({"name": "x", "def": {"widgets": [], "variables": [{"name": "q"}]}},
     'Variable 1 needs "name", "field" and "value" as text.'),
    ({"name": "x", "time": "2h"}, '"time" is a preset: 5m, 15m, 1h, 4h, 1d, 2d, 7d, 30d, or null.'),
])
def test_what_making_one_refuses(body, error):
    assert refused(make().call, "POST", "/api/dashboards", body, ADA) == (400, error)


def test_import_takes_a_definition_as_json_text_and_names_are_cut_to_80():
    a = make()
    text = json.dumps({"variables": [{"name": "queue", "field": "queue", "value": "orders"}], "widgets": [widget()]})
    d = a.call("POST", "/api/dashboards", {"name": "n" * 100, "imported": text, "time": "7d"}, ADA)[1]["dashboard"]
    assert d["name"] == "n" * 80 and d["time"] == "7d"
    assert d["def"]["variables"] == [{"name": "queue", "field": "queue", "value": "orders"}]


def test_a_locked_dashboard_takes_a_star_and_nothing_else():
    a = make()
    did = a.call("POST", "/api/dashboards", {"name": "Orders"}, BEN)[1]["dashboard"]["id"]
    st, out = a.call("POST", f"/api/dashboards/{did}/lock", {"lock": True}, BEN)
    assert st == 200 and out["lockedBy"] == "Ben" and out["dashboard"]["lockedById"] == "2"
    # locked already: locking again changes nothing, so no one takes over someone else's lock
    assert a.call("POST", f"/api/dashboards/{did}/lock", {"lock": True}, ADA)[1]["lockedBy"] == "Ben"
    for body in ({"name": "No"}, {"def": {"widgets": []}}, {"time": "1d"}):
        assert refused(a.call, "PATCH", f"/api/dashboards/{did}", body, BEN) == (400, api.LOCKED)
    assert refused(a.call, "POST", f"/api/dashboards/{did}/widgets", {"widget": widget()}, BEN) == (400, api.LOCKED)
    assert refused(a.call, "DELETE", f"/api/dashboards/{did}", {}, BEN) == (400, api.LOCKED_DELETE)
    assert a.call("PATCH", f"/api/dashboards/{did}", {"starred": True}, BEN)[1]["dashboard"]["starred"] is True


def test_whoever_locked_it_or_an_admin_unlocks_it():
    a = make()
    cy = Who(3, "Cy", "member")
    did = a.call("POST", "/api/dashboards", {"name": "Orders"}, BEN)[1]["dashboard"]["id"]
    a.call("POST", f"/api/dashboards/{did}/lock", {"lock": True}, BEN)
    assert refused(a.call, "POST", f"/api/dashboards/{did}/lock", {"lock": False}, cy) == \
        (403, "Ben locked this dashboard. They, or an admin, can unlock it.")
    # compared by person ID, never by name: anyone can rename themselves
    assert refused(a.call, "POST", f"/api/dashboards/{did}/lock", {"lock": False}, Who(3, "Ben", "member"))[0] == 403
    assert a.call("POST", f"/api/dashboards/{did}/lock", {"lock": False}, BEN)[1]["lockedBy"] is None
    a.call("POST", f"/api/dashboards/{did}/lock", {"lock": True}, BEN)
    assert a.call("POST", f"/api/dashboards/{did}/lock", {"lock": False}, ADA)[1]["dashboard"]["lockedBy"] is None
    # a lock made by a token has no person ID: only an admin lifts it
    a.call("POST", f"/api/dashboards/{did}/lock", {"lock": True}, Who(via="personal_token"))
    assert refused(a.call, "POST", f"/api/dashboards/{did}/lock", {"lock": False}, Who(via="personal_token"))[0] == 403
    assert refused(a.call, "POST", f"/api/dashboards/{did}/lock", {"lock": "yes"}, ADA)[0] == 400


def test_an_unlock_never_lifts_a_lock_someone_else_took_after_the_check():
    """QV-689: Ben checks his own lock; before his write an admin unlocks it and Cy locks it. Ben's write must not
    lift Cy's lock: it is written only at the version checked, then checked again, which Cy's lock now fails."""
    store = MemoryDashboards()
    a = make(store=store)
    cy = Who(3, "Cy", "member")
    did = a.call("POST", "/api/dashboards", {"name": "Orders"}, BEN)[1]["dashboard"]["id"]
    a.call("POST", f"/api/dashboards/{did}/lock", {"lock": True}, BEN)
    real, calls = store.update_dashboard, []

    def racing(ws, d, fields, **kw):
        calls.append(kw.get("version"))
        if len(calls) == 1:  # between Ben's check and Ben's write
            real(ws, d, {"lockedBy": None, "lockedById": None})  # an admin unlocks it
            store.lock_dashboard(ws, d, "Cy", "3", 0)  # and Cy locks it
        return real(ws, d, fields, **kw)

    store.update_dashboard = racing
    assert refused(a.call, "POST", f"/api/dashboards/{did}/lock", {"lock": False}, BEN) == \
        (403, "Cy locked this dashboard. They, or an admin, can unlock it.")
    assert calls == [2] and store.get_dashboard("local", did)["lockedById"] == "3"
    # a race that leaves the lock his (someone stars it) is tried again and unlocks
    store.update_dashboard = real
    a.call("POST", f"/api/dashboards/{did}/lock", {"lock": False}, cy)
    a.call("POST", f"/api/dashboards/{did}/lock", {"lock": True}, BEN)
    calls.clear()

    def starring(ws, d, fields, **kw):
        calls.append(kw.get("version"))
        if len(calls) == 1:
            real(ws, d, {"starred": True})
        return real(ws, d, fields, **kw)

    store.update_dashboard = starring
    out = a.call("POST", f"/api/dashboards/{did}/lock", {"lock": False}, BEN)[1]["dashboard"]
    assert len(calls) == 2 and calls[1] == calls[0] + 1 and out["lockedBy"] is None and out["starred"] is True
    # a race on every try: 409
    store.update_dashboard = lambda *a_, **k: ("conflict", None)
    a.call("POST", f"/api/dashboards/{did}/lock", {"lock": True}, BEN)
    assert refused(a.call, "POST", f"/api/dashboards/{did}/lock", {"lock": False}, BEN) == (409, api.BUSY)


def test_save_duplicate_and_delete():
    a = make()
    did = a.call("POST", "/api/dashboards", {"name": "Orders", "def": {"widgets": [widget()]}}, ADA)[1]["dashboard"]["id"]
    d = a.call("PATCH", f"/api/dashboards/{did}", {"name": "Renamed", "time": "4h"}, BEN)[1]["dashboard"]
    assert (d["name"], d["time"], d["updatedBy"], d["createdBy"], d["version"]) == ("Renamed", "4h", "Ben", "Ada", 2)
    assert refused(a.call, "PATCH", f"/api/dashboards/{did}", {"name": "Old", "version": 1}, BEN) == (409, api.BUSY)
    assert refused(a.call, "PATCH", f"/api/dashboards/{did}", {"name": ""}, BEN) == (400, api.NO_NAME)
    assert refused(a.call, "PATCH", "/api/dashboards/nope", {"name": "x"}, BEN) == (404, api.GONE)
    st, out = a.call("POST", f"/api/dashboards/{did}/duplicate", {}, BEN)
    copy_ = out["dashboard"]
    assert st == 201 and copy_["name"] == "Renamed copy" and copy_["def"] == d["def"] and copy_["createdBy"] == "Ben"
    assert a.call("DELETE", f"/api/dashboards/{did}", {}, BEN) == (200, {"removed": True})
    assert a.call("DELETE", f"/api/dashboards/{did}", {}, BEN) == (200, {"removed": False})
    assert refused(a.call, "GET", f"/api/dashboards/{did}", {}, BEN) == (404, api.GONE)


# ── one widget at a time ─────────────────────────────────────────────────────────────────────────────────────────

def test_widgets_are_added_changed_and_removed_through_the_same_checks():
    a = make()
    did = a.call("POST", "/api/dashboards", {"name": "Orders", "def": {"widgets": [widget()]}}, ADA)[1]["dashboard"]["id"]
    st, out = a.call("POST", f"/api/dashboards/{did}/widgets", {"widget": {**widget(), "id": None, "title": "Age"}}, BEN)
    new_id = out["widget"]["id"]
    assert st == 201 and new_id and [w["title"] for w in out["dashboard"]["def"]["widgets"]] == ["DLQ depth", "Age"]
    assert refused(a.call, "POST", f"/api/dashboards/{did}/widgets", {"widget": {"type": "pie"}}, BEN) == \
        (400, 'Widget 3: "type" is "pie", which is not a widget type.')
    assert refused(a.call, "POST", f"/api/dashboards/{did}/widgets", {"widget": "x"}, BEN)[0] == 400
    out = a.call("PATCH", f"/api/dashboards/{did}/widgets/w1", {"widget": widget("other", title="Renamed")}, BEN)[1]
    assert out["widget"]["id"] == "w1" and out["dashboard"]["def"]["widgets"][0]["title"] == "Renamed"
    assert refused(a.call, "PATCH", f"/api/dashboards/{did}/widgets/gone", {"widget": widget()}, BEN) == \
        (404, api.WIDGET_GONE)
    assert [w["id"] for w in a.call("DELETE", f"/api/dashboards/{did}/widgets/w1", {}, BEN)[1]["dashboard"]["def"]["widgets"]] == [new_id]
    assert refused(a.call, "DELETE", f"/api/dashboards/{did}/widgets/w1", {}, BEN) == (404, api.WIDGET_GONE)
    big = widget("big", text="x" * M.MAX_DEF_BYTES)
    assert "A dashboard can be at most 256 KB." in refused(a.call, "POST", f"/api/dashboards/{did}/widgets",
                                                           {"widget": big}, BEN)[1]


def test_a_widget_change_is_tried_again_after_a_race_and_given_up_after_three():
    store = MemoryDashboards()
    a = make(store=store)
    did = a.call("POST", "/api/dashboards", {"name": "Orders"}, ADA)[1]["dashboard"]["id"]
    real, calls = store.update_dashboard, []

    def racing(ws, d, fields, **kw):
        calls.append(kw.get("version"))
        if len(calls) == 1:  # someone else changes it between this read and this write
            real(ws, d, {"name": "Theirs"})
        return real(ws, d, fields, **kw)

    store.update_dashboard = racing
    out = a.call("POST", f"/api/dashboards/{did}/widgets", {"widget": widget()}, BEN)[1]["dashboard"]
    assert calls == [1, 2] and out["name"] == "Theirs" and len(out["def"]["widgets"]) == 1
    store.update_dashboard = lambda *a_, **k: ("conflict", None)
    assert refused(a.call, "POST", f"/api/dashboards/{did}/widgets", {"widget": widget()}, BEN) == (409, api.BUSY)


# ── measures ─────────────────────────────────────────────────────────────────────────────────────────────────────

def _series_widget(c) -> dict:
    return {"id": "w1", "type": "timeseries", "title": c["name"], "filter": c["filter"],
            "queries": [{"measure": c["measure"], **({"group_by": c["groupBy"]} if c["groupBy"] else {})}],
            "layout": LAYOUT}


SERIES = [c for c in F["series"] if M.problem(_series_widget(c)) is None
          and c["time"]["to"] - c["time"]["from"] <= api.MAX_RANGE_MS]


def _numbers(line: dict) -> dict:
    """A fixture line with "Infinity", "-Infinity" and "NaN" as numbers again."""
    num = lambda v: float(v) if isinstance(v, str) else v
    return {**line, "now": num(line["now"]), "points": [num(p) for p in line["points"]]}


@pytest.mark.parametrize("c", SERIES, ids=lambda c: f"{c['data']}: {c['name']}")
def test_a_widgets_measures_are_the_shared_cases(c):
    """The measures endpoint over the shared cases' data: the lines of a widget with that one query are the case's
    expected series, as the hosted app's measures.ts computes them (measures-parity.json), the largest MAX_LINES
    and the rest as one "Other (k)" line."""
    a = make(c["data"])
    did = a.call("POST", "/api/dashboards", {"name": "Parity", "def": {"widgets": [_series_widget(c)]}}, ADA)[1]["dashboard"]["id"]
    q = {"from": str(c["time"]["from"]), "to": str(c["time"]["to"]), "n": str(c["n"])}
    st, out = a.call("GET", f"/api/dashboards/{did}/widgets/w1/measures", q, BEN)
    assert st == 200
    [w] = out["widgets"]
    full = c["expected"]
    capped = full if len(full) <= api.MAX_LINES else full[:api.MAX_LINES] + [
        api.plain(M.other_line(M.measure(c["measure"]), [_numbers(x) for x in full[api.MAX_LINES:]], c["n"]))]
    assert w["queries"][0]["lines"] == capped
    assert json.loads(json.dumps(out, allow_nan=False)) == out, "JSON with no Infinity or NaN numbers"


def test_a_dashboards_measures_widget_by_widget():
    a = make()
    ws = [widget("n", rule={"crit": 0}),
          {"id": "list", "type": "event_list", "title": "Failed", "filter": "status:error", "layout": LAYOUT},
          {"id": "bars", "type": "timeseries", "title": "Events", "queries": [{"measure": "events"}], "style": "bars",
           "layout": LAYOUT},
          {"id": "table", "type": "queue_table", "title": "Queues", "filter": "tenant:acme", "layout": LAYOUT},
          {"id": "mine", "type": "number", "title": "Sent", "queries": [{"measure": "sent"}], "time": "7d",
           "layout": LAYOUT}]
    did = a.call("POST", "/api/dashboards", {"name": "All", "time": "4h", "def": {"widgets": ws}}, ADA)[1]["dashboard"]["id"]
    out = a.call("GET", f"/api/dashboards/{did}/measures", {}, BEN)[1]
    now = D["own"]["now"]
    assert out["time"] == M.preset("4h", now) and out["now"] == now and out["dashboard"] == did
    n, lst, bars, table, mine = out["widgets"]
    alert = M.alert_of(ws[0], [], D["own"])
    assert n["alert"] == {k: alert[k] for k in ("tone", "value", "limit", "queue")}
    t4 = M.preset("4h", now)
    assert lst["events"] == M.events_for("status:error", t4, D["own"])[:api.MAX_EVENTS]
    assert bars["buckets"] == M.event_buckets("", t4, api.N_DEFAULT, D["own"])
    assert [r["name"] for r in table["queues"]] == M.queues_for([], D["own"]["queues"])
    assert set(table["queues"][0]["values"]) == set(M.TABLE_COLUMNS)
    assert table["queueOnlyTerms"] == ["tenant:acme"]
    assert mine["time"] == M.preset("7d", now)  # its own time range
    assert "data" not in out
    assert a.call("GET", f"/api/dashboards/{did}/measures", {"data": "1"}, BEN)[1]["data"]["now"] == now


@pytest.mark.parametrize("q, error", [
    ({"range": "2h"}, "range is a preset: 5m, 15m, 1h, 4h, 1d, 2d, 7d, 30d."),
    ({"from": "10"}, "from and to are both milliseconds since the epoch."),
    ({"from": "10", "to": "x"}, "from and to are both milliseconds since the epoch."),
    ({"from": "10", "to": "10"}, "to must be after from."),
    ({"from": "0", "to": str(31 * M.DAY)}, "A range can be at most 30 days."),
    ({"n": "0"}, "n is a whole number from 1 to 200."), ({"n": "201"}, "n is a whole number from 1 to 200."),
])
def test_what_a_measures_request_refuses(q, error):
    a = make()
    did = a.call("POST", "/api/dashboards", {"name": "x"}, ADA)[1]["dashboard"]["id"]
    assert refused(a.call, "GET", f"/api/dashboards/{did}/measures", q, BEN) == (400, error)


# ── what a measures request may cost (QV-684) ────────────────────────────────────────────────────────────────────

def _many_messages(count=2000, now=1_764_000_000_000) -> dict:
    """EventData of ``count`` messages, each sent, received and deleted (with a processing time) in the past hour:
    a group-by on message_id makes a line per message."""
    evs = []
    for i in range(count):
        at = now - 3_500_000 + i * 1_700
        for k, kind in enumerate(("send", "receive", "delete")):
            evs.append({"ms": at + k, "event": kind, "status": "ok", "queue": "orders", "messageId": f"m{i:05d}",
                        **({"durationMs": (i * 37) % 900} if kind == "delete" else {})})
    return {"events": evs, "now": now, "queues": ["orders"], "live": True,
            "queueLive": {"orders": {"status": "not_configured", "detail": api.NOT_QUEUE_LIVE}}}


def _on(data, store=None):
    return api.DashboardsApi(store or MemoryDashboards(), "local", lambda since, now, to: data, now=lambda: data["now"])


def _grouped(id_="g", measure="sent"):
    return {"id": id_, "type": "timeseries", "title": "By message", "layout": LAYOUT,
            "queries": [{"measure": measure, "group_by": "message_id"}]}


@pytest.mark.parametrize("measure", ["sent", "processing_time"])
def test_a_group_by_with_a_value_per_message_is_the_top_lines_and_the_rest_as_one(measure):
    """Grouped by message_id, 2000 messages are 2000 lines of 200 points (2 MB a widget, before QV-684); the answer
    is the largest 10 and "Other (1990)", the rest added up (sent) or the largest of them (processing time), the
    same as working out every line."""
    data = _many_messages()
    a = _on(data)
    did = a.call("POST", "/api/dashboards", {"name": "x", "def": {"widgets": [_grouped(measure=measure)]}}, ADA)[1]["dashboard"]["id"]
    out = a.call("GET", f"/api/dashboards/{did}/measures", {"n": "200"}, BEN)[1]
    lines = out["widgets"][0]["queries"][0]["lines"]
    m = M.measure(measure)
    full = M.series_for(m, "", "message_id", out["time"], 200, data)
    assert len(full) == 2000
    assert len(lines) == api.MAX_LINES + 1
    assert lines == api.plain(full[:api.MAX_LINES] + [M.other_line(m, full[api.MAX_LINES:], 200)])
    assert lines[-1]["name"] == "Other (1990)" and lines[-1]["others"] == 1990
    if measure == "sent":  # every message once: the lines add up to the widget without a group-by
        [whole] = M.series_for(m, "", None, out["time"], 200, data)
        assert [sum(x) for x in zip(*(ln["points"] for ln in lines))] == pytest.approx(whole["points"])
    assert len(json.dumps(out)) < 50_000


def test_a_dashboard_holds_at_most_50_widgets_of_at_most_5_queries():
    a = make()
    many = [widget(f"w{i}") for i in range(api.MAX_WIDGETS)]
    did = a.call("POST", "/api/dashboards", {"name": "x", "def": {"widgets": many}}, ADA)[1]["dashboard"]["id"]
    assert refused(a.call, "POST", "/api/dashboards", {"name": "x", "def": {"widgets": many + [widget("one")]}}, ADA) \
        == (400, "That dashboard has 51 widgets. A dashboard can have at most 50.")
    assert refused(a.call, "POST", f"/api/dashboards/{did}/widgets", {"widget": widget("one")}, ADA) \
        == (400, "That dashboard has 51 widgets. A dashboard can have at most 50.")
    six = widget("q", queries=[{"measure": "sent"}] * (api.MAX_QUERIES + 1))
    assert refused(a.call, "PATCH", f"/api/dashboards/{did}/widgets/w3", {"widget": six}, ADA) \
        == (400, "Widget 4: a widget can have at most 5 queries.")
    table = {"id": "t", "type": "queue_table", "title": "Queues", "layout": LAYOUT,
             "columns": ["dlq_depth"] * (api.MAX_COLUMNS + 1)}
    assert refused(a.call, "PATCH", f"/api/dashboards/{did}", {"def": {"widgets": [table]}}, ADA) \
        == (400, "Widget 1: a queue table can have at most 19 columns.")
    assert a.call("PATCH", f"/api/dashboards/{did}", {"def": {"widgets": [widget(queries=[{"measure": "sent"}] * 5)]}},
                  ADA)[0] == 200


def test_a_widgets_own_time_is_a_preset_or_null():
    # QV-688: measures.preset draws an unknown ID as the past hour, so "10y" was stored and drawn as 1h
    a = make()
    said = 'Widget 2: "time" is a preset: 5m, 15m, 1h, 4h, 1d, 2d, 7d, 30d, or null.'
    assert refused(a.call, "POST", "/api/dashboards",
                   {"name": "x", "def": {"widgets": [widget(), widget("w2", time="10y")]}}, ADA) == (400, said)
    did = a.call("POST", "/api/dashboards", {"name": "x", "def": {"widgets": [widget(time=None)]}}, ADA)[1][
        "dashboard"]["id"]
    for bad in ("90d", "", 7, ["7d"]):
        assert refused(a.call, "POST", f"/api/dashboards/{did}/widgets", {"widget": widget("w2", time=bad)}, ADA) \
            == (400, said)
    assert refused(a.call, "PATCH", f"/api/dashboards/{did}", {"def": {"widgets": [widget(time="1y")]}}, ADA) \
        == (400, said.replace("Widget 2", "Widget 1"))
    assert a.call("POST", f"/api/dashboards/{did}/widgets", {"widget": widget("w2", time="7d")}, ADA)[0] == 201
    out = a.call("GET", f"/api/dashboards/{did}/measures", {}, BEN)[1]
    assert [w["time"]["preset"] for w in out["widgets"]] == ["1h", "7d"]


def test_the_widgets_past_8_mb_or_5_seconds_are_skipped_and_asked_for_one_by_one(monkeypatch):
    data = _many_messages(200)
    store = MemoryDashboards()
    a = _on(data, store)
    ws = [_grouped(f"g{i}") for i in range(4)]
    did = a.call("POST", "/api/dashboards", {"name": "x", "def": {"widgets": ws}}, ADA)[1]["dashboard"]["id"]
    one = len(json.dumps(a.call("GET", f"/api/dashboards/{did}/widgets/g0/measures", {}, BEN)[1]["widgets"][0],
                         separators=(",", ":")))
    # two widgets fill the budget: the first two are answered, the last two skipped
    monkeypatch.setattr(api, "MEASURES_BYTES", 2 * one)
    out = a.call("GET", f"/api/dashboards/{did}/measures", {}, BEN)[1]
    assert out["skipped"] == 2 and [("skipped" in w) for w in out["widgets"]] == [False, False, True, True]
    assert out["widgets"][2] == {"id": "g2", "type": "timeseries", "title": "By message",
                                 "skipped": api.SKIPPED.format(id=did, wid="g2")}
    assert f"GET /api/dashboards/{did}/widgets/g2/measures" in out["widgets"][2]["skipped"]
    assert "queries" in a.call("GET", f"/api/dashboards/{did}/widgets/g2/measures", {}, BEN)[1]["widgets"][0]
    # a budget smaller than one widget still answers the first
    monkeypatch.setattr(api, "MEASURES_BYTES", 1)
    assert a.call("GET", f"/api/dashboards/{did}/measures", {}, BEN)[1]["skipped"] == 3
    # time: each widget takes 2 s on this clock, so the third starts past 5 s and is skipped
    monkeypatch.setattr(api, "MEASURES_BYTES", 8 * 1024 * 1024)
    ticks = itertools.count(0, 2)
    a.clock = lambda: next(ticks)
    out = a.call("GET", f"/api/dashboards/{did}/measures", {}, BEN)[1]
    assert [("skipped" in w) for w in out["widgets"]] == [False, False, False, True]
    # a definition stored past the caps (before them, or by hand) is answered for its first 50 widgets only
    d = store.get_dashboard("local", did)
    assert store.update_dashboard("local", did, {"def": {**d["def"], "widgets": [widget(f"n{i}") for i in range(60)]}})[0] == "ok"
    assert len(a.call("GET", f"/api/dashboards/{did}/measures", {}, BEN)[1]["widgets"]) == api.MAX_WIDGETS


def _many_queues(queues=500, messages=2000, now=1_764_000_000_000) -> dict:
    """``messages`` messages over ``queues`` queues (sent, received, then deleted or failed, in the past hour), and
    the store knowing every queue: the queue table QV-713 timed at 44 s."""
    evs = []
    for i in range(messages):
        at, q = now - 3_500_000 + i * 1_700, f"q{i % queues:03d}"
        evs += [{"ms": at, "event": "send", "status": "ok", "queue": q, "messageId": f"m{i:05d}", "sizeBytes": 100 + i % 7},
                {"ms": at + 1, "event": "receive", "status": "ok", "queue": q, "messageId": f"m{i:05d}",
                 "receiveCount": 1 + i % 3, "waitMs": i % 50},
                {"ms": at + 2, "event": "delete" if i % 9 else "processing_error",
                 "status": "ok" if i % 9 else "error", "queue": q, "messageId": f"m{i:05d}", "durationMs": (i * 37) % 900}]
    evs.sort(key=lambda e: -e["ms"])
    names = sorted({f"q{i:03d}" for i in range(queues)} | {"Q000", "q*", 'say"hi'})
    return {"live": True, "events": evs, "now": now, "queues": names, "capped": False,
            "queueLive": {n: {"status": "not_configured", "detail": api.NOT_QUEUE_LIVE} for n in names}}


def _table(id_="t", **over):
    return {"id": id_, "type": "queue_table", "title": "Queues", "layout": LAYOUT,
            "columns": [m.id for m in M.MEASURES][:api.MAX_COLUMNS], **over}


def _rows_before_qv713(cols, f, t, data) -> dict:
    """The rows as widget_measures worked them out before QV-713: a pass over the events per queue and column."""
    out = {}
    for name in M.queues_for(M.parse(f), data["queues"]):
        vals = {}
        for m in cols:
            if m.queue:
                vals[m.id] = M.queue_value(m, name, data, t)
            else:
                s = M.series_for(m, f'queue:"{name}"', None, t, 2, data)
                vals[m.id] = s[0]["now"] if s else 0
        out[name] = {"name": name, "tone": M.queue_tone(name, data), "values": vals}
    return out


def test_a_19_column_queue_table_over_500_queues_answers_in_well_under_a_second(monkeypatch):
    """QV-713: one queue table of every column over 500 queues took 44 s, and the 5 s budget was looked at only
    between widgets. Now its events are grouped by queue in one pass; the rows are the 200 busiest, in name order."""
    data = _many_queues()
    a = _on(data)
    did = a.call("POST", "/api/dashboards", {"name": "x", "def": {"widgets": [_table()]}}, ADA)[1]["dashboard"]["id"]
    began = time.perf_counter()
    out = a.call("GET", f"/api/dashboards/{did}/measures", {"range": "1h"}, BEN)[1]
    took = time.perf_counter() - began
    assert took < 0.5, f"{took:.2f} s"
    table = out["widgets"][0]
    assert out["skipped"] == 0 and table["queuesTotal"] == 503 and table["queuesCapped"] is True
    names = [r["name"] for r in table["queues"]]
    assert len(names) == api.MAX_QUEUE_ROWS and names == sorted(names)
    # the busiest: q000 to q499 have 4 messages each, Q000 shares q000's (a queue term ignores case), and the two
    # names a queue term reads otherwise (a wildcard, a quote) are ranked last
    assert "q*" not in names and 'say"hi' not in names and "Q000" in names


@pytest.mark.parametrize("f", ["", "queue:q00*", "-queue:q1*"])
def test_the_one_pass_rows_are_the_rows_worked_out_per_queue(f):
    """Every value, tone included, as M.series_for/M.queue_value/M.queue_tone gave it per queue before QV-713, for
    queues with a wildcard and a quote in their names too, and with AWS figures for some queues."""
    data = _many_queues(queues=40, messages=300)
    data["queueLive"]["q001"] = {"status": "ok", "visible": 3, "inFlight": 1, "oldestS": 400, "dlq": True,
                                 "dlqDepth": 2, "visibilityTimeoutS": 1, "retentionS": 1000, "windows": []}
    data["queueLive"]["q*"] = dict(data["queueLive"]["q001"])
    t = M.preset("1h", data["now"])
    cols = [m for m in (M.measure(i) for i in _table()["columns"])]
    rows, total, capped = api.queue_rows(cols, f, t, data)
    want = _rows_before_qv713(cols, f, t, data)
    assert {r["name"]: r for r in rows} == want and total == len(want) and capped is False
    assert [r["name"] for r in rows] == list(want)


def test_a_queue_table_stops_adding_rows_once_the_requests_time_is_up_on_either_route():
    """The time budget is looked at inside a widget's rows too, on the one-widget route as well: a widget past it
    keeps the rows it has and says queuesCapped, and the widgets after it are skipped."""
    data = _many_queues(queues=20, messages=200)
    a = _on(data)
    ws = [_table("t"), widget("n")]
    did = a.call("POST", "/api/dashboards", {"name": "x", "def": {"widgets": ws}}, ADA)[1]["dashboard"]["id"]
    for path in (f"/api/dashboards/{did}/measures", f"/api/dashboards/{did}/widgets/t/measures"):
        ticks = itertools.count(0, 1)  # a second a look: the table's sixth row finds 5 s gone
        a.clock = lambda: next(ticks)
        out = a.call("GET", path, {}, BEN)[1]
        table = out["widgets"][0]
        assert table["queuesCapped"] is True and table["queuesTotal"] == 23 and len(table["queues"]) == 5
        if "/widgets/" not in path:
            assert out["skipped"] == 1 and "skipped" in out["widgets"][1]
    a.clock = time.monotonic
    table = a.call("GET", f"/api/dashboards/{did}/widgets/t/measures", {}, BEN)[1]["widgets"][0]
    assert table["queuesCapped"] is False and len(table["queues"]) == 23


# ── EventData from the store ─────────────────────────────────────────────────────────────────────────────────────

T = 1_764_000_000_000  # ms


def _ingest(db, spans):
    from trailbeam.server import otlp
    d = otlp.decode(to_proto(spans).SerializeToString(), "application/x-protobuf")
    assert not d.rejected, d.rejected
    db.insert(d.records)


def test_event_data_reads_the_stored_spans_as_the_event_explorers_rows():
    p, c = Consumer("api-1", service="api"), Consumer()
    ns = T * 1_000_000
    db = Database(":memory:")
    try:
        _ingest(db, [p.send(ns, "m-1"), c.receive(ns + SEC, "m-1", "rh-1"), c.delete(ns + 3 * SEC, "rh-1",
                                                                                     start=ns + 2 * SEC),
                     p.send(ns, "m-2"), c.receive(ns + SEC, "m-2", "rh-2"),
                     c.receive(ns + 100 * SEC, "m-2", "rh-3", count=2)])
        data = api.event_data(db, T + 200_000, T - 1000)
    finally:
        db.close()
    assert data["live"] is True and data["capped"] is False and data["queues"] == ["orders"]
    assert data["queueLive"] == {"orders": {"status": "not_configured", "detail": api.NOT_QUEUE_LIVE}}
    got = [(e["messageId"], e["event"], e["ms"] - T) for e in data["events"]]
    # newest first; at the same time, the message read later (the less recently active) first, as loadEvents sorts
    assert got == [("m-2", "receive", 100_000), ("m-2", "visibility_expired", 100_000), ("m-1", "delete", 3000),
                   ("m-1", "receive", 1000), ("m-2", "receive", 1000), ("m-1", "send", 0), ("m-2", "send", 0)]
    by = {(e["messageId"], e["event"], e["ms"]): e for e in data["events"]}
    d = by[("m-1", "delete", T + 3000)]
    assert (d["durationMs"], d["consumer"], d["producer"], d["status"], d["call"]) == \
        (1000, "orders-worker", "api", "ok", "DeleteMessage")
    r = by[("m-1", "receive", T + 1000)]
    assert (r["waitMs"], r["receiveCount"], r["attrs"]["message.ApproximateReceiveCount"]) == (1000, 1, 1)
    # and the measures engine reads it as it reads the hosted app's
    t = {"from": T - 1000, "to": T + 200_000}
    assert M.series_for(M.measure("sent"), "", None, t, 2, data)[0]["now"] == 2 / ((t["to"] - t["from"]) / 60_000)
    assert M.series_for(M.measure("messages_visible"), "", None, t, 2, data) == []  # no queue figures here
    assert M.no_depth(data) == api.NOT_QUEUE_LIVE
    # the cap: the most recently active messages first
    db = Database(":memory:")
    try:
        _ingest(db, [p.send(ns, "old"), p.send(ns + SEC, "new")])
        capped = api.event_data(db, T + 5000, T - 1000, max_messages=1)
    finally:
        db.close()
    assert capped["capped"] is True and [e["messageId"] for e in capped["events"]] == ["new"]


def test_a_past_range_reads_its_own_messages_not_the_newest_since():
    # QV-685: more messages after the range's end than the cap must not crowd the range out
    p, c = Consumer("api-1", service="api"), Consumer()
    ns = T * 1_000_000
    db = Database(":memory:")
    try:
        spans = [p.send(ns, "in-range"), c.receive(ns + SEC, "in-range", "rh-in"),
                 p.send(ns + 50 * SEC, "straddles"), c.receive(ns + 70 * SEC, "straddles", "rh-st")]
        # 5 messages after the end, past STRADDLE_MS
        spans += [p.send(ns + 60 * SEC + api.STRADDLE_MS * 1_000_000 + i * SEC, f"later-{i}") for i in range(5)]
        _ingest(db, spans)
        # the range [T - 1s, T + 60s), now well after it; a cap below the 5 messages since
        data = api.event_data(db, T + 3_600_000, T - 1000, max_messages=3, to_ms=T + 60_000)
        # the cap reached by the range's own messages: the straddler is not read, and capped says so
        full = api.event_data(db, T + 3_600_000, T - 1000, max_messages=1, to_ms=T + 60_000)
        # and the same read up to now keeps the newest, as before
        now = api.event_data(db, T + 3_600_000, T - 1000, max_messages=3)
        # busy just after the end: the newest there fill what is left, so the straddler is not read; capped says so
        _ingest(db, [p.send(ns + (71 + i) * SEC, f"soon-{i}") for i in range(3)])
        busy = api.event_data(db, T + 3_600_000, T - 1000, max_messages=3, to_ms=T + 60_000)
    finally:
        db.close()
    got = [(e["messageId"], e["event"], e["ms"] - T) for e in data["events"]]
    # the straddling message's send is in the range; its receive at 70s is not
    assert got == [("straddles", "send", 50_000), ("in-range", "receive", 1000), ("in-range", "send", 0)]
    assert data["capped"] is False
    assert full["capped"] is True and {e["messageId"] for e in full["events"]} == {"in-range"}
    assert now["capped"] is True and {e["messageId"] for e in now["events"]} == {"later-4", "later-3", "later-2"}
    assert busy["capped"] is True
    assert {(e["messageId"], e["event"]) for e in busy["events"]} == {("in-range", "send"), ("in-range", "receive")}


def test_measures_reads_a_past_range_up_to_its_end():
    reads, data = [], D["own"]
    now = data["now"]

    def read(since, now_ms, to):
        reads.append((since, to))
        return {**copy.deepcopy(data), "capped": to is not None}

    a = api.DashboardsApi(MemoryDashboards(), "local", read, now=lambda: now)
    body = {"name": "Past", "def": {"widgets": [widget("a"), widget("b", time="1h")]}}
    did = a.call("POST", "/api/dashboards", body, ADA)[1]["dashboard"]["id"]
    frm, to = now - 7 * M.DAY, now - 6 * M.DAY
    out = a.call("GET", f"/api/dashboards/{did}/measures", {"from": str(frm), "to": str(to)}, BEN)[1]
    # the past range up to its end; the widget with its own time, which ends now, up to now
    assert sorted(reads, key=repr) == sorted([(frm, to), (now - M.HOUR, None)], key=repr)
    assert out["capped"] is True and out["widgets"][1]["time"]["from"] == now - M.HOUR
    reads.clear()
    out = a.call("GET", f"/api/dashboards/{did}/measures", {"range": "1d"}, BEN)[1]
    assert reads == [(now - M.DAY, None)] and out["capped"] is False  # ranges that end now: one read


def test_a_lambda_message_and_one_moved_to_the_dead_letter_queue():
    p, fn = Consumer("api-1", service="api"), Consumer("env-1", source="lambda", service="orders-fn")
    dlq = Consumer("dlq-worker", queue="orders-dlq", service="dlq-worker")
    ns = T * 1_000_000
    db = Database(":memory:")
    try:
        _ingest(db, [p.send(ns, "lam"), fn.receive(ns + SEC, "lam", "rh-l", lambda_inv="i-1"),
                     fn.outcome(ns + 2 * SEC, "lam", "failed_exception", "i-1", error_type="ValueError"),
                     p.send(ns, "d"), dlq.receive(ns + 50 * SEC, "d", "rh-d", count=1, dlq_source="orders")])
        data = api.event_data(db, T + 100_000, T - 1000)
    finally:
        db.close()
    kinds = {(e["messageId"], e["event"]) for e in data["events"]}
    assert {("lam", "processing_error"), ("d", "dead_letter")} <= kinds
    err = next(e for e in data["events"] if e["event"] == "processing_error")
    assert (err["detail"], err["durationMs"], err["status"]) == ("The handler threw: ValueError", 1000, "error")
    moved = next(e for e in data["events"] if e["event"] == "dead_letter")
    assert moved["queue"] == "orders" and moved["detail"].startswith("Moved from orders to orders-dlq.")


# ── over HTTP ────────────────────────────────────────────────────────────────────────────────────────────────────

def _people(db):
    from trailbeam.server.accounts import Accounts
    acc = Accounts(db)
    now = time.time_ns() // 1_000_000
    ada = acc.create_first_admin("ada@example.com", "Ada", QUICK.hash("pw pw pw pw"), now)
    ben = acc.create_user("ben@example.com", "Ben", "member", QUICK.hash("pw pw pw pw"), now)
    t = Tokens(acc)
    return ({"Authorization": "Bearer " + t.create_personal(ada, "ada")[1]},
            {"Authorization": "Bearer " + t.create_personal(ben, "ben")[1]})


def _body(o) -> bytes:
    return json.dumps(o).encode()


def test_the_routes_are_not_there_while_dashboards_are_off(monkeypatch):
    monkeypatch.delenv(api.ENV, raising=False)
    db = Database(":memory:")
    try:
        app = server.App(db, ui_dir=None)
        assert app.dashboards is None
        with serve(app) as base:
            assert request(base, "GET", "/api/dashboards")[0] == 404
            assert request(base, "POST", "/api/dashboards", _body({"name": "x"}), J)[0] == 404
    finally:
        db.close()
    assert api.enabled({api.ENV: "on"}) and api.enabled({api.ENV: " TRUE "}) and not api.enabled({api.ENV: "0"})


def test_on_the_servers_tables_members_make_and_admins_unlock(monkeypatch, tmp_path):
    monkeypatch.setenv(api.ENV, "1")
    db = Database(str(tmp_path / "t.sqlite"))
    try:
        admin, member = _people(db)
        app = server.App(db, ui_dir=None)
        app.access.state.ttl_s = 0.0
        assert isinstance(app.dashboards.store, DashboardTables)
        with serve(app) as base:
            assert request(base, "GET", "/api/dashboards")[0] == 401
            st, _, out = request(base, "POST", "/api/dashboards", _body({"name": "Orders", "def": {"widgets": [widget()]}}),
                                 {**J, **member})
            d = json.loads(out)["dashboard"]
            assert st == 201 and d["createdBy"] == "Ben" and d["createdById"] is not None
            assert request(base, "POST", f"/api/dashboards/{d['id']}/lock", _body({"lock": True}), {**J, **member})[0] == 200
            st, _, out = request(base, "GET", "/api/dashboards", headers=admin)
            assert st == 200 and json.loads(out)["canUnlock"] is True
            assert json.loads(out)["dashboards"][0]["lockedBy"] == "Ben"
            assert request(base, "POST", f"/api/dashboards/{d['id']}/lock", _body({"lock": False}), {**J, **admin})[0] == 200
            st, _, out = request(base, "GET", f"/api/dashboards/{d['id']}/measures?range=1h", headers=member)
            assert st == 200 and json.loads(out)["widgets"][0]["noDepth"] == api.NOT_QUEUE_LIVE
        # stored in the file: a new server reads it back
        assert DashboardTables(db).get_dashboard("local", d["id"])["name"] == "Orders"
    finally:
        db.close()


def test_a_lone_surrogate_anywhere_is_a_400_not_a_500(monkeypatch, tmp_path):
    """QV-687: half of a UTF-16 pair ("\\ud800", as a truncated emoji leaves) in a name, a key, a nested widget title
    or the imported text is refused; SQLite can't store it, and the UTF-8 encode in check_def raised before."""
    monkeypatch.setenv(api.ENV, "1")
    db = Database(str(tmp_path / "t.sqlite"))
    try:
        _, member = _people(db)
        app = server.App(db, ui_dir=None)
        app.access.state.ttl_s = 0.0
        h = {**J, **member}
        with serve(app) as base:
            st, _, out = request(base, "POST", "/api/dashboards", _body({"name": "Orders"}), h)
            did = json.loads(out)["dashboard"]["id"]
            for method, path, body in [
                ("POST", "/api/dashboards", {"name": "a\ud800b"}),
                ("POST", "/api/dashboards", {"name": "s", "def": {"widgets": [widget(title="\udc00")]}}),
                ("POST", "/api/dashboards", {"name": "s", "def": {"widgets": [], "\ud83d": 1}}),
                ("POST", "/api/dashboards", {"name": "s", "imported": json.dumps({"widgets": [widget(title="\ud83d")]})}),
                ("PATCH", f"/api/dashboards/{did}", {"name": "\ud83d"}),
                ("POST", f"/api/dashboards/{did}/widgets", {"widget": widget("w9", title="x\udfff")}),
            ]:
                st, _, out = request(base, method, path, _body(body), h)
                assert (st, json.loads(out)) == (400, {"error": HIDDEN_CHARACTERS}), (method, path, body)
            # a whole pair is one character, and kept
            st, _, out = request(base, "POST", "/api/dashboards", _body({"name": "Orders \U0001f600"}), h)
            assert st == 201 and json.loads(out)["dashboard"]["name"] == "Orders \U0001f600"
        assert [d["name"] for d in DashboardTables(db).list_dashboards("local")] == ["Orders \U0001f600", "Orders"]
    finally:
        db.close()


def _dashboard_ops():
    return [op for op in contract().ops if "Dashboards" in op.op.get("tags", [])]


def test_every_dashboards_route_and_status_the_file_lists_is_answered(monkeypatch, record_api_responses):
    """What test_openapi.py checks for the other routes, with dashboards on: every Dashboards operation is served,
    its x-example-request answers 200, and every status the file lists for it is given (and matches the file,
    as the recorder checks every answer)."""
    ops = _dashboard_ops()
    assert len(ops) == 12
    db = Database(":memory:")
    try:
        monkeypatch.delenv(api.ENV, raising=False)
        with serve(server.App(db, ui_dir=None)) as base:  # off: 404
            for op in ops:
                request(base, op.method, re.sub(r"\{[^}]+\}", "x", op.template), b"{}" if op.method != "GET" else None,
                        J if op.method != "GET" else {})
        monkeypatch.setenv(api.ENV, "1")
        _, member = _people(db)
        app = server.App(db, ui_dir=None)
        app.access.state.ttl_s = 0.0
        tables = app.dashboards.store

        def fresh(**over):
            """The example dashboard a1b2c3d4 with widget w1, as each operation's x-example-request names it."""
            tables.remove_dashboard("local", "a1b2c3d4")
            tables.add_dashboard("local", {"id": "a1b2c3d4", "name": "Orders", "def": {"widgets": [widget()]},
                                           "time": None, "starred": False, "lockedBy": None, "lockedById": None,
                                           "createdBy": "Ada", "createdById": "1", "updatedBy": "Ada",
                                           "createdAt": 1, "updatedAt": 1, **over})

        def path(op, did="a1b2c3d4", wid="w1"):
            return op.template.replace("{dashboard_id}", did).replace("{widget_id}", wid)

        with serve(app) as base:
            def r(method, p, data=None, headers=None):
                return request(base, method, p, data, headers)[0]

            for op in ops:
                fresh()
                body = _body({"name": "x", "lock": True, "widget": widget()}) if op.method != "GET" else None
                example = op.op.get("x-example-request", op.template)
                assert r(op.method, example, body, {**J, **member}) in (200, 201), op.key
                r(op.method, path(op), body, J)  # 401
                r(op.method, path(op, "nope"), body, {**J, **member})  # 404 (removing a missing one: 200)
                if op.method != "GET":
                    fresh()
                    r(op.method, path(op), body, {**J, **member, "Sec-Fetch-Site": "cross-site"})  # 403: csrf
                    r(op.method, path(op), None, {**member, "Content-Length": "99999"})  # 413
                    r(op.method, path(op), b"{}", {**member, "Content-Type": "text/plain"})  # 415
            # 400s
            assert r("POST", "/api/dashboards", _body({"name": ""}), {**J, **member}) == 400
            fresh()
            assert r("PATCH", "/api/dashboards/a1b2c3d4", _body({"name": ""}), {**J, **member}) == 400
            assert r("POST", "/api/dashboards/a1b2c3d4/lock", _body({"lock": 1}), {**J, **member}) == 400
            assert r("POST", "/api/dashboards/a1b2c3d4/widgets", _body({"widget": {}}), {**J, **member}) == 400
            assert r("PATCH", "/api/dashboards/a1b2c3d4/widgets/w1", _body({"widget": {}}), {**J, **member}) == 400
            assert r("GET", "/api/dashboards/a1b2c3d4/measures?n=0", None, member) == 400
            assert r("GET", "/api/dashboards/a1b2c3d4/widgets/w1/measures?range=2h", None, member) == 400
            fresh(lockedBy="Ada", lockedById="1")
            assert r("DELETE", "/api/dashboards/a1b2c3d4/widgets/w1", None, member) == 400
            assert r("DELETE", "/api/dashboards/a1b2c3d4", None, member) == 400
            # 403: someone else's lock
            assert r("POST", "/api/dashboards/a1b2c3d4/lock", _body({"lock": False}), {**J, **member}) == 403
            # 409: changed since the version sent, or a race on every try
            fresh()
            assert r("PATCH", "/api/dashboards/a1b2c3d4", _body({"name": "y", "version": 0}), {**J, **member}) == 409
            with pytest.MonkeyPatch.context() as mp:
                mp.setattr(tables, "update_dashboard", lambda *a, **k: ("conflict", None))
                for m, p in (("POST", "/api/dashboards/a1b2c3d4/widgets"), ("PATCH", "/api/dashboards/a1b2c3d4/widgets/w1")):
                    assert r(m, p, _body({"widget": widget()}), {**J, **member}) == 409
                assert r("DELETE", "/api/dashboards/a1b2c3d4/widgets/w1", None, member) == 409
                assert r("POST", "/api/dashboards/a1b2c3d4/lock", _body({"lock": False}), {**J, **member}) == 409

            # 500: the store fails; 503: the access check can't reach it
            def boom(*a, **k):
                raise RuntimeError("a test's broken store")

            def unreachable(*a, **k):
                raise access_mod.AccessUnavailable("a test's unreachable store")

            for name, ex in (("store", boom),):
                with pytest.MonkeyPatch.context() as mp:
                    for meth in ("list_dashboards", "get_dashboard", "add_dashboard", "update_dashboard",
                                 "lock_dashboard", "remove_dashboard"):
                        mp.setattr(tables, meth, ex)
                    for op in ops:
                        r(op.method, path(op), _body({"name": "x", "lock": True, "widget": widget()})
                          if op.method != "GET" else None, {**J, **member})
            with pytest.MonkeyPatch.context() as mp:
                mp.setattr(app.access, "check", unreachable)
                for op in ops:
                    if op.method != "GET":
                        r(op.method, path(op), b"{}", J)

        # 403 on a read: signed in through a proxy as someone who may not be (as test_openapi.py does)
        proxied = server.App(db, ui_dir=None, trusted_proxies=TrustedProxies(parse_networks("127.0.0.1")),
                             proxy_auth=proxy_auth.configure("X-Forwarded-Email",
                                                             proxies=TrustedProxies(parse_networks("127.0.0.1"))))
        proxied.accounts.create_user("root@example.com", "Root", "admin", None, 0)
        proxied.access.state.changed()
        with serve(proxied) as base:
            for op in ops:
                request(base, op.method, path(op), b"{}" if op.method != "GET" else None,
                        {"X-Forwarded-Email": "root@example.com", **(J if op.method != "GET" else {})})
    finally:
        db.close()
    assert not record_api_responses.problems, "\n".join(record_api_responses.problems)
    answered = {(s.op.key, s.status) for s in record_api_responses.seen if s.op is not None}
    never = [f"{op.key} {status}" for op in ops for status in op.op["responses"] if (op.key, int(status)) not in answered]
    assert not never, "the file lists these statuses, the server never gave them:\n" + "\n".join(never)
