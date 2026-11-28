"""The alerts JSON API (alerts_api.py, QV-642): the shared cases in contract/fixtures/alerts-api.json, the handlers
over the in-memory store (rules, mutes, channels and their secrets, a test send through delivery, the state), that
the scheduled check reads what the API wrote, and the routes in the server: off by default, who may change what, and
the request checks they share with the people API."""
import http.server
import importlib.util
import json
import threading
import time
from pathlib import Path

import pytest
from test_openapi import request
from trailbeam.server import alert_delivery
from trailbeam.server import alerts_api as api
from trailbeam.server import alerts_run as run
from trailbeam.server.db import Database
from trailbeam.server.passwords import Hasher
from trailbeam.server.server import make_server
from trailbeam.server.tokens import Tokens

FIXTURES = Path(__file__).resolve().parents[3] / "contract" / "fixtures"
F = json.loads((FIXTURES / "alerts-api.json").read_text())
_spec = importlib.util.spec_from_file_location("make_alerts_api", FIXTURES / "make-alerts-api.py")
MAKE = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(MAKE)

T = 1_764_000_000_000
KEY = "v1|orders|worker|exc:TimeoutError"
CAUSES = [{"id": "c1", "key": KEY, "label": "TimeoutError: read timed out", "queue": "orders"},
          {"id": "c2", "key": "v1|billing|fn|exc:KeyError", "label": "KeyError: 'id'", "queue": "billing"},
          {"id": "c3", "key": "v0|old", "label": "old", "queue": "q"}]
HOOK = "https://hooks.example.com/alerts?token=abc"
SLACK = "https://hooks.slack.com/services/T000/B000/XXXXSECRET"


# ── the shared cases ─────────────────────────────────────────────────────────────────────────────────────────────

def test_the_fixture_is_what_its_script_writes(tmp_path, monkeypatch):
    monkeypatch.setattr(MAKE, "OUT", tmp_path / "alerts-api.json")
    MAKE.main()
    assert json.loads(MAKE.OUT.read_text()) == F, "run contract/fixtures/make-alerts-api.py"


@pytest.mark.parametrize("c", F["rules"], ids=lambda c: c["name"])
def test_rule_case(c):
    assert api.check_rule_form(c["form"], c["channelIds"], c["everyMin"]) == c["expected"]
    assert c["expected"]["ok"] is not c["name"].startswith("refused")


@pytest.mark.parametrize("c", F["mutes"], ids=lambda c: c["name"])
def test_mute_case(c):
    assert api.check_mute(c["forS"], F["at"]) == c["expected"]
    assert c["expected"]["ok"] is not c["name"].startswith("refused")


@pytest.mark.parametrize("c", F["channels"], ids=lambda c: c["name"])
def test_channel_case(c):
    assert api.check_channel_form(c["form"], c["allowLocal"]) == c["expected"]
    assert c["expected"]["ok"] is not c["name"].startswith("refused")


def test_the_cases_cover_every_refusal_text():
    errors = {c["expected"].get("error", "")[:20] for k in ("rules", "mutes", "channels") for c in F[k]}
    for text in ("Pick at least one cha", "Add an alert channel", "Pick when to send.", "Pick a quiet period ",
                 "Snooze for 1, 4 or 24", "Pick webhook, Slack o", "The URL must start wi", "Enter the full URL, s",
                 "Put credentials in th", "The webhook must be o", "Use a Slack incoming ", "Enter at least one em",
                 "At most 10 addresses ", '"nobody" is not an em'):
        assert text[:20] in errors, text


def test_texts_the_hosted_app_says():
    assert api.check_every(1440) == "once a day, at 13:00 UTC"
    assert api.check_every(5) == "every 5 minutes"
    assert api.check_every(60) == "every hour"
    assert api.check_every(120) == "every 2 hours"
    assert api.cadence_note(5) == "Trailbeam checks every 5 minutes, so an alert can arrive up to 5 minutes after it happens."
    assert api.quiet_choices(5) == [1800, 3600, 21600, 86400, 604800]
    assert api.default_quiet(5) == 21600 and api.default_quiet(1440) == 86400


# ── the handlers ─────────────────────────────────────────────────────────────────────────────────────────────────

class Who:
    def __init__(self, via="session", role="member", user_id=7):
        self.via, self.role, self.user_id = via, role, user_id


BEN = Who()


class Api:
    """One AlertsApi over a MemoryAlertConfig, with a clock."""

    def __init__(self, every_min=5, manager=True, **delivery):
        self.at = T
        self.store = api.MemoryAlertConfig()
        self.causes = list(CAUSES)
        self.api = api.AlertsApi(self.store, {"id": "local", "name": "Trailbeam", "slug": "local"},
                                 lambda: self.causes,
                                 alert_delivery.Delivery(base="http://tb.local", now=lambda: self.at, **delivery),
                                 every_min=every_min, is_manager=lambda who: manager, now=lambda: self.at)

    def __call__(self, method, path, body=None, who=BEN):
        return self.api.call(method, path, body, who)

    def fails(self, method, path, body=None, who=BEN):
        with pytest.raises(api.ApiProblem) as e:
            self(method, path, body, who)
        return e.value.status, e.value.message

    def channel(self, kind="webhook", target=HOOK, **o):
        return self("POST", "/api/alerts/channels", {"kind": kind, "name": o.get("name", kind), "target": target,
                                                     "minTone": o.get("minTone", "warning")})[1]

    def rule(self, cause="c1", channels=None, **o):
        if channels is None:
            channels = [c["id"] for c in self.store.list_channels("local")]
        return self("POST", "/api/alerts/rules", {"causeId": cause, "channels": channels, "mode": o.get("mode", "quiet"),
                                                  "quietS": o.get("quietS", 3600)})


def test_a_lone_surrogate_in_a_body_is_a_400():
    """QV-687: neither database can store half of a UTF-16 pair, in a value or a key."""
    a = Api()
    assert a.fails("POST", "/api/alerts/channels", {"kind": "webhook", "name": "Ops \ud83d", "target": HOOK}) \
        == (400, api.HIDDEN_CHARACTERS)
    assert a.fails("POST", "/api/alerts/rules", {"causeId": "c1", "channels": [], "\udc00": 1}) == (400, api.HIDDEN_CHARACTERS)
    assert a.store.list_channels("local") == [] and a("GET", "/api/alerts/rules")[1]["rules"] == []


def test_a_webhook_channel_shows_its_secret_once_and_never_its_target():
    a = Api()
    status, out = a("POST", "/api/alerts/channels", {"kind": "webhook", "name": " Ops ", "target": HOOK,
                                                     "minTone": "critical"})
    assert status == 201
    assert out["secret"].startswith("whsec_") and len(out["secret"]) == 6 + 48
    ch = out["channel"]
    assert ch == {"id": ch["id"], "kind": "webhook", "name": "Ops", "shown": "https://hooks.example.com/…",
                  "minTone": "critical", "signed": True, "createdBy": "7", "createdAt": T, "lastAt": None,
                  "lastOk": None, "lastDetail": None}
    _, listed = a("GET", "/api/alerts/channels")
    assert listed["channels"] == [ch] and listed["max"] == 20 and listed["canManage"] is True
    dumped = json.dumps(listed)
    assert "token=abc" not in dumped and out["secret"] not in dumped
    # the store has both, for delivery
    stored = a.store.get_channel("local", ch["id"])
    assert stored["target"] == HOOK and stored["secret"] == out["secret"]


def test_a_slack_channel_shows_its_team_only_and_has_no_secret():
    a = Api()
    out = a.channel("slack", SLACK)
    assert out["secret"] is None and out["channel"]["signed"] is False
    assert out["channel"]["shown"] == "https://hooks.slack.com/services/T000/…"
    assert "XXXXSECRET" not in json.dumps(a("GET", "/api/alerts/channels")[1])


def test_a_refused_channel_says_why_as_the_hosted_app_does():
    a = Api()
    assert a.fails("POST", "/api/alerts/channels", {"kind": "pager", "target": "x"}) == (
        400, "Pick webhook, Slack or email.")
    assert a.fails("POST", "/api/alerts/channels", {"kind": "webhook", "target": "http://example.com"}) == (
        400, "The URL must start with https://.")
    assert a.fails("POST", "/api/alerts/channels", {"kind": "email"}) == (400, "Enter at least one email address.")
    for i in range(20):
        a.channel("email", f"p{i}@acme.io")
    assert a.fails("POST", "/api/alerts/channels", {"kind": "email", "target": "x@acme.io"}) == (
        400, "A workspace can have up to 20 alert channels.")


def test_changing_a_channel_checks_its_target_again_and_keeps_its_secret():
    a = Api()
    made = a.channel()
    cid = made["channel"]["id"]
    _, out = a("PATCH", f"/api/alerts/channels/{cid}", {"name": "", "minTone": "critical",
                                                       "target": "https://other.example.com/"})
    assert out["channel"]["name"] == "Webhook" and out["channel"]["minTone"] == "critical"
    assert out["channel"]["shown"] == "https://other.example.com"
    assert "secret" not in out and a.store.get_channel("local", cid)["secret"] == made["secret"]
    assert a.fails("PATCH", f"/api/alerts/channels/{cid}", {"target": "https://10.0.0.1/"}) == (
        400, "The webhook must be on a public host, not a private or local address.")
    assert a.fails("PATCH", f"/api/alerts/channels/{cid}", {"kind": "slack"})[0] == 400
    assert a.store.get_channel("local", cid)["target"] == "https://other.example.com/"
    assert a.fails("PATCH", "/api/alerts/channels/nope", {"name": "x"}) == (404, "That channel was removed.")
    assert a("DELETE", f"/api/alerts/channels/{cid}") == (200, {"removed": True})
    assert a("DELETE", f"/api/alerts/channels/{cid}") == (200, {"removed": False})
    assert a("GET", "/api/alerts/channels")[1]["channels"] == []


def test_only_a_manager_changes_channels_but_anyone_reads_them_and_makes_rules():
    a = Api(manager=False)
    a.store.add_channel("local", {"id": "ch1", "kind": "email", "name": "On call", "target": "a@acme.io",
                                  "secret": None, "minTone": "warning", "createdBy": "1", "createdAt": T})
    for method, path, body in (("POST", "/api/alerts/channels", {"kind": "email", "target": "b@acme.io"}),
                               ("PATCH", "/api/alerts/channels/ch1", {"name": "x"}),
                               ("DELETE", "/api/alerts/channels/ch1", None),
                               ("POST", "/api/alerts/channels/ch1/test", {})):
        assert a.fails(method, path, body) == (403, "this needs an admin"), (method, path)
    assert a("GET", "/api/alerts/channels")[1]["canManage"] is False
    assert a.rule()[0] == 201


def test_a_rule_is_made_on_a_cause_once():
    a = Api()
    a.channel()
    status, out = a.rule(quietS=1800)
    assert status == 201 and out["existed"] is False
    r = out["rule"]
    assert r == {"id": r["id"], "kind": "cause", "causeKey": KEY, "causeId": "c1",
                 "label": "TimeoutError: read timed out", "queue": "orders", "channels": r["channels"],
                 "mode": "quiet", "quietS": 1800, "mutedUntil": None, "mutedBy": None, "mutedAt": None,
                 "createdBy": "7", "createdAt": T, "updatedAt": T}
    again = a.rule(mode="every")
    assert again == (200, {"rule": r, "existed": True})
    # the check reads it, with an empty memory
    (x,) = a.store.rules_with_memory("local")
    assert x["rule"] == r and x["memory"]["seen"] is None and x["memory"]["wasMuted"] is False
    assert a.store.workspaces_with_rules() == {"local"} and a.store.workspaces_with_channels() == {"local"}


def test_a_refused_rule_says_why_as_the_hosted_app_does():
    a = Api(every_min=1440)
    assert a.fails("POST", "/api/alerts/rules", {"causeId": "c1", "channels": [], "mode": "every"}) == (
        400, "Add an alert channel in Settings first.")
    a.channel()
    assert a.fails("POST", "/api/alerts/rules", {"causeId": "gone", "channels": [], "mode": "every"}) == (
        404, "That cause has no unfinished messages now. Reload the page.")
    assert a.fails("POST", "/api/alerts/rules", {"causeId": "c3", "channels": [], "mode": "every"}) == (
        404, "That cause can't be found any more. Reload the page.")
    assert a.fails("POST", "/api/alerts/rules", {"causeId": "c1", "channels": [], "mode": "every"}) == (
        400, "Pick at least one channel.")
    assert a.fails("POST", "/api/alerts/rules", {"causeId": "c1", "channels": [], "mode": "every"})[0] == 400
    status, msg = a.fails("POST", "/api/alerts/rules", {"causeId": "c1",
                                                        "channels": [a.store.list_channels("local")[0]["id"]],
                                                        "mode": "quiet", "quietS": 3600})
    assert (status, msg) == (400, ("Pick a quiet period of 24 h, 7 days. Trailbeam checks once a day, at 13:00 UTC, "
                                  "so an alert can arrive up to a day after it happens."))
    assert a.store.list_rules("local") == []


def test_a_workspace_has_at_most_100_rules():
    a = Api()
    a.channel()
    a.causes = [{"id": f"c{i}", "key": f"v1|q|w|exc:E{i}", "label": "x", "queue": "q"} for i in range(101)]
    for i in range(100):
        assert a.rule(f"c{i}", mode="every")[0] == 201
    assert a.fails("POST", "/api/alerts/rules", {"causeId": "c100", "channels": [a.store.list_channels("local")[0]["id"]],
                                                 "mode": "every"}) == (400, "A workspace can have up to 100 alert rules.")


def test_editing_muting_and_deleting_a_rule():
    a = Api()
    a.channel()
    b = a.channel("slack", SLACK)["channel"]["id"]
    rid = a.rule()[1]["rule"]["id"]
    a.at = T + 1000
    _, out = a("PATCH", f"/api/alerts/rules/{rid}", {"channels": [b, "nope"], "mode": "every", "quietS": 3600})
    assert out["rule"]["channels"] == [b] and out["rule"]["mode"] == "every" and out["rule"]["quietS"] == 0
    assert out["rule"]["updatedAt"] == T + 1000
    assert a.fails("PATCH", f"/api/alerts/rules/{rid}", {"channels": [b], "mode": "x"}) == (400, "Pick when to send.")
    assert a.fails("PATCH", "/api/alerts/rules/nope", {"channels": [b], "mode": "every"}) == (
        404, "That alert was deleted.")

    _, out = a("POST", f"/api/alerts/rules/{rid}/mute", {"forS": 3600})
    assert out["rule"]["mutedUntil"] == T + 1000 + 3_600_000 and out["rule"]["mutedBy"] == "7"
    assert out["rule"]["mutedAt"] == T + 1000 and out["rule"]["muted"] is True
    _, out = a("POST", f"/api/alerts/rules/{rid}/mute", {"forS": None})
    assert out["rule"]["mutedUntil"] == 8_640_000_000_000_000
    _, out = a("POST", f"/api/alerts/rules/{rid}/mute", {"forS": 0})
    assert out["rule"]["mutedUntil"] is None and out["rule"]["mutedBy"] is None and out["rule"]["muted"] is False
    assert a.fails("POST", f"/api/alerts/rules/{rid}/mute", {"forS": 60}) == (400, "Snooze for 1, 4 or 24 hours.")
    assert a.fails("POST", f"/api/alerts/rules/{rid}/mute", {}) == (400, "Snooze for 1, 4 or 24 hours.")
    assert a.fails("POST", "/api/alerts/rules/nope/mute", {"forS": 0}) == (404, "That alert was deleted.")

    a.store.write_state("local", {**_state(rid), "tone": "critical"}, None)
    _, listed = a("GET", "/api/alerts/rules")
    assert listed["rules"][0]["state"]["tone"] == "critical"
    assert listed["checkEvery"] == "every 5 minutes" and listed["quietChoices"][0] == 1800
    assert listed["snoozes"] == [3600, 14400, 86400] and listed["max"] == 100
    assert a("DELETE", f"/api/alerts/rules/{rid}") == (200, {"removed": True})
    assert a.store.alert_states("local") == [] and a("GET", "/api/alerts/rules")[1]["rules"] == []
    assert a("DELETE", f"/api/alerts/rules/{rid}") == (200, {"removed": False})


def _state(rid, **o):
    return {"dashboardId": "rule", "widgetId": rid, "dashboardName": "", "title": "TimeoutError: read timed out",
            "tone": "good", "value": "0", "limit": "new messages after 1 h quiet", "queue": "orders", "since": T,
            "checkedAt": T, **o}


def test_the_state_lists_every_rule_and_the_last_check():
    a = Api()
    _, out = a("GET", "/api/alerts/state")
    assert out == {"alerts": [], "checkedAt": None, "hasChannel": False, "checkEvery": "every 5 minutes"}
    a.channel()
    dash = {"dashboardId": "d1", "widgetId": "w1", "dashboardName": "Orders", "title": "Depth", "tone": "warning",
            "value": "12", "limit": "above 10", "queue": "orders", "since": T, "checkedAt": T}
    a.store.write_state("local", dash, None)
    a.store.write_state("local", _state("r1"), None)
    a.store.checked("local", T + 5, None)
    _, out = a("GET", "/api/alerts/state")
    assert out["alerts"] == [dash, _state("r1")] and out["checkedAt"] == T + 5 and out["hasChannel"] is True


def test_the_check_runs_the_rules_the_api_made_and_sends_to_their_channels():
    a = Api()
    a.channel()
    rid = a.rule(mode="every")[1]["rule"]["id"]
    sent = []
    read = {"groups": [{"id": "c1", "key": KEY, "label": "TimeoutError: read timed out", "queue": "orders",
                        "count": 1, "basis": "exception", "inferred": False, "messageIds": ["m1"]}],
            "causeOf": {"m1": "c1"}, "capped": False}
    deps = {"store": a.store, "dashboards": lambda only: {}, "data": lambda ws, b: {}, "causes": lambda ws: read,
                "channels": a.store.workspaces_with_channels, "deliver": lambda ws, ch: sent.extend(ch), "owner": "t"}
    r = run.run_alerts(run.Deps(**deps, now=lambda: T + 60_000))
    assert r["workspaces"] == 1 and r["failed"] == []
    assert [s["widgetId"] for s in a("GET", "/api/alerts/state")[1]["alerts"]] == [rid]
    # muted through the API: the next check's change is not sent
    a("POST", f"/api/alerts/rules/{rid}/mute", {"forS": None})
    read["groups"][0]["messageIds"] = ["m1", "m2"]
    read["causeOf"]["m2"] = "c1"
    sent.clear()
    run.run_alerts(run.Deps(**deps, now=lambda: T + 10 * 60_000))
    assert sent == []


class Hook:
    """A local webhook endpoint that records what it is sent."""

    def __init__(self, status=200):
        got = self.got = []

        class H(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                got.append((dict(self.headers), self.rfile.read(int(self.headers["Content-Length"]))))
                self.send_response(status)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, *a):
                pass

        self.httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}/hook?token=s3cret"

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()


def test_send_test_goes_through_delivery_and_is_noted_on_the_channel():
    hook = Hook()
    try:
        a = Api(allow_local=True)
        made = a.channel(target=hook.url)
        cid = made["channel"]["id"]
        assert a.api.call("POST", f"/api/alerts/channels/{cid}/test", {}, BEN, "https://tb.example.com") == (
            200, {"ok": True})
        (headers, body), = hook.got
        assert headers["x-trailbeam-signature"].startswith("sha256=")
        sent = json.loads(body)
        assert sent["changes"][0]["title"] == "Test alert", sent["changes"][0]
        assert "https://tb.example.com" in body.decode()
        ch = a("GET", "/api/alerts/channels")[1]["channels"][0]
        assert (ch["lastAt"], ch["lastOk"], ch["lastDetail"]) == (T, True, "Test sent.")
    finally:
        hook.close()


def test_a_failed_test_says_why_without_the_secret():
    hook = Hook(status=500)
    try:
        a = Api(allow_local=True)
        cid = a.channel(target=hook.url)["channel"]["id"]
        status, out = a("POST", f"/api/alerts/channels/{cid}/test", {})
        assert status == 200 and out["ok"] is False and out["error"]
        assert "s3cret" not in out["error"] and "token" not in out["error"]
        ch = a("GET", "/api/alerts/channels")[1]["channels"][0]
        assert ch["lastOk"] is False and ch["lastDetail"] == out["error"]
        assert a.fails("POST", "/api/alerts/channels/nope/test", {}) == (404, "That channel was removed.")
    finally:
        hook.close()


def test_routes_are_matched_whole():
    assert api.route("GET", "/api/alerts/rules") == ("list_rules", None, "member")
    assert api.route("POST", "/api/alerts/rules/r-1_x/mute") == ("mute_rule", "r-1_x", "member")
    assert api.route("POST", "/api/alerts/channels/c1/test") == ("test_channel", "c1", "manager")
    for method, path in (("GET", "/api/alerts/rules/"), ("GET", "/api/alerts"), ("PUT", "/api/alerts/rules"),
                         ("DELETE", "/api/alerts/rules/a/b"), ("PATCH", "/api/alerts/rules/..%2f"),
                         ("GET", "/api/alerts/channels/c1")):
        assert api.route(method, path) is None, (method, path)
    with pytest.raises(api.ApiProblem) as e:
        Api()("GET", "/api/alerts/nope")
    assert e.value.status == 404


# ── in the server ────────────────────────────────────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _not_in_openapi_yet(record_api_responses, monkeypatch):
    """/api/alerts goes into contract/openapi.yaml with the part of QV-592 that turns it on (it is off by default,
    and kept in memory, until the alert tables exist): its answers are not checked against the file yet."""
    orig = record_api_responses.record

    def record(method, raw_url, *a, **k):
        if not raw_url.startswith("/api/alerts"):
            orig(method, raw_url, *a, **k)
    monkeypatch.setattr(record_api_responses, "record", record)


QUICK = Hasher(log2_n=10, r=8, p=1)
J = {"Content-Type": "application/json"}


class Server:
    def __init__(self, monkeypatch, check="5", accounts=True):
        if check is None:
            monkeypatch.delenv("TRAILBEAM_ALERT_CHECK", raising=False)
        else:
            monkeypatch.setenv("TRAILBEAM_ALERT_CHECK", check)
        self.db = Database(":memory:")
        self.httpd = make_server(self.db, "127.0.0.1", 0, ui_dir=None)
        self.app = self.httpd.app
        self.app.access.state.ttl_s = 0.0
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}"
        self.admin = self.member = {}
        if accounts:
            acc = self.app.accounts
            now = time.time_ns() // 1_000_000
            ada = acc.create_first_admin("ada@example.com", "Ada", QUICK.hash("pw pw pw pw"), now)
            ben = acc.create_user("ben@example.com", "Ben", "member", QUICK.hash("pw pw pw pw"), now)
            t = Tokens(acc)
            self.admin = {"Authorization": "Bearer " + t.create_personal(ada, "ada")[1]}
            self.member = {"Authorization": "Bearer " + t.create_personal(ben, "ben")[1]}

    def __call__(self, method, path, obj=None, headers=None, raw=None):
        data = raw if raw is not None else (json.dumps(obj).encode() if obj is not None else None)
        status, _hdrs, body = request(self.url, method, path, data, {**(J if data is not None else {}), **(headers or {})})
        return status, json.loads(body) if body else None

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.db.close()


@pytest.fixture
def srv(monkeypatch):
    s = Server(monkeypatch)
    yield s
    s.close()


def test_the_routes_are_not_there_while_the_check_is_off(monkeypatch):
    s = Server(monkeypatch, check=None, accounts=False)
    try:
        assert s.app.alerts is None
        assert s("GET", "/api/alerts/rules")[0] == 404
        assert s("POST", "/api/alerts/channels", {"kind": "email", "target": "a@acme.io"})[0] == 404
    finally:
        s.close()


def test_a_bad_check_setting_stops_the_server(monkeypatch):
    monkeypatch.setenv("TRAILBEAM_ALERT_CHECK", "often")
    with pytest.raises(ValueError, match="TRAILBEAM_ALERT_CHECK"):
        make_server(Database(":memory:"), "127.0.0.1", 0, ui_dir=None)


def test_over_http_members_read_and_make_rules_admins_manage_channels(srv):
    hook = {"kind": "webhook", "name": "Ops", "target": HOOK, "minTone": "warning"}
    assert srv("GET", "/api/alerts/channels")[0] == 401
    assert srv("POST", "/api/alerts/channels", hook, srv.member) == (403, {"error": "this needs an admin"})
    status, out = srv("POST", "/api/alerts/channels", hook, srv.admin)
    assert status == 201 and out["secret"].startswith("whsec_")
    cid = out["channel"]["id"]
    status, listed = srv("GET", "/api/alerts/channels", headers=srv.member)
    assert status == 200 and listed["channels"][0]["shown"] == "https://hooks.example.com/…"
    assert listed["canManage"] is False and srv("GET", "/api/alerts/channels", headers=srv.admin)[1]["canManage"]
    assert "token=abc" not in json.dumps(listed) and out["secret"] not in json.dumps(listed)
    assert srv("DELETE", f"/api/alerts/channels/{cid}", headers=srv.member)[0] == 403
    assert srv("PATCH", f"/api/alerts/channels/{cid}", {"name": "x"}, srv.member)[0] == 403
    assert srv("POST", f"/api/alerts/channels/{cid}/test", {}, srv.member)[0] == 403
    # a refused target, in the hosted app's words
    assert srv("POST", "/api/alerts/channels", {**hook, "target": "http://x.io"}, srv.admin) == (
        400, {"error": "The URL must start with https://."})
    # a member makes a rule on a cause Needs attention has (none here: the store is empty)
    assert srv("POST", "/api/alerts/rules", {"causeId": "c1", "channels": [cid], "mode": "every"}, srv.member) == (
        404, {"error": "That cause has no unfinished messages now. Reload the page."})
    srv.app.alerts.causes = lambda: CAUSES
    status, out = srv("POST", "/api/alerts/rules", {"causeId": "c1", "channels": [cid], "mode": "every"}, srv.member)
    assert status == 201 and out["rule"]["createdBy"].isdigit()
    rid = out["rule"]["id"]
    assert srv("POST", f"/api/alerts/rules/{rid}/mute", {"forS": 3600}, srv.member)[1]["rule"]["muted"] is True
    assert srv("GET", "/api/alerts/rules", headers=srv.member)[1]["rules"][0]["id"] == rid
    assert srv("GET", "/api/alerts/state", headers=srv.member)[0] == 200
    assert srv("DELETE", f"/api/alerts/rules/{rid}", headers=srv.member) == (200, {"removed": True})
    assert srv("DELETE", f"/api/alerts/channels/{cid}", headers=srv.admin) == (200, {"removed": True})


def test_over_http_the_request_checks_of_the_people_api_apply(srv):
    # a body that is not JSON, or not sent as JSON
    assert srv("POST", "/api/alerts/channels", raw=b"kind=email", headers={**srv.admin,
               "Content-Type": "application/x-www-form-urlencoded"})[0] == 415
    assert srv("POST", "/api/alerts/channels", raw=b"{", headers=srv.admin)[0] == 400
    # another site's form
    assert srv("POST", "/api/alerts/rules", {"causeId": "c1"}, {**srv.member, "Origin": "https://evil.example"})[0] \
        == 403
    # no such route under /api/alerts, for each method
    assert srv("GET", "/api/alerts/nope", headers=srv.member)[0] == 404
    assert srv("POST", "/api/alerts/nope", {}, srv.member)[0] == 404
    assert srv("PATCH", "/api/alerts/rules", {}, srv.member)[0] == 404


def test_before_an_admin_exists_whoever_is_let_in_manages_channels(monkeypatch):
    s = Server(monkeypatch, accounts=False)
    try:
        status, out = s("POST", "/api/alerts/channels", {"kind": "email", "name": "", "target": "a@acme.io"})
        assert status == 201 and out["channel"]["name"] == "Email" and out["channel"]["createdBy"] == "none"
        assert out["secret"] is None and out["channel"]["shown"] == "a@acme.io"
        # a webhook on this machine is refused: the server checks targets as in production
        status, out = s("POST", "/api/alerts/channels", {"kind": "webhook", "target": "http://127.0.0.1:9/x"})
        assert status == 400
    finally:
        s.close()
