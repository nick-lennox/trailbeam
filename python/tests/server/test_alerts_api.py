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
    assert listed["everyMin"] == 5  # the shell's "checks are late" note (ui/src/alerts checkStatus)
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
    assert out == {"alerts": [], "checkedAt": None, "hasChannel": False, "checkEvery": "every 5 minutes", "everyMin": 5}
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


# ── the API in the server: its tables, links and mail (QV-674) ───────────────────────────────────────────────────

def test_the_server_keeps_alerts_in_its_tables_links_to_its_public_url_and_mails_through_its_server(
        monkeypatch, tmp_path):
    from trailbeam.server import alert_delivery, alerts_store, mail, server
    monkeypatch.setenv("TRAILBEAM_ALERT_CHECK", "5")
    db = Database(str(tmp_path / "trailbeam.sqlite"))
    try:
        smtp = alert_delivery.SmtpConfig(host="smtp.example.com", sender="tb@example.com")
        app = server.App(db, ui_dir=None, public_url="https://tb.example.com/", mailer=mail.Mailer(smtp))
        assert isinstance(app.alerts.store, alerts_store.AlertTables)
        assert app.alerts.delivery.base == "https://tb.example.com" and app.alerts.delivery.smtp is smtp
        bare = server.App(db, ui_dir=None)
        assert bare.alerts.delivery.base == "http://localhost" and bare.alerts.delivery.smtp is None
    finally:
        db.close()


def test_health_shows_the_alert_check_and_a_late_one(monkeypatch):
    from trailbeam.server import alerts_scheduler, server
    monkeypatch.delenv("TRAILBEAM_ALERT_CHECK", raising=False)
    db = Database(":memory:")
    try:
        assert server.App(db, ui_dir=None).health({}, [])["alert_check"] is None
        now = [1000.0]
        s = alerts_scheduler.AlertScheduler(dict, 300, clock=lambda: now[0])
        s.tick()
        app = server.App(db, ui_dir=None, alert_check=s)
        assert app.health({}, [])["alert_check"]["status"] == "ok"
        now[0] += 2 * 300 + 1
        h = app.health({}, [])["alert_check"]
        assert h["status"] == "late" and h["last_done_at_ms"] == 1_000_000 and h["every_s"] == 300
    finally:
        db.close()


# ── the scheduled check in the server (QV-592) ───────────────────────────────────────────────────────────────────

def _post_failure(url, mid, at_ns, text):
    from spans import SEC, Consumer, to_proto
    p, w = Consumer("producer"), Consumer("worker", source="lambda")
    failed = w.outcome(at_ns + 2 * SEC, mid, "failed_exception", f"inv-{mid}", error_type="TimeoutError")
    failed.attrs["trailbeam.error_message"] = text
    spans = [p.send(at_ns, mid), w.receive(at_ns + SEC, mid, f"rh-{mid}", lambda_inv=f"inv-{mid}"), failed]
    status, _h, _b = request(url, "POST", "/v1/traces", to_proto(spans, service="orders-worker").SerializeToString(),
                             {"Content-Type": "application/x-protobuf"})
    assert status == 200


def test_the_check_in_the_server_reads_stuck_causes_and_sends_to_the_channels(monkeypatch):
    """check_deps: an "Alert me" rule on one of Stuck's causes fires at the check after a new message hits it, and
    its change goes to the rule's channel, its outcome noted there (here: no mail server)."""
    from spans import SEC
    s = Server(monkeypatch, accounts=False)
    try:
        ten_min_ago = time.time_ns() - 600 * SEC
        _post_failure(s.url, "m-1", ten_min_ago, "Stripe timed out after 10000 ms")
        cause = s("GET", "/api/causes")[1]["groups"][0]
        ch = s("POST", "/api/alerts/channels", {"kind": "email", "name": "Ops", "target": "ops@example.com"})[1]["channel"]
        status, made = s("POST", "/api/alerts/rules", {"causeId": cause["id"], "channels": [ch["id"]], "mode": "every", "quietS": 0})
        assert status == 201
        deps = api.check_deps(s.app, s.app.alerts)
        assert run.has_rules(deps) and deps.channels() == {"local"}
        first = run.run_alerts(deps)
        assert (first["workspaces"], first["changes"], first["failed"]) == (1, 0, [])
        state = s("GET", "/api/alerts/rules")[1]["rules"][0]["state"]
        assert state["tone"] == "good"  # what was there when the rule was made is not news
        _post_failure(s.url, "m-2", ten_min_ago + 5 * SEC, "Stripe timed out after 10077 ms")
        deps.store.checked("local", 0, None)  # as if the last check were long ago, so this one isn't skipped
        second = run.run_alerts(deps)
        assert (second["changes"], second["failed"]) == (1, [])
        assert s("GET", "/api/alerts/rules")[1]["rules"][0]["state"]["tone"] == "critical"
        noted = s("GET", "/api/alerts/channels")[1]["channels"][0]
        assert noted["lastOk"] is False and noted["lastDetail"] == "No mail server is set up for alert emails."
    finally:
        s.close()


def test_the_check_starts_with_the_server_only_when_it_is_on(monkeypatch):
    off = Server(monkeypatch, check=None, accounts=False)
    try:
        assert api.start_check(off.app) is None
    finally:
        off.close()
    on = Server(monkeypatch, check="7", accounts=False)
    try:
        sched = api.start_check(on.app)
        try:
            assert sched is not None and sched.every_s == 7 * 60
        finally:
            assert sched.stop()
    finally:
        on.close()


# ── private webhooks (QV-764) ────────────────────────────────────────────────────────────────────────────────────

def test_the_private_webhooks_switch_is_off_unless_turned_on():
    on = api.PRIVATE_WEBHOOKS_ENV
    assert api.allow_private_webhooks({}) is False
    for v in ("", "0", "off", "false", "No"):
        assert api.allow_private_webhooks({on: v}) is False
    for v in ("1", "on", "true", " YES "):
        assert api.allow_private_webhooks({on: v}) is True
    with pytest.raises(ValueError, match=on):
        api.allow_private_webhooks({on: "maybe"})


def _app():
    import types
    return types.SimpleNamespace(access=types.SimpleNamespace(mode=lambda: "open"), causes=lambda *a: {"groups": []},
                                 db=object(), public_url="https://tb.example.com", mailer=None)


def test_for_app_reads_the_switch():
    assert api.for_app(_app(), env={"TRAILBEAM_ALERT_CHECK": "on"}).delivery.allow_private is False
    a = api.for_app(_app(), env={"TRAILBEAM_ALERT_CHECK": "on", api.PRIVATE_WEBHOOKS_ENV: "1"})
    assert a.delivery.allow_private is True and a.delivery.allow_local is False
    with pytest.raises(ValueError):
        api.for_app(_app(), env={"TRAILBEAM_ALERT_CHECK": "on", api.PRIVATE_WEBHOOKS_ENV: "y"})


def test_with_the_switch_on_a_channel_can_be_on_this_network():
    """The QV-762 steps: each target was refused; with the switch on each is taken, a metadata address still isn't."""
    a = Api(allow_private=True)
    for t in ("http://listener:8080/hook", "http://127.0.0.1:9999/hook", "https://listener.internal/hook"):
        assert a.channel(target=t)["channel"]["shown"].startswith(t.rsplit("/", 1)[0])
    assert a.fails("POST", "/api/alerts/channels", {"kind": "webhook", "target": "http://169.254.169.254/"}) == (
        400, "The webhook can't be a cloud metadata address.")
    cid = a.store.list_channels("local")[0]["id"]
    assert a.fails("PATCH", f"/api/alerts/channels/{cid}", {"target": "http://93.184.216.34/"}) == (
        400, "Plain http is allowed only to a private address. Use https for a public host.")
    assert a("PATCH", f"/api/alerts/channels/{cid}", {"target": "http://10.0.0.9/h"})[0] == 200


def test_a_test_send_reaches_a_private_endpoint_with_the_switch_on():
    hook = Hook()
    try:
        a = Api(allow_private=True)
        cid = a.channel(target=hook.url)["channel"]["id"]
        assert a("POST", f"/api/alerts/channels/{cid}/test", {}) == (200, {"ok": True})
        assert len(hook.got) == 1
    finally:
        hook.close()


# ── contract/openapi.yaml (QV-770) ───────────────────────────────────────────────────────────────────────────────

RID, CID = "4f1c2b9e-0d7a-4c55-9a3e-2b8e6f0c1d2a", "0b6c1e52-7d3f-4a8e-9c21-5f4d2a7b8e10"


def test_every_alerts_route_and_status_the_file_lists_is_answered(monkeypatch, record_api_responses):
    """What test_openapi.py checks for the other routes, with alerts on: every Alerts operation is served, its
    x-example-request answers 200 or 201, and every status the file lists for it is given (and matches the file, as
    the recorder checks every answer)."""
    from openapi_contract import contract
    from test_openapi import serve
    from trailbeam.server import access as access_mod
    from trailbeam.server import proxy_auth, server
    from trailbeam.server.proxy import TrustedProxies, parse_networks

    ops = [op for op in contract().ops if "Alerts" in op.op.get("tags", [])]
    assert len(ops) == len(api.ROUTES) == 11
    off = Server(monkeypatch, check=None, accounts=False)
    try:
        for op in ops:  # off: 404
            off(op.method, op.template.replace("{rule_id}", RID).replace("{channel_id}", CID),
                {} if op.method != "GET" else None)
    finally:
        off.close()

    s = Server(monkeypatch)
    store, member, admin = s.app.alerts.store, s.member, s.admin
    s.app.alerts.causes = lambda: CAUSES
    good = {"POST /api/alerts/rules": {"causeId": "c2", "channels": [CID], "mode": "every"},
            "PATCH /api/alerts/rules/{rule_id}": {"channels": [CID], "mode": "every"},
            "POST /api/alerts/rules/{rule_id}/mute": {"forS": 3600},
            "POST /api/alerts/channels": {"kind": "email", "target": "a@acme.io"},
            "PATCH /api/alerts/channels/{channel_id}": {"minTone": "critical"},
            "POST /api/alerts/channels/{channel_id}/test": {}}
    bad = {"POST /api/alerts/rules": {"causeId": "c1", "channels": [], "mode": "every"},
           "PATCH /api/alerts/rules/{rule_id}": {"channels": [CID], "mode": "sometimes"},
           "POST /api/alerts/rules/{rule_id}/mute": {"forS": 5},
           "POST /api/alerts/channels": {"kind": "webhook", "target": "http://x.io"},
           "PATCH /api/alerts/channels/{channel_id}": {"target": "http://x.io"}}

    def fresh():
        """Rule RID on cause c1, sent to channel CID (an email channel, so a test send fails without a mail
        server and is still a 200): the IDs each operation's x-example-request names."""
        store.remove_rule("local", RID)
        for c in store.list_channels("local"):
            store.remove_channel("local", c["id"])
        store.add_channel("local", {"id": CID, "kind": "email", "name": "Ops", "target": "ops@acme.io", "secret": None,
                                    "minTone": "warning", "createdBy": "1", "createdAt": T, "lastAt": None,
                                    "lastOk": None, "lastDetail": None})
        store.add_rule("local", {"id": RID, "kind": "cause", "causeKey": KEY, "causeId": "c1", "label": "l",
                                 "queue": "orders", "channels": [CID], "mode": "every", "quietS": 0,
                                 "mutedUntil": None, "mutedBy": None, "mutedAt": None, "createdBy": "1",
                                 "createdAt": T, "updatedAt": T})
        store.write_state("local", {"dashboardId": "rule", "widgetId": RID, "dashboardName": "", "title": "l",
                                    "tone": "critical", "value": "3", "limit": "new messages at a check",
                                    "queue": "orders", "since": T, "checkedAt": T}, None)

    def path(op, rid=RID, cid=CID):
        return op.template.replace("{rule_id}", rid).replace("{channel_id}", cid)

    try:
        for op in ops:
            fresh()
            body = good.get(op.key)
            raw = json.dumps(body).encode() if body is not None else None
            who = admin if "channels" in op.template and op.method != "GET" else member
            example = op.op.get("x-example-request", op.template)
            assert s(op.method, example, body, who)[0] in (200, 201), op.key
            fresh()
            s(op.method, path(op), body)  # 401
            if who is admin:
                s(op.method, path(op), body, member)  # 403: a member
            s(op.method, path(op, "nope", "nope"), body, who)  # 404 (removing a missing one: 200)
            if op.key in bad:
                assert s(op.method, path(op), bad[op.key], who)[0] == 400, op.key
            if op.method != "GET":
                s(op.method, path(op), body, {**who, "Sec-Fetch-Site": "cross-site"})  # 403: csrf
                s(op.method, path(op), None, {**who, "Content-Length": "99999"})  # 413
                s(op.method, path(op), raw=b"{}", headers={**who, "Content-Type": "text/plain"})  # 415
        fresh()
        assert s("POST", "/api/alerts/rules", {"causeId": "c1", "channels": [CID], "mode": "every"}, member)[1][
            "existed"] is True  # 200

        def boom(*a, **k):
            raise RuntimeError("a test's broken store")

        def unreachable(*a, **k):
            raise access_mod.AccessUnavailable("a test's unreachable store")

        with pytest.MonkeyPatch.context() as mp:  # 500
            for meth in ("list_rules", "list_channels", "get_channel", "alert_states", "update_rule", "remove_rule",
                         "remove_channel", "rule_for_cause"):
                mp.setattr(store, meth, boom)
            for op in ops:
                s(op.method, path(op), good.get(op.key, {}) if op.method != "GET" else None, admin)
        with pytest.MonkeyPatch.context() as mp:  # 503
            mp.setattr(s.app.access, "check", unreachable)
            for op in ops:
                if op.method != "GET":
                    s(op.method, path(op), {})
    finally:
        s.close()

    # 403 on a read: signed in through a proxy as someone who may not be (as test_openapi.py does)
    db = Database(":memory:")
    try:
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
    answered = {(x.op.key, x.status) for x in record_api_responses.seen if x.op is not None}
    never = [f"{op.key} {st}" for op in ops for st in op.op["responses"] if (op.key, int(st)) not in answered]
    assert not never, "the file lists these statuses, the server never gave them:\n" + "\n".join(never)
