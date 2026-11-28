"""The scheduled alert check (alerts_run.py) and its timer (alerts_scheduler.py), QV-639: the shared cases in
contract/fixtures/alerts-run.json, and what they don't cover (triage, overlapping checks, the scheduler thread)."""
import importlib.util
import json
import threading
from pathlib import Path

import pytest
from trailbeam.server import alerts_run as run
from trailbeam.server import alerts_scheduler as sched

FIXTURES = Path(__file__).resolve().parents[3] / "contract" / "fixtures"
F = json.loads((FIXTURES / "alerts-run.json").read_text())
_spec = importlib.util.spec_from_file_location("make_alerts_run", FIXTURES / "make-alerts-run.py")
MAKE = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(MAKE)
T, MIN, H = MAKE.T, MAKE.MIN, MAKE.H


def test_the_fixture_is_what_its_script_writes(tmp_path, monkeypatch):
    monkeypatch.setattr(MAKE, "OUT", tmp_path / "alerts-run.json")
    MAKE.main()
    assert json.loads(MAKE.OUT.read_text()) == F, "run contract/fixtures/make-alerts-run.py"


@pytest.mark.parametrize("c", F["cases"], ids=lambda c: c["name"])
def test_case(c):
    assert json.loads(json.dumps(MAKE.run_case(c))) == c["expected"]


def test_the_cases_cover_what_the_issue_names():
    names = " / ".join(c["name"] for c in F["cases"])
    for word in ("oldest checked first", "budget", "leases", "muted", "snoozed", "still firing", "failing workspace"):
        assert word in names


# ── a small harness ──────────────────────────────────────────────────────────────────────────────────────────────

class Check:
    """One workspace "a" with one cause rule, run check after check on one store."""

    def __init__(self, rule=None, memory=None, triage=None):
        self.store = run.MemoryAlertStore({"a": [{"rule": rule or MAKE.rule(), "memory": memory or MAKE.memory()}]})
        self.read = MAKE.read("m1")
        self.triage = triage  # cause key -> triage, or None
        self.at = T
        self.sent: list = []

    def deps(self, **o):
        d = {"store": self.store, "dashboards": lambda only: {},
             "data": lambda ws, b: pytest.fail("no dashboard rules"),
             "causes": lambda ws: self.read, "deliver": lambda ws, ch: self.sent.extend(ch), "now": lambda: self.at,
             "triage": (lambda ws, keys, read, at: self.triage) if self.triage is not None else None, "owner": "me"}
        d.update(o)
        return run.Deps(**d)

    def __call__(self, **o):
        self.sent = []
        r = run.run_alerts(self.deps(**o))
        self.at += 5 * MIN
        return r

    @property
    def memory(self):
        return self.store.rules["a"][0]["memory"]


def ignored(how="ignored by Sam for 1 h"):
    return {MAKE.KEY: {"triage": {"status": "ignored", "regressed": False}, "ignoredHow": how,
                       "info": {"status": "ignored", "regressed": False, "assignee": None, "notes": 0,
                                "resolvedBy": None}}}


def opened(regressed_at=None, ignored_how=None):
    t = {"status": "open", "regressed": regressed_at is not None, "regressedAt": regressed_at, "regressedCount": 2,
         "regressedVersion": "1.4.2", "statusAt": T - H}
    # ignoredHow: the latest ignore in the cause's thread, which stays after the ignore ends
    return {MAKE.KEY: {"triage": t, "ignoredHow": ignored_how, "info": {
        "status": "open", "regressed": regressed_at is not None, "assignee": "Ana", "notes": 1, "resolvedBy": "Sam"}}}


# ── triage ───────────────────────────────────────────────────────────────────────────────────────────────────────

def test_an_ignored_cause_holds_the_rule_and_its_end_sends_back_from_ignored():
    c = Check(triage=ignored())
    c.read = MAKE.read("m1", "m2")
    assert c()["changes"] == 0 and c.sent == []
    assert c.store.states["a"]["rule/r1"]["tone"] == "critical", "held: worked out as usual"
    assert c.memory["wasMuted"] and c.memory["wasHeld"]
    assert c.store.rules["a"][0]["rule"]["mutedUntil"] is None, "its own mute is left alone"
    c.triage = opened(ignored_how="ignored by Sam for 1 h")
    assert c()["changes"] == 1
    [x] = c.sent
    assert x["kind"] == "fired" and x["alert"]["rule"]["backFromIgnored"] == {"how": "ignored by Sam for 1 h"}
    assert "afterSnooze" not in x["alert"]["rule"]
    assert x["alert"]["since"] == T and x["alert"]["rule"]["triage"]["assignee"] == "Ana"
    assert not c.memory["wasMuted"] and not c.memory["wasHeld"]
    assert c()["changes"] == 0, "once"


def test_a_regression_sends_one_regressed_change_even_when_quiet():
    c = Check(triage=opened(regressed_at=T - MIN))
    c()
    [x] = c.sent
    assert x["kind"] == "fired" and x["alert"]["tone"] == "critical"
    assert x["alert"]["rule"]["regressed"] == {"count": 2, "version": "1.4.2", "by": "Sam", "resolvedAt": T - H}
    assert c.memory["regressedSeen"] == T - MIN
    c()
    assert c.sent == [], "one message per regression"


def test_a_regression_at_a_check_where_the_rule_fires_too_is_one_message():
    c = Check(triage=opened(regressed_at=T - MIN))
    c.read = MAKE.read("m1", "m2")
    c()
    [x] = c.sent
    assert x["alert"]["rule"]["regressed"]["count"] == 2 and x["alert"]["since"] == T


def test_a_regression_while_muted_or_before_the_rule_is_let_pass():
    c = Check(rule=MAKE.rule(mutedUntil=T + H), triage=opened(regressed_at=T - MIN))
    c()
    assert c.sent == [] and c.memory["regressedSeen"] == T - MIN, "let pass, so it is not sent late"
    c = Check(rule=MAKE.rule(createdAt=T), triage=opened(regressed_at=T - MIN))
    c()
    assert c.sent == []


# ── "every" rules ─────────────────────────────────────────────────────────────────────────────────────────────────

def test_an_every_rule_sends_at_each_check_that_finds_new_messages():
    c = Check(rule=MAKE.rule(mode="every", quietS=0))  # m1 seen at the last check
    c()
    assert c.sent == [], "nothing new"
    first = c.at
    for ids, new in ((("m1", "m2"), 1), (("m1", "m2", "m3"), 1), (("m1", "m2", "m3", "m4", "m5"), 2)):
        c.read = MAKE.read(*ids)
        c()
        [x] = c.sent
        assert x["kind"] == "fired" and x["alert"]["rule"]["newCount"] == new
        assert x["alert"]["since"] == first, "since: when it began firing"
    c()
    assert [x["kind"] for x in c.sent] == ["resolved"]


def test_an_every_rule_woken_with_new_messages_sends_one_change():
    c = Check(rule=MAKE.rule(mode="every", quietS=0, mutedUntil=T - MIN),
              memory=MAKE.memory(lastNewAt=T - 20 * MIN, wasMuted=True))
    c.store.states["a"] = {"rule/r1": MAKE.firing()}
    c.read = MAKE.read("m1", "m2")
    c()
    [x] = c.sent
    assert x["kind"] == "fired" and x["alert"]["rule"]["newCount"] == 1


# ── overlapping checks, failures ─────────────────────────────────────────────────────────────────────────────────

def test_a_change_another_check_wrote_first_is_not_sent_twice():
    c = Check()
    c.read = MAKE.read("m1", "m2")
    real = c.store.write_state

    def racing(ws, state, prev_tone):
        # another check records the same change just before this one
        real(ws, state, prev_tone)
        return real(ws, state, prev_tone)

    c.store.write_state = racing
    assert c()["changes"] == 0 and c.sent == []


def test_still_firing_is_sent_by_the_check_that_clears_the_flag_only():
    c = Check(rule=MAKE.rule(mutedUntil=T - MIN), memory=MAKE.memory(lastNewAt=T - 20 * MIN, wasMuted=True))
    c.store.states["a"] = {"rule/r1": MAKE.firing()}
    c.store.clear_was_muted = lambda ws, rid: False  # another check got there first
    c()
    assert c.sent == []


def test_a_delivery_that_raises_fails_the_workspace_and_releases_its_lease():
    c = Check()
    c.read = MAKE.read("m1", "m2")

    def boom(ws, changes):
        raise OSError("smtp down")

    r = c(deliver=boom)
    assert r["failed"] == [{"ws": "a", "error": "smtp down"}]
    assert c.store.checks["a"]["error"] == "smtp down"
    assert c.store.leases["a"]["until"] == 0
    c.at += 0
    assert c()["workspaces"] == 1, "a failed check is tried again at once, not after the gap"


def test_an_error_with_no_message_is_named_by_its_type():
    c = Check()

    def bad(ws):
        raise KeyError()

    assert c(causes=bad)["failed"] == [{"ws": "a", "error": "KeyError"}]


def test_a_lease_of_a_check_that_died_runs_out():
    c = Check()
    c.store.leases["a"] = {"owner": "dead", "until": T + run.LEASE_MS - MIN}
    assert c()["skipped"]["leased"] == 1
    c.at = T + run.LEASE_MS
    assert c()["workspaces"] == 1


def test_without_causes_rules_are_not_checked():
    c = Check()
    assert c(causes=None)["workspaces"] == 0


def test_a_rule_deleted_since_the_last_check_drops_its_state_silently():
    c = Check()
    c.store.states["a"] = {"rule/r1": MAKE.firing(), "rule/gone": MAKE.firing("gone")}
    c()
    assert set(c.store.states["a"]) == {"rule/r1"}
    assert [x["alert"]["widgetId"] for x in c.sent] == ["r1"], "r1 resolves; nothing about the deleted rule"


def test_the_previous_check_is_the_later_of_the_scheduled_one_and_one_at_ingest():
    c = Check()
    c.store.checks = {"a": {"checkedAt": T - H, "error": None}, "a#spans": {"checkedAt": T - 10 * MIN, "error": None}}
    c.read = MAKE.read("m1", "m2")
    c()
    assert c.sent[0]["alert"]["rule"]["prevCheckAt"] == T - 10 * MIN


# ── has_rules, alert_needs ───────────────────────────────────────────────────────────────────────────────────────

def test_has_rules():
    empty = run.MemoryAlertStore()
    deps = run.Deps(store=empty, dashboards=lambda only: {"a": [MAKE.NO_RULE]}, data=lambda ws, b: {},
                    causes=lambda ws: {})
    assert not run.has_rules(deps)
    deps.dashboards = lambda only: {"a": [MAKE.BOARD]}
    assert run.has_rules(deps)
    deps.dashboards = lambda only: {}
    deps.store = Check().store
    assert run.has_rules(deps)
    deps.causes = None
    assert not run.has_rules(deps)


def _w(measure, rule=None, time=None, filter_=None, type_="number"):
    w = {"id": measure, "type": type_, "title": measure, "layout": {}, "queries": [{"measure": measure}]}
    if rule is not None:
        w["rule"] = rule
    if time:
        w["time"] = time
    if filter_ is not None:
        w["filter"] = filter_
    return w


def test_alert_needs():
    def needs(*ws):
        return run.alert_needs([{"def": {"variables": [], "widgets": list(ws)}}])

    n = needs(_w("sent"), _w("dead_lettered", {"warn": 1}, time="1d"), _w("processing_errors", {"crit": 1}, "4h"),
              _w("dlq_depth", {"crit": 0}, type_="toplist"))
    assert (n["events"], n["sinceMs"], n["queues"], n["inQueue"], n["rules"], n["metrics"]) == (
        True, 24 * H, False, False, 2, [])
    n = needs(_w("time_to_drain", {"crit": 600}, filter_="queue:orders*"), _w("oldest_age", {"warn": 60}),
              _w("stuck_in_flight", {"crit": 0}, filter_="queue:billing"))
    assert (n["events"], n["queues"], n["inQueue"], n["rules"], n["metrics"]) == (
        False, True, True, 3, ["deleted", "oldestS", "sent"])
    assert n["queuesOf"](["orders", "orders-dlq", "billing", "email"]) == ["orders", "orders-dlq", "billing", "email"]
    n = needs(_w("visibility_headroom", {"warn": 80}, filter_="queue:billing"))
    assert (n["events"], n["sinceMs"], n["queues"]) == (True, H, True)
    assert n["queuesOf"](["orders", "billing"]) == ["billing"]


# ── the scheduler ────────────────────────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("raw,expected", [
    (None, None), ("", None), ("off", None), ("0", None), ("OFF", None), ("on", 5), ("5", 5), ("1", 1),
    (" 15 ", 15), ("1440", 1440)])
def test_the_setting(raw, expected):
    assert sched.check_every_min({} if raw is None else {sched.ENV: raw}) == expected


@pytest.mark.parametrize("raw", ["5m", "-1", "1441", "every 5"])
def test_a_setting_it_cannot_take_stops_the_server(raw):
    with pytest.raises(ValueError, match=sched.ENV):
        sched.check_every_min({sched.ENV: raw})


def test_off_starts_no_thread():
    assert sched.start_alert_scheduler(lambda: pytest.fail("off"), env={sched.ENV: "off"}) is None
    assert sched.start_alert_scheduler(lambda: pytest.fail("off"), env={}) is None


def test_a_tick_with_no_rules_does_nothing():
    s = sched.AlertScheduler(lambda: pytest.fail("no rules: no check"), 60, has_rules=lambda: False)
    assert s.tick() is None and s.idle == 1 and s.ticks == 1


def test_a_check_that_raises_is_logged_and_the_next_tick_runs(caplog):
    calls = []

    def check():
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("disk full")
        return {"workspaces": 1, "rules": 1, "changes": 1, "failed": [{"ws": "a", "error": "x"}], "skipped": {},
                "ms": 3}

    s = sched.AlertScheduler(check, 60)
    assert s.tick() is None and s.last_error == "RuntimeError: disk full"
    assert s.tick()["changes"] == 1 and s.last_error is None
    assert "disk full" in caplog.text and "workspace a failed: x" in caplog.text


def test_the_thread_runs_the_real_check_at_once_and_stops():
    c = Check()
    c.read = MAKE.read("m1", "m2")
    deps = c.deps(now=run.now_ms)
    ran = threading.Event()

    def check():
        r = run.run_alerts(deps)
        ran.set()
        return r

    s = sched.start_alert_scheduler(check, lambda: run.has_rules(deps), env={sched.ENV: "5"})
    try:
        assert s is not None and s.running and s.every_s == 300
        assert ran.wait(5), "the first check runs at start"
    finally:
        s.stop(5)
    assert not s.running
    assert s.last_result["changes"] == 1 and len(c.sent) == 1


def test_a_check_longer_than_the_interval_is_followed_at_once_and_never_overlaps():
    active, most, n = [0], [0], [0]
    done = threading.Event()
    lock = threading.Lock()

    def check():
        with lock:
            active[0] += 1
            most[0] = max(most[0], active[0])
        done.wait(0.05)
        with lock:
            active[0] -= 1
            n[0] += 1
            if n[0] >= 3:
                done.set()
        return {}

    s = sched.AlertScheduler(check, 0.01).start()
    try:
        assert done.wait(5)
    finally:
        s.stop(5)
    assert most[0] == 1



def test_stopping_as_a_check_begins_does_not_wait_on_a_thread_not_yet_started(monkeypatch):
    # QV-732: stop() landing between the check's thread being made and started joined an unstarted thread
    about_to_start, go = threading.Event(), threading.Event()

    class Held(threading.Thread):
        def start(self):
            if self.name == "trailbeam-alert-check-run":
                about_to_start.set()
                go.wait(5)
            super().start()

    monkeypatch.setattr(sched.threading, "Thread", Held)
    s = sched.AlertScheduler(lambda: {}, 60).start()
    assert about_to_start.wait(5)
    stopped = []
    t = threading.Thread(target=lambda: stopped.append(s.stop(5)))
    t.start()
    t.join(0.2)
    go.set()
    t.join(5)
    assert stopped == [True]

def _wait(cond, s=5.0):
    import time
    end = time.monotonic() + s
    while not cond():
        assert time.monotonic() < end, "timed out"
        time.sleep(0.01)


def test_health_says_late_when_no_check_has_ended_for_two_intervals():
    now = [1000.0]
    s = sched.AlertScheduler(lambda: {"workspaces": 1, "changes": 0, "failed": []}, 60, clock=lambda: now[0])
    s.tick()
    h = s.health()
    assert h["status"] == "ok" and h["problem"] is None and h["last_done_at_ms"] == 1_000_000
    assert h["last"] == {"workspaces": 1, "changes": 0, "resent": None, "dropped": None, "ms": None, "failed": 0}
    now[0] += 2 * 60
    assert s.health()["status"] == "ok"
    now[0] += 1
    h = s.health()
    assert h["status"] == "late" and "No check has ended for 2 min; one should run every 1 min." == h["problem"]


def test_health_says_failing_after_a_check_that_raised():
    def check():
        raise RuntimeError("disk full")

    s = sched.AlertScheduler(check, 60)
    s.tick()
    assert s.health()["status"] == "failing" and "disk full" in s.health()["problem"]


def test_a_check_past_its_deadline_is_logged_and_health_says_stuck(caplog):
    go, began = threading.Event(), threading.Event()

    def check():
        began.set()
        go.wait(10)
        return {}

    s = sched.AlertScheduler(check, 60, deadline_s=0.05).start()
    try:
        assert began.wait(5)
        _wait(lambda: s.overdue == 1)
        h = s.health()
        assert h["status"] == "stuck" and "past its deadline" in h["problem"] and h["running_since_ms"]
        assert "has not returned" in caplog.text
    finally:
        go.set()
        assert s.stop(5)
    assert s.health()["status"] == "late"  # stopped: its thread is not running


def test_stop_waits_a_bounded_time_for_a_check_under_way():
    go, began = threading.Event(), threading.Event()

    def check():
        began.set()
        go.wait(10)
        return {}

    s = sched.AlertScheduler(check, 60).start()
    try:
        assert began.wait(5)
        import time
        t0 = time.monotonic()
        assert s.stop(0.1) is False and time.monotonic() - t0 < 2
    finally:
        go.set()
    assert s.stop(5) is True
    # the default fits the container's stop grace period (docker stop: 10 s)
    assert sched.STOP_WAIT_S < 10 and sched.DEADLINE_S * 1000 < run.LEASE_MS


def test_every_s_must_be_positive():
    with pytest.raises(ValueError):
        sched.AlertScheduler(dict, 0)


# ── a rule whose figure goes missing, and one deleted while firing (QV-660) ──────────────────────────────────────

DEPTH = {"id": "d1", "name": "Orders", "def": {"variables": [], "widgets": [
    {"id": "w1", "type": "number", "title": "Backlog", "layout": {"x": 0, "y": 0, "w": 3, "h": 2},
     "queries": [{"measure": "messages_visible"}], "rule": {"crit": 100}}]}}


def _depth(at, visible):
    """The queue figures at a check: orders with ``visible`` messages, or (None) a read that had no figure."""
    live = {} if visible is None else {"orders": {"status": "ok", "visible": visible}}
    return {"now": at, "queues": ["orders"], "queueLive": live, "events": []}


class Boards:
    """Workspace "a" with dashboards and no "Alert me" rule, checked every 5 min on one store."""

    def __init__(self, boards):
        self.store = run.MemoryAlertStore()
        self.boards, self.at, self.sent, self.visible = boards, T, [], 500

    def __call__(self):
        self.sent = []
        run.run_alerts(run.Deps(store=self.store, dashboards=lambda only: {"a": self.boards},
                                data=lambda ws, b: _depth(self.at, self.visible), causes=None,
                                deliver=lambda ws, ch: self.sent.extend(ch), now=lambda: self.at, owner="me"))
        self.at += 5 * MIN
        return [(x["kind"], x["from"], bool(x["alert"].get("noLongerChecked"))) for x in self.sent]


def test_a_figure_missing_for_one_check_is_not_a_new_incident():
    b = Boards([DEPTH])
    assert b() == [("fired", None, False)]
    since = b.store.states["a"]["d1/w1"]["since"]
    b.visible = None
    assert b() == [], "a missed read sends nothing"
    assert b.store.states["a"]["d1/w1"]["since"] == since
    b.visible = 600
    assert b() == [], "back and still critical: the same incident, not sent again"
    assert b.store.states["a"]["d1/w1"]["since"] == since
    b.visible = 3
    assert b() == [("resolved", "critical", False)]


def test_a_figure_missing_for_good_is_no_longer_checked_after_the_limit():
    b = Boards([DEPTH])
    b()
    b.visible = None
    sent = [b() for _ in range(run.alerts.GONE_AFTER_MS // (5 * MIN))]
    assert sent[:-1] == [[]] * (len(sent) - 1)
    assert sent[-1] == [("resolved", "critical", True)]
    assert "a" not in b.store.states or not b.store.states["a"]
    assert b() == []


def test_a_widget_deleted_while_firing_is_no_longer_checked():
    b = Boards([DEPTH])
    b()
    b.boards = [{**DEPTH, "def": {"variables": [], "widgets": [
        {**DEPTH["def"]["widgets"][0], "id": "w2", "rule": {"crit": 10_000}}]}}]
    assert b() == [("resolved", "critical", True)]
    assert set(b.store.states["a"]) == {"d1/w2"}


def test_of_two_checks_only_the_one_that_drops_the_state_sends_no_longer_checked():
    store = run.MemoryAlertStore(states={"a": [{"dashboardId": "d1", "widgetId": "w1", "dashboardName": "Orders",
                                                "title": "Backlog", "tone": "critical", "value": "500", "limit": "above 100",
                                                "queue": "orders", "since": T - H, "checkedAt": T - 5 * MIN}]})
    stale = store.alert_states("a")
    assert len(run.record(store, "a", [], T, set())) == 1
    store.alert_states = lambda ws: stale  # the second check read the state before the first dropped it
    assert run.record(store, "a", [], T, set()) == []
