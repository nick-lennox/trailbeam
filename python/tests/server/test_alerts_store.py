"""The alert tables (alerts_store.py; SQLite schema 26, Postgres schema 20): the same store tests on the in-memory
store (alerts_run.MemoryAlertStore, and alerts_api.MemoryAlertConfig where it exists), SQLite (a file and :memory:)
and Postgres, the scheduled check's shared cases (contract/fixtures/alerts-run.json) on each, and the lease taken
by one of two Postgres stores at once. Upgrades from every older version are in test_migrations.py and
test_postgres.py.

The Postgres cases run when TRAILBEAM_TEST_POSTGRES_URL names a database, as in test_postgres.py."""
import copy
import importlib.util
import json
import os
import sys
import threading
from pathlib import Path

import pytest
from trailbeam.server import alerts_run as run
from trailbeam.server import alerts_store
from trailbeam.server.alerts_store import AlertTables
from trailbeam.server.db import Database

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT.parent / "contract" / "fixtures"
F = json.loads((FIXTURES / "alerts-run.json").read_text())
_spec = importlib.util.spec_from_file_location("make_alerts_run", FIXTURES / "make-alerts-run.py")
MAKE = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(MAKE)
PG = os.environ.get("TRAILBEAM_TEST_POSTGRES_URL")
needs_pg = pytest.mark.skipif(not PG, reason="TRAILBEAM_TEST_POSTGRES_URL is not set")

try:  # QV-642 (the API's store) may not be merged yet
    from trailbeam.server.alerts_api import MemoryAlertConfig
except ImportError:
    MemoryAlertConfig = None

T = MAKE.T
WS = "local"


def pg_store():
    sys.path.insert(0, str(ROOT / "tests" / "conformance"))
    from pg_helpers import postgres_store
    return postgres_store()


class Opened:
    """A store under test: ``tables`` (the AlertStore), ``config`` (whether it has the API's methods too)."""

    def __init__(self, kind: str, tmp_path):
        self.kind, self.backing = kind, None
        if kind == "memory":
            self.tables = MemoryAlertConfig() if MemoryAlertConfig else run.MemoryAlertStore()
        elif kind == "sqlite":
            self.backing = Database(str(tmp_path / "trailbeam.sqlite"))
        elif kind == "sqlite-memory":
            self.backing = Database(":memory:")
        else:
            self.backing = pg_store()
        if self.backing is not None:
            self.tables = AlertTables(self.backing)
        self.config = hasattr(self.tables, "add_channel")

    def close(self):
        if self.backing is not None:
            self.backing.close()


KINDS = ["memory", "sqlite", "sqlite-memory", pytest.param("postgres", marks=needs_pg)]


@pytest.fixture(params=KINDS)
def opened(request, tmp_path):
    o = Opened(request.param, tmp_path)
    yield o
    o.close()


@pytest.fixture
def store(opened):
    return opened.tables


@pytest.fixture
def config(opened):
    if not opened.config:
        pytest.skip("alerts_api.MemoryAlertConfig (QV-642) is not here yet")
    return opened.tables


def add(store, ws: str, rule: dict, memory: dict | None = None) -> None:
    """A rule and its memory, in any store."""
    if not hasattr(store, "add_rule"):  # alerts_run.MemoryAlertStore: no API methods
        store.rules.setdefault(ws, []).append({"rule": copy.deepcopy(rule), "memory": copy.deepcopy(
            memory or {"seen": None, "lastNewAt": None, "wasMuted": False, "wasHeld": False, "regressedSeen": None})})
        return
    assert store.add_rule(ws, rule)
    if memory is not None:
        if memory.get("seen") is not None or memory.get("lastNewAt") is not None or memory.get("wasMuted") \
                or memory.get("wasHeld"):
            store.remember(ws, rule["id"], memory, None)
        if memory.get("regressedSeen") is not None:
            store.set_regressed_seen(ws, rule["id"], memory["regressedSeen"])


# ── the scheduled check's shared cases, on every store ───────────────────────────────────────────────────────────

class Seeded:
    """A store with a case's starting rules, states, checks and leases, read back in MemoryAlertStore's shape."""

    def __init__(self, tables, rules, states, checks, leases):
        self.t = tables
        for key, held in (leases or {}).items():  # before the checks: a lease isn't taken within the gap
            assert tables.lease(key, held["owner"], held["until"] - 1, 1, 0)
            if held["until"] == 0:
                tables.release(key, held["owner"])
        for ws, items in (rules or {}).items():
            for x in items:
                add(tables, ws, x["rule"], x["memory"])
        for ws, ss in (states or {}).items():
            for s in ss:
                assert tables.write_state(ws, {f: s[f] for f in run.STATE_FIELDS}, None)
        for key, c in (checks or {}).items():
            tables.checked(key, c["checkedAt"], c["error"])
        self._seen_keys = set(leases or {}) | set(checks or {})

    def __getattr__(self, name):
        return getattr(self.t, name)

    def lease(self, key, *a):
        self._seen_keys.add(key)
        return self.t.lease(key, *a)

    def checked(self, key, *a):
        self._seen_keys.add(key)
        return self.t.checked(key, *a)

    @property
    def rules(self):
        return {ws: self.t.rules_with_memory(ws) for ws in sorted(self.t.workspaces_with_rules())}

    @property
    def states(self):
        wss = {k for k in self._seen_keys if "#" not in k} | set(self.t.workspaces_with_rules())
        return {ws: {run.alerts.key_of(s): s for s in self.t.alert_states(ws)} for ws in wss}

    @property
    def checks(self):
        return {k: {"checkedAt": at, "error": _error(self.t, k)} for k, at in self.t.last_checks().items()}

    @property
    def leases(self):
        return {k: v for k in self._seen_keys if (v := _lease(self.t, k))["owner"]}


def _rows(tables, q, args=()):
    return tables._read(q, args)


def _error(tables, key):
    return _rows(tables, "SELECT error FROM alert_checks WHERE key = ?", (key,))[0]["error"]


def _lease(tables, key):
    rows = _rows(tables, "SELECT lease_owner, lease_until_ms FROM alert_checks WHERE key = ?", (key,))
    return {"owner": rows[0]["lease_owner"], "until": rows[0]["lease_until_ms"]} if rows else {"owner": "", "until": 0}


def _normal(store_after: dict) -> dict:
    """The leases nobody took left out (MemoryAlertStore notes a refused one), rules in check order."""
    out = copy.deepcopy(store_after)
    out["leases"] = {k: v for k, v in out["leases"].items() if v["owner"]}
    out["rules"] = {ws: sorted(rs, key=lambda x: (x["rule"]["createdAt"], x["rule"]["id"]))
                    for ws, rs in out["rules"].items() if rs}
    return out


@pytest.mark.parametrize("kind", ["sqlite", "sqlite-memory", pytest.param("postgres", marks=needs_pg)])
@pytest.mark.parametrize("c", F["cases"], ids=lambda c: c["name"][:50])
def test_the_shared_check_cases_come_out_the_same_on_the_tables(kind, c, tmp_path, monkeypatch):
    o = Opened(kind, tmp_path)
    try:
        monkeypatch.setattr(MAKE.run, "MemoryAlertStore", lambda *a: Seeded(o.tables, *a))
        got = json.loads(json.dumps(MAKE.run_case(c)))
    finally:
        o.close()
    want = c["expected"]
    assert got["result"] == want["result"]
    assert got["delivered"] == want["delivered"]
    assert _normal(got["store"]) == _normal(want["store"])


# ── the check's reads and writes ─────────────────────────────────────────────────────────────────────────────────

def test_a_lease_is_refused_while_held_and_within_the_gap_after_a_good_check(store):
    assert store.lease("a", "one", T, 1000, 100)
    assert not store.lease("a", "two", T + 1000, 1000, 100)  # held until T + 1000 included
    store.release("a", "two")  # not two's
    assert not store.lease("a", "two", T + 500, 1000, 100)
    store.release("a", "one")
    store.checked("a", T + 600, None)
    assert not store.lease("a", "two", T + 650, 1000, 100)  # checked fine 50 ms ago, within the 100 ms gap
    assert store.lease("a", "two", T + 700, 1000, 100)  # 100 ms ago: the gap is over
    store.release("a", "two")
    store.checked("a", T + 800, "boom")
    assert store.lease("a", "three", T + 801, 1000, 100)  # an error doesn't hold the next check back
    assert store.last_checks() == {"a": T + 800} and store.last_check("a") == T + 800
    assert store.last_check("b") is None and not store.lease("a", "four", T + 900, 1, 0)


def test_state_writes_are_conditional_on_the_tone_they_saw(store):
    s = MAKE.firing("r1")
    assert store.write_state(WS, s, None)
    assert not store.write_state(WS, {**s, "tone": "ok"}, None)  # one there already
    assert not store.write_state(WS, {**s, "tone": "ok"}, "warning")  # it is critical, not warning
    assert store.write_state(WS, {**s, "tone": "ok", "checkedAt": T}, "critical")
    assert store.alert_states(WS) == [{**s, "tone": "ok", "checkedAt": T}]
    assert store.alert_states("other") == []
    store.drop_state(WS, "rule", "r1")
    assert store.alert_states(WS) == []


def test_the_memory_of_a_rule_is_kept_and_its_flags_cleared_once(store):
    add(store, WS, MAKE.rule("r2", createdAt=T - 1), MAKE.memory())
    add(store, WS, MAKE.rule("r1", createdAt=T - 1, causeKey="v1|b"), MAKE.memory())
    add(store, "other", MAKE.rule("r9"), MAKE.memory())
    assert store.workspaces_with_rules() == {WS, "other"}
    assert [x["rule"]["id"] for x in store.rules_with_memory(WS)] == ["r1", "r2"]  # by createdAt, then ID
    store.remember(WS, "r1", {"seen": ["m1", "m2"], "lastNewAt": T, "wasMuted": True, "wasHeld": True},
                   {"label": "x" * 400, "queue": "billing", "causeId": "c7"})
    [r1, _] = store.rules_with_memory(WS)
    assert r1["memory"] == {"seen": ["m1", "m2"], "lastNewAt": T, "wasMuted": True, "wasHeld": True,
                            "regressedSeen": None}
    assert (r1["rule"]["label"], r1["rule"]["queue"], r1["rule"]["causeId"]) == ("x" * 300, "billing", "c7")
    assert store.clear_was_muted(WS, "r1") and not store.clear_was_muted(WS, "r1")
    assert not store.clear_was_muted(WS, "nope")
    assert store.set_regressed_seen(WS, "r1", T) and not store.set_regressed_seen(WS, "r1", T)
    assert store.set_regressed_seen(WS, "r1", T + 1)
    assert store.rules_with_memory(WS)[0]["memory"]["regressedSeen"] == T + 1
    assert not store.set_regressed_seen(WS, "nope", T)
    store.remember(WS, "nope", {"seen": []}, None)  # no such rule: nothing
    store.remember(WS, "r1", {"seen": None, "lastNewAt": None}, None)
    assert store.rules_with_memory(WS)[0]["memory"]["seen"] == []


# ── a NUL in a cause's text (QV-658) ─────────────────────────────────────────────────────────────────────────────

NUL_KEY = "v1|orders|worker|exc:ValueError:invalid literal for int(): '\x00\x01'"
NUL_LABEL = "worker raised ValueError: invalid literal for int(): '\x00\x01'"


def _nul_deps(store, delivered, at=T):
    group = {**MAKE.group(2, key=NUL_KEY), "label": NUL_LABEL}
    return run.Deps(store=store, dashboards=lambda only: {}, data=lambda ws, boards: {},
                    causes=lambda ws: {"groups": [group], "causeOf": {"m1": "c1", "m2": "c1"}, "capped": False},
                    deliver=lambda ws, changes: delivered.append((ws, copy.deepcopy(changes))),
                    now=lambda: at, owner="me")


def test_a_nul_in_a_cause_round_trips_and_its_rule_still_alerts(store):
    # Postgres text refuses NUL: the label, the key and the texts the check writes keep it on every store
    rule = MAKE.rule("r1", causeKey=NUL_KEY, label=NUL_LABEL)
    add(store, WS, rule, MAKE.memory())
    [x] = store.rules_with_memory(WS)
    assert x["rule"] == rule
    delivered: list = []
    result = run.run_alerts(_nul_deps(store, delivered))
    assert result["failed"] == [] and result["changes"] == 1
    [(ws, [change])] = delivered
    assert ws == WS and change["kind"] == "fired"
    [x] = store.rules_with_memory(WS)
    assert x["rule"]["label"] == NUL_LABEL and x["rule"]["causeKey"] == NUL_KEY
    assert x["memory"]["seen"] == ["m1", "m2"]
    store.checked("w\x00", T, "DataError: a\x00b")
    assert store.last_check("w\x00") == T
    if hasattr(store, "rule_for_cause"):
        assert store.rule_for_cause(WS, NUL_KEY)["id"] == "r1"
        assert store.update_rule(WS, "r1", {"mutedBy": "u\x00"})["mutedBy"] == "u\x00"


class _ForgetfulStore:
    """A store whose memory writes fail, as Postgres's did on a NUL before QV-658."""

    def __init__(self, t):
        self.t = t

    def __getattr__(self, name):
        return getattr(self.t, name)

    def remember(self, *a):
        raise RuntimeError("could not write the memory")


def test_a_memory_that_cannot_be_written_does_not_stop_the_changes_being_sent(store):
    add(store, WS, MAKE.rule("r1", causeKey=NUL_KEY), MAKE.memory())
    delivered: list = []
    result = run.run_alerts(_nul_deps(_ForgetfulStore(store), delivered))
    assert [c["kind"] for _, cs in delivered for c in cs] == ["fired"]  # what record() won is sent
    assert result["failed"] == [{"ws": WS, "error": "could not write the memory"}]  # and the workspace says why
    assert store.alert_states(WS)[0]["tone"] == "critical"


# ── the API's reads and writes ───────────────────────────────────────────────────────────────────────────────────

def channel(id_: str, at: int, **o) -> dict:
    return {"id": id_, "kind": "webhook", "name": "Webhook", "target": f"https://hooks.example.com/{id_}",
            "secret": f"whsec_{id_}", "minTone": "warning", "createdBy": "u1", "createdAt": at, "lastAt": None,
            "lastOk": None, "lastDetail": None, **o}


def test_one_rule_per_cause_and_its_state_goes_with_it(config):
    r = MAKE.rule("r1")
    assert config.add_rule(WS, r)
    assert not config.add_rule(WS, {**r, "id": "r2"})  # the same cause
    assert config.add_rule("other", {**r, "id": "r2"})  # another workspace's
    assert config.list_rules(WS) == [r] and config.get_rule(WS, "r1") == r and config.get_rule(WS, "r2") is None
    assert config.rule_for_cause(WS, r["causeKey"]) == r and config.rule_for_cause(WS, "v1|x") is None
    muted = config.update_rule(WS, "r1", {"mutedUntil": run.alerts.FOREVER, "mutedBy": "u2", "mutedAt": T,
                                          "channels": ["ch1", "ch2"]})
    assert muted == {**r, "mutedUntil": run.alerts.FOREVER, "mutedBy": "u2", "mutedAt": T, "channels": ["ch1", "ch2"]}
    assert config.get_rule(WS, "r1") == muted
    assert config.update_rule(WS, "nope", {"mode": "every"}) is None
    assert config.rules_with_memory(WS)[0]["memory"] == {"seen": None, "lastNewAt": None, "wasMuted": False,
                                                       "wasHeld": False, "regressedSeen": None}
    config.write_state(WS, MAKE.firing("r1"), None)
    config.write_state(WS, {**MAKE.firing("w1"), "dashboardId": "d1"}, None)
    assert config.remove_rule(WS, "r1") and not config.remove_rule(WS, "r1")
    assert [s["widgetId"] for s in config.alert_states(WS)] == ["w1"]
    assert config.workspaces_with_rules() == {"other"}


def test_channels_keep_their_target_and_secret_for_delivery(config):
    assert config.workspaces_with_channels() == set()
    config.add_channel(WS, channel("ch2", T + 1))
    config.add_channel(WS, channel("ch1", T, kind="email", target="ops@example.com", secret=None))
    assert [c["id"] for c in config.list_channels(WS)] == ["ch1", "ch2"]  # oldest first
    assert config.get_channel(WS, "ch2") == channel("ch2", T + 1)
    assert config.update_channel(WS, "ch2", {"name": "Pager", "minTone": "critical"}) == channel(
        "ch2", T + 1, name="Pager", minTone="critical")
    assert config.update_channel(WS, "nope", {"name": "x"}) is None
    config.note_delivery(WS, "ch2", T + 5, False, "e" * 400)
    after = config.get_channel(WS, "ch2")
    assert (after["lastAt"], after["lastOk"], after["lastDetail"]) == (T + 5, False, "e" * 300)
    config.note_delivery(WS, "ch2", T + 6, True, None)
    assert config.get_channel(WS, "ch2")["lastOk"] is True
    assert config.workspaces_with_channels() == {WS}
    assert config.remove_channel(WS, "ch1") and not config.remove_channel(WS, "ch1")
    assert [c["id"] for c in config.list_channels(WS)] == ["ch2"] and config.get_channel(WS, "ch1") is None
    try:
        from trailbeam.server.alerts_api import channel_view
    except ImportError:
        return
    shown = json.dumps([channel_view(c) for c in config.list_channels(WS)])
    assert "whsec_" not in shown and "hooks.example.com/ch2" not in shown


@pytest.mark.parametrize("kind", ["sqlite", pytest.param("postgres", marks=needs_pg)])
def test_unknown_fields_are_refused_not_dropped(kind, tmp_path):
    o = Opened(kind, tmp_path)
    try:
        with pytest.raises(ValueError, match="no such rule field: colour"):
            o.tables.add_rule(WS, {**MAKE.rule(), "colour": "red"})
        with pytest.raises(ValueError, match="no such channel field: to"):
            o.tables.update_channel(WS, "ch1", {"to": "x"})
    finally:
        o.close()


# ── kept across restarts, and two checks at once ─────────────────────────────────────────────────────────────────

def test_a_sqlite_file_keeps_the_alerts_when_the_server_starts_again(tmp_path):
    path = str(tmp_path / "trailbeam.sqlite")
    db = Database(path)
    t = AlertTables(db)
    t.add_rule(WS, MAKE.rule())
    t.add_channel(WS, channel("ch1", T))
    t.write_state(WS, MAKE.firing(), None)
    t.checked(WS, T, None)
    db.close()
    db = Database(path)
    try:
        t = AlertTables(db)
        assert t.list_rules(WS) == [MAKE.rule()] and t.list_channels(WS) == [channel("ch1", T)]
        assert t.alert_states(WS) == [MAKE.firing()] and t.last_check(WS) == T
    finally:
        db.close()


def test_the_tables_are_in_a_new_file_and_hold_no_spans(tmp_path):
    db = Database(str(tmp_path / "trailbeam.sqlite"))
    try:
        names = {r["name"] for r in db.query("SELECT name FROM sqlite_master WHERE type = 'table'")}
        assert set(alerts_store.TABLES) <= names
    finally:
        db.close()


@needs_pg
def test_of_two_postgres_stores_leasing_at_once_exactly_one_gets_it():
    """Two stores, each with pools of its own (as two servers on one database), several threads each."""
    from trailbeam.server.postgres import PostgresStore

    first = pg_store()
    second = PostgresStore(first.url)
    try:
        for round_ in range(5):
            stores = [AlertTables(first), AlertTables(second)] * 4
            go = threading.Barrier(len(stores))
            won: list = []

            def take(i, t, go=go, won=won, ws=f"ws{round_}"):
                go.wait()
                if t.lease(ws, f"owner{i}", T, run.LEASE_MS, run.MIN_GAP_MS):
                    won.append(i)

            threads = [threading.Thread(target=take, args=(i, t)) for i, t in enumerate(stores)]
            for th in threads:
                th.start()
            for th in threads:
                th.join()
            assert len(won) == 1, won
            # and of two checks recording the same change, one wins it
            go2 = threading.Barrier(2)
            wrote: list = []

            def write(t, go2=go2, wrote=wrote, ws=f"ws{round_}"):
                go2.wait()
                wrote.append(t.write_state(ws, MAKE.firing(), None))

            pair = [threading.Thread(target=write, args=(AlertTables(s),)) for s in (first, second)]
            for th in pair:
                th.start()
            for th in pair:
                th.join()
            assert sorted(wrote) == [False, True]
    finally:
        second.close()
        first.close()


@needs_pg
def test_a_postgres_database_at_19_gets_the_alert_tables_a_new_one_has():
    from trailbeam.server.postgres import PostgresStore

    def columns(c) -> dict:
        return {t: sorted(tuple(r.values()) for r in c.execute(
            """SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns
               WHERE table_schema = current_schema() AND table_name = %s""", (t,)).fetchall())
            for t in alerts_store.TABLES}

    s = pg_store()
    try:
        with s.ingest.connection() as c:
            new = columns(c)
            c.execute("DROP TABLE alert_rules, alert_channels, alert_state, alert_checks")
            c.execute("UPDATE schema_version SET version = 19")
        again = PostgresStore(s.url)
        try:
            assert again.migrated == [20]
            with again.ingest.connection() as c:
                assert columns(c) == new and all(new.values())
        finally:
            again.close()
    finally:
        s.close()
