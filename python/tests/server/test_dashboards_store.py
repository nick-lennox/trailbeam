"""The dashboard tables (dashboards_store.py; SQLite schema 27, Postgres schema 21): the same store tests on the
in-memory store (MemoryDashboards), SQLite (a file and :memory:) and Postgres, and two changes made from one read
of which exactly one is stored. Upgrades from every older version are in test_migrations.py and test_postgres.py.

The Postgres cases run when TRAILBEAM_TEST_POSTGRES_URL names a database, as in test_postgres.py."""
import os
import sys
import threading
from pathlib import Path

import pytest
from trailbeam.server import dashboards_store, migrations, pg_schema
from trailbeam.server.dashboards_store import DashboardTables, MemoryDashboards
from trailbeam.server.db import Database

ROOT = Path(__file__).resolve().parents[2]
PG = os.environ.get("TRAILBEAM_TEST_POSTGRES_URL")
needs_pg = pytest.mark.skipif(not PG, reason="TRAILBEAM_TEST_POSTGRES_URL is not set")
WS = "local"
LAYOUT = {"x": 0, "y": 0, "w": 3, "h": 2}


def pg_store():
    sys.path.insert(0, str(ROOT / "tests" / "conformance"))
    from pg_helpers import postgres_store
    return postgres_store()


class Opened:
    def __init__(self, kind: str, tmp_path):
        self.kind, self.backing = kind, None
        if kind == "memory":
            self.store = MemoryDashboards()
        else:
            self.backing = {"sqlite": lambda: Database(str(tmp_path / "trailbeam.sqlite")),
                            "sqlite-memory": lambda: Database(":memory:"), "postgres": pg_store}[kind]()
            self.store = DashboardTables(self.backing)

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
    return opened.store


def widget(id_, title="DLQ depth", **over):
    return {"id": id_, "type": "number", "title": title, "queries": [{"measure": "dlq_depth"}], "layout": LAYOUT,
            **over}


def dash(id_, name="Orders", at=1000, widgets=None, **over):
    return {"id": id_, "name": name, "time": None, "starred": False, "lockedBy": None, "lockedById": None,
            "createdBy": "Ada", "createdById": "1", "updatedBy": "Ada", "createdAt": at, "updatedAt": at,
            "def": {"variables": [{"name": "queue", "field": "queue", "value": "*"}],
                    "sections": [{"id": "s1", "title": "Backlog", "color": "blue"}],
                    "widgets": widgets if widgets is not None else [widget("w1"), widget("w2", "Age")]}, **over}


def test_a_dashboard_reads_back_as_stored(store):
    d = dash("a1", widgets=[widget("w1", rule={"crit": 0}, extra={"kept": [1, "two"]}), widget("w2", "Ünïcode")])
    assert store.add_dashboard(WS, d)
    got = store.get_dashboard(WS, "a1")
    assert got == {**d, "version": 1}
    assert store.list_dashboards(WS) == [got]
    assert store.get_dashboard(WS, "nope") is None
    assert store.get_dashboard("elsewhere", "a1") is None
    assert store.list_dashboards("elsewhere") == []


def test_an_id_is_stored_once(store):
    assert store.add_dashboard(WS, dash("a1"))
    assert not store.add_dashboard(WS, dash("a1", name="Again"))
    assert store.get_dashboard(WS, "a1")["name"] == "Orders"
    assert store.add_dashboard("other", dash("a1", name="Theirs"))  # another workspace's
    assert store.all_dashboards() == {WS: [store.get_dashboard(WS, "a1")], "other": [store.get_dashboard("other", "a1")]}


def test_empty_definitions_and_widgets_in_their_order(store):
    assert store.add_dashboard(WS, dash("e", widgets=[]))
    assert store.get_dashboard(WS, "e")["def"]["widgets"] == []
    ws = [widget(f"w{i}", f"#{i}") for i in (3, 1, 2)] + [widget("w1", "the same ID again")]
    assert store.add_dashboard(WS, dash("o", widgets=ws))
    assert store.get_dashboard(WS, "o")["def"]["widgets"] == ws


def test_starred_first_then_the_latest_changed(store):
    for id_, at, starred in (("old", 1, False), ("new", 3, False), ("mid", 2, False), ("star", 0, True)):
        store.add_dashboard(WS, dash(id_, at=at, starred=starred))
    assert [d["id"] for d in store.list_dashboards(WS)] == ["star", "new", "mid", "old"]


def test_update_sets_what_it_is_given_and_counts_versions(store):
    store.add_dashboard(WS, dash("a1"))
    new_def = {"variables": [], "sections": [], "widgets": [widget("w9", "Only")]}
    out, d = store.update_dashboard(WS, "a1", {"name": "Renamed", "def": new_def, "time": "7d", "updatedBy": "Ben",
                                              "updatedAt": 2000})
    assert out == "ok" and d == store.get_dashboard(WS, "a1")
    assert (d["name"], d["def"], d["time"], d["updatedBy"], d["updatedAt"], d["version"]) == \
        ("Renamed", new_def, "7d", "Ben", 2000, 2)
    assert d["createdBy"] == "Ada" and d["createdAt"] == 1000
    out, d = store.update_dashboard(WS, "a1", {"starred": True})
    assert out == "ok" and d["starred"] is True and d["def"] == new_def and d["version"] == 3
    assert store.update_dashboard(WS, "nope", {"starred": True}) == ("missing", None)
    with pytest.raises(ValueError, match="no such dashboard field: owner"):
        store.update_dashboard(WS, "a1", {"owner": "x"})


def test_a_locked_dashboard_changes_only_when_the_caller_allows_it(store):
    store.add_dashboard(WS, dash("a1"))
    d = store.lock_dashboard(WS, "a1", "Ben", "2", 5)
    assert (d["lockedBy"], d["lockedById"]) == ("Ben", "2")
    again = store.lock_dashboard(WS, "a1", "Cy", "3", 6)  # no one takes over a lock
    assert (again["lockedBy"], again["lockedById"]) == ("Ben", "2")
    assert store.update_dashboard(WS, "a1", {"name": "No"}, unlocked=True) == ("locked", None)
    assert store.remove_dashboard(WS, "a1", unlocked=True) == "locked"
    out, d = store.update_dashboard(WS, "a1", {"starred": True})  # a star always
    assert out == "ok" and d["name"] == "Orders"
    out, d = store.update_dashboard(WS, "a1", {"lockedBy": None, "lockedById": None})
    assert out == "ok" and d["lockedBy"] is None and d["lockedById"] is None
    assert store.update_dashboard(WS, "a1", {"name": "Yes"}, unlocked=True)[0] == "ok"
    assert store.lock_dashboard(WS, "nope", "Ben", "2", 5) is None


def test_a_change_from_an_old_read_is_refused(store):
    store.add_dashboard(WS, dash("a1"))
    v = store.get_dashboard(WS, "a1")["version"]
    assert store.update_dashboard(WS, "a1", {"name": "First"}, version=v)[0] == "ok"
    assert store.update_dashboard(WS, "a1", {"name": "Second"}, version=v) == ("conflict", None)
    assert store.get_dashboard(WS, "a1")["name"] == "First"


def test_remove_takes_the_widgets_too(opened, store):
    store.add_dashboard(WS, dash("a1"))
    store.add_dashboard(WS, dash("a2"))
    assert store.remove_dashboard(WS, "a1") == "ok"
    assert store.remove_dashboard(WS, "a1") == "missing"
    assert [d["id"] for d in store.list_dashboards(WS)] == ["a2"]
    if opened.kind.startswith("sqlite"):
        assert opened.backing.query("SELECT COUNT(*) AS n FROM dashboard_widgets WHERE dashboard_id = 'a1'")[0]["n"] == 0


def test_of_two_changes_from_one_read_exactly_one_is_stored(store):
    """Each thread reads the version and writes its own widget list at it: one wins, the others conflict."""
    store.add_dashboard(WS, dash("a1"))
    v = store.get_dashboard(WS, "a1")["version"]
    out: list = []
    go = threading.Barrier(4)

    def change(i):
        go.wait()
        out.append(store.update_dashboard(WS, "a1", {"def": {"widgets": [widget(f"t{i}")]}}, unlocked=True,
                                          version=v)[0])

    ts = [threading.Thread(target=change, args=(i,)) for i in range(4)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert sorted(out) == ["conflict"] * 3 + ["ok"]
    [w] = store.get_dashboard(WS, "a1")["def"]["widgets"]
    assert w["id"].startswith("t")


def test_a_nul_in_any_text_is_stored_and_found_on_every_store(store):
    """QV-686: Postgres text refuses NUL (0x00), which a name, a widget's ID, a display name and a path's dashboard
    ID can hold. Every store keeps it as given and finds a dashboard by an ID that holds one; on Postgres this was a
    DataError, so a 500."""
    nul = dash("a\x00b", name="Ord\x00ers", createdBy="A\x00da", updatedBy="A\x00da",
               widgets=[widget("w\x00", "DLQ\x00depth")])
    assert store.add_dashboard(WS, nul) is True
    assert store.add_dashboard(WS, nul) is False
    d = store.get_dashboard(WS, "a\x00b")
    assert d == {**nul, "version": 1}
    assert store.get_dashboard(WS, "a") is None and store.get_dashboard(WS, "a\x00c") is None
    assert store.get_dashboard("w\x00s", "a\x00b") is None and store.list_dashboards("w\x00s") == []
    assert store.update_dashboard(WS, "x\x00", {"name": "N"}) == ("missing", None)
    assert store.lock_dashboard(WS, "x\x00", "B\x00en", "2", 2000) is None
    assert store.remove_dashboard(WS, "x\x00") == "missing"

    locked = store.lock_dashboard(WS, "a\x00b", "B\x00en", "2\x00", 2000)
    assert locked["lockedBy"] == "B\x00en" and locked["lockedById"] == "2\x00"
    assert store.update_dashboard(WS, "a\x00b", {"name": "x\x00"}, unlocked=True) == ("locked", None)
    out, d = store.update_dashboard(WS, "a\x00b", {"lockedBy": None, "lockedById": None, "name": "N\x00",
                                                    "updatedBy": "B\x00en",
                                                    "def": {"widgets": [widget("v\x00")]}})
    assert out == "ok" and d["name"] == "N\x00" and d["updatedBy"] == "B\x00en" and d["lockedBy"] is None
    assert [w["id"] for w in d["def"]["widgets"]] == ["v\x00"]
    assert [x["id"] for x in store.list_dashboards(WS)] == ["a\x00b"]
    assert store.all_dashboards() == {WS: [d]}
    assert store.remove_dashboard(WS, "a\x00b") == "ok" and store.get_dashboard(WS, "a\x00b") is None


def test_the_tables_are_in_a_new_file_and_in_the_schema_text():
    db = Database(":memory:")
    try:
        names = {r["name"] for r in db.query("SELECT name FROM sqlite_master WHERE type = 'table'")}
        assert set(dashboards_store.TABLES) <= names
    finally:
        db.close()
    assert "BIGINT" in dashboards_store.PG_CREATE and "starred SMALLINT" in dashboards_store.PG_CREATE


def test_a_sqlite_file_at_26_gets_the_dashboard_tables_a_new_one_has(tmp_path):
    import sqlite3
    path = str(tmp_path / "t.sqlite")
    Database(path).close()

    def cols(c):
        return {t: sorted(tuple(r) for r in c.execute(f"PRAGMA table_info({t})").fetchall())
                for t in dashboards_store.TABLES}

    c = sqlite3.connect(path, isolation_level=None)
    new = cols(c)
    c.executescript(f"""DROP TABLE dashboards; DROP TABLE dashboard_widgets;
                       UPDATE schema_version SET version = 26 WHERE version = {migrations.LATEST};
                       UPDATE meta SET value = '26' WHERE key = 'schema_version';""")
    c.close()
    db = Database(path)
    try:
        assert db.migrated == list(range(27, migrations.LATEST + 1))  # 28 on: later tables, already there
        assert DashboardTables(db).add_dashboard(WS, dash("a1"))
    finally:
        db.close()
    c = sqlite3.connect(path)
    try:
        assert cols(c) == new and all(new.values())
    finally:
        c.close()


@needs_pg
def test_a_postgres_database_at_20_gets_the_dashboard_tables_a_new_one_has():
    from trailbeam.server.postgres import PostgresStore

    def cols(c) -> dict:
        return {t: sorted(tuple(r.values()) for r in c.execute(
            """SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns
               WHERE table_schema = current_schema() AND table_name = %s""", (t,)).fetchall())
            for t in dashboards_store.TABLES}

    s = pg_store()
    try:
        with s.ingest.connection() as c:
            new = cols(c)
            c.execute("DROP TABLE dashboards, dashboard_widgets")
            c.execute("UPDATE schema_version SET version = 20")
        again = PostgresStore(s.url)
        try:
            assert again.migrated == list(range(21, pg_schema.LATEST + 1))
            with again.ingest.connection() as c:
                assert cols(c) == new and all(new.values())
            assert DashboardTables(again).add_dashboard(WS, dash("a1"))
        finally:
            again.close()
    finally:
        s.close()
