"""Schema versions and upgrades (migrations.py): a file written by an older Trailbeam opens with every span
kept and the same timelines, a file from a newer one is refused untouched, and a failed upgrade leaves the
file at the last version that completed."""
import importlib.util
import json
import shutil
import sqlite3
from pathlib import Path

import pytest

from trailbeam.server import body_table, migrations, queries
from trailbeam.server.db import Database
from trailbeam.server.migrations import LATEST, Migration, SchemaError, SchemaTooNew
from trailbeam.server.otlp import decode

HERE = Path(__file__).resolve().parent
SCHEMA_5 = HERE / "fixtures" / "schema-5.sqlite"  # written by Trailbeam 0.1 (fixtures/make-schema-5.py)
NOW = 1_763_090_000_000_000_000

# Schema 1, as Trailbeam created it before start_ns existed.
SCHEMA_1 = """
CREATE TABLE spans (row INTEGER PRIMARY KEY, trace_id TEXT NOT NULL, span_id TEXT NOT NULL,
    kind TEXT NOT NULL, time_ns INTEGER NOT NULL, service_name TEXT, source TEXT, operation TEXT,
    queue TEXT NOT NULL, queue_url TEXT, message_id TEXT, receipt_hmac TEXT, key_id TEXT,
    receive_count INTEGER, sent_ts_ms INTEGER, first_receive_ts_ms INTEGER, result TEXT, error_code TEXT,
    visibility_timeout INTEGER, outcome TEXT, error_type TEXT, batch_entry_id TEXT, client_id TEXT,
    pid INTEGER, thread_name TEXT, faas_name TEXT, faas_invocation_id TEXT, attributes TEXT NOT NULL,
    mid TEXT, mid_source TEXT, receive_row INTEGER REFERENCES spans(row), UNIQUE (trace_id, span_id));
CREATE INDEX spans_mid ON spans (mid, time_ns);
CREATE INDEX spans_hash ON spans (receipt_hmac, key_id);
CREATE INDEX spans_kind_time ON spans (kind, time_ns);
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
INSERT INTO meta VALUES ('schema_version', '1');
INSERT INTO spans (trace_id, span_id, kind, time_ns, queue, message_id, attributes, mid)
    VALUES ('t', 's', 'send', 5, 'orders', 'm1', '{}', 'm1');
"""


def raw(path) -> list[dict]:
    """Every span as it arrived: from schema 10 its body is back in ``attributes`` (body_table.py), which are
    compared parsed, and ``body_id``, where the body is kept, is left out, as is a ``service_version`` (schema 16)
    that is NULL: a row stored before 16 gets one, which says the version was not kept."""
    c = sqlite3.connect(path)
    c.row_factory = sqlite3.Row
    try:
        if c.execute("SELECT 1 FROM sqlite_master WHERE name = 'bodies'").fetchone():
            sql = body_table.rows_sql(body_table.span_columns(c)) + " ORDER BY s.row"
        else:
            sql = "SELECT * FROM spans ORDER BY row"
        return [{k: json.loads(r[k]) if k == "attributes" else r[k] for k in r.keys()
                 if k != "body_id" and not (k == "service_version" and r[k] is None)}
                for r in c.execute(sql)]
    finally:
        c.close()


def schema(path) -> dict:
    """Columns (name, type, not null, primary key) and indexes of a file, in a form that ignores column order:
    ALTER TABLE ADD COLUMN appends, so a migrated file has the columns in another order than a new one."""
    c = sqlite3.connect(path)
    try:
        tables = [r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")]
        return {
            "tables": {t: sorted((r[1], r[2], r[3], r[5]) for r in c.execute(f"PRAGMA table_info({t})")) for t in tables},
            "indexes": sorted((r[0], r[1]) for r in c.execute(
                "SELECT name, tbl_name FROM sqlite_master WHERE type = 'index' AND name NOT LIKE 'sqlite_autoindex%'")),
            "index_columns": {i: [r[2] for r in c.execute(f"PRAGMA index_info({i})")]
                              for i in ("spans_identity", "spans_receive_hash", "spans_unjoined_hash",
                                        "spans_kind_time", "messages_last", "messages_stuck",
                                        "message_tenants_search",
                                        "spans_receive_row")},
        }
    finally:
        c.close()


def version_rows(path) -> list[tuple]:
    c = sqlite3.connect(path)
    try:
        return c.execute("SELECT version, name, applied_at_ms IS NOT NULL FROM schema_version ORDER BY version").fetchall()
    finally:
        c.close()


def meta_version(path) -> str:
    c = sqlite3.connect(path)
    try:
        return c.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()[0]
    finally:
        c.close()


def file_at(tmp_path, version: int) -> str:
    """A schema 1 file, upgraded to ``version`` the way the Trailbeam at that version would have left it."""
    path = str(tmp_path / f"v{version}.sqlite")
    c = sqlite3.connect(path)
    c.executescript(SCHEMA_1)
    for m in migrations.MIGRATIONS:
        if m.version <= version:
            for sql in m.statements:
                c.execute(sql)
            if m.run is not None:
                m.run(c)
            migrations._record(c, m.version, m.name)  # as open_schema does: meta, and schema_version from 6
    c.commit()
    c.close()
    return path


def timelines(db: Database) -> dict:
    mids = [r["mid"] for r in db.query("SELECT DISTINCT mid FROM spans WHERE mid IS NOT NULL ORDER BY mid")]
    return {m: queries.message(db, m, NOW, 900) for m in mids}


def summaries(db: Database) -> dict:
    """Every row of the summary tables (summary_table.py), sorted."""
    from trailbeam.server.summary_table import TABLES
    return {t: sorted(tuple(r) for r in db.query(f"SELECT * FROM {t}")) for t in TABLES}


def _generator():
    spec = importlib.util.spec_from_file_location("make_schema_5", HERE / "fixtures" / "make-schema-5.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---- a file from Trailbeam 0.1 ---------------------------------------------------------------------

@pytest.fixture
def from_main(tmp_path) -> str:
    path = str(tmp_path / "trailbeam.sqlite")
    shutil.copyfile(SCHEMA_5, path)
    return path


def test_the_fixture_is_a_schema_5_file():
    assert meta_version(SCHEMA_5) == "5"
    c = sqlite3.connect(SCHEMA_5)
    assert "schema_version" not in {r[0] for r in c.execute("SELECT name FROM sqlite_master")}
    c.close()
    assert len(raw(SCHEMA_5)) == 37


def test_a_file_from_main_upgrades_in_place_and_keeps_every_span(from_main):
    before = raw(from_main)
    db = Database(from_main, store_bodies=frozenset({"orders"}))
    assert db.migrated == [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26]
    db.close()
    assert raw(from_main) == before  # every row, every column, same row IDs
    assert meta_version(from_main) == str(LATEST)
    assert version_rows(from_main) == [(5, "before schema_version", False), (6, "schema_version table", True),
                                       (7, "message summary tables", True), (8, "spans_receive_row index", True),
                                       (9, "summary tables keyed for the pages", True),
                                       (10, "bodies stored once", True),
                                       (11, "spans indexes at the right edge", True),
                                       (12, "hourly tenant rollup", True), (13, "probes (trailbeam doctor)", True),
                                       (14, "FIFO deduplicated sends out of the tenant events", True),
                                       (15, "users, sessions and tokens", True),
                                       (16, "spans.service_version", True),
                                       (17, "queue events (the queue page)", True),
                                       (18, "users.proxy_linked_at_ms", True),
                                       (19, "hourly step rollup (/api/events)", True),
                                       (20, "SNS fan-out indexes", True),
                                       (21, "OpenID Connect sign-ins and identities", True),
                                       (22, "RabbitMQ settle kinds in the unjoined settle index", True),
                                       (23, "EventBridge PutEvents indexes", True),
                                       (24, "S3 upload indexes", True),
                                       (25, "S3 receives by decoded object key", True),
                                       (26, "alert rules, channels, state and checks", True)]
    assert schema(from_main) == schema_new_file(from_main + ".new")


def test_schema_19_builds_the_step_rollup_from_the_spans(from_main):
    """The backfill gives what ingest keeps: per (hour, queue, service, kind), the stored steps, no purges."""
    Database(from_main, store_bodies=frozenset({"orders"})).close()
    c = sqlite3.connect(from_main)
    try:
        got = sorted(c.execute("SELECT hour, queue, service, kind, n FROM step_rollup").fetchall())
        want = sorted(c.execute("""SELECT time_ns / 3600000000000, queue, COALESCE(service_name, ''), kind, COUNT(*)
                                   FROM spans WHERE kind <> 'purge' GROUP BY 1, 2, 3, 4""").fetchall())
    finally:
        c.close()
    assert got == want and sum(r[4] for r in got) > 0


def test_schema_10_takes_the_bodies_out_of_the_spans_and_keeps_each_once(from_main):
    before = {r["row"]: r["attributes"].get("trailbeam.body") for r in raw(from_main)}
    assert sum(b is not None for b in before.values()) >= 4  # the 0.1 file has bodies on sends and receives
    Database(from_main, store_bodies=frozenset({"orders"})).close()
    c = sqlite3.connect(from_main)
    try:
        assert c.execute("""SELECT COUNT(*) FROM spans
                            WHERE json_type(attributes, '$."trailbeam.body"') IS NOT NULL""").fetchone()[0] == 0
        carried = c.execute("SELECT row, mid, body_id FROM spans WHERE body_id IS NOT NULL").fetchall()
        stored = c.execute("SELECT id, mid, body FROM bodies").fetchall()
    finally:
        c.close()
    assert {row for row, *_ in carried} == {row for row, b in before.items() if b is not None}
    by_id = {i: (m, b) for i, m, b in stored}
    for row, mid, body_id in carried:
        assert by_id[body_id] == (mid, before[row])
    # the send and the receive of a message carried the same text: one row
    assert len(stored) == len({(m, b) for m, b in by_id.values()}) < len(carried)


def test_schema_25_lists_the_s3_receives_stored_before_it(tmp_path):
    """QV-563: a file at 24 has its S3 receives only in spans; the upgrade puts each in s3_receives, key decoded."""
    import sys
    sys.path.insert(0, str(HERE))
    from spans import SEC, T0, Consumer, to_proto

    path = str(tmp_path / "s3.sqlite")
    db = Database(path, store_bodies=frozenset({"orders"}))
    et = T0 // 1_000_000 + 800
    spans = [Consumer().s3_receive(T0 + 3 * SEC, f"sqs-{k}", f"rh-{k}", event_time_ms=et, sent_ms=et + 300,
                                   start=T0 + 2 * SEC, key=k, etag="9dd4e461268c8034f5c8564e155c67a6")
             for k in ("a+dir%2Fred.jpg", "other.jpg")]
    db.insert(decode(to_proto(spans).SerializeToString(), "application/x-protobuf").records)
    want = [(r["mid"], r["event_time_ms"]) for r in db.s3_receivers("photos", "a dir/red.jpg", 0, 2 ** 62)]
    db.close()
    assert want == [("sqs-a+dir%2Fred.jpg", et)]
    c = sqlite3.connect(path, isolation_level=None)  # back to 24, as 24 left it
    c.executescript(f"""DROP TABLE s3_receives; {migrations.S3_INDEXES}
                        UPDATE schema_version SET version = 24 WHERE version = 26;
                        UPDATE meta SET value = '24' WHERE key = 'schema_version';""")
    c.close()
    db = Database(path, store_bodies=frozenset({"orders"}))
    try:
        assert db.migrated == [25, 26]
        assert [(r["mid"], r["event_time_ms"]) for r in db.s3_receivers("photos", "a dir/red.jpg", 0, 2 ** 62)] == want
        assert db.query("SELECT COUNT(*) AS n FROM s3_receives")[0]["n"] == 2
    finally:
        db.close()
    assert schema(path) == schema_new_file(str(tmp_path / "new.sqlite"))


def schema_new_file(path) -> dict:
    Database(path).close()
    return schema(path)


def test_an_upgraded_file_gives_the_timelines_a_new_file_gives(from_main, tmp_path):
    """The same spans, ingested into a new file by this version, give the same timelines and Body tabs as the
    upgraded 0.1 file: nothing about a message changed on the way."""
    fresh = str(tmp_path / "fresh.sqlite")
    _generator().main(fresh)  # this version's insert, the same spans and store_bodies
    upgraded = Database(from_main, store_bodies=frozenset({"orders"}))
    new = Database(fresh, store_bodies=frozenset({"orders"}))
    try:
        assert len(timelines(upgraded)) > 10
        assert timelines(upgraded) == timelines(new)
        assert summaries(upgraded) == summaries(new)  # the backfill built what insert builds
        assert len(summaries(new)["messages"]) == len(timelines(new))
        from trailbeam.server import bodies
        for m in timelines(new):
            assert (bodies.body_view(m, upgraded.message_rows(m), upgraded.store_bodies)
                    == bodies.body_view(m, new.message_rows(m), new.store_bodies)), m
    finally:
        upgraded.close()
        new.close()


def test_an_upgraded_file_takes_new_spans_and_joins_them_to_old_ones(from_main):
    import sys
    sys.path.insert(0, str(HERE))
    from spans import SEC, T0, Consumer, to_proto

    db = Database(from_main)
    [unjoined] = db.query("SELECT row FROM spans WHERE kind = 'delete' AND receive_row IS NULL")
    c = Consumer(client_id="c-after-upgrade")
    db.insert(decode(to_proto([c.receive(T0 + 3 * SEC, "m-late-receive", "rh-never-received")]).SerializeToString(),
                     "application/x-protobuf").records)
    [joined] = db.query("SELECT mid, mid_source FROM spans WHERE row = ?", (unjoined["row"],))
    assert (joined["mid"], joined["mid_source"]) == ("m-late-receive", "receipt_hash")
    assert db.counts()["unjoined_settles"] == 0
    db.close()


def test_opening_again_does_not_migrate_again(from_main):
    Database(from_main).close()
    db = Database(from_main)
    assert db.migrated == []
    db.close()
    assert [v for v, *_ in version_rows(from_main)] == list(range(5, LATEST + 1))


# ---- every older version ---------------------------------------------------------------------------

@pytest.mark.parametrize("version", range(migrations.FIRST, LATEST))
def test_every_older_version_upgrades_to_the_schema_of_a_new_file(tmp_path, version):
    path = file_at(tmp_path, version)
    db = Database(path)
    assert db.migrated == list(range(version + 1, LATEST + 1))
    [row] = db.message_rows("m1")
    db.close()
    assert (row["time_ns"], row["queue"], row["attributes"]) == (5, "orders", "{}")
    assert schema(path) == schema_new_file(str(tmp_path / "new.sqlite"))
    assert meta_version(path) == str(LATEST)


def test_a_schema_1_file_gets_nulls_for_what_it_never_recorded(tmp_path):
    db = Database(file_at(tmp_path, 1))
    [row] = db.message_rows("m1")
    db.close()
    assert (row["time_ns"], row["start_ns"]) == (5, None)
    assert (row["order_scope"], row["order_issued"], row["order_returned"]) == (None, None, None)
    assert row["dlq_source_arn"] is None
    assert row["body_dropped"] is None


def test_a_new_file_starts_at_the_latest_version(tmp_path):
    path = str(tmp_path / "new.sqlite")
    db = Database(path)
    assert db.migrated == []
    db.close()
    assert version_rows(path) == [(LATEST, "create", True)]
    assert meta_version(path) == str(LATEST)


def test_memory_databases_get_the_same_schema():
    db = Database(":memory:")
    assert db.query("SELECT MAX(version) AS v FROM schema_version")[0]["v"] == LATEST
    db.close()


def test_a_start_killed_before_the_version_was_written_is_created_again(tmp_path):
    """Trailbeam before schema 6 committed meta, the tables and the version row separately."""
    path = str(tmp_path / "half.sqlite")
    c = sqlite3.connect(path)
    c.executescript("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);"
                    "CREATE TABLE spans (row INTEGER PRIMARY KEY, attributes TEXT);")
    c.close()
    Database(path).close()
    assert schema(path) == schema_new_file(str(tmp_path / "new.sqlite"))


# ---- refused ---------------------------------------------------------------------------------------

def test_a_file_from_a_newer_trailbeam_is_refused_and_left_as_it_was(from_main):
    Database(from_main).close()
    c = sqlite3.connect(from_main)
    c.execute("INSERT INTO schema_version (version, applied_at_ms, name) VALUES (?, 0, 'from the future')", (LATEST + 1,))
    c.execute("UPDATE meta SET value = ?", (str(LATEST + 1),))
    c.commit()
    c.close()
    before = Path(from_main).read_bytes()
    with pytest.raises(SchemaTooNew, match=f"schema version {LATEST + 1}, written by a newer Trailbeam"):
        Database(from_main)
    assert Path(from_main).read_bytes() == before
    assert not Path(from_main + "-wal").exists()


def test_the_command_exits_with_the_reason_on_a_newer_file(from_main, caplog):
    from trailbeam.server.__main__ import main

    c = sqlite3.connect(from_main)
    c.execute("UPDATE meta SET value = '99'")
    c.commit()
    c.close()
    with pytest.raises(SystemExit) as e:
        main(["--db", from_main, "--port", "0"])
    assert e.value.code == 2
    assert "schema version 99, written by a newer Trailbeam" in caplog.text


def test_an_older_trailbeam_would_refuse_an_upgraded_file(from_main):
    """Trailbeam 0.1 reads meta.schema_version and stops on a version it has no migration for: keeping that
    row current is what stops it from writing into a file it cannot read."""
    Database(from_main).close()
    assert int(meta_version(from_main)) > 5


@pytest.mark.parametrize("script, message", [
    ("CREATE TABLE something_else (x);", "not a Trailbeam database"),
    ("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL); INSERT INTO meta VALUES ('schema_version', 'x');",
     "is not a number"),
    ("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL); INSERT INTO meta VALUES ('schema_version', '0');",
     "older than any Trailbeam release"),
    ("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL); CREATE TABLE spans (row INTEGER PRIMARY KEY);"
     "INSERT INTO spans VALUES (1);", "no schema version"),
])
def test_files_it_cannot_place_are_refused(tmp_path, script, message):
    path = str(tmp_path / "other.sqlite")
    c = sqlite3.connect(path)
    c.executescript(script)
    c.close()
    with pytest.raises(SchemaError, match=message):
        Database(path)


# ---- a failed migration ----------------------------------------------------------------------------

def test_a_failed_migration_leaves_the_file_at_the_last_version_that_completed(from_main, monkeypatch):
    before = raw(from_main)
    broken = Migration(LATEST + 1, "broken", ("ALTER TABLE spans ADD COLUMN half_done TEXT", "SELECT * FROM missing"))
    monkeypatch.setattr(migrations, "MIGRATIONS", migrations.MIGRATIONS + (broken,))
    monkeypatch.setattr(migrations, "LATEST", LATEST + 1)
    with pytest.raises(SchemaError, match=f"at schema version {LATEST}.*no such table: missing"):
        Database(from_main)
    assert meta_version(from_main) == str(LATEST)  # 5 -> LATEST completed, the broken one rolled back
    assert [v for v, *_ in version_rows(from_main)] == list(range(5, LATEST + 1))
    assert "half_done" not in raw(from_main)[0]
    assert raw(from_main) == before

    fixed = Migration(LATEST + 1, "fixed", ("ALTER TABLE spans ADD COLUMN half_done TEXT",))
    monkeypatch.setattr(migrations, "MIGRATIONS", migrations.MIGRATIONS[:-1] + (fixed,))
    db = Database(from_main)  # the next start carries on from LATEST
    assert db.migrated == [LATEST + 1]
    db.close()
    assert [r["half_done"] for r in raw(from_main)] == [None] * len(before)


def test_migrations_go_up_one_version_at_a_time():
    assert [m.version for m in migrations.MIGRATIONS] == list(range(migrations.FIRST + 1, LATEST + 1))
    assert migrations.statements(migrations.CREATE)  # the create script splits into whole statements
