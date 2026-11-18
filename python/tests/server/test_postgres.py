"""PostgresStore (postgres.py) beyond the conformance suite: selection by URL, no driver for SQLite installs, the
schema versions, retention by whole daily partitions with retention classes, joins under concurrent exports,
and abandoned exports.

The tests that need a database run when TRAILBEAM_TEST_POSTGRES_URL names one the tests may create schemas in
(CI: Postgres 14 and 16 service containers); the conformance suite runs on it too (tests/conformance).
"""
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from spans import SEC, Consumer, to_proto
from trailbeam.server.otlp import decode
from trailbeam.server.retention import Retention
from trailbeam.server.store import Cutoffs, ExportAbandoned

ROOT = Path(__file__).resolve().parents[2]
PG = os.environ.get("TRAILBEAM_TEST_POSTGRES_URL")
needs_pg = pytest.mark.skipif(not PG, reason="TRAILBEAM_TEST_POSTGRES_URL is not set")
DAY = 86_400 * SEC
D0 = 20_400 * DAY  # a bucket boundary (2025-11-08 00:00 UTC)


def records(spans):
    return decode(to_proto(spans).SerializeToString(), "application/x-protobuf").records


# ---- no new dependency for SQLite --------------------------------------------------------------------

def test_the_postgres_driver_is_an_extra_and_not_a_dependency():
    try:
        import tomllib
    except ImportError:  # Python 3.10
        import tomli as tomllib
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]
    assert not any("psycopg" in d for d in project["dependencies"])
    assert not any("psycopg" in d for d in project["optional-dependencies"]["server"])
    assert any(d.startswith("psycopg") for d in project["optional-dependencies"]["postgres"])


def test_sqlite_runs_without_the_postgres_driver(tmp_path):
    """With psycopg impossible to import, the server and a SQLite store work, and a Postgres URL says what to
    install."""
    code = f"""
import sys
class NoPsycopg:
    def find_spec(self, name, path=None, target=None):
        if name.split(".")[0] in ("psycopg", "psycopg_pool"):
            raise ImportError("No module named " + repr(name))
sys.meta_path.insert(0, NoPsycopg())
from trailbeam.server.__main__ import open_store, StoreUrlError
from trailbeam.server.retention import Retention
s = open_store(None, {str(tmp_path / "x.sqlite")!r}, store_bodies=frozenset(), retention=Retention())
assert s.counts()["messages"] == 0
s.close()
try:
    open_store("postgres://u:secret@db.example:5432/tb", "", store_bodies=frozenset(), retention=Retention())
except StoreUrlError as e:
    assert 'pip install "trailbeam[postgres]"' in str(e) and "secret" not in str(e), str(e)
else:
    raise AssertionError("expected StoreUrlError")
assert not any(m.split(".")[0] in ("psycopg", "psycopg_pool") for m in sys.modules)
"""
    subprocess.run([sys.executable, "-c", code], check=True, timeout=60)


def test_store_url_is_checked_and_its_password_kept_out_of_logs():
    from trailbeam.server.__main__ import StoreUrlError, open_store, redact_url

    assert redact_url("postgres://tb:s3cret@db:5432/trailbeam") == "postgres://tb:***@db:5432/trailbeam"
    assert redact_url("postgresql://db/trailbeam") == "postgresql://db/trailbeam"
    with pytest.raises(StoreUrlError, match="expected postgres"):
        open_store("mysql://x:pw@db/tb", "", store_bodies=frozenset(), retention=Retention())


# ---- with a database ---------------------------------------------------------------------------------

@pytest.fixture
def pg_store():
    sys.path.insert(0, str(ROOT / "tests" / "conformance"))
    from pg_helpers import postgres_store

    made = []

    def make(**kw):
        s = postgres_store(**kw)
        made.append(s)
        return s
    yield make
    for s in made:
        s.close()


def _partitions(s) -> set:
    with s.reads.connection() as c:
        return {r["tablename"] for r in c.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = current_schema() AND tablename LIKE 'spans_p%%'")}


@needs_pg
def test_schema_is_versioned_and_a_newer_one_refused(pg_store):
    from trailbeam.server import pg_schema
    from trailbeam.server.migrations import SchemaTooNew
    from trailbeam.server.postgres import PostgresStore

    s = pg_store()
    assert s.migrated == []
    with s.ingest.connection() as c:
        rows = c.execute("SELECT version, name FROM schema_version").fetchall()
        assert [(r["version"], r["name"]) for r in rows] == [(pg_schema.LATEST, "create")]
    again = PostgresStore(s.url)  # opening a current database changes nothing
    assert again.migrated == []
    again.close()
    with s.ingest.connection() as c:
        c.execute("INSERT INTO schema_version (version, name) VALUES (%s, 'from the future')", (pg_schema.LATEST + 1,))
    with pytest.raises(SchemaTooNew):
        PostgresStore(s.url)


@needs_pg
def test_retention_drops_daily_partitions_by_retention_class(pg_store):
    """7d default, audit forever, orders-dlq 30d: each queue's spans are in its class's daily partitions, and a
    pass drops whole partitions. 7d keeps a message 7 to 8 days (the end of its day, plus 7 days)."""
    s = pg_store(retention=Retention(7 * 86_400, {"audit": None, "orders-dlq": 30 * 86_400}))
    assert s.retention_granularity_ns == DAY
    w, dlq = Consumer(), Consumer(client_id="dlq-1", queue="orders-dlq")
    t = D0 + 10 * 3600 * SEC  # day 0, 10:00
    s.insert(records([w.send(t, "plain"), w.receive(t + SEC, "plain", "h1"), w.delete(t + 2 * SEC, "h1", "plain")]))
    s.insert(records([w.send(t, "kept", queue="audit")]))
    s.insert(records([w.send(t, "moved"), w.receive(t + SEC, "moved", "h2"),
                      dlq.receive(t + 3 * SEC, "moved", "h3", dlq_source="orders")]))
    before = _partitions(s)
    retention = Retention(7 * 86_400, {"audit": None, "orders-dlq": 30 * 86_400})

    def purge(now):
        resume, n = None, 0
        while True:
            st = s.purge_step(retention.cutoffs(now), resume, 50)
            n, resume = n + st.deleted, st.resume
            if resume is None:
                return n
    assert purge(t + 7 * DAY + 3600 * SEC) == 0  # 7 days and an hour: its day has not been 7 days gone yet
    assert set(s.recent_messages(10)) == {"plain", "kept", "moved"}
    assert purge(D0 + 8 * DAY) == 3  # the end of day 0 plus 7 days: the 7-day partition goes whole
    assert set(s.recent_messages(10)) == {"kept", "moved"}
    assert len(s.message_rows("moved")) == 3  # 30-day class: its spans on orders went with it
    gone = before - _partitions(s)
    assert gone == {f"spans_p{7 * 86_400}_{D0 // DAY}"}
    assert purge(D0 + 31 * DAY) == 3
    assert s.recent_messages(10) == ["kept"]
    assert purge(D0 + 10_000 * DAY) == 0


@needs_pg
def test_a_message_across_midnight_stays_whole(pg_store):
    s = pg_store(retention=Retention(86_400, {}))
    w = Consumer()
    late = D0 + DAY - SEC  # 23:59:59 on day 0
    s.insert(records([w.send(late, "m1"), w.receive(late + 2 * SEC, "m1", "h1")]))  # the receive is on day 1
    s.insert(records([w.delete(late + 3 * SEC, "h1")]))
    rows = [dict(r) for r in s.message_rows("m1")]
    assert len(rows) == 3
    cut = lambda now: Retention(86_400, {}).cutoffs(now)  # noqa: E731
    # day 0's partition can go (its end is a day old), day 1's not: the send moves to day 1 first
    resume = None
    while True:
        st = s.purge_step(cut(D0 + 2 * DAY), resume)
        resume = st.resume
        if resume is None:
            break
    assert [dict(r) for r in s.message_rows("m1")] == rows
    assert f"spans_p86400_{D0 // DAY}" not in _partitions(s)
    while True:
        st = s.purge_step(cut(D0 + 3 * DAY), resume)
        resume = st.resume
        if resume is None:
            break
    assert s.message_rows("m1") == [] and s.counts()["spans"] == {}


@needs_pg
def test_concurrent_exports_join_settles_to_receives(pg_store):
    """A delete and its receive in two exports at once, on two threads, many times: every delete is joined."""
    s = pg_store()
    w = Consumer()
    t = D0
    n = 60
    errors = []

    def put(spans):
        try:
            s.insert(records(spans))
        except Exception as e:  # noqa: BLE001
            errors.append(e)
    threads = []
    for i in range(n):
        threads.append(threading.Thread(target=put, args=([w.delete(t + i * SEC + 2, f"h{i}")],)))
        threads.append(threading.Thread(target=put, args=([w.receive(t + i * SEC + 1, f"m{i}", f"h{i}")],)))
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    assert errors == []
    assert s.counts()["unjoined_settles"] == 0
    assert all(len(s.message_rows(f"m{i}")) == 2 for i in range(n))


@needs_pg
def test_an_abandoned_export_is_cancelled_and_stores_nothing(pg_store):
    import psycopg

    from trailbeam.server.postgres import _key

    s = pg_store()
    s.WATCH_INTERVAL_S = 0.05
    gone = threading.Event()
    spans = records([Consumer().send(D0, "m-abandoned")])
    with psycopg.connect(s.url, autocommit=True) as blocker:
        blocker.execute("SELECT pg_advisory_lock(%s)", (_key("m:m-abandoned"),))  # the export waits on it
        result = []
        th = threading.Thread(target=lambda: result.append(_try(s.insert, spans, abandoned=gone.is_set)))
        th.start()
        time.sleep(0.3)
        assert th.is_alive()  # still waiting in the database
        gone.set()  # the client hangs up
        th.join(timeout=10)
        assert not th.is_alive()
        blocker.execute("SELECT pg_advisory_unlock(%s)", (_key("m:m-abandoned"),))
    assert isinstance(result[0], ExportAbandoned)
    assert s.counts()["spans"] == {}
    assert s.insert(spans) == (1, 0)  # and the next export goes through


def _try(f, *a, **kw):
    try:
        return f(*a, **kw)
    except Exception as e:  # noqa: BLE001
        return e


@needs_pg
def test_the_server_ingests_into_postgres(pg_store):
    from trailbeam.server import queries
    from trailbeam.server.server import App

    s = pg_store()
    w = Consumer()
    body = to_proto([w.send(D0, "m1"), w.receive(D0 + SEC, "m1", "h1"), w.delete(D0 + 2 * SEC, "h1")]).SerializeToString()
    status, _, _ = App(s).ingest(body, "application/x-protobuf", abandoned=lambda: False)
    assert status == 200
    t = queries.message(s, "m1", D0 + 10 * SEC, 900)
    assert t["state"] == "deleted" and len(t["events"]) == 3
    assert Cutoffs(None).keeps_everything()


# ---- liveness, readiness and an unreachable database (QV-81) ------------------------------------------

@needs_pg
def test_ready_fails_and_ingest_answers_503_while_postgres_is_unreachable(pg_store):
    """A replica cut off from its database stays alive (/api/health 200: not restarted), is not ready
    (/api/ready 503: out of rotation), and answers exports 503 with Retry-After (the exporter, or the load
    balancer on another replica, tries again), with nothing stored."""
    import json
    import urllib.error
    import urllib.request

    from psycopg.conninfo import conninfo_to_dict, make_conninfo

    from tcp_proxy import Proxy
    from trailbeam.server.postgres import PostgresStore
    from trailbeam.server.server import make_server

    direct = pg_store()  # makes the schema
    d = conninfo_to_dict(direct.url)
    proxy = Proxy([(d.get("host") or "localhost", int(d.get("port") or 5432))])
    s = PostgresStore(make_conninfo(direct.url, host="127.0.0.1", port=str(proxy.port)))
    s.INGEST_CONNECTION_WAIT_S = 1.0
    httpd = make_server(s, "127.0.0.1", 0, ui_dir=None)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{httpd.server_address[1]}"

    def call(path, body=None):
        req = urllib.request.Request(base + path, data=body, headers={"Content-Type": "application/x-protobuf"})
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.status, dict(r.headers), r.read()
        except urllib.error.HTTPError as e:
            return e.code, dict(e.headers), e.read()

    export = to_proto([Consumer().send(D0, "m-down")]).SerializeToString()
    try:
        assert call("/api/ready")[0] == 200
        assert json.loads(call("/api/health")[2])["data"]["last_row"] == 0
        proxy.cut()
        t0 = time.monotonic()
        status, _, body = call("/api/ready")
        assert (status, json.loads(body)["status"]) == (503, "not ready")
        assert time.monotonic() - t0 < 10
        status, _, body = call("/api/health")
        assert status == 200 and json.loads(body)["data"] == {"last_row": None, "store": "unreachable"}
        status, headers, _ = call("/v1/traces", export)
        assert status == 503 and headers["Retry-After"] == "1"
    finally:
        httpd.shutdown()
        httpd.server_close()
        s.close()
    assert direct.counts()["spans"] == {}
