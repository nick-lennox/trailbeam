"""PostgresStore (postgres.py) beyond the conformance suite: selection by URL, no driver for SQLite installs, the
schema versions, retention by whole daily partitions with retention classes, joins under concurrent exports,
and abandoned exports.

The tests that need a database run when TRAILBEAM_TEST_POSTGRES_URL names one the tests may create schemas in
(CI: Postgres 14 and 16 service containers); the conformance suite runs on it too (tests/conformance).
"""
import contextlib
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
    status, _, _, _ = App(s).ingest(body, "application/x-protobuf", abandoned=lambda: False)
    assert status == 200
    t = queries.message(s, "m1", D0 + 10 * SEC, 900)
    assert t["state"] == "deleted" and len(t["events"]) == 3
    assert Cutoffs(None).keeps_everything()


@needs_pg
def test_every_ingest_connection_busy_is_writer_busy_with_nothing_written(pg_store):
    """QV-74: with all of a replica's ingest connections held by other exports, an export that can't get one
    within wait_s raises WriterBusy (503 writer_behind, not store_unreachable) and stores nothing; once a
    connection is free it goes through."""
    from trailbeam.server.store import WriterBusy

    s = pg_store(ingest_connections=2)
    w = Consumer()
    records = decode(to_proto([w.send(D0, "m1"), w.receive(D0 + SEC, "m1", "h1")]).SerializeToString(),
                     "application/x-protobuf").records
    held = [s.ingest.getconn() for _ in range(2)]
    try:
        t0 = time.monotonic()
        with pytest.raises(WriterBusy):
            s.insert(records, wait_s=0.3)
        assert time.monotonic() - t0 < 5  # bounded by wait_s, not INGEST_CONNECTION_WAIT_S
        assert s.last_row() == 0
    finally:
        for c in held:
            s.ingest.putconn(c)
    timings: dict = {}
    assert s.insert(records, wait_s=0.3, timings=timings) == (2, 0)
    assert timings["wait_s"] >= 0 and timings["write_s"] > 0


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


# ---- the lock table (QV-107) ------------------------------------------------------------------------------

STOCK = {"max_locks_per_transaction": 64, "max_connections": 100, "autovacuum_max_workers": 3,
         "max_worker_processes": 8, "max_wal_senders": 10, "max_prepared_transactions": 0}


def test_lock_need_is_what_a_page_batch_can_lock_against_the_lock_table():
    """lock.c's NLOCKENTS: max_locks_per_transaction x server processes objects, twice that (object, backend)
    pairs. A stock Postgres 14 has 122 processes: 7,808 objects, 15,616 pairs. The connections that count are
    the read pool's (8 by default). Since QV-116 a page statement names at most PAGE_PLACES places (8), so the
    need is the same with 10 places or 400."""
    from trailbeam.server.pg_schema import LockNeed

    few = LockNeed(places=10, relations_per_place=24, connections=8, settings=STOCK)
    assert (few.backends, few.objects, few.holders) == (122, 192, 1_536)
    assert few.required == 7 and not few.short  # ceil(1,536 / 244)
    many = LockNeed(places=400, relations_per_place=24, connections=8, settings=STOCK)
    assert (many.objects, many.holders, many.required) == (192, 1_536, 7) and not many.short
    tiny = LockNeed(places=3, relations_per_place=24, connections=8, settings=STOCK)
    assert tiny.objects == 72  # fewer places than a batch
    pool = LockNeed(places=400, relations_per_place=24, connections=200, settings=STOCK)
    assert pool.required == 158 and pool.short  # ceil(38,400 / 244): a read pool far past the stock one
    assert "max_locks_per_transaction to at least 158" in pool.message()
    assert "8 places a statement" in pool.message()
    assert not LockNeed(400, 24, 200, {**STOCK, "max_locks_per_transaction": 158}).short


def _fill_the_lock_table(url) -> "psycopg.Connection":
    """A connection holding session advisory locks until the lock table has no room left (one more lock: "out of
    shared memory"). Session advisory locks outlive the statement that failed, so the table stays full until
    the connection is closed."""
    import psycopg

    c = psycopg.connect(url, autocommit=True)
    k = 0
    try:
        while k < 10_000_000:
            c.execute("SELECT pg_advisory_lock(k) FROM generate_series(%s::bigint, %s::bigint) k", (k, k + 999))
            k += 1000
    except psycopg.errors.OutOfMemory:
        return c
    c.close()
    raise AssertionError("the lock table never filled")


@needs_pg
def test_many_places_dont_need_a_bigger_lock_table_and_a_full_one_is_reported(pg_store, caplog):
    """QV-107, after QV-116: what a page locks no longer grows with the places, so a stock Postgres
    (max_locks_per_transaction = 64) with 150 daily places and a 32-connection read pool needs no warning, and 32
    pages at once all run. When the lock table is full all the same (here: session advisory locks), the store
    reports it as that, not as an unreachable database: a page in the log, an export in its error. The places
    are empty: their partitions are locked all the same."""
    import psycopg

    from trailbeam.server.store import StoreUnavailable

    s = pg_store(read_connections=32)
    if s.lock_need.settings != STOCK:
        pytest.skip(f"needs a Postgres with stock lock settings, not {s.lock_need.settings}")
    day0 = D0 // DAY
    caplog.set_level("WARNING", logger="trailbeam.server.postgres")
    s._ensure_places({(0, day0 + d) for d in range(150)})
    need = s.lock_need
    assert need.places == 150 and not need.short and need.required <= 64
    assert "max_locks_per_transaction" not in caplog.text

    failed: list = []

    def page():
        try:
            s.recent_messages(50)
            s.stuck_messages(D0, 50)
            s.counts()
        except Exception as e:
            failed.append(e)
    threads = [threading.Thread(target=page) for _ in range(32)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(120)
    assert failed == []

    caplog.clear()
    w = Consumer()
    export = records([w.send((day0 + 149) * DAY + SEC, "full"), w.receive((day0 + 149) * DAY + 2 * SEC, "full", "hf")])
    filler = _fill_the_lock_table(s.url)
    try:
        with pytest.raises(psycopg.errors.OutOfMemory):
            s.recent_messages(50)
        assert "read failed: Postgres's lock table is full" in caplog.text
        with pytest.raises(StoreUnavailable) as e:
            s.insert(export)
        assert "lock table is full" in str(e.value) and "unreachable" not in str(e.value), str(e.value)
        assert "Raise max_locks_per_transaction" in str(e.value)
    finally:
        pid = filler.info.backend_pid
        filler.close()
    # its locks go when its backend exits, a moment after the connection closes
    with psycopg.connect(s.url, autocommit=True) as c:
        deadline = time.monotonic() + 30
        while c.execute("SELECT 1 FROM pg_stat_activity WHERE pid = %s", (pid,)).fetchone():
            assert time.monotonic() < deadline, "the filler's backend never exited"
            time.sleep(0.05)
    assert s.insert(export)[0] == 2  # room again: the same export is stored
    assert s.recent_messages(50) == ["full"]


# ---- what one export locks (QV-113) -----------------------------------------------------------------------

T0 = D0  # one-second buckets from here: a place a second


def _history(s, places: int) -> None:
    """``places`` one-second places, each with a message sent, received and deleted; place 0 also has "late",
    received and not yet deleted. Written 20 places an export."""
    w = Consumer()
    s.insert(records([w.send(T0, "late"), w.receive(T0 + 1, "late", "h-late")]))
    for k0 in range(0, places, 20):
        spans = []
        for k in range(k0, min(places, k0 + 20)):
            t = T0 + k * SEC + 10
            spans += [w.send(t, f"old-{k}"), w.receive(t + 1, f"old-{k}", f"h-old-{k}"), w.delete(t + 2, f"h-old-{k}")]
        s.insert(records(spans))


def _export_locks(s, spans) -> list:
    """The relations one export holds locks on at the end of its transaction (it holds every lock it took until
    then): one name per lock entry, sorted."""
    from trailbeam.server.postgres import PostgresStore

    held = []
    refresh = PostgresStore._refresh

    def counting(conn, mids):
        refresh(s, conn, mids)
        held[:] = sorted(r["relname"] or f"<{r['locktype']}>" for r in conn.execute(
            """SELECT l.locktype, c.relname FROM pg_locks l LEFT JOIN pg_class c ON c.oid = l.relation
               WHERE l.pid = pg_backend_pid() AND l.locktype <> 'advisory'""").fetchall())
    s._refresh = counting
    try:
        assert s.insert(records(spans))[0] == len(spans)
    finally:
        del s._refresh
    return held


def _the_export(places: int) -> list:
    """A new message sent (with a body), received and deleted in the place after the history, and the delete of
    "late", whose receive is in place 0: a join across places that moves "late" to the new place."""
    w = Consumer()
    t = T0 + places * SEC + 10
    send = w.send(t, "new")
    send.attrs["trailbeam.body"] = "hello"
    return [send, w.receive(t + 1, "new", "h-new"), w.delete(t + 2, "h-new"), w.delete(t + 3, "h-late")]


@needs_pg
@pytest.mark.parametrize("places", [10, 400])
def test_what_one_export_locks_does_not_grow_with_the_places(pg_store, places):
    """QV-113: an export locks the partitions of the places it writes and reads (here the new place, and place 0
    for the late delete's join), and the store's lookup tables; never one of the other places. So the same export
    takes as many locks with 400 places of history as with 10."""
    import re

    s = pg_store(bucket_ns=SEC, store_bodies={"orders"})
    _history(s, places)
    held = _export_locks(s, _the_export(places))
    buckets = {int(m.group(1)) for n in held for m in [re.search(r"_p0_(\d+)", n)] if m}
    assert buckets == {T0 // SEC, T0 // SEC + places}, buckets
    _EXPORT_LOCKS[places] = len(held)
    if len(_EXPORT_LOCKS) == 2:
        assert _EXPORT_LOCKS[400] == _EXPORT_LOCKS[10], _EXPORT_LOCKS
    rows = s.message_rows("late")
    assert [r["kind"] for r in rows] == ["send", "receive", "delete"] and rows[2]["mid"] == "late"
    assert rows[2]["receive_row"] == rows[1]["row"] and s.summaries(["late"])[0]["final_delete"]
    assert s.body_spans(["new"])[0]["body"] == "hello"


_EXPORT_LOCKS: dict = {}


@needs_pg
def test_version_2_fills_the_lookup_tables_place_by_place(pg_store):
    """A version-1 database (no lookup tables) is brought to version 2: the lookups are filled from the rows,
    one place a transaction, as ingest would have written them; then a late delete joins a receive stored
    before the upgrade, and a message stored before it reads whole."""
    from trailbeam.server.postgres import PostgresStore

    s = pg_store(bucket_ns=SEC, store_bodies={"orders"})
    _history(s, 30)
    w = Consumer()
    send = w.send(T0 + 5 * SEC, "with-body")
    send.attrs["trailbeam.body"] = "kept"
    s.insert(records([send, w.receive(T0 + 40 * SEC, "with-body", "h-wb")]))  # its body moves to place 40
    with s.ingest.connection() as c:
        lookups = {t: c.execute(f"SELECT * FROM {t} ORDER BY 1, 2, 3").fetchall() for t in ("mid_places", "hash_places")}
        assert len(lookups["mid_places"]) == 30 + 3 and len(lookups["hash_places"]) == 30 + 2
        c.execute("DROP TABLE mid_places, hash_places")
        c.execute("UPDATE schema_version SET version = 1")
    again = PostgresStore(s.url, bucket_ns=SEC, store_bodies={"orders"})
    try:
        assert again.migrated == [2]
        with again.ingest.connection() as c:
            assert {t: c.execute(f"SELECT * FROM {t} ORDER BY 1, 2, 3").fetchall() for t in lookups} == lookups
            assert [r["version"] for r in c.execute("SELECT version FROM schema_version ORDER BY 1")] == [1, 2]
        again.insert(records([w.delete(T0 + 50 * SEC, "h-late")]))
        rows = again.message_rows("late")
        assert [r["kind"] for r in rows] == ["send", "receive", "delete"] and rows[2]["receive_row"] == rows[1]["row"]
        assert again.body_spans(["with-body"])[0]["body"] == "kept"
        assert len(again.message_rows("old-7")) == 3
    finally:
        again.close()


# ---- what one page locks (QV-116) -------------------------------------------------------------------------

def _page_history(places: int) -> list:
    """Exports of ``places`` one-second places: in place k a message of tenant ``tenant-<k % 3>`` on "orders" or
    "billing", sent and received; deleted, except every 7th (stuck); every 5th received twice (a duplicate)."""
    w = Consumer()
    exports = []
    for k0 in range(0, places, 20):
        spans = []
        for k in range(k0, min(places, k0 + 20)):
            t, mid, q = T0 + k * SEC + 10, f"m-{k}", ("orders", "billing")[k % 2]
            send = w.send(t, mid, queue=q)
            send.attrs["trailbeam.tenant"] = f"tenant-{k % 3}"
            spans += [send, w.receive(t + 1, mid, f"h-{k}", queue=q)]
            if k % 5 == 0:
                spans.append(w.receive(t + 2, mid, f"h2-{k}", count=2, queue=q))
            if k % 7:
                spans.append(w.delete(t + 3, f"h-{k}", queue=q))
        exports.append(spans)
    return exports


def _pages(s, places: int) -> dict:
    """Every page that walks the places, as the API asks for them: the first and a later page of each list."""
    first = s.newest_messages(5)
    mid_ns = T0 + places // 2 * SEC
    return {
        "recent": s.recent_messages(5), "recent_after": s.recent_messages(5, after=first[-1]),
        "recent_queue": s.recent_messages(5, "billing"), "recent_tenant": s.recent_messages(5, tenant="tenant-1"),
        "recent_tenant_queue": s.recent_messages(5, "orders", "tenant-2"),
        "recent_window": s.recent_messages(5, from_ns=mid_ns - 10 * SEC, to_ns=mid_ns),
        "recent_old": s.recent_messages(5, to_ns=T0 + 3 * SEC), "newest": first,
        "duplicates": s.duplicate_messages(5), "duplicates_queue": s.duplicate_messages(5, "orders"),
        "stuck": s.stuck_messages(T0 + places * SEC, 5),
        "stuck_queue": s.stuck_messages(T0 + places * SEC, 5, "billing"),
        "stuck_after": s.stuck_messages(T0 + places * SEC, 5, after=(T0 + 7 * SEC + 10, "m-7")),
        "queue": sorted(s.queue_messages("orders")),
        "activity": sorted((a.tenant, a.sent, a.dead_lettered, sorted(a.waits))
                           for a in s.tenant_activity((T0 + SEC) // 10 ** 6, (T0 + places * SEC) // 10 ** 6)),
        "find": s.find_tenants("TEN", 8), "find_queue": s.find_tenants("tenant-", 2, queue="billing"),
        "find_window": s.find_tenants("t", 8, from_ns=T0 + (places - 2) * SEC), "counts": s.counts(),
    }


def _read_locks(s, call) -> list:
    """Run ``call``, each of its statements in a transaction of its own (as autocommit runs it) that, before it
    ends, lists what the connection holds. Returns one sorted list of relation names for each statement."""
    import contextlib

    import psycopg

    held = []
    pool_connection = s.reads.connection

    class Counted:
        def __init__(self, conn):
            self._conn = conn

        def __getattr__(self, name):
            return getattr(self._conn, name)

        def execute(self, query, params=None):
            idle = self._conn.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
            with self._conn.transaction() if idle else contextlib.nullcontext():
                cur = self._conn.execute(query, params)
                rows = cur.fetchall() if cur.description else []
                held.append(sorted(r["relname"] or f"<{r['locktype']}>" for r in self._conn.execute(
                    """SELECT l.locktype, c.relname FROM pg_locks l LEFT JOIN pg_class c ON c.oid = l.relation
                       WHERE l.pid = pg_backend_pid() AND l.locktype <> 'advisory'""").fetchall()))
            return Rows(rows)

    class Rows(list):
        def fetchall(self):
            return list(self)

        def fetchone(self):
            return self[0] if self else None

    @contextlib.contextmanager
    def counted(*a, **kw):
        with pool_connection(*a, **kw) as conn:
            yield Counted(conn)

    s.reads.connection = counted
    try:
        call()
    finally:
        del s.reads.connection
    return held


@needs_pg
@pytest.mark.parametrize("places", [10, 400])
def test_what_one_page_locks_does_not_grow_with_the_places(pg_store, places, tmp_path):
    """QV-116: a page walks the places a batch a statement, so no statement locks the partitions of more than
    PAGE_PLACES places, and the most lock entries a page holds at once (system catalogs aside) are as many with
    400 places as with 10.
    The pages are those of a SQLite store given the same spans."""
    import re

    from trailbeam.server.db import SqliteStore

    s = pg_store(bucket_ns=SEC)
    lite = SqliteStore(str(tmp_path / "lite.sqlite"))
    try:
        for spans in _page_history(places):
            assert s.insert(records(spans)) == lite.insert(records(spans))
        assert _pages(s, places) == _pages(lite, places)
        assert s.last_row() == lite.last_row() == sum(s.counts()["spans"].values())
    finally:
        lite.close()
    most = 0
    for name, call in [("pages", lambda: _pages(s, places)), ("last_row", s.last_row)]:
        for held in _read_locks(s, call):
            buckets = {int(m.group(1)) for n in held for m in [re.search(r"_p0_(\d+)", n)] if m}
            assert len(buckets) <= s.PAGE_PLACES, (name, sorted(buckets))
            # system catalogs are left out: which of them a statement locks depends on Postgres's relation
            # cache (a cache miss reads pg_attribute and more), not on the places
            most = max(most, len([n for n in held if not n.startswith("pg_")]))
    _PAGE_LOCKS[places] = most
    if len(_PAGE_LOCKS) == 2:
        assert _PAGE_LOCKS[400] == _PAGE_LOCKS[10], _PAGE_LOCKS


_PAGE_LOCKS: dict = {}
