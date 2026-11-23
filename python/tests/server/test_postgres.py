"""PostgresStore (postgres.py) beyond the conformance suite: selection by URL, no driver for SQLite installs, the
schema versions, retention by whole daily partitions with retention classes, joins under concurrent exports,
and abandoned exports.

The tests that need a database run when TRAILBEAM_TEST_POSTGRES_URL names one the tests may create schemas in
(CI: Postgres 14 and 16 service containers); the conformance suite runs on it too (tests/conformance).
"""
import contextlib
import json
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



@needs_pg
def test_pool_sizes_reach_the_store():
    """QV-127 (3c): --pg-ingest-connections and --pg-read-connections size the pools; unset, 4 and 8."""
    sys.path.insert(0, str(ROOT / "tests" / "conformance"))
    from pg_helpers import drop_schema, new_schema
    from trailbeam.server.__main__ import open_store

    for given, want in (((None, None), (4, 8)), ((2, 11), (2, 11))):
        conninfo = new_schema()
        schema = conninfo.split("search_path=")[1].split()[0].strip("'")
        url = PG + ("&" if "?" in PG else "?") + "options=-csearch_path%3D" + schema  # a URL, as the server takes
        s = open_store(url, "", store_bodies=frozenset(), retention=Retention(), ingest_connections=given[0],
                       read_connections=given[1])
        try:
            assert (s.ingest.max_size, s.reads.max_size) == want
            assert s.lock_need is None or s.lock_need.connections == want[1]
        finally:
            s.close()
            drop_schema(conninfo)

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
        assert status == 200 and json.loads(body)["data"] == {"last_row": None, "last_span_ns": None, "store": "unreachable"}
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
    # its locks go when its backend exits, a moment after the connection closes: until then even a new
    # connection may not find room (it takes a lock as it starts)
    deadline = time.monotonic() + 30
    while True:
        try:
            c = psycopg.connect(s.url, autocommit=True)
            break
        except psycopg.OperationalError:
            assert time.monotonic() < deadline, "the lock table stayed full after the filler closed"
            time.sleep(0.05)
    with c:
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
@pytest.mark.parametrize("sealed", [False, True])
@pytest.mark.parametrize("places", [10, 400])
def test_what_one_export_locks_does_not_grow_with_the_places(pg_store, places, sealed):
    """QV-113: an export locks the partitions of the places it writes and reads (here the new place, and place 0
    for the late delete's join), and the store's lookup tables; never one of the other places. So the same export
    takes as many locks with 400 places of history as with 10. QV-127: whether the old places are sealed (the
    filter rules them out) or not (they are read for every key, on a read connection of their own)."""
    import re

    s = pg_store(bucket_ns=SEC, store_bodies={"orders"})
    _history(s, places)
    if sealed:
        _pass(s)
    held = _export_locks(s, _the_export(places))
    buckets = {int(m.group(1)) for n in held for m in [re.search(r"_p0_(\d+)", n)] if m}
    hot = {b for b in buckets if b >= s._hot_from()}  # the recent places (the pass made today's): a few, always
    assert buckets - hot == {T0 // SEC, T0 // SEC + places} and len(hot) <= s.hot_buckets + 1 + s.PRECREATE_AHEAD
    _EXPORT_LOCKS[sealed, places] = len(held)
    if (sealed, 10) in _EXPORT_LOCKS and (sealed, 400) in _EXPORT_LOCKS:
        assert _EXPORT_LOCKS[sealed, 400] == _EXPORT_LOCKS[sealed, 10], _EXPORT_LOCKS
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
        _as_version(c, 1)
    again = PostgresStore(s.url, bucket_ns=SEC, store_bodies={"orders"})
    try:
        assert again.migrated == [2, 3, 4, 5, 6, 7, 8, 9]
        with again.ingest.connection() as c:
            assert {t: c.execute(f"SELECT * FROM {t} ORDER BY 1, 2, 3").fetchall() for t in lookups} == lookups
            assert [r["version"] for r in c.execute("SELECT version FROM schema_version ORDER BY 1")] == list(range(1, pg_schema_latest() + 1))
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
        "activity": sorted((a.tenant, a.sent, a.dead_lettered, sorted(a.waits.counts.items()))
                           for a in s.tenant_activity((T0 + SEC) // 10 ** 6, (T0 + places * SEC) // 10 ** 6)),
        "find": s.find_tenants("TEN", 8), "find_queue": s.find_tenants("tenant-", 2, queue="billing"),
        "find_window": s.find_tenants("t", 8, from_ns=T0 + (places - 2) * SEC), "counts": s.counts(),
    }


def _read_locks(s, call) -> list:
    """Run ``call``, each of its statements in a transaction of its own (as autocommit runs it) that, before it
    ends, lists what the connection holds. Returns one sorted list of relation names for each statement."""
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


def pg_schema_latest() -> int:
    from trailbeam.server import pg_schema
    return pg_schema.LATEST


def _as_version(c, version: int) -> None:
    """Make a current database look as Postgres schema ``version`` left it: 1 is 0.2.0's (no lookup tables, no
    message-ID indexes on the list tables), 2 adds the lookup tables (QV-113), 3 the message-ID indexes (QV-118),
    4 the tenant rollup (QV-128); 5 partitions the lookup tables (QV-127); 6 adds the probes of trailbeam
    doctor (QV-148); 7 takes deduplicated FIFO sends out of the tenant events (QV-246, no table change); 8 the
    tables of people and tokens (QV-225); 9 the purges (QV-245)."""
    if version < 9:
        c.execute("DROP TABLE purges")
    if version < 8:
        c.execute("DROP TABLE setup_codes, personal_tokens, ingest_tokens, sessions, users")
    if version < 6:
        c.execute("DROP TABLE probes")
    if version < 5:
        for t, cols in (("mid_places", 'mid TEXT COLLATE "C" NOT NULL'),
                        ("hash_places", "receipt_hmac TEXT NOT NULL, key_id TEXT NOT NULL")):
            keys = ", ".join(x.split()[0] for x in cols.split(", "))
            c.execute(f"CREATE TABLE {t}_old ({cols}, rclass BIGINT NOT NULL, bucket BIGINT NOT NULL, "
                      f"PRIMARY KEY ({keys}, rclass, bucket))")
            c.execute(f"INSERT INTO {t}_old SELECT {keys}, rclass, bucket FROM {t}")
            c.execute(f"DROP TABLE {t}")
            c.execute(f"ALTER TABLE {t}_old RENAME TO {t}")
            c.execute(f"ALTER INDEX {t}_old_pkey RENAME TO {t}_pkey")
            c.execute(f"CREATE INDEX {t}_place ON {t} (rclass, bucket)")
        c.execute("DROP TABLE mid_places_late, hash_places_late, lookup_filters, lookup_filter")
        c.execute("ALTER TABLE places DROP COLUMN sealed_in, DROP COLUMN lookup_keys")
    if version < 4:
        c.execute("DROP TABLE tenant_rollup")  # with its partitions
        c.execute("DROP INDEX tenant_events_again")
        for r in c.execute("SELECT rclass, bucket, mid, tenant, events_json FROM messages WHERE moved <> 0").fetchall():
            ev = [e for e in json.loads(r["events_json"]) if e[1] != "dead_lettered_again"]
            c.execute("UPDATE messages SET events_json = %s WHERE rclass = %s AND bucket = %s AND mid = %s",
                      (json.dumps(ev), r["rclass"], r["bucket"], r["mid"]))
            c.execute("DELETE FROM tenant_events WHERE rclass = %s AND bucket = %s AND mid = %s",
                      (r["rclass"], r["bucket"], r["mid"]))
            for i, (at, kind, wait) in enumerate(ev):
                c.execute("""INSERT INTO tenant_events (rclass, bucket, at_ms, mid, seq, kind, tenant, wait_ms)
                             VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                          (r["rclass"], r["bucket"], at, r["mid"], i, kind, r["tenant"], wait))
    if version < 3:
        c.execute("DROP INDEX message_queues_mid, message_tenants_mid, tenant_events_mid")  # with the partitions'
    if version < 2:
        c.execute("DROP TABLE mid_places, hash_places")
    c.execute("DELETE FROM schema_version")
    c.execute("INSERT INTO schema_version (version, name) SELECT v, 'test' FROM generate_series(1, %s) v", (version,))


# ---- QV-118: the summary upkeep DELETEs, statistics on a new place, partition creation behind a reader -------

def _mid_index_partitions(c) -> dict:
    """{table: {partition, ...}}: the partitions of the three list tables with a valid index on (mid) alone,
    attached to the parent's message-ID index."""
    out: dict = {}
    for r in c.execute(
            """SELECT pa.relname AS parent, ch.relname AS part FROM pg_inherits i
               JOIN pg_class ch ON ch.oid = i.inhrelid JOIN pg_class pa ON pa.oid = i.inhparent
               JOIN pg_index x ON x.indrelid = ch.oid AND x.indisvalid
               JOIN pg_inherits ii ON ii.inhrelid = x.indexrelid
               JOIN pg_class pi ON pi.oid = ii.inhparent AND pi.relname = pa.relname || '_mid'
               WHERE ch.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema())
                 AND pa.relname IN ('message_queues', 'message_tenants', 'tenant_events')
                 AND pg_get_indexdef(x.indexrelid) LIKE '%%(mid)'""").fetchall():
        out.setdefault(r["parent"], set()).add(r["part"])
    return out


def _list_partitions(c) -> dict:
    out: dict = {}
    for r in c.execute(
            """SELECT pa.relname AS parent, ch.relname AS part FROM pg_inherits i
               JOIN pg_class ch ON ch.oid = i.inhrelid JOIN pg_class pa ON pa.oid = i.inhparent
               WHERE ch.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema())
                 AND pa.relname IN ('message_queues', 'message_tenants', 'tenant_events')""").fetchall():
        out.setdefault(r["parent"], set()).add(r["part"])
    return out


def _delete_plans(c, rclass: int, bucket: int) -> list:
    """The plans of the summary upkeep's three DELETEs, with sequential scans priced out: a plan that still
    scans has no index it can use."""
    plans = []
    with c.transaction():
        c.execute("SET LOCAL enable_seqscan = off")
        for table in ("message_queues", "message_tenants", "tenant_events"):
            plans.append("\n".join(r["QUERY PLAN"] for r in c.execute(
                f"EXPLAIN DELETE FROM {table} WHERE rclass = %s AND bucket = %s AND mid = %s", (rclass, bucket, "m1"))))
    return plans


@needs_pg
def test_summary_upkeep_deletes_by_the_message_id_index(pg_store):
    """3a: rewriting a message's summary deletes its old rows of message_queues, message_tenants and
    tenant_events. Each DELETE names the place and the message, and has an index on mid to find them by,
    in a place made before the export and in one made by it."""
    s = pg_store()
    w = Consumer()
    send = w.send(D0, "m1")
    send.attrs["trailbeam.tenant"] = "acme"
    s.insert(records([send]))
    s.insert(records([w.receive(D0 + SEC, "m1", "h1"), w.delete(D0 + 2 * SEC, "h1")]))  # rewrites m1's rows
    s.insert(records([w.send(D0 + 3 * DAY, "m2")]))  # another place, made by ingest
    with s.ingest.connection() as c:
        for b in (D0 // DAY, D0 // DAY + 3):
            for plan in _delete_plans(c, 0, b):
                assert "Seq Scan" not in plan and "_mid_idx" in plan, plan
        assert _mid_index_partitions(c) == _list_partitions(c)
        assert c.execute("SELECT COUNT(*) AS n FROM message_queues WHERE mid = 'm1'").fetchone()["n"] == 1
        assert c.execute("SELECT COUNT(*) AS n FROM message_tenants WHERE mid = 'm1'").fetchone()["n"] == 1
    assert s.summaries(["m1"])[0]["final_delete"]


@needs_pg
def test_version_3_indexes_the_list_tables_place_by_place(pg_store):
    """A schema-2 database is brought to version 3: each place's list-table partitions get their (mid) index,
    attached to the parent's, which ends valid; a place stopped part way through a drop (its partitions gone)
    is skipped. Run again, the step changes nothing; a place made after it has the index too."""
    from trailbeam.server.postgres import PostgresStore

    s = pg_store(bucket_ns=SEC)
    _history(s, 12)
    with s.ingest.connection() as c:
        for table in ("message_queues", "message_tenants", "tenant_events"):
            part = f"{table}_p0_{T0 // SEC + 5}"
            c.execute(f"ALTER TABLE {table} DETACH PARTITION {part}")
            c.execute(f"DROP TABLE {part}")
        c.execute("UPDATE places SET state = 'dropping' WHERE rclass = 0 AND bucket = %s", (T0 // SEC + 5,))
        _as_version(c, 2)
        assert _mid_index_partitions(c) == {}
    for attempt in range(2):
        again = PostgresStore(s.url, bucket_ns=SEC)
        try:
            assert again.migrated == [3, 4, 5, 6, 7, 8, 9]
            with again.ingest.connection() as c:
                parts = _list_partitions(c)
                assert len(parts["message_queues"]) == 11 + attempt  # 12 places less the dropped one, + "after-0"
                assert _mid_index_partitions(c) == parts
                assert all(r["indisvalid"] for r in c.execute(
                    """SELECT indisvalid FROM pg_index WHERE indexrelid IN
                       ('message_queues_mid'::regclass, 'message_tenants_mid'::regclass, 'tenant_events_mid'::regclass)"""))
                assert [r["version"] for r in c.execute("SELECT version FROM schema_version ORDER BY 1")] == list(range(1, pg_schema_latest() + 1))
                if attempt == 0:  # as if stopped before recording 3: 3, 4 and 5 run again
                    c.execute("DELETE FROM schema_version WHERE version >= 3")
            again.insert(records([Consumer().send(T0 + 100 * SEC, f"after-{attempt}")]))
            with again.ingest.connection() as c:
                assert _mid_index_partitions(c) == _list_partitions(c)
            assert len(again.message_rows("old-7")) == 3
        finally:
            again.close()


def _planned_rows(c, table: str, rclass: int, bucket: int) -> float:
    plan = c.execute(f"EXPLAIN (FORMAT JSON) SELECT * FROM {table} WHERE rclass = %s AND bucket = %s",
                     (rclass, bucket)).fetchone()["QUERY PLAN"]
    return plan[0]["Plan"]["Plan Rows"]


@needs_pg
def test_a_new_place_is_analysed_once_it_has_rows(pg_store):
    """3b: a partition no ANALYZE has reached has no statistics, so Postgres estimates the place's own WHERE
    (rclass = c AND bucket = b) at 1 row, however many it holds, and plans the export's statements as if the
    partition were empty. Once this replica has written ANALYZE_AFTER_SPANS spans to the place, it analyses
    the place's partitions, and the estimate is the rows it holds."""
    from trailbeam.server import pg_schema

    s = pg_store()
    s.ANALYZE_AFTER_SPANS = 600
    w = Consumer()
    b = D0 // DAY
    s.insert(records([w.send(D0, "first")]))
    with s.ingest.connection() as c:
        for t in pg_schema.PARTITIONED:  # so that autovacuum's ANALYZE doesn't come first
            c.execute(f"ALTER TABLE {pg_schema.partition_name(t, 0, b)} SET (autovacuum_enabled = false)")
    for k in range(0, 498, 166):
        spans = []
        for i in range(k, k + 166):
            spans += [w.send(D0 + i * 1000, f"m{i}"), w.receive(D0 + i * 1000 + 1, f"m{i}", f"h{i}"),
                      w.delete(D0 + i * 1000 + 2, f"h{i}")]
        s.insert(records(spans[:len(spans) // 2]))
        s.insert(records(spans[len(spans) // 2:]))
        with s.ingest.connection() as c:
            n = c.execute("SELECT COUNT(*) AS n FROM spans").fetchone()["n"]
            planned = _planned_rows(c, "spans", 0, b)
        if n < s.ANALYZE_AFTER_SPANS:
            assert planned == 1, (n, planned)  # the cause: an unanalysed partition looks empty
        else:
            assert planned > n / 2, (n, planned)
    with s.ingest.connection() as c:
        analysed = {r["tablename"] for r in c.execute(
            "SELECT DISTINCT tablename FROM pg_stats WHERE schemaname = current_schema() AND attname = 'bucket'")}
        assert analysed == {pg_schema.partition_name(t, 0, b) for t in pg_schema.PARTITIONED  # the rest are empty
                            if t not in ("bodies", "message_tenants")}
        assert _planned_rows(c, "messages", 0, b) > 400


@needs_pg
def test_partition_creation_does_not_hold_exports_behind_an_open_reader(pg_store):
    """3d: a transaction that has read the tables (as a pg_dump does) holds ACCESS SHARE on them until it ends.
    A partition is made as a table of its own and then attached (SHARE UPDATE EXCLUSIVE on the parent), which
    doesn't conflict with that or with exports' writes: retention's pass makes the places (now and the next
    PRECREATE_AHEAD buckets), an export that needs a new place stores it, and no export waits on the reader.
    Before, all of them waited until the reader ended. A transaction holding a lock that ATTACH does conflict
    with: the DDL waits at most DDL_LOCK_TIMEOUT_MS a try, and the export that needed the place is answered busy
    after DDL_ATTEMPTS tries, nothing stored, while exports into existing places go on."""
    import psycopg

    from trailbeam.server.store import WriterBusy

    s = pg_store()
    w = Consumer()
    s.insert(records([w.send(D0, "existing-0")]))
    now = s.bucket(time.time_ns())
    ahead = {f"spans_p0_{b}" for b in range(now, now + s.PRECREATE_AHEAD + 1)}
    with psycopg.connect(s.url) as reader:  # not autocommit: the transaction stays open
        reader.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        reader.execute("SELECT COUNT(*) FROM spans").fetchone()
        # The reader stays open until after these, so DDL that waited on it would wait out DDL_LOCK_TIMEOUT_MS
        # and fail: the places wouldn't be there and the export needing one would be answered busy. No wall-clock
        # bound: a loaded CI runner can take seconds over these without anything waiting on the reader (QV-121).
        s._precreate()
        assert ahead <= _partitions(s)
        assert s.insert(records([w.send(D0 + 5 * DAY, "needs-a-place")])) == (1, 0)
        assert s.insert(records([w.send(D0 + SEC, "existing-1")])) == (1, 0)
        reader.rollback()

    # A DDL lock timeout far longer than an export takes, even on a loaded runner: an export that queued behind
    # the waiting ATTACH would wait it out (at least 4.7 s here), one that didn't takes well under a second.
    s.DDL_LOCK_TIMEOUT_MS, s.DDL_ATTEMPTS = 5_000, 2

    with psycopg.connect(s.url) as holder:  # as a schema change or a manual ANALYZE of the parent would
        holder.execute("BEGIN")
        holder.execute("LOCK TABLE spans IN SHARE UPDATE EXCLUSIVE MODE")
        new_place, held = [], []

        def export_new():
            t = time.monotonic()
            new_place.append((_try(s.insert, records([w.send(D0 + 9 * DAY, "busy")])), time.monotonic() - t))
        th = threading.Thread(target=export_new)
        th.start()
        time.sleep(0.3)  # its ATTACH is now waiting
        for i in range(2, 5):
            t = time.monotonic()
            assert s.insert(records([w.send(D0 + i * SEC, f"existing-{i}")])) == (1, 0)
            held.append(time.monotonic() - t)
        th.join(timeout=30)
        assert not th.is_alive()
        assert isinstance(new_place[0][0], WriterBusy), new_place
        assert new_place[0][1] < s.DDL_ATTEMPTS * s.DDL_LOCK_TIMEOUT_MS / 1000 + 5, new_place
        assert max(held) < 4, held  # not behind the ATTACH (>= DDL_LOCK_TIMEOUT_MS - 0.3 s)
        assert s.message_rows("busy") == []
        holder.rollback()
    assert s.insert(records([w.send(D0 + 9 * DAY, "busy")])) == (1, 0)
    assert len(s.message_rows("needs-a-place")) == 1 and len(s.message_rows("busy")) == 1
    assert len(s.recent_messages(10)) == 7


@needs_pg
def test_replicas_creating_the_same_place_at_once_all_go_on(pg_store):
    """Several replicas need the same new place at the same moment. Without the place's advisory lock, CREATE
    TABLE IF NOT EXISTS races: the loser fails on the table's row type in pg_type ("type ... already exists",
    DuplicateObject), or ATTACHes what the other attached and waits behind its exports. With it, one makes the
    place, the others wait for it and find it made: every replica's export goes on."""
    from trailbeam.server.postgres import PostgresStore

    s = pg_store()
    replicas = [s] + [PostgresStore(s.url) for _ in range(3)]
    try:
        for k in range(30):
            bucket = s.bucket(D0) + 100 + k
            go, errs = threading.Barrier(len(replicas)), []

            def make(r):
                go.wait()
                try:
                    r._ensure_places_for_ingest({(0, bucket)})
                except Exception as e:  # noqa: BLE001 - the test reports whatever a replica raised
                    errs.append(e)
            ts = [threading.Thread(target=make, args=(r,)) for r in replicas]
            for t in ts:
                t.start()
            for t in ts:
                t.join()
            assert errs == []
            assert f"spans_p0_{bucket}" in _partitions(s)
    finally:
        for r in replicas[1:]:
            r.close()


# ---- QV-128: the hourly tenant rollup ------------------------------------------------------------------------

def _rollup_traffic(seed, messages=300, hours=6):
    sys.path.insert(0, str(ROOT / "tests" / "server"))
    import test_rollup  # noqa: PLC0415
    return test_rollup, test_rollup._traffic(seed, messages=messages, hours=hours)


def _rollup_page(s, a, b):
    return sorted((x.tenant or "", x.sent, x.dead_lettered, sorted(x.waits.counts.items()))
                  for x in s.tenant_activity(a, b))


@needs_pg
def test_the_rollup_gives_the_pages_sqlite_gives_with_late_spans_and_moving_messages(pg_store):
    """Hour-long places, so messages move place as their spans come (late and out of order), leaving rows that
    take back what they added in the place they left; a sweep takes out what it deletes. The page is SQLite's,
    on whole hours, parts of hours and across them."""
    import random

    from trailbeam.server.db import SqliteStore
    from trailbeam.server.rollup import HOUR_MS

    tr, spans = _rollup_traffic(7)
    random.Random(7).shuffle(spans)
    pg, lite = pg_store(bucket_ns=HOUR_MS * 10 ** 6), SqliteStore()
    for k in range(0, len(spans), 40):
        assert pg.insert(spans[k:k + 40]) == lite.insert(spans[k:k + 40])
    with pg.reads.connection() as c:
        assert c.execute("SELECT COUNT(DISTINCT (rclass, bucket)) AS n FROM tenant_rollup").fetchone()["n"] > 3
        assert c.execute("SELECT COUNT(*) AS n FROM tenant_events WHERE kind = 'dead_lettered_again'").fetchone()["n"]
    for a, b in tr._ranges(7, 6):
        assert _rollup_page(pg, a, b) == _rollup_page(lite, a, b), (a, b)
    lite.close()
    # retention: "orders" expires in the first places, which mix it with "payments", so they are swept
    from trailbeam.server.store import Cutoffs
    cut, resume = (tr.T0 + 3 * HOUR_MS) * 10 ** 6, None
    deleted = 0
    while True:
        step = pg.purge_step(Cutoffs(None, {"orders": cut}), resume, 25)
        deleted += step.deleted
        if step.resume is None:
            break
        resume = step.resume
    assert deleted
    for a, b in tr._ranges(7, 6):
        assert _rollup_page(pg, a, b) == _pg_brute(pg, a, b), (a, b)


def _pg_brute(s, a, b):
    """The page as every tenant event in the range adds it up (test_rollup._brute, on Postgres)."""
    from trailbeam.server.rollup import Sketch
    by: dict = {}
    with s.reads.connection() as c:
        for r in c.execute("SELECT * FROM tenant_events WHERE at_ms BETWEEN %s AND %s", (a, b)).fetchall():
            g = by.setdefault(r["tenant"] or "", {"sent": 0, "waits": [], "dead": set()})
            if r["kind"] == "sent":
                g["sent"] += 1
            elif r["kind"] == "first_delivery":
                g["waits"].append(r["wait_ms"])
            elif r["kind"] == "dead_lettered":
                g["dead"].add(r["mid"])
    return sorted((t, g["sent"], len(g["dead"]), sorted(Sketch.of(g["waits"]).counts.items()))
                  for t, g in by.items() if g["sent"] or g["dead"] or g["waits"])


@needs_pg
def test_version_4_builds_the_rollup_place_by_place(pg_store):
    """A version-3 database: no rollup and no dead_lettered_again events. The upgrade gives the pages a database
    that stored the same spans at 4 gives, and the same tenant events."""
    from trailbeam.server.postgres import PostgresStore
    from trailbeam.server.rollup import HOUR_MS

    tr, spans = _rollup_traffic(8)
    s = pg_store(bucket_ns=HOUR_MS * 10 ** 6)
    s.insert(spans)
    ranges = tr._ranges(8, 6)
    want = [_rollup_page(s, a, b) for a, b in ranges]
    with s.ingest.connection() as c:
        events = c.execute("SELECT * FROM tenant_events ORDER BY rclass, bucket, at_ms, mid, seq").fetchall()
        assert any(e["kind"] == "dead_lettered_again" for e in events)
        _as_version(c, 3)
    again = PostgresStore(s.url, bucket_ns=HOUR_MS * 10 ** 6)
    try:
        assert again.migrated == [4, 5, 6, 7, 8, 9]
        assert [_rollup_page(again, a, b) for a, b in ranges] == want
        with again.ingest.connection() as c:
            assert c.execute("SELECT * FROM tenant_events ORDER BY rclass, bucket, at_ms, mid, seq").fetchall() == events
        again.insert(tr._traffic(9, messages=50, hours=6))  # and it goes on taking spans
    finally:
        again.close()


@needs_pg
def test_version_7_takes_deduplicated_fifo_sends_out_of_the_tenant_events(pg_store):
    """A version-6 database counted every FIFO send as a sent tenant event (QV-246). The upgrade gives the tenant
    events and the Tenants page a database that stored the same spans at 7 gives."""
    import unittest.mock
    from trailbeam.server import queries
    from trailbeam.server.postgres import PostgresStore

    p = Consumer(client_id="producer-1", service="orders-api", queue="orders.fifo")
    spans = [p.send(T0, "f1"), p.send(T0 + SEC, "f1"), p.send(T0 + 2 * SEC, "f1"),
             p.receive(T0 + 3 * SEC, "f1", "hf1"), p.send(T0, "s1", queue="orders")]
    for sp in spans:
        sp.attrs["trailbeam.tenant"] = "acme"
    ms = T0 // 10 ** 6
    fresh = pg_store(bucket_ns=SEC)
    fresh.insert(records(spans))
    want = queries.tenants(fresh, T0, 900, ms - 3_600_000, ms + 3_600_000)
    assert want["total_sent"] == 2
    with unittest.mock.patch("trailbeam.server.timeline.deduplicated_sends", return_value=set()):
        s = pg_store(bucket_ns=SEC)
        s.insert(records(spans))  # as version 6 stored them
    assert queries.tenants(s, T0, 900, ms - 3_600_000, ms + 3_600_000)["total_sent"] == 4
    with s.ingest.connection() as c:
        _as_version(c, 6)
    again = PostgresStore(s.url, bucket_ns=SEC)
    try:
        assert again.migrated == [7, 8, 9]
        assert queries.tenants(again, T0, 900, ms - 3_600_000, ms + 3_600_000) == want
        with again.ingest.connection() as c:
            assert [r["kind"] for r in c.execute("SELECT kind FROM tenant_events ORDER BY at_ms, kind")] == \
                ["sent", "sent", "first_delivery"]
            assert c.execute("SELECT SUM(sent) AS n FROM tenant_rollup").fetchone()["n"] == 2
        again.insert(records([p.send(T0 + 4 * SEC, "f1")]))  # and a later deduplicated send still isn't counted
        assert queries.tenants(again, T0, 900, ms - 3_600_000, ms + 3_600_000)["total_sent"] == 2
    finally:
        again.close()


# ---- QV-127: lookups by place, sealing and the filter ------------------------------------------------------

def _sealed(s) -> dict:
    """{(rclass, bucket): sealed_in} of every place."""
    with s.ingest.connection() as c:
        return {(r["rclass"], r["bucket"]): r["sealed_in"] for r in c.execute("SELECT * FROM places")}


def _pass(s, cutoffs=None) -> None:
    """One whole retention pass (its upkeep seals first); by default one that deletes nothing."""
    s.maintain()
    resume = None
    while True:
        resume = s.purge_step(cutoffs or Cutoffs(None), resume).resume
        if resume is None:
            return


def _timeline(store, mid) -> list:
    rows = store.message_rows(mid)
    index = {r["row"]: i for i, r in enumerate(rows)}
    return [(r["kind"], r["time_ns"], r["mid"], index.get(r["receive_row"])) for r in rows]


def _same_as_sqlite(s, exports, mids, tmp_path, between=None) -> None:
    """Each export stored on ``s`` and on a SQLite store; ``between(k)`` (sealing, say) runs on ``s`` before the
    k-th. Then both give the same timelines, summaries and counts: what the lookups changed is not the answers."""
    from trailbeam.server.db import SqliteStore

    tmp_path.mkdir(parents=True, exist_ok=True)
    lite = SqliteStore(str(tmp_path / "same.sqlite"))
    try:
        for k, spans in enumerate(exports):
            if between is not None:
                between(k)
            assert s.insert(records(spans)) == lite.insert(records(spans))
        for m in mids:
            assert _timeline(s, m) == _timeline(lite, m), m
        ours = [dict(r) for r in s.summaries(mids)]
        assert ours == [{k: dict(r)[k] for k in ours[0]} for r in lite.summaries(mids)]  # SQLite has rows_json too
        assert s.counts() == lite.counts()
    finally:
        lite.close()


@needs_pg
def test_a_settle_after_midnight_joins_its_receive(pg_store, tmp_path):
    """Day buckets: a delete stored after midnight joins its receive from before it, in either order of the
    two exports, whether day 0 is still hot or already sealed when the delete comes."""
    late = D0 + DAY - SEC  # 23:59:59 on day 0
    w = Consumer()
    exports = [[w.send(late - SEC, "m1"), w.receive(late, "m1", "h1")], [w.delete(late + 2 * SEC, "h1")],
               [w.send(late - SEC, "m2")], [w.delete(late + 2 * SEC, "h2")], [w.receive(late, "m2", "h2")]]
    for hot_buckets, name in ((10 ** 6, "hot"), (0, "sealed")):
        s = pg_store(hot_buckets=hot_buckets)
        _same_as_sqlite(s, exports, ["m1", "m2"], tmp_path / name, between=lambda k: _pass(s) if k in (1, 4) else None)
        assert s.counts()["unjoined_settles"] == 0
        old = {v for p, v in _sealed(s).items() if p[1] <= D0 // DAY + 1}
        assert old == ({None} if name == "hot" else {1, 2})


@needs_pg
def test_a_settle_past_the_oldest_hot_bucket_joins(pg_store, tmp_path):
    """Day buckets, two hot: a receive stored five days ago, its day since sealed into the filter; its delete,
    stored now, joins it. The other way round too: a receive now joins a delete stored (unjoined) in a sealed
    day. And a span late for a sealed day (its lookup rows went to the late tables) is found: a receive
    written to day -5 after it was sealed, whose delete comes now."""
    today = time.time_ns() // DAY
    old, now = (today - 5) * DAY + 3600 * SEC, today * DAY + 3600 * SEC
    s = pg_store(hot_buckets=2)
    w = Consumer()
    exports = [[w.send(old, "m1"), w.receive(old + SEC, "m1", "h1")],
               [w.send(old, "m2"), w.delete(old + 2 * SEC, "h2")],
               [w.send(old, "m3")],
               # (sealed here)
               [w.receive(old + 5 * SEC, "m3", "h3"), w.receive(old + 6 * SEC, "m4", "h4")],  # late for day -5
               [w.delete(now, "h1")], [w.receive(now + SEC, "m2", "h2")], [w.delete(now + 2 * SEC, "h3")],
               [w.delete(now + 3 * SEC, "h4"), w.send(now + 4 * SEC, "m5")]]
    _same_as_sqlite(s, exports, ["m1", "m2", "m3", "m4", "m5"], tmp_path,
                    between=lambda k: _pass(s) if k == 3 else None)
    sealed = _sealed(s)
    assert sealed[(0, today - 5)] > 0 and sealed[(0, today)] is None
    with s.ingest.connection() as c:  # m3 was in day -5's lookups already (its send); m4 and both hashes weren't
        assert [tuple(r.values()) for r in c.execute("SELECT * FROM mid_places_late")] == [("m4", 0, today - 5)]
        assert len(c.execute("SELECT * FROM hash_places_late").fetchall()) == 2
    assert s.counts()["unjoined_settles"] == 0
    rows = s.message_rows("m1")
    assert rows[2]["mid"] == "m1" and rows[2]["receive_row"] == rows[1]["row"]
    # retention drops day -5 whole: its lookup partitions with it, and its late rows
    _pass(s, Cutoffs(today * DAY))
    with s.ingest.connection() as c:
        assert c.execute("SELECT COUNT(*) AS n FROM mid_places_late").fetchone()["n"] == 0
        assert c.execute("SELECT COUNT(*) AS n FROM hash_places_late").fetchone()["n"] == 0
        assert c.execute("SELECT COUNT(*) AS n FROM mid_places WHERE bucket = %s", (today - 5,)).fetchone()["n"] == 0
        assert not c.execute("SELECT 1 FROM pg_tables WHERE tablename = %s",
                             (f"mid_places_p0_{today - 5}",)).fetchone()


@needs_pg
def test_new_messages_read_no_sealed_place(pg_store):
    """Once the old places are sealed, an export of new messages reads none of them (the filter says their keys
    are in none), while one that joins a receive in a sealed place reads them. Before the seal, a lookup reads
    every old place it can't rule out (on a read connection: none of their locks is held by the export)."""
    s = pg_store(bucket_ns=SEC)
    _history(s, 40)
    walks = []
    connection = s.reads.connection

    @contextlib.contextmanager
    def counted(*a, **kw):
        walks.append(1)
        with connection(*a, **kw) as c:
            yield c
    s.reads.connection = counted
    try:
        w = Consumer()
        t = time.time_ns()
        s.insert(records([w.send(t, "fresh-1"), w.receive(t + 1, "fresh-1", "h-fresh-1")]))
        assert walks, "before sealing, the old places are read for every key"
        s.reads.connection = connection
        _pass(s)
        assert all(v for p, v in _sealed(s).items() if p[1] < s._hot_from())
        s.reads.connection = counted
        walks.clear()
        s.insert(records([w.send(t + 2, "fresh-2"), w.receive(t + 3, "fresh-2", "h-fresh-2"),
                          w.delete(t + 4, "h-fresh-2")]))
        assert walks == []
        s.insert(records([w.delete(t + 5, "h-late")]))
        assert walks
    finally:
        s.reads.connection = connection
    rows = s.message_rows("late")
    assert [r["kind"] for r in rows] == ["send", "receive", "delete"] and rows[2]["receive_row"] == rows[1]["row"]
    assert [r["kind"] for r in s.message_rows("fresh-2")] == ["send", "receive", "delete"]


@needs_pg
def test_forever_retention_still_seals_the_old_places(pg_store):
    """Every queue kept forever: nothing expires, but the background purge still runs its passes (store upkeep),
    and they seal the places past the hot ones. Then an export of new messages reads no sealed place, as with a
    retention that deletes."""
    from trailbeam.server.retention import Purger, Retention

    r = Retention(None)
    s = pg_store(bucket_ns=SEC, retention=r)
    _history(s, 40)
    old = [p for p in _sealed(s) if p[1] < s._hot_from()]
    assert len(old) >= 40 and not any(_sealed(s)[p] for p in old)
    p = Purger(s, r, interval_s=3600, pause_s=0)
    p.start()
    try:
        assert p._thread is not None, "a store with upkeep gets passes even when everything is kept"
        deadline = time.monotonic() + 30
        while p.passes == 0 and time.monotonic() < deadline:
            time.sleep(0.05)
    finally:
        p.stop()
    assert p.passes >= 1 and p.last_pass["deleted_spans"] == 0
    sealed = _sealed(s)
    assert all(sealed[q] for q in old)
    walks = []
    connection = s.reads.connection

    @contextlib.contextmanager
    def counted(*a, **kw):
        walks.append(1)
        with connection(*a, **kw) as c:
            yield c
    s.reads.connection = counted
    try:
        w = Consumer()
        t = time.time_ns()
        s.insert(records([w.send(t, "fresh-1"), w.receive(t + 1, "fresh-1", "h-fresh-1"),
                          w.delete(t + 2, "h-fresh-1")]))
        assert walks == []
    finally:
        s.reads.connection = connection
    assert s.message_rows("old-0") and s.message_rows("late")  # kept forever


@needs_pg
def test_the_filter_grows_and_other_replicas_load_it(pg_store, monkeypatch):
    """Sealing adds the places' keys to the filter while it has room, and builds it anew, twice what it must
    hold, when it hasn't. Each is stored as the next version, the older ones removed. Every key of a sealed
    place is in it (no false negatives), and another replica on the database loads the newest."""
    from trailbeam.server import lookup_filter
    from trailbeam.server.postgres import PostgresStore

    monkeypatch.setattr(lookup_filter, "MIN_CAPACITY", 1)
    s = pg_store(bucket_ns=SEC)
    other = PostgresStore(s.url, bucket_ns=SEC)
    try:
        def seal_up_to(k):
            s.hot_buckets = s.bucket(time.time_ns()) - (T0 // SEC + k)
            assert s._seal_due() > 0
            return s._filter
        _history(s, 30)
        f1 = seal_up_to(10)  # "late" and old-0 .. old-9
        assert (f1.version, f1.keys) == (1, 1 + 1 + 10 * 2) and f1.capacity == 2 * f1.keys
        f2 = seal_up_to(12)  # 4 more keys: room
        assert (f2.version, f2.m, f2.capacity, f2.keys) == (2, f1.m, f1.capacity, f1.keys + 4)
        f3 = seal_up_to(30)  # 36 more: built anew
        assert f3.version == 3 and f3.keys == 2 + 30 * 2 and f3.capacity == 2 * f3.keys
        with s.ingest.connection() as c:
            assert [r["version"] for r in c.execute("SELECT version FROM lookup_filters")] == [3]
            assert {r["version"] for r in c.execute("SELECT version FROM lookup_filter")} == {3}
            assert all(lookup_filter.mid_key(r["mid"]) in f3 for r in c.execute("SELECT mid FROM mid_places"))
            assert all(lookup_filter.hash_key(r["receipt_hmac"], r["key_id"]) in f3
                       for r in c.execute("SELECT receipt_hmac, key_id FROM hash_places"))
        assert other._filter is None
        other._check_filter()
        assert other._filter.version == 3 and other._filter.bits == f3.bits
        other.insert(records([Consumer().delete(T0 + 100 * SEC, "h-late")]))
        assert [r["kind"] for r in s.message_rows("late")] == ["send", "receive", "delete"]
        assert len(other.message_rows("old-17")) == 3 and other.summaries(["old-29"])[0]["final_delete"]
    finally:
        other.close()


@needs_pg
def test_version_5_partitions_the_lookup_tables_place_by_place(pg_store, tmp_path):
    """A schema-4 database (unpartitioned lookup tables) is brought to version 5: each place's lookup rows move
    to its own partitions, the old tables go, and every place is unsealed until the next pass seals the old
    ones. Messages stored before read whole, and a late delete joins a receive stored before the upgrade."""
    from trailbeam.server.postgres import PostgresStore

    s = pg_store(bucket_ns=SEC, store_bodies={"orders"})
    _history(s, 20)
    with s.ingest.connection() as c:
        lookups = {t: c.execute(f"SELECT * FROM {t} ORDER BY 1, 2, 3").fetchall() for t in ("mid_places", "hash_places")}
        _as_version(c, 4)
        assert c.execute("SELECT relkind FROM pg_class WHERE relname = 'mid_places' "
                         "AND relnamespace = current_schema()::regnamespace").fetchone()["relkind"] == "r"
    again = PostgresStore(s.url, bucket_ns=SEC, store_bodies={"orders"})
    try:
        assert again.migrated == [5, 6, 7, 8, 9]
        with again.ingest.connection() as c:
            assert {t: c.execute(f"SELECT * FROM {t} ORDER BY 1, 2, 3").fetchall() for t in lookups} == lookups
            assert c.execute("SELECT to_regclass('mid_places_v4') AS t").fetchone()["t"] is None
            parts = {r["relname"] for r in c.execute(
                """SELECT ch.relname FROM pg_inherits i JOIN pg_class ch ON ch.oid = i.inhrelid
                   JOIN pg_class pa ON pa.oid = i.inhparent WHERE pa.relname = 'mid_places'
                     AND ch.relnamespace = current_schema()::regnamespace""")}
            assert len(parts) == 20
        assert all(v is None for v in _sealed(again).values())
        _pass(again)
        assert all(v for p, v in _sealed(again).items() if p[1] < again._hot_from())  # today's places: not
        assert len(again.message_rows("old-7")) == 3
        again.insert(records([Consumer().delete(T0 + 50 * SEC, "h-late")]))
        rows = again.message_rows("late")
        assert [r["kind"] for r in rows] == ["send", "receive", "delete"] and rows[2]["receive_row"] == rows[1]["row"]
    finally:
        again.close()


# ---- QV-225: the tables of people and tokens --------------------------------------------------------------

def _account_schema(c) -> dict:
    """Columns (name, type, nullable, default) and index definitions of the tables of people and tokens."""
    from trailbeam.server import account_tables
    cols = {t: sorted(tuple(r.values()) for r in c.execute(
        """SELECT column_name, data_type, is_nullable, is_identity, column_default FROM information_schema.columns
           WHERE table_schema = current_schema() AND table_name = %s""", (t,)).fetchall())
        for t in account_tables.TABLES}
    indexes = sorted((r["tablename"], r["indexname"], r["indexdef"].replace(r["schemaname"] + ".", ""))
                     for r in c.execute("SELECT schemaname, tablename, indexname, indexdef FROM pg_indexes "
                                        "WHERE schemaname = current_schema() AND tablename = ANY(%s)",
                                        (list(account_tables.TABLES),)).fetchall())
    return {"columns": cols, "indexes": indexes}


@needs_pg
@pytest.mark.parametrize("version", range(1, 8))
def test_every_older_version_gets_the_tables_of_people_and_tokens_a_new_database_has(pg_store, version):
    from trailbeam.server.postgres import PostgresStore

    s = pg_store(bucket_ns=SEC)
    s.insert(records([Consumer().send(T0, "m1")]))
    with s.ingest.connection() as c:
        new = _account_schema(c)
        assert all(new["columns"].values()) and len(new["indexes"]) == 11
        _as_version(c, version)
        assert not any(_account_schema(c)["columns"].values())
    again = PostgresStore(s.url, bucket_ns=SEC)
    try:
        assert again.migrated == list(range(version + 1, pg_schema_latest() + 1))
        with again.ingest.connection() as c:
            assert _account_schema(c) == new
        assert len(again.message_rows("m1")) == 1
    finally:
        again.close()
