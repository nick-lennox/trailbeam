"""Retention (retention.py, SqliteStore.purge_step): the settings, the purge pass, and what is SQLite's own:
the in-write check, the foreign key index, and giving space back. What every backend must do (global,
per-queue and forever cutoffs, the straddle rule) is in tests/conformance."""
import sqlite3
import threading
import time

import pytest

from spans import SEC, T0, Consumer
from trailbeam.server import migrations
from trailbeam.server.__main__ import main
from trailbeam.server.db import Database
from trailbeam.server.retention import Purger, Retention, describe, parse_duration, parse_queue_retentions
from trailbeam.server.store import Cutoffs

DAY = 86_400
c = Consumer()


def message(mid, t, queue="orders", handle=None):
    """send, receive, delete: one delivered message, its spans from t to t + 2 s."""
    h = handle or f"rh-{mid}"
    return [c.send(t, mid, queue=queue), c.receive(t + SEC, mid, h, queue=queue), c.delete(t + 2 * SEC, h, queue=queue)]


def mids(db):
    return set(db.recent_messages(10_000))


# ---- settings ------------------------------------------------------------------------------------

@pytest.mark.parametrize("text, seconds", [("7d", 7 * DAY), ("36h", 36 * 3600), ("90m", 5400), ("3600s", 3600),
                                           (" 1D ", DAY), ("forever", None), ("FOREVER", None)])
def test_durations(text, seconds):
    assert parse_duration(text) == seconds


@pytest.mark.parametrize("text", ["", "7", "0d", "-1d", "1w", "1.5d", "never", "7 days", "99999999d"])
def test_bad_durations(text):
    with pytest.raises(ValueError):
        parse_duration(text)


def test_queue_retentions():
    assert parse_queue_retentions("orders-dlq=30d, audit=forever,,") == {"orders-dlq": 30 * DAY, "audit": None}
    assert parse_queue_retentions("") == {}
    for bad in ("orders", "=7d", "orders=7", "a=1d,a=2d"):
        with pytest.raises(ValueError):
            parse_queue_retentions(bad)


def test_the_default_is_seven_days():
    assert Retention() == Retention.parse() == Retention(7 * DAY, {})
    assert describe(7 * DAY) == "7d" and describe(5400) == "90m" and describe(61) == "61s" and describe(None) == "forever"


def test_cutoffs_are_now_minus_the_retention():
    r = Retention(DAY, {"audit": None, "orders-dlq": 30 * DAY})
    assert r.cutoffs(T0) == Cutoffs(T0 - DAY * SEC, {"audit": None, "orders-dlq": T0 - 30 * DAY * SEC})
    assert Retention(None, {"a": None}).keeps_everything() and not Retention(None, {"a": DAY}).keeps_everything()


def test_the_command_takes_the_settings(monkeypatch):
    seen = {}

    class Stop(Exception):
        pass

    def fake_server(db, host, port, purger=None, **kw):
        seen["retention"] = purger.retention
        raise Stop

    monkeypatch.setattr("trailbeam.server.__main__.make_server", fake_server)
    with pytest.raises(Stop):
        main(["--db", ":memory:", "--retention", "36h", "--retention-queues", "audit=forever"])
    assert seen["retention"] == Retention(36 * 3600, {"audit": None})
    monkeypatch.setenv("TRAILBEAM_RETENTION", "forever")
    monkeypatch.setenv("TRAILBEAM_RETENTION_QUEUES", "orders=1d")
    with pytest.raises(Stop):
        main(["--db", ":memory:"])
    assert seen["retention"] == Retention(None, {"orders": DAY})
    monkeypatch.delenv("TRAILBEAM_RETENTION")
    monkeypatch.delenv("TRAILBEAM_RETENTION_QUEUES")
    with pytest.raises(Stop):
        main(["--db", ":memory:"])
    assert seen["retention"] == Retention(7 * DAY, {})
    with pytest.raises(SystemExit):
        main(["--db", ":memory:", "--retention", "7"])


# ---- the purge -----------------------------------------------------------------------------------

def test_a_pass_deletes_what_is_older_than_the_retention(db, ingest):
    now = T0 + 10 * DAY * SEC
    ingest(*message("old", T0), *message("new", now - DAY * SEC), *message("audit-old", T0, queue="audit"))
    p = Purger(db, Retention(7 * DAY, {"audit": None}), now=lambda: now, pause_s=0)
    r = p.run_pass()
    assert mids(db) == {"new", "audit-old"}
    assert r["deleted_spans"] == 3 and r["steps"] >= 2 and not r["stopped"]
    assert 0 <= r["longest_write_ms"] <= r["longest_step_ms"]  # the write is part of the step
    assert p.status()["last_pass"]["deleted_spans"] == 3
    assert p.status()["default"] == "7d" and p.status()["queues"] == {"audit": "forever"}
    assert p.run_pass()["deleted_spans"] == 0


def test_forever_everywhere_never_deletes(db, ingest):
    ingest(*message("m1", T0))
    p = Purger(db, Retention(None), now=lambda: T0 + 1000 * DAY * SEC)
    assert p.run_pass()["deleted_spans"] == 0
    p.start()  # no thread for a policy that keeps everything
    assert p._thread is None
    assert mids(db) == {"m1"}


def test_the_background_purge_runs_at_start_and_stops(db, ingest):
    ingest(*message("old", T0))
    p = Purger(db, Retention(DAY), now=lambda: T0 + 2 * DAY * SEC, interval_s=3600, pause_s=0)
    p.start()
    deadline = time.monotonic() + 10
    while p.passes == 0 and time.monotonic() < deadline:
        time.sleep(0.01)
    p.stop()
    assert not p._thread.is_alive()
    assert mids(db) == set() and p.status()["passes"] == 1


def test_a_message_that_gets_a_new_span_during_the_step_is_not_deleted(tmp_path):
    """The step reads without the writer lock; a span that arrives between that read and the delete keeps
    its message, because the delete checks again in its write transaction. So does a join that gives an
    unjoined settle span a message."""
    db = Database(str(tmp_path / "t.sqlite"))
    from trailbeam.server.otlp import decode
    from spans import to_proto

    def ingest(*spans):
        db.insert(decode(to_proto(list(spans)).SerializeToString(), "application/x-protobuf").records)

    ingest(*message("m1", T0), c.delete(T0, "rh-late"))  # the delete: unjoined, no message ID
    real_delete = db._delete

    def delete_after_new_spans(*args):
        ingest(c.change_visibility(T0 + 5 * DAY * SEC, "rh-m1", 30),  # joins m1: m1 is recent now
               c.receive(T0 + 5 * DAY * SEC, "m2", "rh-late"))  # joins the unjoined delete to m2
        return real_delete(*args)

    db._delete = delete_after_new_spans
    resume, deleted = None, 0
    while True:
        step = db.purge_step(Cutoffs(T0 + DAY * SEC), resume)
        deleted += step.deleted
        resume = step.resume
        if resume is None:
            break
    assert deleted == 0
    assert len(db.message_rows("m1")) == 4 and len(db.message_rows("m2")) == 2
    assert {r["mid"] for r in db.query("SELECT mid FROM messages")} == {"m1", "m2"}  # their summaries too
    db.close()


def test_purge_steps_do_not_hold_up_ingest(tmp_path):
    """Ingest goes on while a pass runs: between steps the writer lock is free."""
    db = Database(str(tmp_path / "t.sqlite"))
    from trailbeam.server.otlp import decode
    from spans import to_proto

    def records(*spans):
        return decode(to_proto(list(spans)).SerializeToString(), "application/x-protobuf").records

    for i in range(40):
        db.insert(records(*[s for j in range(50) for s in message(f"old-{i}-{j}", T0 + j)]))
    now = T0 + 30 * DAY * SEC
    p = Purger(db, Retention(DAY), now=lambda: now, batch=50, pause_s=0.001)
    done = threading.Event()
    inserted = []

    def ingest_loop():
        i = 0
        while not done.is_set():
            db.insert(records(*message(f"new-{i}", now)))
            inserted.append(i)
            i += 1

    t = threading.Thread(target=ingest_loop)
    t.start()
    r = p.run_pass()
    done.set()
    t.join()
    assert r["deleted_spans"] == 40 * 50 * 3
    assert inserted and mids(db) == {f"new-{i}" for i in inserted}
    db.close()


# ---- SQLite: the foreign key index, and space ----------------------------------------------------

def test_deleting_a_receive_uses_the_receive_row_index(tmp_path):
    """Settle spans point at their receive (receive_row REFERENCES spans(row)): every deleted receive makes
    SQLite look for them. With spans_receive_row that is an index lookup, not a scan of the table."""
    db = Database(str(tmp_path / "t.sqlite"))
    [plan] = [r["detail"] for r in db.query("EXPLAIN QUERY PLAN SELECT 1 FROM spans WHERE receive_row = ?", (1,))]
    assert "spans_receive_row" in plan
    db.close()


def test_a_step_reads_what_it_deletes_before_it_takes_the_writer(tmp_path, monkeypatch):
    """The pages a delete changes are read first, on the read connection (db._prefetch_delete): at 1M spans
    reading them from disk inside the write held ingest up for seconds. The prefetch writes nothing, uses an
    index for every lookup, and runs before the write, on the messages and settle spans the write deletes."""
    from trailbeam.server import db as dbmod
    from trailbeam.server.otlp import decode
    from spans import to_proto

    db = Database(str(tmp_path / "t.sqlite"))
    db.insert(decode(to_proto(message("m1", T0) + message("m2", T0 + 3 * DAY * SEC) + [c.delete(T0, "rh-late")]
                              ).SerializeToString(), "application/x-protobuf").records)
    order, real_prefetch, real_delete = [], dbmod._prefetch_delete, db._delete

    def prefetch(conn, mids, rows):
        before = conn.total_changes
        plans = []
        conn.set_trace_callback(lambda sql: plans.append(sql) if sql.lstrip().upper().startswith("SELECT") else None)
        try:
            real_prefetch(conn, mids, rows)
        finally:
            conn.set_trace_callback(None)
        assert conn.total_changes == before
        for sql in set(plans):
            detail = " ".join(r[-1] for r in conn.execute("EXPLAIN QUERY PLAN " + sql.replace("?", "NULL")))
            assert "SCAN spans" not in detail and "SCAN messages" not in detail, (sql, detail)
        order.append(("prefetch", sorted(mids), len(rows)))

    def delete(expired, unjoined, *rest):
        order.append(("delete", sorted(expired), len(unjoined)))
        return real_delete(expired, unjoined, *rest)

    monkeypatch.setattr(dbmod, "_prefetch_delete", prefetch)
    db._delete = delete
    Purger(db, Retention(DAY), now=lambda: T0 + 2 * DAY * SEC, pause_s=0).run_pass()
    assert order == [("prefetch", ["m1"], 1), ("delete", ["m1"], 1)]
    assert mids(db) == {"m2"}
    db.close()


def test_a_new_file_gives_space_back(tmp_path):
    from trailbeam.server.otlp import decode
    from spans import to_proto

    path = str(tmp_path / "t.sqlite")
    db = Database(path)
    assert db.storage()["auto_vacuum"] == "incremental"
    big = "x" * 2000
    for i in range(20):
        spans = [s for j in range(50) for s in message(f"m-{i}-{j}", T0)]
        for s in spans:
            s.attrs["padding"] = big
        db.insert(decode(to_proto(spans).SerializeToString(), "application/x-protobuf").records)
    full = db.storage()["pages"]
    r = Purger(db, Retention(DAY), now=lambda: T0 + 2 * DAY * SEC, pause_s=0, reclaim_pages=100).run_pass()
    after = db.storage()
    assert r["reclaimed_pages"] > 0 and after["free_pages"] == 0
    assert after["pages"] < full / 4
    db.close()


def test_an_old_file_keeps_its_size_until_vacuum(tmp_path):
    """A file created before incremental vacuum (auto_vacuum NONE) can only switch with a full VACUUM. Purged
    pages stay free inside it until then; --vacuum does it once."""
    path = str(tmp_path / "old.sqlite")
    raw = sqlite3.connect(path)
    raw.execute("PRAGMA auto_vacuum = NONE")
    raw.execute("CREATE TABLE placeholder (x)")  # fixes auto_vacuum NONE, as files from before QV-72 are
    raw.execute("DROP TABLE placeholder")
    raw.close()
    db = Database(path)
    assert db.storage()["auto_vacuum"] == "none"
    from trailbeam.server.otlp import decode
    from spans import to_proto
    db.insert(decode(to_proto([s for j in range(200) for s in message(f"m{j}", T0)]).SerializeToString(),
                     "application/x-protobuf").records)
    Purger(db, Retention(DAY), now=lambda: T0 + 2 * DAY * SEC, pause_s=0).run_pass()
    assert db.storage()["free_pages"] > 0 and db.reclaim_space(1000) == 0
    db.close()

    with pytest.raises(SystemExit) as e:
        main(["--db", path, "--vacuum"])
    assert e.value.code == 0
    db = Database(path)
    assert db.storage()["auto_vacuum"] == "incremental" and db.storage()["free_pages"] == 0
    assert db.query("SELECT MAX(version) AS v FROM schema_version")[0]["v"] == migrations.LATEST
    db.close()


def test_the_purge_holds_the_writer_for_at_most_its_duty():
    """The pause after a step is long enough that steps fill at most ``duty`` of the time (QV-72: at 1M spans
    an unpaced purge took two thirds of the writer and starved ingest)."""
    p = Purger(Database(), Retention(DAY), pause_s=0.05, duty=0.25)
    assert p.pause(0.1) == pytest.approx(0.3)  # 0.1 s of 0.4 s
    assert p.pause(0.001) == 0.05  # never less than pause_s
    assert Purger(Database(), Retention(DAY), pause_s=0, duty=1).pause(5) == 0
    with pytest.raises(ValueError):
        Purger(Database(), Retention(DAY), duty=0)


def test_the_purge_batch_and_duty_are_settable_and_shown_in_health():
    """``trailbeam --purge-batch``/``--purge-duty`` (QV-79: the bench sets a smaller step than the default)."""
    p = Purger(Database(), Retention(DAY), batch=10, duty=0.5)
    assert p.status()["purge_batch"] == 10 and p.status()["purge_duty"] == 0.5
    with pytest.raises(ValueError):
        Purger(Database(), Retention(DAY), batch=0)
