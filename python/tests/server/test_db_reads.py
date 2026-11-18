"""On a file, reads use their own read-only connections (WAL) and never wait for the writer (QV-70).

QV-68: with one connection behind one lock, message detail took 1.5 ms idle and 75.7 s while ingest ran.
"""
import sqlite3
import threading
import time

import pytest

from spans import SEC, T0, Consumer, to_proto
from trailbeam.server.db import Database
from trailbeam.server.otlp import decode


def records(*spans):
    return decode(to_proto(list(spans)).SerializeToString(), "application/x-protobuf").records


@pytest.fixture
def filedb(tmp_path):
    d = Database(str(tmp_path / "t.sqlite"))
    yield d
    d.close()


def test_file_is_in_wal_mode(filedb):
    assert filedb.conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    assert filedb.shared_reads


def test_reads_do_not_wait_for_an_open_write_transaction(filedb):
    c = Consumer()
    filedb.insert(records(c.receive(T0, "m1", "rh-1")))
    with filedb.lock:  # an export in progress: the writer holds the lock and an open transaction
        filedb.conn.execute("BEGIN IMMEDIATE")
        filedb.conn.execute("UPDATE spans SET queue = 'changed'")
        out = {}

        def read():
            s = time.monotonic()
            out["rows"] = filedb.message_rows("m1")
            out["counts"] = filedb.counts()
            out["s"] = time.monotonic() - s

        t = threading.Thread(target=read)
        t.start()
        t.join(5)
        assert not t.is_alive(), "a read waited for the writer"
        filedb.conn.execute("ROLLBACK")
    assert out["s"] < 1
    assert [r["queue"] for r in out["rows"]] == ["orders"]  # the last committed snapshot, not the open write
    assert out["counts"]["spans"] == {"receive": 1}


def test_reads_see_each_committed_write(filedb):
    c = Consumer()
    filedb.query("SELECT 1")  # a pooled reader exists before the write
    filedb.insert(records(c.receive(T0, "m1", "rh-1")))
    assert [r["kind"] for r in filedb.message_rows("m1")] == ["receive"]
    filedb.insert(records(c.delete(T0 + SEC, "rh-1")))
    assert [r["kind"] for r in filedb.message_rows("m1")] == ["receive", "delete"]


def test_read_connections_cannot_write(filedb):
    with pytest.raises(sqlite3.OperationalError, match="readonly|read-only|query_only"):
        filedb.query("DELETE FROM spans")


def test_readers_are_pooled_and_closed(filedb):
    with filedb.reader() as a:
        with filedb.reader() as b:
            assert a is not b and a is not filedb.conn
    with filedb.reader() as again:
        assert again in (a, b)
    filedb.close()
    with pytest.raises(sqlite3.ProgrammingError):
        a.execute("SELECT 1")


def test_a_failed_read_does_not_return_its_connection_to_the_pool(filedb):
    with pytest.raises(sqlite3.OperationalError):
        with filedb.reader() as bad:
            bad.execute("SELECT nope FROM spans")
    with filedb.reader() as c:
        assert c is not bad


def test_memory_database_reads_through_the_writer():
    db = Database(":memory:")
    with db.reader() as c:
        assert c is db.conn
    db.close()


def test_commits_do_not_checkpoint_a_thread_does(tmp_path, monkeypatch):
    """QV-73: SQLite's automatic checkpoint ran inside nearly every export's COMMIT. On a file the writer
    never checkpoints; the checkpoint thread copies the log back, with PASSIVE checkpoints."""
    monkeypatch.setattr(Database, "CHECKPOINT_INTERVAL_S", 0.05)
    seen = []
    real = Database.checkpoint

    def checkpoint(self, conn=None):
        out = real(self, conn)
        seen.append((threading.current_thread().name, out))
        return out

    monkeypatch.setattr(Database, "checkpoint", checkpoint)
    d = Database(str(tmp_path / "t.sqlite"))
    try:
        assert d.conn.execute("PRAGMA wal_autocheckpoint").fetchone()[0] == 0
        c = Consumer()
        d.insert(records(*[c.receive(T0 + i * SEC, f"m{i}", f"rh-{i}") for i in range(50)]))
        deadline = time.monotonic() + 5
        while not any(log > 0 and done == log for _, (busy, log, done) in seen) and time.monotonic() < deadline:
            time.sleep(0.05)
        assert {name for name, _ in seen} == {"trailbeam-checkpoint"}
        assert any(busy == 0 and log > 0 and done == log for _, (busy, log, done) in seen)
        assert len(d.message_rows("m7")) == 1
    finally:
        d.close()
    assert not d._checkpointer.is_alive()


def test_memory_database_has_no_checkpoint_thread():
    d = Database(":memory:")
    assert d._checkpointer is None
    d.close()
