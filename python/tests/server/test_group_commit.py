"""Group commit (SqliteStore.insert, QV-78): concurrent exports share one transaction, and each is still
its own: its own counts, its own failure, and acknowledged only once its spans are committed."""
import threading
import time

from spans import SEC, T0, Consumer, to_proto
from trailbeam.server.otlp import decode


def records(spans):
    return decode(to_proto(spans).SerializeToString(), "application/x-protobuf").records


def queued(db, calls):
    """Run each call in a thread while the test holds the writer lock, so they all queue; then let them go.
    Returns each call's result (or the exception it raised), in order."""
    out = [None] * len(calls)

    def run(i, fn):
        try:
            out[i] = fn()
        except Exception as e:  # noqa: BLE001 - the test looks at it
            out[i] = e

    threads = [threading.Thread(target=run, args=(i, fn)) for i, fn in enumerate(calls)]
    with db.lock:
        for t in threads:
            t.start()
        deadline = time.monotonic() + 10
        while len(db._queue) < len(calls) and time.monotonic() < deadline:
            time.sleep(0.005)
        assert len(db._queue) == len(calls)
    for t in threads:
        t.join(10)
    return out


def test_exports_waiting_for_the_writer_share_one_transaction(db):
    c = Consumer()
    a = records([c.send(T0, "m1"), c.receive(T0 + SEC, "m1", "rh-1")])
    b = records([c.delete(T0 + 2 * SEC, "rh-1")])  # joins the receive in the export before it, same transaction
    again = list(a)  # an exporter retry of the first export, in the same group
    before = dict(db.write_stats)
    assert queued(db, [lambda: db.insert(a), lambda: db.insert(b), lambda: db.insert(again)]) == [(2, 0), (1, 0), (0, 2)]
    assert db.write_stats["transactions"] - before["transactions"] == 1
    assert db.write_stats["exports"] - before["exports"] == 3
    rows = db.query("SELECT kind, mid, mid_source FROM spans ORDER BY row")
    assert [tuple(r) for r in rows] == [("send", "m1", "span"), ("receive", "m1", "span"), ("delete", "m1", "receipt_hash")]
    assert [r["mid"] for r in db.query("SELECT mid FROM messages")] == ["m1"]


def test_an_export_that_fails_fails_alone(db):
    c = Consumer()
    good = records([c.send(T0, "m1")])
    bad = records([c.send(T0, "m2")])
    del bad[0]["kind"]  # the insert raises on this export
    other = records([c.send(T0 + SEC, "m3")])
    res = queued(db, [lambda: db.insert(good), lambda: db.insert(bad), lambda: db.insert(other)])
    assert res[0] == (1, 0) and res[2] == (1, 0)
    assert isinstance(res[1], KeyError)
    assert sorted(r["mid"] for r in db.query("SELECT mid FROM spans")) == ["m1", "m3"]
    assert sorted(r["mid"] for r in db.query("SELECT mid FROM messages")) == ["m1", "m3"]
    # and the store goes on working
    assert db.insert(records([c.send(T0 + 2 * SEC, "m4")])) == (1, 0)


def test_an_export_returns_only_after_its_commit(db, monkeypatch):
    """Nothing is acknowledged that a crash could still lose: when insert returns, the COMMIT has run."""
    c = Consumer()
    commits = []
    real = db.conn

    class Spy:
        def execute(self, sql, *a):
            r = real.execute(sql, *a)
            if sql == "COMMIT":
                commits.append(time.monotonic())
            return r

        def __getattr__(self, k):
            return getattr(real, k)

    monkeypatch.setattr(db, "conn", Spy())
    returned = []
    res = queued(db, [lambda: (db.insert(records([c.send(T0, "m1")])), returned.append(time.monotonic()))[0],
                      lambda: (db.insert(records([c.send(T0, "m2")])), returned.append(time.monotonic()))[0]])
    assert res == [(1, 0), (1, 0)]
    assert len(commits) == 1 and all(t >= commits[0] for t in returned)


def test_a_group_is_capped_in_spans(db, monkeypatch):
    monkeypatch.setattr(type(db), "GROUP_MAX_SPANS", 2)
    c = Consumer()
    exports = [records([c.send(T0 + i * SEC, f"m{i}")]) for i in range(5)]
    before = db.write_stats["transactions"]
    assert queued(db, [lambda e=e: db.insert(e) for e in exports]) == [(1, 0)] * 5
    assert db.write_stats["transactions"] - before == 3  # 2 + 2 + 1


def test_a_wait_that_runs_out_while_queued_writes_nothing(db):
    """Backpressure (QV-74): past ``wait_s`` an export no writer has taken leaves the queue: WriterBusy, nothing
    written, and a later writer doesn't write it."""
    from trailbeam.server.store import WriterBusy
    c = Consumer()
    late = records([c.send(T0, "m1")])
    out = []
    with db.lock:  # a long write
        t = threading.Thread(target=lambda: out.append(_raised(lambda: db.insert(late, wait_s=0.05))))
        t.start()
        t.join(10)
    assert isinstance(out[0], WriterBusy)
    assert db._queue == []
    assert db.insert(records([c.send(T0 + SEC, "m2")])) == (1, 0)
    assert [r["mid"] for r in db.query("SELECT mid FROM spans")] == ["m2"]


def test_a_wait_that_runs_out_after_a_writer_took_the_export_waits_for_its_commit(db, monkeypatch):
    """An export a writer has taken is being written: it is answered by that COMMIT, not by WriterBusy, so the
    answer matches what is on disk. ``timings`` covers the wait and the group's write."""
    c = Consumer()
    first = records([c.send(T0, "m1")])
    second = records([c.send(T0 + SEC, "m2")])
    entered, go, slow = threading.Event(), threading.Event(), threading.Event()
    write_group, insert_rows = db._write_group, db._insert_rows

    def held_write_group():
        entered.set()
        go.wait(10)  # the first writer holds the lock until the second export has queued
        write_group()

    def slow_insert_rows(recs, moves):
        if not slow.is_set():
            slow.set()
            time.sleep(0.3)  # longer than the second export's wait_s, with it already taken
        return insert_rows(recs, moves)

    monkeypatch.setattr(db, "_write_group", held_write_group)
    monkeypatch.setattr(db, "_insert_rows", slow_insert_rows)
    timings: dict = {}
    out = [None, None]
    t1 = threading.Thread(target=lambda: out.__setitem__(0, db.insert(first)))
    t2 = threading.Thread(target=lambda: out.__setitem__(1, _raised(lambda: db.insert(second, wait_s=0.1,
                                                                                      timings=timings))))
    t1.start()
    assert entered.wait(10)  # the first export's thread is the writer, holding the lock
    t2.start()
    deadline = time.monotonic() + 10
    while len(db._queue) < 2 and time.monotonic() < deadline:
        time.sleep(0.005)
    go.set()
    t1.join(10)
    t2.join(10)
    assert out == [(1, 0), (1, 0)]
    assert db.write_stats["exports"] - db.write_stats["transactions"] >= 1  # the two shared one transaction
    assert sorted(r["mid"] for r in db.query("SELECT mid FROM spans")) == ["m1", "m2"]
    assert timings["write_s"] >= 0.25 and timings["wait_s"] >= 0


def _raised(fn):
    try:
        return fn()
    except Exception as e:  # noqa: BLE001 - the test looks at it
        return e


def test_an_export_queued_behind_more_than_one_group_is_written_before_its_call_returns(db):
    """A group takes the queue from its head, up to GROUP_MAX_SPANS. The writer whose own export is further
    back writes groups until it is in one: before QV-238 it wrote one and returned None, an export not yet
    stored (the server answered 500, and stored it later)."""
    from trailbeam.server.db import _Export

    c = Consumer()
    per = 64
    n_ahead = db.GROUP_MAX_SPANS // per + 3  # more than one group's worth
    ahead = [_Export(records([c.receive(T0 + (i * per + j) * SEC, f"a{i}-{j}", f"rh-a{i}-{j}") for j in range(per)]))
             for i in range(n_ahead)]
    db._queue.extend(ahead)  # queued by threads that haven't got the writer lock yet
    mine = records([c.send(T0, "mine")])
    assert db.insert(mine) == (1, 0)
    assert db._queue == [] or all(j not in db._queue for j in ahead[:-1])
    assert [r["mid"] for r in db.query("SELECT mid FROM spans WHERE mid = 'mine'")] == ["mine"]
