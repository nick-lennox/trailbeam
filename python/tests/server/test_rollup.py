"""rollup.py: the hourly tenant rollup the Tenants page reads (QV-128).

The page from the rollup must give what adding up every tenant event in the range gives: the same counts, and a
p95 that is the sketch's of the exact times in queue (so within rollup.BOUND of the exact nearest rank), for any
range, on whole hours, parts of hours and across them. And the rollup must stay what a rebuild from the summaries
gives, whatever order spans come in and after retention (test_summary.py checks that with the other tables).
"""
import json
import math
import random

import pytest

from trailbeam.server import queries, rollup, summary_table
from trailbeam.server.db import SqliteStore
from trailbeam.server.rollup import HOUR_MS, Sketch
from trailbeam.server.store import Cutoffs

MS = 1_000_000
T0 = 1_794_000_000_000  # ms; an hour boundary is T0 - T0 % HOUR_MS
ARN = "arn:aws:sqs:us-east-1:123456789012:"


# ---- the sketch -----------------------------------------------------------------------------------------

def test_the_sketch_is_exact_up_to_100_ms_and_within_the_bound_above():
    rnd = random.Random(1)
    for v in list(range(0, 5000)) + [rnd.randrange(1, 400 * 86_400_000) for _ in range(20000)]:
        got = rollup.value(rollup.bucket(v))
        if v <= rollup.EXACT_MS:
            assert got == v
        else:
            assert abs(got - v) <= rollup.BOUND * v, v


def test_buckets_keep_the_order_of_times():
    keys = [rollup.bucket(v) for v in range(0, 200000)]
    assert keys == sorted(keys)


@pytest.mark.parametrize("seed", range(20))
def test_p95_is_the_nearest_rank_within_the_bound(seed):
    rnd = random.Random(seed)
    waits = [int(rnd.lognormvariate(5, 2)) for _ in range(rnd.randrange(1, 3000))]
    exact = queries.p95(waits)
    got = Sketch.of(waits).p95()
    assert abs(got - exact) <= rollup.BOUND * exact
    assert Sketch.of(w for w in waits if w <= 100).p95() == queries.p95([w for w in waits if w <= 100])


def test_sketches_merge_and_subtract_exactly():
    a, b = Sketch.of([1, 2, 300, 300, 10 ** 6]), Sketch.of([2, 5000])
    both = Sketch.of([1, 2, 300, 300, 10 ** 6, 2, 5000])
    assert Sketch(a.counts).merge(b) == both
    assert Sketch(both.counts).merge(b, -1) == a
    assert not Sketch(a.counts).merge(a, -1) and Sketch().p95() is None
    assert Sketch.loads(both.dumps()) == both and Sketch.loads("") == Sketch()


def test_ranges_split_into_whole_hours_and_the_parts_at_either_end():
    h = 1000
    assert rollup.split(h * HOUR_MS, (h + 3) * HOUR_MS - 1) == ((h, h + 2), [])
    assert rollup.split(h * HOUR_MS + 5, (h + 3) * HOUR_MS) == (
        (h + 1, h + 2), [(h * HOUR_MS + 5, (h + 1) * HOUR_MS - 1), ((h + 3) * HOUR_MS, (h + 3) * HOUR_MS)])
    assert rollup.split(h * HOUR_MS + 5, h * HOUR_MS + 9) == (None, [(h * HOUR_MS + 5, h * HOUR_MS + 9)])
    assert rollup.split(h * HOUR_MS + 5, (h + 1) * HOUR_MS + 9) == (None, [(h * HOUR_MS + 5, (h + 1) * HOUR_MS + 9)])
    assert rollup.split(0, 0) == (None, [(0, 0)])


# ---- the page from the rollup ---------------------------------------------------------------------------

def _span(n, kind, at_ms, queue, mid, tenant=None, **more):
    attrs = {"trailbeam.tenant": tenant} if tenant else {}
    return {"trace_id": f"{n:032x}", "span_id": f"{n:016x}", "kind": kind, "time_ns": at_ms * MS + 17, "queue": queue,
            "message_id": mid, "receipt_hmac": f"h{n}", "key_id": "k", "attributes": attrs, **more}


def _traffic(seed: int, messages: int = 600, hours: int = 9) -> list:
    """Sends, first deliveries (some with no send: SQS's SentTimestamp), redeliveries, and dead-letter moves,
    once, twice (a chain of dead-letter queues) or three times, over ``hours`` hours, tenants and none."""
    rnd = random.Random(seed)
    out, n = [], 0
    for i in range(messages):
        mid = f"m{seed}-{i}"
        tenant = rnd.choice(["acme", "globex", "initech", None])
        q = rnd.choice(["orders", "payments"])
        sent = T0 + rnd.randrange(0, hours * HOUR_MS)
        if rnd.random() < 0.85:
            n += 1
            out.append(_span(n, "send", sent, q, mid, tenant))
        t = sent + int(rnd.lognormvariate(6, 2.5))
        n += 1
        out.append(_span(n, "receive", t, q, mid, tenant, receive_count=1, sent_ts_ms=sent))
        if rnd.random() < 0.3:
            n += 1
            out.append(_span(n, "receive", t + 30_000, q, mid, tenant, receive_count=2, sent_ts_ms=sent))
        if rnd.random() < 0.25:
            chain = [q] + [f"{q}-dlq{k}" for k in range(1, rnd.choice([1, 1, 2, 3]) + 1)]
            at = t
            for frm, to in zip(chain, chain[1:]):
                at += rnd.choice([1, 10, HOUR_MS // 3, HOUR_MS * 2])
                n += 1
                out.append(_span(n, "receive", at, to, mid, tenant, receive_count=1, sent_ts_ms=sent,
                                 dlq_source_arn=ARN + frm))
    return out


def _brute(s, from_ms, to_ms) -> dict:
    """The page as it was before the rollup: every tenant event in the range."""
    by: dict = {}
    for r in s.query("SELECT * FROM tenant_events WHERE at_ms BETWEEN ? AND ?", (from_ms, to_ms)):
        g = by.setdefault(r["tenant"], {"sent": 0, "waits": [], "dead": set()})
        if r["kind"] == "sent":
            g["sent"] += 1
        elif r["kind"] == "first_delivery":
            g["waits"].append(r["wait_ms"])
        elif r["kind"] == "dead_lettered":
            g["dead"].add(r["mid"])
    return {t: (g["sent"], len(g["dead"]), len(g["waits"]), Sketch.of(g["waits"]))
            for t, g in by.items() if g["sent"] or g["dead"] or g["waits"]}


def _page(s, from_ms, to_ms) -> dict:
    return {a.tenant: (a.sent, a.dead_lettered, a.waits.n, a.waits) for a in s.tenant_activity(from_ms, to_ms)}


def _ranges(seed: int, hours: int) -> list:
    rnd = random.Random(seed)
    h0 = T0 // HOUR_MS
    out = [(0, 2 ** 50), (T0, T0 + hours * HOUR_MS * 3), (h0 * HOUR_MS, (h0 + 3) * HOUR_MS - 1),
           ((h0 + 2) * HOUR_MS, (h0 + 2) * HOUR_MS), ((h0 + 2) * HOUR_MS - 1, (h0 + 4) * HOUR_MS)]
    for _ in range(40):
        a = T0 + rnd.randrange(-HOUR_MS, (hours + 2) * HOUR_MS)
        b = a + rnd.choice([0, 1, 59_000, HOUR_MS - 1, HOUR_MS, rnd.randrange(0, hours * HOUR_MS)])
        if rnd.random() < 0.3:
            a -= a % HOUR_MS
        out.append((a, b))
    return out


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_the_page_from_the_rollup_is_every_event_in_the_range_added_up(seed):
    s = SqliteStore()
    spans = _traffic(seed)
    random.Random(seed).shuffle(spans)  # late spans: tenants, sends and moves arrive after what they change
    for k in range(0, len(spans), 50):
        s.insert(spans[k:k + 50])
    assert s.query("SELECT COUNT(*) AS n FROM tenant_events WHERE kind = 'dead_lettered_again'")[0]["n"] > 10
    for a, b in _ranges(seed, 9):
        assert _page(s, a, b) == _brute(s, a, b), (a, b)
    s.close()


def test_a_message_moved_twice_is_dead_lettered_once():
    s = SqliteStore()
    h = (T0 // HOUR_MS + 1) * HOUR_MS
    s.insert([_span(1, "send", h - 10, "q", "m", "acme"), _span(2, "receive", h - 5, "q", "m", "acme", receive_count=1),
              _span(3, "receive", h + 10, "dlq1", "m", "acme", receive_count=1, dlq_source_arn=ARN + "q"),
              _span(4, "receive", h + 3 * HOUR_MS + 10, "dlq2", "m", "acme", receive_count=1, dlq_source_arn=ARN + "dlq1")])
    dead = lambda a, b: {r["tenant"]: r["dead_lettered"] for r in queries.tenants(s, 0, 900, a, b)["rows"]}
    assert dead(0, 2 ** 50) == {"acme": 1}                                   # two moves, one message
    assert dead(h, h + 4 * HOUR_MS) == {"acme": 1}                           # both in whole hours
    assert dead(h + 3 * HOUR_MS, h + 4 * HOUR_MS) == {"acme": 1}             # only the second
    assert dead(h + HOUR_MS, h + 2 * HOUR_MS) == {}                          # neither
    assert dead(h + 11, h + 4 * HOUR_MS) == {"acme": 1}
    s.close()


def test_retention_takes_what_it_removes_out_of_the_rollup():
    s = SqliteStore()
    s.insert(_traffic(4, messages=300, hours=4))
    cut = (T0 + 2 * HOUR_MS) * MS
    resume = None
    while True:
        step = s.purge_step(Cutoffs(cut), resume, limit=40)
        if step.resume is None:
            break
        resume = step.resume
    rows = lambda: sorted(tuple(r) for r in s.query("SELECT * FROM tenant_rollup"))
    kept = rows()
    with s.lock:
        s.conn.execute("BEGIN IMMEDIATE")
        try:
            summary_table.rebuild_rollup(s.conn)
            rebuilt = rows()
        finally:
            s.conn.execute("ROLLBACK")
    assert kept == rebuilt and kept
    for a, b in _ranges(4, 4):
        assert _page(s, a, b) == _brute(s, a, b), (a, b)
    s.close()


def test_rows_say_what_their_messages_add():
    s = SqliteStore()
    h = T0 // HOUR_MS
    s.insert([_span(1, "send", h * HOUR_MS + 1, "orders", "a", "acme"),
              _span(2, "receive", h * HOUR_MS + 401, "orders", "a", "acme", receive_count=1),
              _span(3, "send", h * HOUR_MS + 2, "orders", "b"),
              _span(4, "send", (h + 1) * HOUR_MS, "payments", "c", "acme")])
    got = {tuple(r)[:5]: json.loads(r["waits"]) for r in s.query("SELECT * FROM tenant_rollup ORDER BY hour, tenant")}
    assert got == {(h, "", 1, 0, 0): {}, (h, "acme", 1, 1, 0): {str(rollup.bucket(400)): 1}, (h + 1, "acme", 1, 0, 0): {}}
    assert math.isclose(rollup.value(rollup.bucket(400)), 400, rel_tol=rollup.BOUND)
    s.close()


def test_schema_12_builds_the_rollup_of_a_schema_11_file(tmp_path):
    """A file from before QV-128: no rollup, and no dead_lettered_again events. The upgrade gives the tables a
    file that stored the same spans at 12 has."""
    path = str(tmp_path / "trailbeam.sqlite")
    s = SqliteStore(path)
    s.insert(_traffic(5, messages=300, hours=5))
    want = {t: sorted(tuple(r) for r in s.query(f"SELECT * FROM {t}")) for t in summary_table.TABLES}
    with s.lock:  # back to schema 11
        c = s.conn
        c.execute("BEGIN IMMEDIATE")
        c.execute("DROP TABLE tenant_rollup")
        c.execute("DROP INDEX tenant_events_again")
        c.execute("DELETE FROM tenant_events")
        for mid, tenant, ev in c.execute("SELECT mid, tenant, events_json FROM messages").fetchall():
            ev = [e for e in json.loads(ev) if e[1] != "dead_lettered_again"]
            c.execute("UPDATE messages SET events_json = ? WHERE mid = ?", (json.dumps(ev), mid))
            c.executemany("INSERT INTO tenant_events (at_ms, mid, seq, kind, tenant, wait_ms) VALUES (?, ?, ?, ?, ?, ?)",
                          [(at, mid, i, kind, tenant, wait) for i, (at, kind, wait) in enumerate(ev)])
        c.execute("UPDATE schema_version SET version = 11 WHERE version = 12")  # made at 12: "create"
        c.execute("UPDATE meta SET value = '11' WHERE key = 'schema_version'")
        c.execute("COMMIT")
    s.close()
    s = SqliteStore(path)
    assert s.migrated == [12]
    assert {t: sorted(tuple(r) for r in s.query(f"SELECT * FROM {t}")) for t in summary_table.TABLES} == want
    assert want["tenant_rollup"] and any(r[3] == "dead_lettered_again" for r in want["tenant_events"])
    s.close()
