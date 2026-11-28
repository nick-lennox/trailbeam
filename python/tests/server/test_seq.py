"""When each span reached the server (spans.seq, store.next_seq, QV-593): written once at ingest, carried by the cause
that a span decided, and kept out of /api/causes. Triage's "failures after the resolve" counts by it (triage.py)."""
import math
import time

from spans import SEC, T0, Consumer
from trailbeam.server import causes, store, triage
from test_causes import failed, get, loaded  # noqa: F401  (loaded: the stuck messages /api/causes groups)


def test_next_seq_is_the_ms_times_1000_and_never_goes_back(monkeypatch):
    monkeypatch.setattr(store, "_seq_last", 0)
    assert [store.next_seq(5), store.next_seq(5), store.next_seq(5)] == [5000, 5001, 5002]
    assert store.next_seq(4) == 5003  # the clock went back: still after the last
    assert store.next_seq(9) == 9000
    before = time.time_ns() // 1_000_000
    got = store.next_seq()
    assert before * 1000 <= got <= (time.time_ns() // 1_000_000) * 1000 + 1


def _seqs(db, mid):
    return [(r["kind"], r["seq"]) for r in db.message_rows(mid)]


def test_ingest_writes_each_span_s_arrival_once(db, ingest):
    p, c = Consumer("producer"), Consumer("poller")
    first = time.time_ns() // 1_000_000 * 1000
    sent, received = p.send(T0, "m-1"), c.receive(T0 + SEC, "m-1", "rh-1")
    ingest(sent, received)
    got = _seqs(db, "m-1")
    assert [k for k, _ in got] == ["send", "receive"]
    seqs = [s for _, s in got]
    assert all(isinstance(s, int) for s in seqs) and len(set(seqs)) == 2 and min(seqs) >= first
    assert seqs[0] < seqs[1]  # stored in the export's order
    # the server's clock, not the spans' (T0 is years ago)
    assert min(seqs) // 1000 > T0 // 10 ** 6
    # an exporter's retry of the same spans keeps the first copy's seq; a later span gets a later one
    ingest(sent, received, c.delete(T0 + 2 * SEC, "rh-1"))
    again = _seqs(db, "m-1")
    assert len(again) == 3
    assert [s for _, s in again[:2]] == seqs and again[2][0] == "delete" and again[2][1] > max(seqs)


def _read(db):
    return causes.read_unfinished(db, T0 + 3600 * SEC, 900, None, 300)


def test_a_cause_carries_the_seq_of_the_span_that_decided_it(db, ingest):
    p, w = Consumer("producer"), Consumer("worker", source="lambda")
    ingest(p.send(T0, "m-1"), w.receive(T0 + SEC, "m-1", "rh-1", lambda_inv="inv-1"))
    ingest(failed(w, T0 + 2 * SEC, "m-1", "inv-1", "boom"))
    ingest(p.send(T0, "m-2"), Consumer("poller").receive(T0 + SEC, "m-2", "rh-2"))
    rows = {r["kind"]: r["seq"] for r in db.message_rows("m-1")}
    read = _read(db)
    by = {x["mid"]: x for x in read["classified"]}
    assert by["m-1"]["kind"] == "exc" and by["m-1"]["seq"] == rows["outcome"]  # the failed outcome, not the receive
    assert by["m-2"]["kind"] == "not_deleted"
    assert by["m-2"]["seq"] == {r["kind"]: r["seq"] for r in db.message_rows("m-2")}["receive"]
    groups = causes.group(read["classified"])
    exc = next(g for g in groups if g["kind"] == "exc")
    assert exc["arrivals"] == [{"mid": "m-1", "seq": rows["outcome"], "ms": T0 // 10 ** 6 + 2000, "version": None}]
    # what triage counts: a failure after a resolve whose watermark is below it, none after one above it (the resolve
    # at the spans' own time, so the failure is not a late export)
    at = T0 // 10 ** 6
    assert [a["mid"] for a in triage.new_since(exc["arrivals"], rows["outcome"] - 1, at)] == ["m-1"]
    assert triage.new_since(exc["arrivals"], rows["outcome"], at) == []


def test_spans_stored_before_seq_never_count_as_new(db, ingest):
    """Rows from before schema 29 have no seq: their causes have none, and no watermark counts them."""
    p, w = Consumer("producer"), Consumer("worker", source="lambda")
    ingest(p.send(T0, "m-1"), w.receive(T0 + SEC, "m-1", "rh-1", lambda_inv="inv-1"),
           failed(w, T0 + 2 * SEC, "m-1", "inv-1", "boom"))
    with db.lock:
        db.conn.execute("UPDATE spans SET seq = NULL")
    x = _read(db)["classified"][0]
    assert x["kind"] == "exc" and math.isnan(x["seq"])
    g = causes.group([x])[0]
    assert math.isnan(g["arrivals"][0]["seq"]) and triage.new_since(g["arrivals"], 0, T0 // 10 ** 6) == []


def test_api_causes_leaves_seq_and_arrivals_out(loaded):  # noqa: F811
    _, d = get(loaded, "/api/causes")
    assert d["groups"] and all("arrivals" not in g and "seq" not in g for g in d["groups"])
