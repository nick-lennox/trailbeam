"""The Store conformance suite: what any storage backend must do, checked through the Store interface only.

Every backend gives the same answers as SQLite from the same spans: expected.json holds every read the server
makes (cases.snapshot) on three datasets built from contract/fixtures and from spans that exercise the joins.
The shared expected outputs in contract/fixtures (tenants-expected.json, body-views.json) are checked too.
Then what the snapshot can't show: arrival order doesn't matter, exporter retries change nothing, a failed
insert stores nothing, ingest and reads run concurrently, and retention deletes whole messages
(global, per queue and forever cutoffs, and a message that straddles the cutoff).

The backends are in conftest.py. A test here must not use SQL, ``query`` or anything else outside Store.
"""
import base64
import json
import threading

import pytest

import cases
from trailbeam.server import bodies, queries
from trailbeam.server.otlp import decode
from trailbeam.server.store import Cutoffs, Store

EXPECTED = json.loads((cases.Path(__file__).resolve().parent / "expected.json").read_text())
DATASETS = sorted(cases.DATASETS)
FOREVER = 2 ** 63 - 1  # a cutoff after every span: the largest time a 64-bit store holds


def load(new_store, name, exports=None, store_name="a") -> Store:
    s = new_store(cases.store_bodies(name), store_name)
    for records in exports if exports is not None else cases.exports(name):
        s.insert(records)
    return s


def assert_snapshot(store, name, skip=()):
    got = json.loads(cases.canonical(cases.snapshot(store, name)))
    want = {k: v for k, v in EXPECTED[name].items() if k != "ingest"}
    for key in skip:
        del got[key], want[key]
    for key in want:  # one key at a time, so a failure names the read that differs
        assert got[key] == want[key], f"{name}: {key}"
    assert got.keys() == want.keys()


def test_it_is_a_store(new_store):
    s = new_store()
    assert isinstance(s, Store)
    assert s.store_bodies == frozenset()
    assert isinstance(s.retention_granularity_ns, int) and s.retention_granularity_ns >= 1
    assert (s.last_row(), s.counts()) == (0, {"spans": {}, "messages": 0, "unjoined_settles": 0})
    assert s.message_rows("m1") == []
    assert s.recent_messages(10) == [] and s.duplicate_messages(10) == [] and s.stuck_messages(FOREVER, 10) == []


@pytest.mark.parametrize("name", DATASETS)
def test_every_read_gives_the_expected_answer(new_store, name):
    s = new_store(cases.store_bodies(name))
    assert [list(s.insert(records)) for records in cases.exports(name)] == EXPECTED[name]["ingest"]
    assert_snapshot(s, name)


@pytest.mark.parametrize("name", DATASETS)
def test_rows_carry_the_columns_the_server_reads(new_store, name):
    s = load(new_store, name)
    rows = [r for m in s.recent_messages(1000) for r in s.message_rows(m)]
    assert rows
    for r in rows:
        assert isinstance(r["row"], int) and isinstance(r["time_ns"], int)
        assert isinstance(json.loads(r["attributes"]), dict)
    for m in s.recent_messages(1000):
        rs = s.message_rows(m)
        assert [(r["time_ns"], r["row"]) for r in rs] == sorted((r["time_ns"], r["row"]) for r in rs)
        assert {r["mid"] for r in rs} == {m}
    assert s.last_row() >= len(rows)


# ---- arrival order and retries ---------------------------------------------------------------------

@pytest.mark.parametrize("name", DATASETS)
def test_exports_in_reverse_order_give_the_same_answers(new_store, name):
    """Settle spans arrive before their receives: the join runs from both sides."""
    assert_snapshot(load(new_store, name, list(reversed(cases.exports(name)))), name)


@pytest.mark.parametrize("name", DATASETS)
def test_one_span_per_export_in_reverse_order_gives_the_same_answers(new_store, name):
    """All but the kept body: contract 15.4 keeps the first send's body that arrived, so when a message's sends
    arrive in another order its Body tab, and what body search reads, can be another send's."""
    spans = [[r] for records in cases.exports(name) for r in records]
    assert_snapshot(load(new_store, name, list(reversed(spans))), name, skip=("body_view", "body_search"))


@pytest.mark.parametrize("name", DATASETS)
def test_an_exporter_retry_changes_nothing(new_store, name):
    s = load(new_store, name)
    stored = sum(s.counts()["spans"].values())
    for records in cases.exports(name):
        assert s.insert(records) == (0, len(records))
    assert sum(s.counts()["spans"].values()) == stored
    assert_snapshot(s, name)


def test_a_failed_insert_stores_nothing(new_store):
    """An export is stored whole or not at all: here the records stop with an error part way through."""
    s = new_store()
    good = cases.exports("joins")[0]

    def records():
        yield from good[:3]
        raise RuntimeError("the export broke off")
    with pytest.raises(RuntimeError, match="broke off"):
        s.insert(records())
    assert s.counts()["spans"] == {}
    assert s.recent_messages(10) == []
    assert s.insert(good) == (len(good), 0)  # and the store still works


@pytest.mark.parametrize("name", DATASETS)
def test_backpressure_options_change_no_answer(new_store, name):
    """What the server's ingest gate passes to insert (backpressure.py): a bound on the wait, which an idle store
    never reaches, and a dict that gets the wait and write times. The answers are the same as without them."""
    s = new_store(cases.store_bodies(name))
    for records, want in zip(cases.exports(name), EXPECTED[name]["ingest"]):
        timings = {}
        assert list(s.insert(records, wait_s=5.0, timings=timings)) == want
        assert set(timings) == {"wait_s", "write_s"}
        assert all(isinstance(v, float) and v >= 0 for v in timings.values())
    assert_snapshot(s, name)


def test_joins_are_reported_to_on_join(new_store):
    """/metrics times every receipt-hash join through Store.on_join; a store that never calls it hides them."""
    s = new_store()
    seen = []
    s.on_join = lambda join, seconds: seen.append((join, seconds))
    for records in cases.exports("joins"):
        s.insert(records)
    kinds = [r["kind"] for records in cases.exports("joins") for r in records]
    assert {j for j, _ in seen} <= {"settle", "receive"}
    assert sum(j == "receive" for j, _ in seen) == kinds.count("receive")
    assert sum(j == "settle" for j, _ in seen) == sum(k in ("delete", "change_visibility") for k in kinds)
    assert all(sec >= 0 for _, sec in seen)


def test_file_sizes_are_none_or_bytes(new_store):
    sizes = new_store().file_sizes()
    assert sizes is None or (sizes and all(isinstance(n, int) and n >= 0 for n in sizes.values()))


def test_ingest_and_reads_at_once(new_store):
    """Four exporters and a reader on different threads, as the server runs them."""
    name = "joins"
    s = new_store(cases.store_bodies(name))
    exports = cases.exports(name)
    errors, done = [], threading.Event()

    def write(part):
        try:
            for records in part:
                s.insert(records)
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    def read():
        try:
            while not done.is_set():
                for m in s.recent_messages(100):
                    s.message_rows(m)
                s.counts()
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    reader = threading.Thread(target=read)
    reader.start()
    writers = [threading.Thread(target=write, args=(exports[i::4],)) for i in range(4)]
    for t in writers:
        t.start()
    for t in writers:
        t.join()
    done.set()
    reader.join()
    assert errors == []
    assert_snapshot(s, name)


# ---- the shared expected outputs in contract/fixtures -------------------------------------------------

TENANTS = json.loads((cases.FIXTURES / "tenants-expected.json").read_text())
BODY_VIEWS = json.loads((cases.FIXTURES / "body-views.json").read_text())



def test_tenants_match_the_shared_expected_outputs(new_store):
    s = load(new_store, "contract-3.2")
    now, lt = int(TENANTS["now_ns"]), TENANTS["lambda_timeout_s"]
    for case in TENANTS["stats"]:
        got = queries.tenants(s, now, lt, case["from_ms"], case["to_ms"])
        assert {k: got[k] for k in ("total_sent", "untagged_sent")} == \
               {k: case[k] for k in ("total_sent", "untagged_sent")}, case["name"]
        assert cases.rows_match(got["rows"], case["rows"], TENANTS["p95_relative_error"]), case["name"]
    for case in TENANTS["filter"]:
        assert sorted(s.recent_messages(100, tenant=case["tenant"])) == case["message_ids"], case["tenant"]
    for case in TENANTS["search"]:
        assert queries.find_tenants(s, case["text"]) == case["tenants"], case["text"]


@pytest.mark.parametrize("case", BODY_VIEWS["cases"], ids=lambda c: c["name"])
def test_body_views_match_the_shared_expected_outputs(new_store, case):
    s = new_store(frozenset(case["store_bodies"]))
    for e in json.loads((cases.FIXTURES / "contract-3.2-spans.json").read_text())["exports"]:
        if case.get("services") is None or e["service"] in case["services"]:
            s.insert(decode(base64.b64decode(e["base64"]), e["contentType"]).records)
    mids = sorted(s.recent_messages(1000))
    assert {m: bodies.body_view(m, s.message_rows(m), s.store_bodies) for m in mids} == case["views"]
    for q in case["searches"]:
        assert sorted(bodies.search(s, bodies.parse_query(q["query"]), s.store_bodies).mids) == q["message_ids"], q["query"]
        # with a tenant: the same matches, only those naming it (QV-96), as the Messages page filters them
        for tenant in ("acme", "hooli", "nobody-xyz"):
            want = sorted(set(q["message_ids"]) & set(s.recent_messages(1000, tenant=tenant)))
            got = bodies.search(s, bodies.parse_query(q["query"]), s.store_bodies, tenant=tenant).mids
            assert sorted(got) == want, (q["query"], tenant)


# ---- retention ---------------------------------------------------------------------------------------

def purge(s, cutoffs, limit=500) -> int:
    """One whole retention pass, as the server runs it. Returns the spans deleted."""
    deleted, resume, steps = 0, None, 0
    while True:
        step = s.purge_step(cutoffs, resume, limit)
        deleted, resume, steps = deleted + step.deleted, step.resume, steps + 1
        assert steps < 10_000, "the pass never ends"
        if resume is None:
            return deleted


def expires(s, t: int) -> int:
    """The earliest cutoff that expires a message whose newest span is at ``t``: ``t + 1`` on a store with
    exact retention, the end of ``t``'s bucket on one that drops whole time buckets (Store.retention_granularity_ns)."""
    g = s.retention_granularity_ns
    return (t // g + 1) * g


def _last_ns(s, m):
    return max(r["time_ns"] for r in s.message_rows(m))


def _queues(s, m):
    return {r["queue"] for r in s.message_rows(m)}


def _timelines(s, mids, name):
    now, lt = cases.DATASETS[name]["now_ns"], cases.LAMBDA_TIMEOUT_S
    return {m: queries.message(s, m, now, lt) for m in mids}


def test_retention_deletes_whole_messages_last_seen_before_the_cutoff(new_store):
    name = "joins"
    s, untouched = load(new_store, name), load(new_store, name, store_name="b")
    mids = s.recent_messages(1000)
    last = {m: _last_ns(s, m) for m in mids}
    cutoff = sorted(last.values())[len(last) // 2]
    old = {m for m, t in last.items() if expires(s, t) <= cutoff}
    assert old and old != set(mids)
    spans_before = sum(s.counts()["spans"].values())

    deleted = purge(s, Cutoffs(cutoff))
    assert deleted > 0
    assert purge(s, Cutoffs(cutoff)) == 0
    assert set(s.recent_messages(1000)) == set(mids) - old
    for m in old:
        assert s.message_rows(m) == []
    kept = set(mids) - old  # every kept message is exactly as it was, timeline and all
    assert _timelines(s, kept, name) == _timelines(untouched, kept, name)
    assert sum(s.counts()["spans"].values()) == spans_before - deleted
    assert s.counts()["messages"] == len(kept)


def test_a_message_that_straddles_the_cutoff_is_kept_whole_until_its_newest_span_expires(new_store):
    """The straddle rule: a message whose spans fall on both sides of the cutoff keeps every span, old ones
    too, and goes all at once when its newest span is older than the cutoff."""
    name = "joins"
    s, untouched = load(new_store, name), load(new_store, name, store_name="b")
    spread = {m: sorted({r["time_ns"] for r in s.message_rows(m)}) for m in s.recent_messages(1000)}
    m, times = max(spread.items(), key=lambda kv: len(kv[1]))
    assert len(times) >= 3
    straddling = times[len(times) // 2]  # spans before it and at or after it
    before = s.message_rows(m)
    purge(s, Cutoffs(straddling))
    assert [dict(r) for r in s.message_rows(m)] == [dict(r) for r in before]
    assert _timelines(s, [m], name) == _timelines(untouched, [m], name)
    purge(s, Cutoffs(times[-1]))  # the newest span is at the cutoff, not before it: still kept
    assert len(s.message_rows(m)) == len(before)
    purge(s, Cutoffs(expires(s, times[-1]) - 1))  # exact retention: the same cutoff; buckets: its bucket's end - 1
    assert len(s.message_rows(m)) == len(before)
    purge(s, Cutoffs(expires(s, times[-1])))
    assert s.message_rows(m) == []


def test_retention_deletes_unjoined_settle_spans_by_their_own_time(new_store):
    s = load(new_store, "joins")
    assert s.counts()["unjoined_settles"] == 2  # two deletes whose handles no receive returned
    assert s.recent_messages(1000)  # the one with a message ID belongs to that message ("m-e")
    purge(s, Cutoffs(FOREVER))
    assert s.counts() == {"spans": {}, "messages": 0, "unjoined_settles": 0}


def test_forever_keeps_everything(new_store):
    name = "joins"
    s = load(new_store, name)
    before = s.counts()
    assert purge(s, Cutoffs(None)) == 0
    assert purge(s, Cutoffs(None, {"billing": None, "orders": None})) == 0
    assert s.counts() == before
    assert_snapshot(s, name)


def test_a_queue_retention_applies_to_messages_on_that_queue_only(new_store):
    s = load(new_store, "joins")
    only_billing = {m for m in s.recent_messages(1000) if _queues(s, m) == {"billing"}}
    others = set(s.recent_messages(1000)) - only_billing
    assert only_billing and others
    purge(s, Cutoffs(None, {"billing": FOREVER}))  # billing: everything is old; the rest: kept forever
    assert set(s.recent_messages(1000)) == others


def test_the_longest_retention_of_a_message_s_queues_wins(new_store):
    """A message that moved to a dead-letter queue has spans on both: a queue kept longer (here forever)
    keeps the whole message, its spans on the other queue too."""
    s = load(new_store, "joins")
    mids = s.recent_messages(1000)
    on_dlq = {m for m in mids if "orders-dlq" in _queues(s, m)}
    assert on_dlq and any(len(_queues(s, m)) > 1 for m in on_dlq)
    rows = {m: len(s.message_rows(m)) for m in on_dlq}
    purge(s, Cutoffs(FOREVER, {"orders-dlq": None}))
    assert set(s.recent_messages(1000)) == on_dlq
    assert {m: len(s.message_rows(m)) for m in on_dlq} == rows

    s2 = load(new_store, "joins", store_name="b")
    newest = max(_last_ns(s2, m) for m in mids)
    purge(s2, Cutoffs(FOREVER, {"orders-dlq": expires(s2, newest)}))  # both expired: deleted
    assert s2.recent_messages(1000) == []


def test_retention_works_in_batches(new_store):
    s = load(new_store, "joins")
    n = len(s.recent_messages(1000))
    steps = []
    resume = None
    while True:
        step = s.purge_step(Cutoffs(FOREVER), resume, limit=2)
        steps.append(step)
        resume = step.resume
        if resume is None:
            break
    assert len(steps) > 2 and sum(st.examined for st in steps) >= n
    assert s.counts()["spans"] == {}


def test_a_store_takes_spans_again_after_retention(new_store):
    name = "joins"
    s = load(new_store, name)
    purge(s, Cutoffs(FOREVER))
    for records in cases.exports(name):
        s.insert(records)
    assert_snapshot(s, name)


def test_reclaim_space_is_safe_to_call(new_store):
    s = load(new_store, "joins")
    purge(s, Cutoffs(FOREVER))
    assert s.reclaim_space(1000) >= 0
    assert s.reclaim_space(1000) == 0
    assert s.counts()["spans"] == {}


# ---- trailbeam doctor (QV-148): the newest span, and probes -------------------------------------------------

def test_last_span_ns_is_the_time_of_the_span_stored_last(new_store):
    s = new_store()
    assert s.last_span_ns() is None
    s = load(new_store, "joins", store_name="b")
    got = s.last_span_ns()
    if got is None:
        pytest.skip("this store does not say when its last span was stored (Store.last_span_ns)")
    records = [r for export in cases.exports("joins") for r in export]
    assert got in {r["time_ns"] for r in records}
    later = dict(records[0], span_id="0123456789abcdef", time_ns=min(r["time_ns"] for r in records) - 10 ** 9)
    later["start_ns"] = later["time_ns"]
    s.insert([later])  # one new span, stored last but older than every other: its time, not the newest one's
    assert s.last_span_ns() == later["time_ns"]


def test_probes_are_kept_apart_from_the_spans(new_store):
    s = new_store()
    try:
        s.record_probes(["doctor-0123456789ab"], 1_000)
    except NotImplementedError:
        pytest.skip("this store does not keep probes (Store.record_probes): trailbeam doctor's test span is rejected")
    assert s.probe("doctor-0123456789ab") == 1_000
    assert s.probe("doctor-not-stored") is None
    s.record_probes(["doctor-0123456789ab"], 2_000)  # the same ID again: replaced
    assert s.probe("doctor-0123456789ab") == 2_000
    assert s.last_row() == 0 and s.last_span_ns() is None and s.counts()["spans"] == {}
    s.record_probes(["doctor-later-000001"], 2_000 + s.PROBE_KEEP_NS + 1)  # a day later: the first is deleted
    assert s.probe("doctor-0123456789ab") is None and s.probe("doctor-later-000001") is not None


# ---- a FIFO send SQS deduplicated (QV-246) ---------------------------------------------------------------

def _fifo_dedup_spans():
    """Two sends of one message ID on a FIFO queue (the second deduplicated: SQS returned the first's MessageId and
    enqueued nothing), then its one delivery; and a standard queue's message sent once, for a second tenant."""
    from cases import SEC, T0, Consumer  # noqa: PLC0415
    p = Consumer(client_id="producer-1", service="orders-api", queue="orders.fifo")
    c = Consumer(queue="orders.fifo")
    spans = [p.send(T0, "f1"), p.send(T0 + SEC, "f1"), c.receive(T0 + 2 * SEC, "f1", "hf1"),
             c.delete(T0 + 3 * SEC, "hf1", mid="f1"), p.send(T0 + SEC, "s1", queue="orders")]
    for sp, tenant in zip(spans, ("acme", "acme", "acme", None, "globex")):
        if tenant:
            sp.attrs["trailbeam.tenant"] = tenant
    return spans


def test_a_deduplicated_fifo_send_is_marked_and_not_counted_as_sent(new_store):
    from cases import T0, to_proto  # noqa: PLC0415
    from trailbeam.server import timeline  # noqa: PLC0415
    s = new_store()
    s.insert(decode(to_proto(_fifo_dedup_spans()).SerializeToString(), "application/x-protobuf").records)
    t = timeline.build(s.message_rows("f1"), T0 + 3600 * 10 ** 9)
    sends = [e for e in t["events"] if e["type"] == "sent"]
    assert [e.get("deduplicated", False) for e in sends] == [False, True]
    assert sends[1]["reason"] == timeline.DEDUPLICATED_REASON and "reason" not in sends[0]
    assert t["first_seen"] == sends[0]["end"] and t["deliveries"] == 1
    assert not any(e.get("deduplicated") for e in timeline.build(s.message_rows("s1"), T0)["events"])
    ms = T0 // 10 ** 6
    got = queries.tenants(s, T0, 900, ms - 3_600_000, ms + 3_600_000)
    assert got["total_sent"] == 2 and got["untagged_sent"] == 0
    assert {r["tenant"]: (r["sent"], r["share"]) for r in got["rows"]} == {"acme": (1, 0.5), "globex": (1, 0.5)}
