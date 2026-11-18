"""The Store conformance suite: what any storage backend must do, checked through the Store interface only.

Every backend gives the same answers as SQLite from the same spans: expected.json holds every read the server
makes (cases.snapshot) on three datasets built from contract/fixtures and from spans that exercise the joins.
The shared expected outputs in contract/fixtures (tenants-expected.json, body-views.json) are checked too.
Then what the snapshot can't show: arrival order doesn't matter, exporter retries change nothing, a failed
insert stores nothing, ingest and reads run concurrently, and retention deletes whole messages.

The backends are in conftest.py. A test here must not use SQL, ``query`` or anything else outside Store.
"""
import base64
import json
import threading

import pytest

import cases
from trailbeam.server import bodies, queries
from trailbeam.server.otlp import decode
from trailbeam.server.store import Store

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
        assert {k: got[k] for k in ("total_sent", "untagged_sent", "rows")} == \
               {k: case[k] for k in ("total_sent", "untagged_sent", "rows")}, case["name"]
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
        assert sorted(bodies.search(s, bodies.parse_query(q["query"]), s.store_bodies)) == q["message_ids"], q["query"]


# ---- retention ---------------------------------------------------------------------------------------

def _last_ns(s, m):
    return max(r["time_ns"] for r in s.message_rows(m))


def test_retention_deletes_whole_messages_last_seen_before_the_cutoff(new_store):
    name = "joins"
    s, untouched = load(new_store, name), load(new_store, name, store_name="b")
    now, lt = cases.DATASETS[name]["now_ns"], cases.LAMBDA_TIMEOUT_S
    mids = s.recent_messages(1000)
    last = {m: _last_ns(s, m) for m in mids}
    cutoff = sorted(last.values())[len(last) // 2]
    old = {m for m, t in last.items() if t < cutoff}
    assert old and old != set(mids)
    spans_before = sum(s.counts()["spans"].values())

    deleted = s.delete_messages_before(cutoff)
    assert deleted > 0
    assert s.delete_messages_before(cutoff) == 0
    assert set(s.recent_messages(1000)) == set(mids) - old
    for m in old:
        assert s.message_rows(m) == []
    for m in set(mids) - old:  # every kept message is exactly as it was, timeline and all
        assert queries.message(s, m, now, lt) == queries.message(untouched, m, now, lt), m
    assert sum(s.counts()["spans"].values()) == spans_before - deleted
    assert s.counts()["messages"] == len(set(mids) - old)


def test_retention_deletes_unjoined_settle_spans_by_their_own_time(new_store):
    s = load(new_store, "joins")
    assert s.counts()["unjoined_settles"] == 2  # two deletes whose handles no receive returned
    assert s.recent_messages(1000)  # the one with a message ID belongs to that message ("m-e")
    s.delete_messages_before(FOREVER)
    assert s.counts() == {"spans": {}, "messages": 0, "unjoined_settles": 0}


def test_retention_on_one_queue_keeps_the_others(new_store):
    s = load(new_store, "joins")
    on_billing = s.queue_messages("billing")
    others = set(s.recent_messages(1000)) - on_billing
    assert on_billing and others
    s.delete_messages_before(FOREVER, queue="billing")
    assert set(s.recent_messages(1000)) == others
    assert s.queue_messages("billing") == set()


def test_retention_works_in_batches(new_store):
    s = load(new_store, "joins")
    n = len(s.recent_messages(1000))
    rounds = 0
    while s.delete_messages_before(FOREVER, limit=2):
        rounds += 1
    assert rounds >= n // 2
    assert s.counts()["spans"] == {}


def test_a_store_takes_spans_again_after_retention(new_store):
    name = "joins"
    s = load(new_store, name)
    s.delete_messages_before(FOREVER)
    for records in cases.exports(name):
        s.insert(records)
    assert_snapshot(s, name)
