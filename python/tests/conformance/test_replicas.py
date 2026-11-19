"""Several Trailbeam replicas on one Postgres (QV-81): each replica is a PostgresStore of its own (own pools, own
caches) on the same schema, as two server processes behind a load balancer are.

Every span of a dataset is sent to a random replica, in a random order, from many threads at once, so a settle
span and its receive land on different replicas in either order, and two exports of the same message are
written at the same moment. The answers must be exactly the conformance suite's (expected.json, built from
contract/fixtures and the join spans) and the shared outputs in contract/fixtures.

Runs when TRAILBEAM_TEST_POSTGRES_URL is set (CI: Postgres 14 and 16).
"""
import concurrent.futures
import os
import random
import threading

import pytest

import cases
from test_store_conformance import TENANTS, assert_snapshot
from trailbeam.server import summary
from trailbeam.server.otlp import decode

pytestmark = pytest.mark.skipif(not os.environ.get("TRAILBEAM_TEST_POSTGRES_URL"),
                                reason="TRAILBEAM_TEST_POSTGRES_URL is not set")
REPLICAS = 3
THREADS = 16


@pytest.fixture
def replicas():
    """``make(store_bodies)`` -> REPLICAS stores on one fresh schema, all opened at once (as replicas that start
    together run the migrations at once)."""
    from pg_helpers import drop_schema, new_schema
    from trailbeam.server.postgres import PostgresStore

    made = []

    def make(store_bodies=frozenset(), n=REPLICAS, **kw):
        url = new_schema()
        with concurrent.futures.ThreadPoolExecutor(n) as pool:
            stores = list(pool.map(lambda _: PostgresStore(url, store_bodies=store_bodies, bucket_ns=10 ** 9,
                                                           ingest_connections=4, **kw), range(n)))
        made.append((url, stores))
        return stores
    yield make
    for url, stores in made:
        for s in stores:
            s.close()
        drop_schema(url)


def scatter(stores, exports, seed):
    """Send every export to a random replica, in a random order, THREADS at a time."""
    rng = random.Random(seed)
    work = [(rng.choice(stores), records) for records in exports]
    rng.shuffle(work)
    with concurrent.futures.ThreadPoolExecutor(THREADS) as pool:
        for f in [pool.submit(s.insert, records) for s, records in work]:
            f.result()


@pytest.mark.parametrize("seed", range(4))
@pytest.mark.parametrize("name", sorted(cases.DATASETS))
def test_one_span_per_export_to_random_replicas_in_random_order(replicas, name, seed):
    """All but the kept body: contract 15.4 keeps the first send's body that arrived, so a message whose sends
    arrive in another order can show another send's body (as test_one_span_per_export_in_reverse_order)."""
    stores = replicas(cases.store_bodies(name))
    spans = [[r] for records in cases.exports(name) for r in records]
    scatter(stores, spans, seed)
    for s in stores:  # every replica gives the same answers
        assert_snapshot(s, name, skip=("body_view", "body_search"))


@pytest.mark.parametrize("seed", range(3))
@pytest.mark.parametrize("name", sorted(cases.DATASETS))
def test_whole_exports_to_random_replicas_in_random_order(replicas, name, seed):
    stores = replicas(cases.store_bodies(name))
    scatter(stores, cases.exports(name), seed)
    assert_snapshot(random.Random(seed).choice(stores), name)


def test_retries_on_another_replica_change_nothing(replicas):
    """An exporter whose request timed out sends it again, and the load balancer picks another replica."""
    name = "joins"
    stores = replicas(cases.store_bodies(name))
    exports = cases.exports(name)
    scatter(stores, exports + exports + exports, seed=11)
    assert_snapshot(stores[0], name)


def test_tenants_match_the_shared_expected_outputs_across_replicas(replicas):
    from trailbeam.server import queries

    stores = replicas()
    spans = [[r] for records in cases.exports("contract-3.2") for r in records]
    scatter(stores, spans, seed=5)
    s = stores[1]
    now, lt = int(TENANTS["now_ns"]), TENANTS["lambda_timeout_s"]
    for case in TENANTS["stats"]:
        got = queries.tenants(s, now, lt, case["from_ms"], case["to_ms"])
        assert {k: got[k] for k in ("total_sent", "untagged_sent", "rows")} == \
               {k: case[k] for k in ("total_sent", "untagged_sent", "rows")}, case["name"]
    for case in TENANTS["filter"]:
        assert sorted(s.recent_messages(100, tenant=case["tenant"])) == case["message_ids"], case["tenant"]
    for case in TENANTS["search"]:
        assert queries.find_tenants(s, case["text"]) == case["tenants"], case["text"]


def _records(spans):
    return decode(cases.to_proto(spans).SerializeToString(), "application/x-protobuf").records


def test_a_joined_settle_and_its_message_s_own_span_at_the_same_moment(replicas):
    """A delete carries no message ID: its join reaches the message through the receipt hash. At the same moment
    another replica stores a redelivery of that message. Neither summary may lose the other's span."""
    a, b = replicas(n=2)
    w = cases.Consumer()
    for i in range(60):
        m, t = f"m{i}", cases.T0 + i * 10 * cases.SEC
        a.insert(_records([w.send(t, m), w.receive(t + cases.SEC, m, f"h{i}")]))
        both = threading.Barrier(2)
        delete = _records([w.delete(t + 3 * cases.SEC, f"h{i}")])
        again = _records([w.receive(t + 2 * cases.SEC, m, f"h{i}-2", count=2)])
        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            for f in [pool.submit(lambda s, r: (both.wait(), s.insert(r)), s, r) for s, r in ((a, delete), (b, again))]:
                f.result()
        want = summary.summarize(a.message_rows(m))
        got = a.summaries([m])[0]
        assert {c: got[c] for c in ("last_ns", "deliveries", "final_delete")} == \
               {"last_ns": want["last_ns"], "deliveries": want["deliveries"], "final_delete": int(want["final_delete"])}, m


# ---- retention: one replica at a time ---------------------------------------------------------------------

def _purge(store, cutoffs):
    resume, n = None, 0
    while True:
        st = store.purge_step(cutoffs, resume, 50)
        n, resume = n + st.deleted, st.resume
        if resume is None:
            return n


def test_one_replica_runs_retention_and_another_takes_over_when_it_dies(replicas):
    import psycopg

    from trailbeam.server.store import Cutoffs

    a, b = replicas(cases.store_bodies("joins"), n=2)
    for records in cases.exports("joins"):
        a.insert(records)
    everything = Cutoffs(2 ** 63 - 1)
    assert _purge(a, Cutoffs(None)) == 0 and a.retention_lease_held  # a takes the lease at its first pass
    assert _purge(b, everything) == 0 and not b.retention_lease_held  # b does nothing while a holds it
    assert b.counts()["messages"] > 0
    with psycopg.connect(a.url, autocommit=True) as c:  # a's lease connection dies (as when its replica is killed)
        # with a timeout, pg_terminate_backend waits for the backend to exit (and so release the lock); without one it
        # returns once the signal is sent, and b's try-lock can still find the lock held (Postgres 14+)
        killed = c.execute("SELECT pg_terminate_backend(pid, 10000) FROM pg_locks"
                           " WHERE locktype = 'advisory' AND objid = %s", (a._lease_key & 0xFFFFFFFF,)).fetchall()
        assert killed == [(True,)]
    stored = sum(b.counts()["spans"].values())
    assert _purge(b, everything) == stored and b.retention_lease_held  # b takes over
    assert b.counts() == {"spans": {}, "messages": 0, "unjoined_settles": 0}
    assert _purge(a, everything) == 0 and not a.retention_lease_held  # a sees it lost the lease
    b.close()  # closing gives the lease back
    assert _purge(a, everything) == 0 and a.retention_lease_held
