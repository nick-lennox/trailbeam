"""summary.py: a message's summary says what its timeline says, and picks the messages the SQL of Duplicates,
Stuck and the Tenants page picked, on the conformance datasets and on generated traffic with every kind of
message in it (redeliveries, visibility changes, dead-letter moves, stuck messages, Lambda consumers and
settle spans that arrive before their receive, Lambda invocations with no outcome, messages with no send)."""
import sys
import time
import zlib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / "tests" / "conformance"), str(ROOT / "bench")]

import cases  # noqa: E402
import loadgen  # noqa: E402

from trailbeam.server import queries, summary, summary_table, timeline  # noqa: E402
from trailbeam.server.db import SqliteStore  # noqa: E402

PROFILE = loadgen.Profile(queues=6, tenants=4, body_size=0, body_queues=0, redelivery=0.2, visibility_change=0.2,
                          dlq=0.1, stuck=0.1, lambda_consumer=0.3, late_join=0.4, batch_spans=40)
NOW = time.time_ns()


def _generated():
    s = SqliteStore()
    for batch in loadgen.Stream(PROFILE, seed=9).batches_of_messages(1500):
        # Some Lambda invocations with no outcome yet ("processing" until the Lambda timeout), and some messages
        # whose producer isn't instrumented (first seen on SQS's clock).
        s.insert([r for r in loadgen.records(batch) if not _drop(r)])
    return s, NOW


def _drop(r) -> bool:
    n = zlib.crc32(r["message_id"].encode()) if r.get("message_id") else 1
    return (r["kind"] == "outcome" and n % 4 == 0) or (r["kind"] == "send" and n % 7 == 0)


def _dataset(name):
    s = SqliteStore(store_bodies=cases.store_bodies(name))
    for records in cases.exports(name):
        s.insert(records)
    return s, cases.DATASETS[name]["now_ns"]


@pytest.fixture(scope="module", params=sorted(cases.DATASETS) + ["generated"])
def data(request):
    s, now = _generated() if request.param == "generated" else _dataset(request.param)
    mids = s.recent_messages(10 ** 6)
    rows = {m: s.message_rows(m) for m in mids}
    yield s, now, mids, rows, {m: summary.summarize(rs) for m, rs in rows.items()}
    s.close()


def _timeline_item(rows, now, lt, mrc):
    """What the list pages showed before summaries: the fields of the full timeline."""
    t = timeline.build(rows, now, lt, mrc)
    consumers = sorted({(e["consumer"].get("service_name"), e["consumer"].get("client_id"))
                        for e in t["events"] if e["type"] == "received"}, key=str)
    keys = ("state", "source_queue", "current_queue", "in_dead_letter_queue", "dead_letter_queue_move",
            "deliveries", "successful_deliveries", "first_seen", "first_seen_clock", "last_seen")
    return {"message_id": t["message_id"], **{k: t[k] for k in keys},
            "consumers": [{"service_name": a, "client_id": b} for a, b in consumers]}


def test_the_generated_traffic_has_every_kind_of_message(data):
    s, now, mids, rows, sums = data
    if len(mids) < 100:
        pytest.skip("a fixture dataset")
    states = {summary.state(x, now, 900) for x in sums.values()} | {summary.state(x, 0, 900) for x in sums.values()}
    assert states == {"deleted", "deleted_inferred", "processing", "not_deleted"}
    assert any(x["moved"] for x in sums.values()) and any(x["dup_received"] for x in sums.values())
    assert any(x["stuck_sqs_clock"] for x in sums.values())


@pytest.mark.parametrize("later_s", [0, 1, 1000, 10 ** 6])
@pytest.mark.parametrize("lambda_timeout_s", [1, 900])
def test_a_list_item_is_what_the_timeline_says(data, later_s, lambda_timeout_s):
    s, now, mids, rows, sums = data
    for mrc in (None, 3, 1):
        for m in mids:
            limits = {q: mrc for q in sums[m]["queues"]} if mrc else None
            got = summary.item(sums[m], now + later_s * 10 ** 9, lambda_timeout_s, limits)
            assert got == _timeline_item(rows[m], now + later_s * 10 ** 9, lambda_timeout_s, limits), m


def test_duplicates_are_the_messages_the_sql_picked(data):
    s, now, mids, rows, sums = data
    by_last = lambda m: (-sums[m]["last_ns"], m)  # noqa: E731
    for reason, test in ((None, lambda x: x["dup_received"] or x["dup_succeeded"]),
                         ("received_more_than_once", lambda x: x["dup_received"]),
                         ("succeeded_more_than_once", lambda x: x["dup_succeeded"])):
        assert sorted((m for m in mids if test(sums[m])), key=by_last) == _old_duplicates(s, reason)


def test_stuck_messages_are_the_ones_the_sql_picked(data):
    s, now, mids, rows, sums = data
    for cutoff in (now, now - 10 ** 12, 2 ** 62):
        got = sorted(((m, x["stuck_first_ns"], x["stuck_sqs_clock"]) for m, x in sums.items()
                      if x["stuck_first_ns"] is not None and x["stuck_open"] and x["stuck_first_ns"] < cutoff),
                     key=lambda t: (t[1], t[0]))
        assert got == _old_stuck(s, cutoff)


def test_tenant_events_count_what_the_timeline_counts(data):
    s, now, mids, rows, sums = data
    for m in mids:
        events = timeline.build(rows[m], now, 900)["events"]
        ms = [r["time_ns"] // 1_000_000 for r in rows[m]]
        lo, hi = min(ms), max(ms)
        for a, b in ((0, 2 ** 60), (lo, hi), (lo + 1, hi), (lo, hi - 1), ((lo + hi) // 2, hi), (lo, (lo + hi) // 2)):
            mine = [e for e in sums[m]["tenant_events"] if a <= e[0] <= b]
            assert queries.tenant_counts(events, summary.message_tenant(rows[m]), a, b) == {
                "tenant": sums[m]["tenant"], "sent": sum(e[1] == summary.SENT for e in mine),
                "waits": [e[2] for e in mine if e[1] == summary.FIRST_DELIVERY],
                "dead_lettered": any(e[1] == summary.DEAD_LETTERED for e in mine)}, (m, a, b)


def test_tenants_and_queues_are_every_span_naming_them(data):
    s, now, mids, rows, sums = data
    for m in mids:
        named: dict = {}
        for r in rows[m]:
            if (t := summary.span_tenant(r)) is not None:
                named[t] = max(named.get(t, 0), r["time_ns"])
        assert sums[m]["tenants"] == named
        assert sums[m]["queues"] == sorted({r["queue"] for r in rows[m]})


# ---- the SQL the list pages ran before the summary table (QV-73), kept here as the reference -------------

def _old_duplicates(s, reason):
    having = {"received_more_than_once": "receives > 1", "succeeded_more_than_once": "successes > 1",
              None: "receives > 1 OR successes > 1"}[reason]
    return [r["mid"] for r in s.query(
        f"""SELECT s.mid, SUM(s.kind = 'receive') AS receives,
                   COUNT(DISTINCT CASE WHEN s.kind = 'delete' AND s.result = 'ok' THEN s.receive_row END)
                   + COUNT(DISTINCT CASE WHEN s.kind = 'outcome' AND s.outcome = 'success'
                                         THEN s.faas_invocation_id END) AS successes,
                   MAX(s.time_ns) AS last
            FROM spans s WHERE s.mid IS NOT NULL GROUP BY s.mid HAVING {having} ORDER BY last DESC, s.mid""")]


def _old_stuck(s, cutoff_ns):
    return [(r["mid"], r["first_ns"], bool(r["on_sqs_clock"])) for r in s.query(
        """WITH latest AS (
                SELECT mid, row AS rrow, faas_invocation_id AS inv FROM (
                    SELECT mid, row, faas_invocation_id,
                           ROW_NUMBER() OVER (PARTITION BY mid ORDER BY time_ns DESC, row DESC) AS rn
                    FROM spans WHERE kind = 'receive')
                WHERE rn = 1),
            seen AS (
                SELECT s.mid, MIN(CASE WHEN s.kind = 'send' THEN s.time_ns END) AS send_ns,
                       MIN(CASE WHEN s.kind = 'receive' THEN s.sent_ts_ms END) * 1000000 AS sqs_ns,
                       MIN(s.time_ns) AS any_ns
                FROM spans s WHERE s.mid IS NOT NULL AND s.kind IN ('send', 'receive') GROUP BY s.mid),
            first AS (
                SELECT mid, COALESCE(send_ns, sqs_ns, any_ns) AS first_ns,
                       send_ns IS NULL AND sqs_ns IS NOT NULL AS on_sqs_clock FROM seen)
            SELECT f.mid, f.first_ns, f.on_sqs_clock FROM first f LEFT JOIN latest l ON l.mid = f.mid
            WHERE f.first_ns < ?
              AND NOT EXISTS (SELECT 1 FROM spans d WHERE d.kind = 'delete' AND d.result = 'ok' AND d.receive_row = l.rrow)
              AND NOT EXISTS (SELECT 1 FROM spans o WHERE o.kind = 'outcome' AND o.outcome = 'success'
                              AND o.mid = f.mid AND o.faas_invocation_id = l.inv)
            ORDER BY f.first_ns, f.mid""", (cutoff_ns,))]


# ---- the summary tables (summary_table.py) ---------------------------------------------------------------

def _tables(s) -> dict:
    return {t: sorted(tuple(r) for r in s.query(f"SELECT * FROM {t}")) for t in summary_table.TABLES}


def _rebuilt(s) -> dict:
    """The tables as a backfill from the stored spans builds them."""
    with s.lock:
        s.conn.execute("BEGIN IMMEDIATE")
        try:
            before = _tables(s)
            summary_table.backfill(s.conn)
            after = _tables(s)
        finally:
            s.conn.execute("ROLLBACK")
    assert _tables(s) == before
    return after


def _generated_exports():
    return [[r for r in loadgen.records(b) if not _drop(r)]
            for b in loadgen.Stream(PROFILE, seed=11).batches_of_messages(400)]


@pytest.mark.parametrize("order", ["as exported", "reversed", "shuffled spans", "one span at a time, reversed"])
def test_insert_keeps_the_tables_what_a_backfill_builds(order):
    exports = _generated_exports()
    spans = [r for e in exports for r in e]
    if order == "reversed":
        exports = exports[::-1]
    elif order == "shuffled spans":
        import random
        random.Random(5).shuffle(spans)
        exports = [spans[i:i + 37] for i in range(0, len(spans), 37)]
    elif order == "one span at a time, reversed":
        exports = [[r] for r in spans[::-1]]
    s = SqliteStore()
    for e in exports:
        s.insert(e)
        s.insert(e[: len(e) // 2])  # an exporter retry changes nothing
    tables = _tables(s)
    assert len(tables["messages"]) == len(s.recent_messages(10 ** 6)) > 300
    assert all(tables[t] for t in summary_table.TABLES)
    assert tables == _rebuilt(s)
    s.close()


def test_retention_removes_the_summaries_of_what_it_deletes():
    s = SqliteStore()
    for e in _generated_exports():
        s.insert(e)
    lasts = sorted(r["last_ns"] for r in s.query("SELECT last_ns FROM messages"))
    s.delete_messages_before(lasts[len(lasts) // 2])
    s.delete_messages_before(lasts[-1] + 1, queue="bench-q001")
    assert _tables(s) == _rebuilt(s)
    assert 0 < len(_tables(s)["messages"]) < len(lasts)
    s.close()
