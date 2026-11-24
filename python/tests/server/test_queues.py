"""The queue page's series (QV-363): queue keys, steps, queue events and /api/queues/{queue}/series."""
import json
import urllib.error
import urllib.parse
import urllib.request

import pytest

from spans import SEC, T0, Consumer, to_proto
from trailbeam.server import queues
from trailbeam.server.server import ApiError, App

MS = T0 // 1_000_000
ORDERS = "000000000000/us-east-1/orders"  # the source ARN's key (the move); the emulator's URL has no region
ORDERS_URL_KEY = "000000000000//orders"


# ---- keys ------------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("url, expected", [
    ("https://sqs.eu-west-1.amazonaws.com/123456789012/orders", "123456789012/eu-west-1/orders"),
    ("https://eu-west-1.queue.amazonaws.com/123456789012/orders", "123456789012/eu-west-1/orders"),
    ("https://queue.amazonaws.com/123456789012/orders", "123456789012/us-east-1/orders"),
    ("https://sqs.cn-north-1.amazonaws.com.cn/123456789012/orders", "123456789012/cn-north-1/orders"),
    ("https://vpce-0a1b-xyz.sqs.us-gov-west-1.vpce.amazonaws.com/123456789012/orders",
     "123456789012/us-gov-west-1/orders"),
    ("http://localhost:9324/000000000000/orders", "000000000000//orders"),
    ("http://localhost:9324/queue/orders", "http://localhost:9324/queue/orders"),
    (None, "orders"),
    ("not a url", "not a url"),
])
def test_queue_key_is_the_apps(url, expected):
    assert queues.key("orders", url) == expected


def test_a_key_gives_back_its_place():
    assert queues.place_of_key("123456789012/eu-west-1/orders") == ("orders", "123456789012", "eu-west-1")
    assert queues.place_of_key("000000000000//orders") == ("orders", "000000000000", None)
    assert queues.place_of_key("orders") == ("orders", None, None)
    assert queues.place_of_key("http://localhost:9324/queue/orders") == ("orders", None, None)
    assert queues.parse_arn("arn:aws:sqs:us-east-1:000000000000:orders") == ("orders", "000000000000", "us-east-1")


def test_resolve_takes_the_keys_that_say_less_when_they_fit_one_queue():
    keys = [ORDERS, ORDERS_URL_KEY, "orders", "999999999999/eu-west-1/payments"]
    assert queues.resolve("orders", keys)[0] == [ORDERS, ORDERS_URL_KEY, "orders"]
    assert queues.resolve(ORDERS_URL_KEY, keys)[0] == [ORDERS, ORDERS_URL_KEY, "orders"]
    assert queues.resolve("payments", keys)[0] == ["999999999999/eu-west-1/payments"]
    # two accounts: a bare name is ambiguous, and spans without a URL belong to neither
    two = [ORDERS, "111111111111/us-east-1/orders", "orders"]
    with pytest.raises(ValueError, match="more than one queue is called orders"):
        queues.resolve("orders", ["111111111111/us-east-1/orders", ORDERS])
    assert queues.resolve(ORDERS, two)[0] == [ORDERS]
    assert queues.resolve("orders", two)[0] == ["orders"]  # its own key, asked by it
    assert queues.resolve("nope", keys) == (["nope"], ("nope", None, None))  # no spans yet: no events


# ---- steps -----------------------------------------------------------------------------------------------------

def test_the_default_step_is_the_pages_rule():
    hour, day = 3_600_000, 86_400_000
    assert queues.default_step(0, hour - 1) == 60
    assert queues.default_step(0, day - 1) == 900
    assert queues.default_step(0, 7 * day - 1) == 10800  # 84 min, rounded up (the design text says 1 h)
    assert queues.default_step(0, 30 * day - 1) == 21600
    assert queues.default_step(0, 5 * 60_000) == 60  # never below a minute


def test_a_step_must_be_one_cloudwatch_lines_up_with():
    queues.check_step(300, 0, 86_400_000)
    for bad in (30, 61, 120, 7200, 86400):
        with pytest.raises(ValueError, match="step_s must be one of"):
            queues.check_step(bad, 0, 86_400_000)
    with pytest.raises(ValueError, match="at most 2000"):
        queues.check_step(60, 0, 2 * 86_400_000)


# ---- the page --------------------------------------------------------------------------------------------------

def spans_of_a_busy_minute():
    """Six messages on orders, in the 27th minute of an hour (T0 is 40 s into its minute)."""
    prod, worker, fn = Consumer("producer", service="checkout"), Consumer("w-1"), Consumer("fn", source="lambda")
    s = [
        # m1: sent, received after 2 s, deleted; 500 ms in the handler (receive end to delete start)
        prod.send(T0, "m1"), worker.receive(T0 + 2 * SEC, "m1", "h1"),
        worker.delete(T0 + 3 * SEC, "h1", start=T0 + 2 * SEC + SEC // 2),
        # m2: no recorded send; received twice, never deleted
        worker.receive(T0 + 5 * SEC, "m2", "h2a"), worker.receive(T0 + 40 * SEC, "m2", "h2b", count=2),
        # m3: a Lambda invocation that threw after 3 s
        prod.send(T0, "m3"), fn.receive(T0 + SEC, "m3", "h3", lambda_inv="inv-3"),
        fn.outcome(T0 + 4 * SEC, "m3", "failed_exception", "inv-3", error_type="RuntimeError"),
        # m4: a Lambda success, 200 ms
        prod.send(T0 + SEC, "m4"), fn.receive(T0 + 2 * SEC, "m4", "h4", lambda_inv="inv-4"),
        fn.outcome(T0 + 2 * SEC + 200_000_000, "m4", "success", "inv-4"),
        # m5: moved to orders-dlq, as the receive from it says
        prod.send(T0, "m5"), worker.receive(T0 + 3 * SEC, "m5", "h5"),
        worker.receive(T0 + 50 * SEC, "m5", "h5d", queue="orders-dlq", dlq_source="orders"),
        # m6: received five times, never deleted, no DLQ receive: at maxReceiveCount
        prod.send(T0, "m6"),
        *[worker.receive(T0 + (5 + i) * SEC, "m6", f"h6-{i}", count=i) for i in range(1, 6)],
    ]
    return s


def test_queue_events_of_a_message(db, ingest):
    ingest(*spans_of_a_busy_minute())
    [s] = db.summaries(["m1"])
    ev = json.loads(db.query("SELECT qevents_json FROM messages WHERE mid = 'm1'")[0]["qevents_json"])
    assert [e[1] for e in ev] == ["send", "receive", "wait", "delete"]
    assert {e[2] for e in ev} == {ORDERS_URL_KEY}
    assert ev[2][5] == 2000 and ev[3][5] == 500
    assert [e[1] for e in json.loads(db.query("SELECT qevents_json FROM messages WHERE mid = 'm5'")[0]["qevents_json"])
            if e[1] == "dlq_seen"] == ["dlq_seen"]
    assert db.queue_keys() == sorted({ORDERS, ORDERS_URL_KEY, "000000000000//orders-dlq"})
    assert s["mid"] == "m1"


def api(db, path, **q):
    return App(db, max_receive_counts={"orders": 5}).api(path, {k: str(v) for k, v in q.items()})


def test_one_bucket_has_every_figure(db, ingest):
    ingest(*spans_of_a_busy_minute())
    d = api(db, "queues/orders/series", from_ms=MS - 60_000, to_ms=MS + 120_000, step_s=3600)
    assert d["queue"] == {"key": ORDERS, "name": "orders", "account": "000000000000", "region": "us-east-1",
                          "fifo": False}
    assert d["step_s"] == 3600 and d["max_receive_count"] == 5
    [b] = d["series"]
    assert b["t_ms"] == MS // 3_600_000 * 3_600_000
    assert {k: v for k, v in b.items() if k != "t_ms"} == {
        "sends": 5, "receives": 11, "re_receives": 5, "handled": 2, "failed": 1, "dlq_seen": 1, "dlq_inferred": 1,
        "no_send_receives": 1, "p95_queue_ms": 6000, "p95_handler_ms": 3000, "queue_samples": 5,
        "handler_samples": 3}


def test_buckets_are_steps_since_the_epoch_and_empty_ones_are_left_out(db, ingest):
    ingest(*spans_of_a_busy_minute())
    d = api(db, "queues/orders/series", from_ms=MS - 3_600_000, to_ms=MS + 3_600_000, step_s=60)
    minute = MS // 60_000 * 60_000
    assert [b["t_ms"] for b in d["series"]] == [minute, minute + 60_000]  # T0 is 40 s into its minute
    first, second = d["series"]
    assert first["sends"] == 5 and second["sends"] == 0
    assert second["receives"] == second["re_receives"] == 1 and second["dlq_seen"] == 1  # m2's second; m5's move
    assert sum(b["receives"] for b in d["series"]) == 11


def test_the_range_bounds_the_events(db, ingest):
    ingest(*spans_of_a_busy_minute())
    d = api(db, "queues/orders/series", from_ms=MS + 10_000, to_ms=MS + 60_000, step_s=3600)
    [b] = d["series"]
    # from_ms is included: m6's 5th receive (T0 + 10 s, and with it its at_max), m2's second; m5's move at 50 s
    assert (b["sends"], b["receives"], b["dlq_inferred"], b["dlq_seen"]) == (0, 2, 1, 1)


def get(url, path):
    try:
        with urllib.request.urlopen(url + path, timeout=5) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def test_over_http_a_key_is_url_encoded_and_a_shared_name_is_refused(server, ingest):
    a = Consumer("a", queue="orders")
    ingest(a.send(T0, "x1"), a.receive(T0 + SEC, "x1", "hx"))
    b = Consumer("b")
    s = b.send(T0, "y1", queue="orders")
    s.attrs["aws.sqs.queue.url"] = "https://sqs.eu-west-1.amazonaws.com/123456789012/orders"
    ingest(s)
    q = f"from_ms={MS - 60_000}&to_ms={MS + 60_000}&step_s=300"
    status, d = get(server, f"/api/queues/{urllib.parse.quote(ORDERS_URL_KEY, safe='')}/series?{q}")
    assert status == 200 and d["queue"]["key"] == ORDERS_URL_KEY
    assert [(b["sends"], b["receives"]) for b in d["series"]] == [(1, 1)]
    status, d = get(server, f"/api/queues/orders/series?{q}")
    assert status == 400 and "more than one queue is called orders" in d["error"]
    assert get(server, f"/api/queues/orders/series?{q.replace('300', '120')}")[0] == 400
    status, d = get(server, f"/api/queues/never-seen/series?{q}")
    assert status == 200 and d["series"] == []


# ---- the flows -------------------------------------------------------------------------------------------------

DLQ = "000000000000//orders-dlq"


def rollup_from_events(db) -> dict:
    """queue_rollup as it must be: queue_events added up per row."""
    return {tuple(r[:6]): tuple(r[6:]) for r in db.query(
        "SELECT at_ms / 3600000, queue_key, kind, COALESCE(service, ''), lambda, COALESCE(peer, ''), COUNT(*), "
        "COUNT(value), COALESCE(SUM(value), 0) FROM queue_events GROUP BY 1, 2, 3, 4, 5, 6")}


def rollup_rows(db) -> dict:
    return {tuple(r[:6]): tuple(r[6:]) for r in db.query(
        "SELECT hour, queue_key, kind, service, lambda, peer, n, vn, vsum FROM queue_rollup")}


def test_flows_are_the_apps_shape_and_counts(db, ingest):
    s = spans_of_a_busy_minute()
    s[0].attrs["messaging.message.body.size"] = 100
    s[5].attrs["messaging.message.body.size"] = 300
    s.append(Consumer("w-1").change_visibility(T0 + 4 * SEC, "h5", 30))
    ingest(*s)
    d = api(db, "flows", from_ms=MS - 60_000, to_ms=MS + 120_000)
    orders, dlq = d["flows"]
    assert orders == {
        "key": ORDERS, "queue": "orders", "name": "orders", "url": None, "account": "000000000000",
        "region": "us-east-1", "lastSeen": MS + 50_000, "sent": 5, "received": 11, "deleted": 1,
        "visibilityChanges": 1,
        "producers": [{"name": "checkout", "n": 5, "lambda": False}],
        "consumers": [{"name": "orders-worker", "n": 11, "lambda": True}],  # one service: w-1, and fn's Lambda
        "avgBytes": 200.0, "dlq": {"name": "orders-dlq", "received": 1}}
    assert dlq["key"] == DLQ and dlq["received"] == 1 and dlq["sent"] == 0 and dlq["dlq"] is None
    # out of the range: zero counts, the link to the dead-letter queue kept (SQS reported it, ever)
    later = api(db, "flows", from_ms=MS + 3_600_000, to_ms=MS + 7_200_000)["flows"]
    assert [(f["key"], f["sent"] + f["received"]) for f in later] == [(ORDERS, 0), (DLQ, 0)]
    assert later[0]["dlq"] == {"name": "orders-dlq", "received": 0}


def test_two_queues_of_one_name_are_two_flows_labelled_apart(db, ingest):
    a = Consumer("a")
    ingest(a.send(T0, "x1"), a.receive(T0 + SEC, "x1", "hx"))
    s = Consumer("b", service="billing").send(T0, "y1")
    s.attrs["aws.sqs.queue.url"] = "https://sqs.eu-west-1.amazonaws.com/123456789012/orders"
    ingest(s)
    flows = api(db, "flows", from_ms=MS - 60_000, to_ms=MS + 60_000)["flows"]
    assert [(f["key"], f["queue"], f["sent"], f["received"]) for f in flows] == [
        (ORDERS_URL_KEY, "orders (000000000000)", 1, 1), ("123456789012/eu-west-1/orders", "orders (123456789012)", 1, 0)]


def test_the_rollup_is_its_events_added_up_through_refresh_and_forget(db, ingest):
    from trailbeam.server import summary_table
    w = Consumer("w-1")
    hour = 3_600 * SEC
    ingest(*spans_of_a_busy_minute())
    ingest(w.send(T0 + hour, "m7"), w.receive(T0 + 2 * hour, "m7", "h7"))
    ingest(w.delete(T0 + 2 * hour + SEC, "h7"), w.receive(T0 + 50 * SEC, "m2", "h2c", count=3))
    assert rollup_rows(db) == rollup_from_events(db) and rollup_rows(db)
    c = db.conn
    c.execute("BEGIN IMMEDIATE")
    summary_table.forget(c, ["m1", "m7"])
    c.execute("COMMIT")
    assert rollup_rows(db) == rollup_from_events(db)
    c.execute("BEGIN IMMEDIATE")
    summary_table.rebuild_queue_events(c)
    c.execute("COMMIT")
    assert rollup_rows(db) == rollup_from_events(db)


@pytest.mark.parametrize("now_off_s", [30, 1_800, 3_599, 20_000])
def test_flows_read_from_the_rollup_equal_the_events(db, ingest, now_off_s):
    """Whatever rollup.plan reads (rollup rows less events, or events), the counts are the events in the range."""
    w = Consumer("w-1")
    spans = []
    for i in range(40):
        t = T0 + i * 7 * 60 * SEC  # every 7 min, over 4.6 h
        spans += [w.send(t, f"r{i}"), w.receive(t + SEC, f"r{i}", f"hr{i}")]
    ingest(*spans)
    lo, hi = MS + 1_234_567, MS + 3 * 3_600_000 + 777_777
    keys, last, counts, links = db.queue_flows(lo, hi, MS + now_off_s * 1000)
    got = {(k, kind): n for k, kind, _s, _l, _p, n, _vn, _vs in counts}
    want = {(r[0], r[1]): r[2] for r in db.query(
        "SELECT queue_key, kind, COUNT(*) FROM queue_events WHERE at_ms BETWEEN ? AND ? GROUP BY 1, 2", (lo, hi))}
    assert got == want and keys == [ORDERS_URL_KEY] and links == []
