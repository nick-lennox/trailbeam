"""The data and the probes of the Store conformance suite (test_store_conformance.py).

A dataset is a list of OTLP exports (decoded records) with the server's store_bodies and a fixed "now".
``snapshot`` runs every read the server makes through ``queries`` and ``bodies`` on a store holding a
dataset, and returns the results as JSON-able data. expected.json is that snapshot for each dataset; any
Store must give the same, byte for byte once serialised.

Datasets:

- ``contract-3.2``: the 32 spans recorded from the Node.js package (contract/fixtures/contract-3.2-spans.json),
  bodies stored for ``orders``. ``contract-3.2, every queue``: the same spans, bodies for every queue.
- ``joins``: spans built here for what the recorded ones don't show: settle spans exported before the receive
  they belong to (only the receipt-hash join links them), redeliveries, a move to a dead-letter queue, Lambda
  outcomes that succeed twice, settle spans that never join, failed settles, a span exported twice, tenants
  and bodies on two queues of which one is stored.
"""
from __future__ import annotations

import base64
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
FIXTURES = ROOT / "contract" / "fixtures"
sys.path.insert(0, str(ROOT / "python" / "tests" / "server"))

from spans import SEC, T0, Consumer, to_proto  # noqa: E402

from trailbeam.server import bodies, events, queries  # noqa: E402
from trailbeam.server.otlp import decode  # noqa: E402

LAMBDA_TIMEOUT_S = 900


def _contract_exports() -> list[list[dict]]:
    out = []
    for e in json.loads((FIXTURES / "contract-3.2-spans.json").read_text())["exports"]:
        d = decode(base64.b64decode(e["base64"]), e["contentType"])
        assert d.rejected == []
        out.append(d.records)
    return out


def _joins_exports() -> list[list[dict]]:
    w = Consumer(client_id="worker-1", scope="worker-1")
    w2 = Consumer(client_id="worker-2", service="orders-worker-2", version="2.0.1")  # service.version (QV-362)
    fn = Consumer(client_id="fn-1", service="billing-fn", source="lambda", queue="billing", version="1.4.2")
    p = Consumer(client_id="producer-1", service="checkout-api")
    s = lambda n: T0 + n * SEC  # noqa: E731

    def tag(span, tenant=None, body=None, fmt="json"):
        if tenant:
            span.attrs["trailbeam.tenant"] = tenant
        if body is not None:
            span.attrs.update({"trailbeam.body": body, "trailbeam.body.format": fmt,
                               "trailbeam.body.capture_status": "captured",
                               "messaging.message.body.size": len(body)})
        return span

    sends = [
        tag(p.send(s(0), "m-a", start=s(0) - SEC // 20), "acme", '{"order":{"sku":"A-1","qty":2}}'),
        tag(p.send(s(1), "m-b", queue="billing"), "globex", '{"invoice":7}'),
        tag(p.send(s(2), "m-c"), "acme"),                                   # never received: stuck
        tag(p.send(s(3), "m-d"), "initech", '{"order":{"sku":"D-4","qty":1}}'),
        tag(p.send(s(4), "m-pay", queue="payments"), "acme", '{"card":"[REDACTED]"}'),  # body not stored
    ]
    exports = [
        sends,
        # m-a: the first delivery is released with ChangeMessageVisibility 0; both settle spans are exported
        # by another process before the receives, without message IDs.
        [w2.change_visibility(s(11), "rh-a1", 0), w2.delete(s(21), "rh-a2")],
        [w.receive(s(10), "m-a", "rh-a1", order=(1, 1)), w.receive(s(20), "m-a", "rh-a2", count=2, order=(2, 2))],
        # m-b on a Lambda: fails, then succeeds twice (a duplicate success)
        [fn.receive(s(12), "m-b", "rh-b1", lambda_inv="inv-1"), fn.outcome(s(13), "m-b", "failed_exception", "inv-1",
                                                                          error_type="TypeError"),
         fn.receive(s(40), "m-b", "rh-b2", count=2, lambda_inv="inv-2"), fn.outcome(s(41), "m-b", "success", "inv-2"),
         fn.receive(s(42), "m-b", "rh-b3", count=3, lambda_inv="inv-3"), fn.outcome(s(43), "m-b", "success", "inv-3")],
        # m-d: three deliveries on orders that time out, then the dead-letter queue, deleted there
        [w.receive(s(30 + 40 * i), "m-d", f"rh-d{i}", count=i + 1) for i in range(3)]
        + [w.delete(s(200), "rh-d-dlq", mid="m-d", queue="orders-dlq")],
        [tag(w.receive(s(190), "m-d", "rh-d-dlq", count=4, queue="orders-dlq", dlq_source="orders"),
             body='{"order":{"sku":"D-4","qty":1}}')],
        # settle spans that never join: one with the message ID, one without; a failed delete of m-a's old handle
        [w.delete(s(50), "rh-unknown", mid="m-e"), w.delete(s(51), "rh-unknown-2"),
         w.delete(s(52), "rh-a1", ok=False)],
    ]
    exports.append(list(exports[0][:2]))  # an exporter retry: the same two spans again
    ids = {}
    for spans in exports:
        for sp in spans:
            if id(sp) not in ids:  # fixed IDs: a retry keeps its span's IDs
                ids[id(sp)] = len(ids) + 1
                sp.span_id = ids[id(sp)].to_bytes(8, "big")
                sp.trace_id = (1000 + ids[id(sp)]).to_bytes(16, "big")
    return [decode(to_proto(spans).SerializeToString(), "application/x-protobuf").records for spans in exports]


DATASETS = {
    "contract-3.2": {"exports": _contract_exports, "store_bodies": ["orders"], "now_ns": 1_794_852_397_716_000_000,
                     "body_queries": ["body.order.sku:A-100", "body:declined", "-body.order.qty:1 body:order"]},
    "contract-3.2, every queue": {"exports": _contract_exports, "store_bodies": ["*"],
                                  "now_ns": 1_794_852_397_716_000_000,
                                  "body_queries": ["body:card", "body.order.qty:>=2"]},
    "joins": {"exports": _joins_exports, "store_bodies": ["orders", "orders-dlq", "billing"], "now_ns": T0 + 3600 * SEC,
              "body_queries": ["body.order.sku:A-1", "body.order.qty:1", "body:card", "body.invoice:7"]},
}


def exports(name: str) -> list[list[dict]]:
    return DATASETS[name]["exports"]()


def store_bodies(name: str) -> frozenset:
    return frozenset(DATASETS[name]["store_bodies"])


def snapshot(store, name: str) -> dict:
    """Every read the server makes, on a store holding the dataset ``name``."""
    now = DATASETS[name]["now_ns"]
    lt = LAMBDA_TIMEOUT_S
    everything = queries.recent(store, now, lt, queries.MAX_LIMIT)
    mids = [m["message_id"] for m in everything]
    rows = {m: store.message_rows(m) for m in mids}
    queues = sorted({r["queue"] for rs in rows.values() for r in rs})
    tenants = sorted({t for rs in rows.values() if (t := queries.message_tenant(rs))})
    versions = sorted({r["service_version"] for rs in rows.values() for r in rs if r["service_version"]}) + ["9.9.9"]
    first_ms = min(r["time_ns"] for rs in rows.values() for r in rs) // 1_000_000
    last_ms = max(r["time_ns"] for rs in rows.values() for r in rs) // 1_000_000
    mid_ms = (first_ms + last_ms) // 2
    return {
        "events": _events(store, rows, queues, first_ms, last_ms),
        "counts": store.counts(),
        "messages": everything,
        "messages_limit_2": _ids(queries.recent(store, now, lt, 2)),
        "messages_by_queue": {q: _ids(queries.recent(store, now, lt, 100, q)) for q in queues},
        "messages_by_tenant": {t: _ids(queries.recent(store, now, lt, 100, tenant=t)) for t in tenants + ["nobody"]},
        "message": {m: queries.message(store, m, now, lt) for m in mids},
        "message_short_lambda_timeout": {m: queries.message(store, m, now, 1)["state"] for m in mids},
        "missing_message": queries.message(store, "no-such-message", now, lt),
        "messages_by_version": {v: _ids(queries.recent_page(store, now, lt, 100, version=v).items) for v in versions},
        "duplicates": queries.duplicates(store, now, lt, 100),
        "duplicates_by_version": {v: _ids(queries.duplicates_page(store, now, lt, 100, version=v).items)
                                  for v in versions},
        "duplicates_by_reason": {reason: _ids(queries.duplicates(store, now, lt, 100, None, reason))
                                 for reason in ("received_more_than_once", "succeeded_more_than_once")},
        "duplicates_by_queue": {q: _ids(queries.duplicates(store, now, lt, 100, q)) for q in queues},
        "stuck": queries.stuck(store, now, lt, 0, 100),
        "stuck_older_than": {str(after): _ids(queries.stuck(store, now, lt, after, 100)) for after in (60, 10 ** 9)},
        "stuck_by_queue": {q: _ids(queries.stuck(store, now, lt, 0, 100, q)) for q in queues},
        "stuck_limit_1": _ids(queries.stuck(store, now, lt, 0, 1)),
        "stuck_by_version": {v: _ids(queries.stuck_page(store, now, lt, 0, 100, version=v).items) for v in versions},
        "tenants": {
            "all": queries.tenants(store, now, lt, 0, now // 1_000_000),
            "first half": queries.tenants(store, now, lt, first_ms, mid_ms),
            "second half": queries.tenants(store, now, lt, mid_ms + 1, last_ms),
        },
        "find_tenants": {t: queries.find_tenants(store, t) for t in ["", "a", "AC", "acme", "g", "z", "%", "_"]},
        "body_view": {m: bodies.body_view(m, rows[m], store.store_bodies) for m in mids},
        "body_search": {q: {str(queue): _ids(queries.body_search(store, now, lt, 100, queue, bodies.parse_query(q)))
                            for queue in [None] + queues}
                        for q in DATASETS[name]["body_queries"]},
    }


def _events(store, rows: dict, queues: list, first_ms: int, last_ms: int) -> dict:
    """/api/events (events.py): the whole window, paged 3 steps at a time to the end, and each filter."""
    page = lambda **kw: events.page(store, first_ms, last_ms, 100, **kw)  # noqa: E731
    pages, cursor = [], None
    while True:
        p = events.page(store, first_ms, last_ms, 3, cursor=cursor)
        pages.append(p)
        if not (cursor := p["next_cursor"]):
            break
    services = sorted({r["service_name"] for rs in rows.values() for r in rs if r["service_name"]})
    return {
        "all": page(),
        "pages_of_3": pages,
        "first_half": events.page(store, first_ms, (first_ms + last_ms) // 2, 100),
        # hourly buckets: whole hours from a rollup, if the store keeps one, and the partial ends from the spans
        "week": events.page(store, first_ms - 3 * 86_400_000 + 1234, last_ms + 3 * 86_400_000, 5),
        "week_by_kind": events.page(store, first_ms - 3 * 86_400_000, last_ms + 3 * 86_400_000, 5,
                                    kinds=("receive", "outcome")),
        "by_queue": {q: page(queue=q) for q in queues + ["no-such-queue"]},
        "by_service": {s: page(service=s) for s in services},
        "by_kind": {k: page(kinds=(k,)) for k in events.KINDS},
        "settles": page(kinds=("delete", "change_visibility")),
        "by_message": {m: page(message_id=m) for m in sorted(rows)[:6]},
        "by_message_and_kind": {m: page(message_id=m, kinds=("receive",)) for m in sorted(rows)[:3]},
    }


def _ids(items: list[dict]) -> list[str]:
    return [m["message_id"] for m in items]


def canonical(obj) -> str:
    return json.dumps(obj, sort_keys=True, indent=1, default=str)


def rows_match(got: list, want: list, bound: float) -> bool:
    """Tenants page rows as contract/tenants.md compares them: every field equal, except p95_in_queue_ms, which is
    within ``bound`` (relative) of the expected exact nearest-rank value."""
    if len(got) != len(want):
        return False
    for g, w in zip(got, want):
        if {k: v for k, v in g.items() if k != "p95_in_queue_ms"} != {k: v for k, v in w.items() if k != "p95_in_queue_ms"}:
            return False
        a, b = g["p95_in_queue_ms"], w["p95_in_queue_ms"]
        if (a is None) != (b is None) or (b is not None and abs(a - b) > bound * b):
            return False
    return True
