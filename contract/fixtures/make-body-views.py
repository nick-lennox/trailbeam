"""Writes body-views.json: what contract/body-view.md gives for the recorded spans in contract-3.2-spans.json,
from open-source Trailbeam's implementation (python/src/trailbeam/server/bodies.py).

The hosted app checks its own implementation against the file, so a change to the rules shows up as a
diff here and a failing test there. The term cases at the end are written by hand from the spec; this
script fails if the server disagrees with any of them.

Run from the repository root, with the server installed (pip install -e "python[server]"):

    python contract/fixtures/make-body-views.py
"""
import base64
import json
import sys
from pathlib import Path

from trailbeam.server import bodies
from trailbeam.server.db import Database
from trailbeam.server.otlp import decode

HERE = Path(__file__).resolve().parent
SPANS = json.loads((HERE / "contract-3.2-spans.json").read_text())

CASES = [
    {"name": "default", "store_bodies": [], "services": None,
     "about": "TRAILBEAM_STORE_BODIES unset: every captured body is dropped"},
    {"name": "orders", "store_bodies": ["orders"], "services": None,
     "about": "only orders opted in: billing's captured bodies are dropped"},
    {"name": "every queue", "store_bodies": ["*"], "services": None, "about": "TRAILBEAM_STORE_BODIES=*"},
    {"name": "consumer only", "store_bodies": ["orders", "payments"], "services": ["orders-worker"],
     "about": "only the consumer's spans arrived: no message has a send"},
]

SEARCHES = [
    "body.order.sku:A-100",
    "body.order.sku:a-*",
    "body.order.qty:>=2",
    "body.order.qty:1..3",
    "body.customer.id:acme body.customer.id:globex",
    "body.customer.id:acme -body.order.qty:2",
    "body:REDACTED",
    "body:inv-",
    "body.password:hunter2",
    "body.invoice:inv-1",
    "body.missing.path:x",
]

# (body as stored, format, query, matches): by hand, from contract/body-view.md
TERMS = [
    ('{"Records":[{"key":"a.jpg"},{"key":"b.heic"}]}', "json", "body.Records.key:b.heic", True),
    ('{"Records":[{"key":"a.jpg"},{"key":"b.heic"}]}', "json", "body.Records.0.key:b.heic", False),
    ('{"Records":[{"key":"a.jpg"},{"key":"b.heic"}]}', "json", "body.Records[1].key:b.heic", True),
    ('{"Records":[{"key":"a.jpg"},{"key":"b.heic"}]}', "json", "body.Records.5.key:a.jpg", False),
    ('{"Records":[{"key":"a.jpg"},{"key":"b.heic"}]}', "json", "body.Records.key:*.JPG", True),
    ('{"a":{"b":1}}', "json", "body.a:1", False),
    ('{"amount":10000}', "json", "body.amount:>=10000", True),
    ('{"amount":10000}', "json", "body.amount:>10000", False),
    ('{"amount":10000}', "json", "body.amount:10000", True),
    ('{"amount":10000}', "json", "body.amount:10kb", False),
    ('{"amount":2048}', "json", "body.amount:2kb", True),
    ('{"amount":1.5}', "json", "body.amount:<2", True),
    ('{"amount":2.0}', "json", "body.amount:2", True),
    ('{"amount":2.0}', "json", "body.amount:2*", True),
    ('{"amount":5}', "json", "body.amount:abc", False),
    ('{"amount":"5"}', "json", "body.amount:>=1", False),
    ('{"amount":"5"}', "json", "body.amount:5", True),
    ('{"paid":true}', "json", "body.paid:true", True),
    ('{"paid":true}', "json", "body.paid:TRUE", True),
    ('{"paid":false}', "json", "body.paid:true", False),
    ('{"x":null}', "json", "body.x:null", False),
    ('{"note":"call me back"}', "json", 'body.note:"call me back"', True),
    ('{"note":"call me back"}', "json", "body.note:call", False),
    ('{"note":"call me back"}', "json", "body.note:call*", True),
    ('{"note":"Call Me"}', "json", 'body:"call me"', True),
    ('{"note":"Call Me"}', "json", "body:CALL", True),
    ('{"customer":{"id":"acme"}}', "json", "body.customer.id:acme -body.customer.id:acme", False),
    ('{"customer":{"id":"acme"}}', "json", "-body.customer.id:globex", True),
    ('{"customer":{"id":"acme"}}', "json", "body.customer.id:", True),
    ('{"constructor":"x"}', "json", "body.constructor:x", True),
    ('{"a":1}', "json", "body.constructor:*", False),
    ('{"a":1}', "json", "body.toString:*", False),
    ("plain text, not JSON", "text", 'body:"not json"', True),
    ("plain text, not JSON", "text", "body.anything:plain*", False),
    ('{"looks":"like json"}', "text", 'body.looks:"like json"', False),
    ('{"looks":"like json"}', "text", 'body:"like json"', True),
    ("not json after all {", "json", 'body:"after all"', True),
]


def store(case: dict) -> Database:
    db = Database(store_bodies=frozenset(case["store_bodies"]))
    for e in SPANS["exports"]:
        if case["services"] is None or e["service"] in case["services"]:
            db.insert(decode(base64.b64decode(e["base64"]), e["contentType"]).records)
    return db


def main() -> None:
    out_cases = []
    for case in CASES:
        db = store(case)
        mids = [r["mid"] for r in db.query("SELECT DISTINCT mid FROM spans WHERE mid IS NOT NULL ORDER BY mid")]
        out_cases.append({
            **case,
            "views": {m: bodies.body_view(m, db.message_rows(m), db.store_bodies) for m in mids},
            "searches": [{"query": q, "message_ids": sorted(bodies.search(db, bodies.parse_query(q), db.store_bodies).mids)}
                         for q in SEARCHES],
        })
        db.close()
    wrong = []
    for body, fmt, query, want in TERMS:
        got = bodies.matches(bodies.parsed_body(body, fmt), bodies.parse_query(query))
        if got != want:
            wrong.append(f"{query!r} on {body!r} ({fmt}): spec says {want}, server says {got}")
    if wrong:
        sys.exit("\n".join(wrong))
    doc = {
        "about": "What contract/body-view.md gives for contract-3.2-spans.json. Written by make-body-views.py; "
                 "don't edit by hand. message_ids are sorted.",
        "cases": out_cases,
        "terms": [{"body": b, "format": f, "query": q, "matches": w} for b, f, q, w in TERMS],
    }
    (HERE / "body-views.json").write_text(json.dumps(doc, indent=1, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
