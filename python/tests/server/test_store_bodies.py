"""The server's body gate (TRAILBEAM_STORE_BODIES): bodies are stored only for queues opted in on the server.

Inputs are the recorded contract 3.2 exports in contract/fixtures/contract-3.2-spans.json, the same file the
hosted app tests against. In them a package captured bodies on `orders` (sends and receives) and `billing`
(Lambda receives), with capture off for `payments` and `orders.fifo`, and `legacy-svc` predates 3.2.
"""
import base64
import json
from pathlib import Path

import pytest

from trailbeam.server.__main__ import body_warning, main
from trailbeam.server.db import BODY_ATTRIBUTES, Database, parse_queue_list
from trailbeam.server.otlp import decode

FIXTURE = Path(__file__).resolve().parents[3] / "contract" / "fixtures" / "contract-3.2-spans.json"


def load(db: Database) -> None:
    doc = json.loads(FIXTURE.read_text())
    for e in doc["exports"]:
        d = decode(base64.b64decode(e["base64"]), e["contentType"])
        assert (d.rejected, len(d.records)) == ([], e["spans"])
        db.insert(d.records)


def rows(db: Database):
    return [(r["kind"], r["queue"], r["mid"], json.loads(r["attributes"]), r["body_dropped"])
            for r in db.query("SELECT * FROM spans ORDER BY row")]


def with_body(db: Database):
    return {(k, q, m) for k, q, m, a, _ in rows(db) if "trailbeam.body" in a}


def dropped(db: Database):
    return {(k, q, m) for k, q, m, _, d in rows(db) if d is not None}


CAPTURED_ORDERS = {("send", "orders", "m-orders-1"), ("send", "orders", "m-orders-2"),
                   ("receive", "orders", "m-orders-1"), ("receive", "orders", "m-orders-2"),
                   ("receive", "orders", "m-orphan")}
CAPTURED_BILLING = {("receive", "billing", "m-bill-1"), ("receive", "billing", "m-bill-2")}


def test_fixture_is_the_recorded_set():
    db = Database()
    load(db)
    assert len(rows(db)) == 32
    db.close()


def test_default_stores_no_body():
    db = Database()
    load(db)
    assert with_body(db) == set()
    assert dropped(db) == CAPTURED_ORDERS | CAPTURED_BILLING
    for _, _, _, a, d in rows(db):
        assert not any(k in a for k in BODY_ATTRIBUTES)
        if d is not None:
            # the package's own status is kept, so the Body tab can say "captured, not stored here"
            assert (d, a["trailbeam.body.capture_status"]) == ("not_opted_in", "captured")
            assert "messaging.message.body.size" in a
    db.close()


def test_opted_in_queue_keeps_its_body_and_others_do_not():
    db = Database(store_bodies=parse_queue_list("orders"))
    load(db)
    assert with_body(db) == CAPTURED_ORDERS
    assert dropped(db) == CAPTURED_BILLING
    send = next(a for k, q, m, a, _ in rows(db) if (k, m) == ("send", "m-orders-1"))
    assert (send["trailbeam.body.format"], send["trailbeam.body.redacted_count"]) == ("json", 3)
    assert json.loads(send["trailbeam.body"])["customer"]["id"] == "acme"
    db.close()


def test_star_keeps_every_body():
    db = Database(store_bodies=parse_queue_list("*"))
    load(db)
    assert with_body(db) == CAPTURED_ORDERS | CAPTURED_BILLING
    assert dropped(db) == set()
    db.close()


def test_spans_without_a_body_are_not_marked():
    db = Database()
    load(db)
    not_marked = {(k, q) for k, q, _, a, d in rows(db) if d is None}
    assert ("send", "payments") in not_marked      # capture off in the package
    assert ("delete", "orders") in not_marked      # settle spans never carry a body
    legacy = [(a, d) for k, q, m, a, d in rows(db) if m == "m-legacy"]
    assert legacy and all(d is None and "trailbeam.body.capture_status" not in a for a, d in legacy)
    db.close()


def test_queue_list_parsing():
    assert parse_queue_list("") == frozenset()
    assert parse_queue_list(" orders, payments ,,") == {"orders", "payments"}
    assert parse_queue_list("*") == {"*"}


def test_command_reads_the_environment_and_warns(monkeypatch, tmp_path, caplog):
    made = {}

    class Stop(Exception):
        pass

    def fake_server(db, host, port, **kw):
        made["store_bodies"] = db.store_bodies
        raise Stop

    monkeypatch.setattr("trailbeam.server.__main__.make_server", fake_server)
    monkeypatch.setenv("TRAILBEAM_STORE_BODIES", "orders,payments")
    with pytest.raises(Stop):
        main(["--db", str(tmp_path / "a.sqlite")])
    assert made["store_bodies"] == {"orders", "payments"}
    assert any("queues orders, payments" in r.getMessage() for r in caplog.records if r.levelname == "WARNING")
    caplog.clear()

    monkeypatch.delenv("TRAILBEAM_STORE_BODIES")
    with pytest.raises(Stop):
        main(["--db", str(tmp_path / "b.sqlite")])
    assert made["store_bodies"] == frozenset()
    assert not [r for r in caplog.records if "message bodies" in r.getMessage()]


def test_warning_text():
    w = body_warning(frozenset({"payments", "orders"}), "t.sqlite")
    assert "queues orders, payments" in w and "no logins" in w and "trusted network" in w
    assert "every queue" in body_warning(frozenset({"*"}), "t.sqlite")
