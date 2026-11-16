"""The receipt-hash join: every delete and visibility change is attached to the delivery it settles."""
from spans import SEC, T0, Consumer, key_id


def rows(db, kind=None):
    sql = "SELECT * FROM spans" + (" WHERE kind = ?" if kind else "") + " ORDER BY row"
    return db.query(sql, (kind,) if kind else ())


def receive_row(db, mid, count=1):
    return db.query("SELECT row FROM spans WHERE kind='receive' AND message_id=? AND receive_count=?",
                       (mid, count))[0]["row"]


def test_delete_without_message_id_joins_by_receipt_hash(db, ingest):
    """The deleting process never saw the receive (contract section 8): only the hash links them."""
    p, c = Consumer("producer"), Consumer("consumer-a")
    ingest(p.send(T0, "m1"), c.receive(T0 + SEC, "m1", "rh-1"))
    ingest(Consumer("consumer-b").delete(T0 + 2 * SEC, "rh-1"))
    d = rows(db, "delete")[0]
    assert d["message_id"] is None
    assert (d["mid"], d["mid_source"], d["receive_row"]) == ("m1", "receipt_hash", receive_row(db, "m1"))
    assert [r["kind"] for r in db.message_rows("m1")] == ["send", "receive", "delete"]


def test_delete_with_message_id_is_also_joined_to_its_receive(db, ingest):
    c = Consumer()
    ingest(c.receive(T0, "m1", "rh-1"), c.delete(T0 + SEC, "rh-1", mid="m1"))
    d = rows(db, "delete")[0]
    assert (d["mid"], d["mid_source"], d["receive_row"]) == ("m1", "receipt_hash", receive_row(db, "m1"))


def test_batch_delete_joins_each_entry_to_its_own_message(db, ingest):
    """One ReceiveMessage returns three messages; one DeleteMessageBatch call deletes all three.

    The batch call is one stock span with one Trailbeam span per entry (contract section 2), all in one
    trace. No entry carries a message ID. One entry fails. Each entry must land on its own message.
    """
    c = Consumer()
    mids = ["m-a", "m-b", "m-c"]
    recv_trace, del_trace = b"\x01" * 16, b"\x02" * 16
    ingest(*[c.receive(T0, m, f"rh-{m}", trace_id=recv_trace) for m in mids])
    ingest(
        c.delete(T0 + SEC, "rh-m-a", batch_entry="0", trace_id=del_trace),
        c.delete(T0 + SEC, "rh-m-b", batch_entry="1", trace_id=del_trace, ok=False),
        c.delete(T0 + SEC, "rh-m-c", batch_entry="2", trace_id=del_trace),
    )
    dels = rows(db, "delete")
    assert len(dels) == 3 and {d["trace_id"] for d in dels} == {del_trace.hex()}
    by_entry = {d["batch_entry_id"]: d for d in dels}
    for entry, m in zip("012", mids):
        d = by_entry[entry]
        assert (d["mid"], d["receive_row"]) == (m, receive_row(db, m))
        assert d["operation"] == "DeleteMessageBatch"
    assert by_entry["1"]["result"] == "failed" and by_entry["1"]["error_code"] == "ReceiptHandleIsInvalid"
    for m in mids:
        assert [r["kind"] for r in db.message_rows(m)] == ["receive", "delete"]


def test_batch_delete_across_receives_from_different_consumers(db, ingest):
    """A batch delete may hold handles from several receives; each still joins to its own delivery."""
    a, b = Consumer("consumer-a"), Consumer("consumer-b")
    ingest(a.receive(T0, "m1", "rh-1"), b.receive(T0, "m2", "rh-2"))
    ingest(a.delete(T0 + SEC, "rh-1", batch_entry="x"), a.delete(T0 + SEC, "rh-2", batch_entry="y"))
    assert {(d["batch_entry_id"], d["mid"]) for d in rows(db, "delete")} == {("x", "m1"), ("y", "m2")}


def test_delete_that_arrives_before_its_receive_is_joined_when_the_receive_arrives(db, ingest):
    c = Consumer()
    ingest(c.delete(T0 + 2 * SEC, "rh-1", batch_entry="0"), c.delete(T0 + 2 * SEC, "rh-2", batch_entry="1"))
    assert [d["mid"] for d in rows(db, "delete")] == [None, None]
    assert db.counts()["unjoined_settles"] == 2
    ingest(c.receive(T0 + SEC, "m1", "rh-1"))
    ingest(c.receive(T0 + SEC, "m2", "rh-2"))
    assert {d["batch_entry_id"]: d["mid"] for d in rows(db, "delete")} == {"0": "m1", "1": "m2"}
    assert db.counts()["unjoined_settles"] == 0


def test_early_delete_with_message_id_is_listed_under_its_message_before_the_join(db, ingest):
    c = Consumer()
    ingest(c.delete(T0 + SEC, "rh-1", mid="m1"))
    d = rows(db, "delete")[0]
    assert (d["mid"], d["mid_source"], d["receive_row"]) == ("m1", "span", None)
    ingest(c.receive(T0, "m1", "rh-1"))
    d = rows(db, "delete")[0]
    assert (d["mid_source"], d["receive_row"]) == ("receipt_hash", receive_row(db, "m1"))


def test_redelivery_deletes_join_to_the_delivery_whose_handle_they_used(db, ingest):
    """Receive 1 times out, receive 2 happens, then a late delete with receive 1's handle arrives."""
    a, b = Consumer("consumer-a"), Consumer("consumer-b")
    ingest(a.receive(T0, "m1", "rh-1", count=1), b.receive(T0 + 40 * SEC, "m1", "rh-2", count=2))
    ingest(a.delete(T0 + 45 * SEC, "rh-1"), b.delete(T0 + 46 * SEC, "rh-2"))
    d1, d2 = rows(db, "delete")
    assert d1["receive_row"] == receive_row(db, "m1", 1)
    assert d2["receive_row"] == receive_row(db, "m1", 2)


def test_change_visibility_joins_like_a_delete(db, ingest):
    c = Consumer()
    ingest(c.receive(T0, "m1", "rh-1"), Consumer("other").change_visibility(T0 + SEC, "rh-1", 0))
    v = rows(db, "change_visibility")[0]
    assert (v["mid"], v["visibility_timeout"]) == ("m1", 0)


def test_hash_from_another_key_does_not_join(db, ingest):
    c, wrong = Consumer(), Consumer("other", key="a-different-key-0000")
    ingest(c.receive(T0, "m1", "rh-1"), wrong.delete(T0 + SEC, "rh-1"))
    d = rows(db, "delete")[0]
    assert d["mid"] is None and d["key_id"] == key_id("a-different-key-0000")


def test_same_hash_with_another_key_id_does_not_join(db, ingest):
    """The join key is (hash, key ID), not the hash alone."""
    c = Consumer()
    ingest(c.receive(T0, "m1", "rh-1"))
    d = c.delete(T0 + SEC, "rh-1")
    d.attrs["trailbeam.receipt_handle.key_id"] = "00000000"
    ingest(d)
    assert rows(db, "delete")[0]["mid"] is None


def test_a_span_received_twice_is_stored_once(db, ingest):
    c = Consumer()
    r = c.receive(T0, "m1", "rh-1")
    assert ingest(r) == (1, 0)
    assert ingest(r) == (0, 1)
    assert len(rows(db)) == 1


def test_lambda_receive_and_outcome_belong_to_the_message(db, ingest):
    fn = Consumer("2026/11/13/[$LATEST]abc", source="lambda")
    ingest(fn.receive(T0, "m1", "rh-1", lambda_inv="inv-1"), fn.outcome(T0 + SEC, "m1", "success", "inv-1"))
    assert [r["kind"] for r in db.message_rows("m1")] == ["receive", "outcome"]


def test_rows_survive_a_restart(tmp_path):
    from trailbeam.server.db import Database
    from trailbeam.server.otlp import decode
    from spans import to_proto

    path = str(tmp_path / "trailbeam.sqlite")
    c = Consumer()
    s = Database(path)
    s.insert(decode(to_proto([c.receive(T0, "m1", "rh-1")]).SerializeToString(), "").records)
    s.close()
    s = Database(path)
    s.insert(decode(to_proto([c.delete(T0 + SEC, "rh-1")]).SerializeToString(), "").records)
    assert [(r["kind"], r["mid"]) for r in s.message_rows("m1")] == [("receive", "m1"), ("delete", "m1")]
    s.close()
