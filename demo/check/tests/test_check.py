"""Unit tests for the concurrency checker's comparison (demo/check/check.py). No Docker, no network."""

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import check  # noqa: E402

MID = "m-1"


def rec(op, t, rh=None, **kw):
    r = {"op": op, "id": MID, "t_ns": t, "result": "ok", "pid": 1, "host": "h"}
    if rh:
        r["rh"] = rh
    r.update(kw)
    return r


def ev(type_, t, **kw):
    return {"type": type_, "inferred": False, "time_unix_nano": str(t), **kw}


RETRY_TRUTH = [
    rec("send", 1, kind="retry_once"),
    rec("receive", 2, rh="a", rc=1),
    rec("change_visibility", 3, rh="a"),
    rec("receive", 4, rh="b", rc=2),
    rec("delete", 5, rh="b"),
]


def retry_timeline(**overrides):
    events = [
        ev("sent", 1),
        ev("received", 2, delivery=1, receive_count=1),
        ev("visibility_changed", 3, delivery=1, result="ok", visibility_timeout_s=0),
        {"type": "visibility_timeout_expired", "inferred": True, "time": None},
        ev("received", 4, delivery=2, receive_count=2),
        ev("deleted", 5, delivery=2, result="ok", status="final"),
    ]
    tl = {"message_id": MID, "state": "deleted", "events": events, "notices": []}
    tl.update(overrides)
    return tl


def test_matching_timeline_passes_and_ignores_inferred_events():
    r = check.evaluate(RETRY_TRUTH, retry_timeline())
    assert r["passed"], r
    assert r["inferred"] == ["visibility_timeout_expired"]
    assert r["expected"] == r["observed"] == ["send", "receive", "change_visibility", "receive", "delete"]


def test_truth_is_ordered_by_time_not_by_file_order(tmp_path):
    shuffled = [RETRY_TRUTH[i] for i in (3, 0, 4, 2, 1)]
    (tmp_path / "a.jsonl").write_text("".join(json.dumps(r) + "\n" for r in shuffled[:2]))
    (tmp_path / "b.jsonl").write_text("".join(json.dumps(r) + "\n" for r in shuffled[2:]) + "\n")
    truth = check.load_truth(str(tmp_path))
    assert [r["op"] for r in truth[MID]] == ["send", "receive", "change_visibility", "receive", "delete"]
    assert check.evaluate(truth[MID], retry_timeline())["passed"]


def test_handoff_lines_are_not_sqs_calls(tmp_path):
    (tmp_path / "a.jsonl").write_text(json.dumps(rec("handoff", 3, rh="a")) + "\n")
    assert check.load_truth(str(tmp_path)) == {}


def test_missing_message():
    r = check.evaluate(RETRY_TRUTH, None)
    assert not r["found"] and not r["passed"]


def test_missing_event_is_incomplete():
    tl = retry_timeline()
    tl["events"] = [e for e in tl["events"] if e["type"] != "visibility_changed"]
    r = check.evaluate(RETRY_TRUTH, tl)
    assert not r["complete"] and not r["passed"]


def test_extra_event_is_incomplete():
    tl = retry_timeline()
    tl["events"].insert(2, ev("received", 2, delivery=2, receive_count=2))
    assert not check.evaluate(RETRY_TRUTH, tl)["complete"]


@pytest.mark.parametrize("order", [
    ["received", "sent", "visibility_changed", "received", "deleted"],   # receive before its send
    ["sent", "received", "received", "visibility_changed", "deleted"],   # redelivery before the change that caused it
])
def test_wrong_order_fails(order):
    tl = retry_timeline()
    by_type = {}
    for e in tl["events"]:
        if not e["inferred"]:
            by_type.setdefault(e["type"], []).append(e)
    tl["events"] = [by_type[t].pop(0) for t in order]
    r = check.evaluate(RETRY_TRUTH, tl)
    assert r["complete"] and not r["in_order"] and not r["passed"]


def test_settle_joined_to_wrong_delivery_fails():
    tl = retry_timeline()
    tl["events"][5]["delivery"] = 1
    r = check.evaluate(RETRY_TRUTH, tl)
    assert r["in_order"] and not r["joined"] and not r["passed"]


def test_unjoined_settle_fails():
    tl = retry_timeline()
    tl["events"][5]["delivery"] = None
    assert not check.evaluate(RETRY_TRUTH, tl)["joined"]


def test_settle_result_must_match():
    tl = retry_timeline()
    tl["events"][2]["result"] = "failed"
    assert not check.evaluate(RETRY_TRUTH, tl)["joined"]


def test_receive_count_must_match():
    tl = retry_timeline()
    tl["events"][4]["receive_count"] = 3
    r = check.evaluate(RETRY_TRUTH, tl)
    assert not r["receive_count"] and not r["passed"]


def test_state_must_be_deleted():
    r = check.evaluate(RETRY_TRUTH, retry_timeline(state="not_deleted"))
    assert r["in_order"] and not r["final"] and not r["passed"]


def test_cross_process_delete_joins_by_handle():
    truth = [rec("send", 1), rec("receive", 2, rh="a", rc=1, pid=10), rec("delete", 3, rh="a", pid=99, by="deleter")]
    tl = {"state": "deleted", "events": [ev("sent", 1), ev("received", 2, delivery=1, receive_count=1),
                                         ev("deleted", 3, delivery=1, result="ok", status="final")]}
    assert check.evaluate(truth, tl)["passed"]


def test_summary_counts_and_result():
    truth = {MID: RETRY_TRUTH, "m-2": [dict(r, id="m-2") for r in RETRY_TRUTH]}
    good = check.evaluate(RETRY_TRUTH, retry_timeline())
    bad = check.evaluate(RETRY_TRUTH, retry_timeline(state="not_deleted"))
    s = check.summarize(truth, {MID: good, "m-2": bad}, 2)
    assert (s["messages_sent"], s["passed"], s["final"], s["result"]) == (2, 1, 1, "FAIL")
    assert s["with_change_visibility"] == 2 and s["received_more_than_once"] == 2
    s = check.summarize(truth, {MID: good, "m-2": good}, 2)
    assert s["result"] == "PASS"
    # Fewer messages sent than expected never passes.
    assert check.summarize(truth, {MID: good, "m-2": good}, 3)["result"] == "FAIL"


def test_order_failure_does_not_hide_joins_and_receive_counts():
    """Order is checked on its own: a message the store shows out of order still reports its joins."""
    tl = retry_timeline()
    e = tl["events"]
    tl["events"] = [e[0], e[1], e[4], e[2], e[3], e[5]]  # redelivery shown before the change that caused it
    r = check.evaluate(RETRY_TRUTH, tl)
    assert r["complete"] and not r["in_order"] and not r["passed"]
    assert r["joined"] and r["receive_count"] and r["final"]
    s = check.summarize({MID: RETRY_TRUTH}, {MID: r}, 1)
    assert (s["in_order"], s["joined"], s["receive_count"], s["failed_only_in_order"]) == (0, 1, 1, 1)


def test_receive_before_send_still_measures_joins():
    tl = retry_timeline()
    e = tl["events"]
    tl["events"] = [e[1], e[0]] + e[2:]
    r = check.evaluate(RETRY_TRUTH, tl)
    assert not r["in_order"] and r["joined"] and r["receive_count"]


def test_out_of_order_and_wrongly_joined_reports_both():
    tl = retry_timeline()
    e = tl["events"]
    e[5]["delivery"] = 1
    e[4]["receive_count"] = 3
    tl["events"] = [e[1], e[0]] + e[2:]
    r = check.evaluate(RETRY_TRUTH, tl)
    assert not r["in_order"] and not r["joined"] and not r["receive_count"]
    s = check.summarize({MID: RETRY_TRUTH}, {MID: r}, 1)
    assert s["failed_only_in_order"] == 0
