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


def ev(type_, t, group=None, start=None, **kw):
    """An observed store event (contract v2): interval [start, end], in a group. By default the interval
    is [10 t, 10 t + 5] and the group is t - 1, so events at t = 1, 2, ... are one per group, in order."""
    start = 10 * t if start is None else start
    end = kw.pop("end", 10 * t + 5)
    g = t - 1 if group is None else group
    return {"type": type_, "inferred": False, "group": g,
            "start_unix_nano": None if start == "unknown" else str(start), "end_unix_nano": str(end),
            "time_unix_nano": str(end), **kw}


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


def by_type(tl):
    out = {}
    for e in tl["events"]:
        if not e["inferred"]:
            out.setdefault(e["type"], []).append(e)
    return out


def test_effect_interval_ending_before_its_cause_starts_fails():
    """The new rule fails: the receive's interval ends before the send's starts (and the store, following
    the clocks, shows it first). The store did nothing wrong; the timeline still shows the impossible."""
    tl = retry_timeline()
    sent, rcv = by_type(tl)["sent"][0], by_type(tl)["received"][0]
    sent.update(start_unix_nano="22", end_unix_nano="28", group=1)
    rcv.update(start_unix_nano="10", end_unix_nano="20", group=0)
    r = check.evaluate(RETRY_TRUTH, tl)
    assert r["complete"] and not r["in_order"] and not r["passed"]
    assert r["violations"] == [{"pair": "send->receive", "cause": ["send", None, 0], "effect": ["receive", 1, 0]}]
    assert r["joined"] and r["receive_count"] and r["final"]


def test_overlap_passes_as_concurrent():
    """The receive's response came back before the send's, but the intervals overlap and the store put
    them in one concurrent group: not shown out of order, so in order; and counted as concurrent."""
    tl = retry_timeline()
    sent, rcv = by_type(tl)["sent"][0], by_type(tl)["received"][0]
    rcv.update(start_unix_nano="5", end_unix_nano="12", group=0)
    sent.update(start_unix_nano="10", end_unix_nano="15", group=0)
    tl["events"] = [rcv, sent] + [e for e in tl["events"] if e is not rcv and e is not sent]
    r = check.evaluate(RETRY_TRUTH, tl)
    assert r["in_order"] and r["passed"], r
    assert r["pairs"]["send->receive"] == {"in_order": 1, "concurrent": 1, "violated": 0, "concurrent_via_chain": 0,
                                                 "concurrent_tied": 0}
    assert r["concurrent_groups"] == 1


def test_effect_placed_in_an_earlier_group_fails_even_if_intervals_overlap():
    tl = retry_timeline()
    cv, rcv2 = by_type(tl)["visibility_changed"][0], by_type(tl)["received"][1]
    cv.update(start_unix_nano="30", end_unix_nano="45", group=3)   # overlaps the redelivery (40 to 45)
    rcv2.update(group=2)                                            # but the store shows it first
    r = check.evaluate(RETRY_TRUTH, tl)
    assert not r["in_order"]
    assert {"pair": "change_visibility->receive", "cause": ["change_visibility", 1, 0],
            "effect": ["receive", 2, 0]} in r["violations"]


def test_order_the_clocks_do_not_prove_fails():
    """The store must not claim an order the intervals do not show (cause ends after effect starts)."""
    tl = retry_timeline()
    by_type(tl)["sent"][0].update(end_unix_nano="22")  # send 10 to 22, receive 20 to 25, separate groups
    r = check.evaluate(RETRY_TRUTH, tl)
    assert not r["in_order"] and r["violations"][0]["pair"] == "send->receive"


def test_unknown_start_cannot_be_ordered_after_its_cause():
    truth = [rec("send", 1), rec("receive", 2, rh="a", rc=1), rec("delete", 3, rh="a")]
    tl = {"state": "deleted", "events": [ev("sent", 1, group=0), ev("received", 2, group=0, start="unknown"),
                                         ev("deleted", 3, group=1, delivery=1, result="ok", status="final")]}
    tl["events"][1].update(delivery=1, receive_count=1)
    r = check.evaluate(truth, tl)
    assert r["passed"] and r["pairs"]["send->receive"]["concurrent"] == 1
    tl["events"][1]["group"] = 1  # a store that ordered it after the send without a start to prove it
    tl["events"][2]["group"] = 2
    assert not check.evaluate(truth, tl)["in_order"]


def test_cause_effect_pairs_of_a_retried_message():
    pairs = check.cause_effect_pairs(RETRY_TRUTH)
    assert sorted(p[0] for p in pairs) == sorted([
        "send->receive", "send->receive", "receive->receive", "receive->settle", "receive->settle",
        "change_visibility->receive"])


def test_change_visibility_after_the_redelivery_is_not_a_cause():
    truth = [rec("send", 1), rec("receive", 2, rh="a", rc=1), rec("receive", 4, rh="b", rc=2),
             rec("change_visibility", 5, rh="a", result="failed"), rec("delete", 6, rh="b")]
    assert "change_visibility->receive" not in [p[0] for p in check.cause_effect_pairs(truth)]


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


def reversed_receive_and_send(tl):
    """Make the first receive's interval end before the send's starts, as the clocks would show it if they
    were wrong: the receive is then definitely before its cause."""
    sent, rcv = by_type(tl)["sent"][0], by_type(tl)["received"][0]
    sent.update(start_unix_nano="22", end_unix_nano="28", group=1)
    rcv.update(start_unix_nano="10", end_unix_nano="20", group=0)
    return tl


def test_order_failure_does_not_hide_joins_and_receive_counts():
    """Order is checked on its own: a message shown out of order still reports its joins."""
    r = check.evaluate(RETRY_TRUTH, reversed_receive_and_send(retry_timeline()))
    assert r["complete"] and not r["in_order"] and not r["passed"]
    assert r["joined"] and r["receive_count"] and r["final"]
    s = check.summarize({MID: RETRY_TRUTH}, {MID: r}, 1)
    assert (s["in_order"], s["joined"], s["receive_count"], s["failed_only_in_order"]) == (0, 1, 1, 1)


def test_out_of_order_and_wrongly_joined_reports_both():
    tl = reversed_receive_and_send(retry_timeline())
    e = tl["events"]
    e[5]["delivery"] = 1
    e[4]["receive_count"] = 3
    r = check.evaluate(RETRY_TRUTH, tl)
    assert not r["in_order"] and not r["joined"] and not r["receive_count"]
    s = check.summarize({MID: RETRY_TRUTH}, {MID: r}, 1)
    assert s["failed_only_in_order"] == 0


def test_summary_counts_pairs_and_concurrent_groups():
    """The two columns that keep in_order from passing vacuously."""
    ordered = check.evaluate(RETRY_TRUTH, retry_timeline())
    tl = retry_timeline()
    for e in tl["events"]:  # everything in one group: in order, but nothing resolved
        if not e["inferred"]:
            e["group"] = 0
    blob = check.evaluate(RETRY_TRUTH, tl)
    assert blob["in_order"] and blob["concurrent_groups"] == 1
    s = check.summarize({MID: RETRY_TRUTH, "m-2": [dict(r, id="m-2") for r in RETRY_TRUTH]},
                        {MID: ordered, "m-2": blob}, 2)
    assert s["with_concurrent_group"] == 1
    assert (s["pairs_in_order"], s["pairs_concurrent"], s["pairs_violated"]) == (6, 6, 0)
    # In the one-group timeline every interval is separate: all 6 are concurrent only through the group.
    assert s["pairs_concurrent_via_chain"] == 6
    assert s["pairs_concurrent_tied"] == 0
    assert s["pairs_by_kind"]["send->receive"] == {"in_order": 2, "concurrent": 2, "violated": 0,
                                                   "concurrent_via_chain": 2, "concurrent_tied": 0}
    assert s["pairs_by_kind"]["change_visibility->receive"] == {"in_order": 1, "concurrent": 1, "violated": 0,
                                                                "concurrent_via_chain": 1, "concurrent_tied": 0}


def test_a_tie_at_the_clock_resolution_is_concurrent_and_counted():
    tl = retry_timeline()
    sent, rcv = by_type(tl)["sent"][0], by_type(tl)["received"][0]
    sent.update(start_unix_nano="10", end_unix_nano="20", group=0)
    rcv.update(start_unix_nano="20", end_unix_nano="25", group=0)  # starts the instant the send ended
    r = check.evaluate(RETRY_TRUTH, tl)
    assert r["passed"]
    assert r["pairs"]["send->receive"]["concurrent_tied"] == 1
