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


# -- the store's proven order (order.edges, QV-31) -------------------------------------------------

def with_order(tl, edges, groups=None):
    """Give the observed events IDs e1, e2, ... (in listing order) and the store's proven ``order.edges``,
    given as (from index, to index, sources) over the observed events. ``groups`` overrides their groups."""
    obs = [e for e in tl["events"] if not e["inferred"]]
    for i, e in enumerate(obs):
        e["id"] = f"e{i + 1}"
        if groups is not None:
            e["group"] = groups[i]
    tl["order"] = {"edges": [{"from": obs[a]["id"], "to": obs[b]["id"], "sources": list(src)} for a, b, src in edges]}
    return tl


def one_group(tl):
    """Every observed event in group 0, with overlapping intervals: the clocks prove nothing."""
    for e in tl["events"]:
        if not e["inferred"]:
            e.update(group=0, start_unix_nano="0", end_unix_nano="100")
    return tl


SIMPLE_TRUTH = [rec("send", 1), rec("receive", 2, rh="a", rc=1), rec("delete", 3, rh="a")]


def simple_timeline():
    return {"state": "deleted", "events": [ev("sent", 1), ev("received", 2, delivery=1, receive_count=1),
                                           ev("deleted", 3, delivery=1, result="ok", status="final")]}


@pytest.mark.parametrize("source,kind,cause,effect", [
    ("clock", "send->receive", 0, 1),
    ("sqs_timestamps", "send->receive", 0, 1),
    ("process_order", "receive->delete", 1, 2),
    ("clock", "receive->delete", 1, 2),
])
def test_a_pair_resolved_by_each_source(source, kind, cause, effect):
    """The intervals all overlap and every step is in one box, except the one edge the store proves by
    ``source``: that pair is resolved and attributed to it; the rest stay concurrent."""
    tl = with_order(one_group(simple_timeline()), [(cause, effect, [source])],
                    groups=[0, 1, 1] if cause == 0 else [0, 0, 1])
    r = check.evaluate(SIMPLE_TRUTH, tl)
    assert r["passed"], r
    res = r["resolution"][kind]
    assert (res["pairs"], res["resolved"]) == (1, 1)
    assert res["direct:" + source] == 1 and res["only:" + source] == 1


def test_receive_count_resolves_receive_n_before_receive_n_plus_1():
    tl = with_order(one_group(retry_timeline()), [(1, 3, ["receive_count"])])
    r = check.evaluate(RETRY_TRUTH, tl)
    assert r["passed"], r
    assert r["resolution"]["receive n->receive n+1"] == {"pairs": 1, "resolved": 1, "direct:receive_count": 1,
                                                        "only:receive_count": 1}
    assert r["resolution"]["send->receive"] == {"pairs": 2, "concurrent": 2}


def test_release_before_redelivery_resolved_by_process_order():
    tl = with_order(one_group(retry_timeline()), [(2, 3, ["process_order"])])
    r = check.evaluate(RETRY_TRUTH, tl)
    assert r["passed"] and r["resolution"]["release->redelivery"]["resolved"] == 1


def test_a_chain_of_sources_resolves_a_pair_and_is_attributed_to_the_chain():
    """send -(SQS's timestamps)-> receive 1 -(receive count)-> receive 2: the send is before receive 2."""
    tl = with_order(one_group(retry_timeline()), [(0, 1, ["sqs_timestamps"]), (1, 3, ["receive_count"])],
                    groups=[0, 1, 1, 1, 1])
    r = check.evaluate(RETRY_TRUTH, tl)
    assert r["passed"], r
    sr = r["resolution"]["send->receive"]
    assert (sr["resolved"], sr["direct:sqs_timestamps"], sr["via_chain"]) == (2, 1, 1)
    assert sr["chain:sqs_timestamps"] == 1 and sr["chain:receive_count"] == 1


def test_a_store_proof_that_the_effect_came_first_is_a_violation():
    tl = with_order(one_group(simple_timeline()), [(1, 0, ["clock"])])
    r = check.evaluate(SIMPLE_TRUTH, tl)
    assert not r["in_order"] and r["violations"][0]["pair"] == "send->receive"


def test_groups_in_order_without_a_proof_are_a_violation():
    """With order.edges present, a later group must be backed by an edge, even if the intervals agree."""
    tl = with_order(simple_timeline(), [(1, 2, ["process_order"])])  # send in group 0 but no edge from it
    r = check.evaluate(SIMPLE_TRUTH, tl)
    assert not r["in_order"]
    assert {v["pair"] for v in r["violations"]} == {"send->receive"}


def summary_of(timelines):
    truth, results = {}, {}
    for i, tl in enumerate(timelines):
        mid = f"m-{i}"
        truth[mid] = [dict(r, id=mid) for r in SIMPLE_TRUTH]
        results[mid] = check.evaluate(truth[mid], tl)
    return check.summarize(truth, results, len(timelines))


def test_floor_fails_when_every_pair_is_concurrent():
    """Every message passes on its own (nothing shown out of order), but nothing is resolved."""
    s = summary_of([with_order(one_group(simple_timeline()), []) for _ in range(3)])
    assert s["passed"] == 3 and s["messages_result"] == "PASS"
    assert s["resolution"]["send->receive"]["resolved_share"] == 0.0
    assert s["floor"] == {"min_resolved_share": 0.9, "kinds": ["send->receive", "receive->delete"],
                          "failed": ["send->receive", "receive->delete"], "result": "FAIL"}


def test_floor_counts_the_share_per_kind():
    """9 of 10 send->receive resolved passes 90%; 8 of 10 receive->delete fails."""
    tls = []
    for i in range(10):
        edges = ([(0, 1, ["sqs_timestamps"])] if i < 9 else []) + ([(1, 2, ["process_order"])] if i < 8 else [])
        tls.append(with_order(one_group(simple_timeline()), edges,
                              groups=[0, 0, 0]))
    s = summary_of(tls)
    assert (s["resolution"]["send->receive"]["resolved"], s["resolution"]["receive->delete"]["resolved"]) == (9, 8)
    assert s["floor"]["failed"] == ["receive->delete"]
    assert s["resolution"]["receive->delete"]["by_source"]["process_order"] == 8
    assert s["resolution"]["release->redelivery"]["pairs"] == 0  # no floor, reported anyway


def test_floor_with_no_pairs_fails():
    assert check.floor_check(check.resolution_table({}))["result"] == "FAIL"
