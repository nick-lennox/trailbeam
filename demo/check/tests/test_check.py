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


def test_a_millisecond_tie_between_processes_is_broken_by_delivery(tmp_path):
    """Node.js stamps whole milliseconds (QV-38): the redelivery in another process can tie with the first
    receive and its release. The file order would put the redelivery first and join the release to it."""
    truth = [rec("send", 1, kind="retry_once"), rec("receive", 7, rh="a", rc=1),
             rec("change_visibility", 7, rh="a"), rec("receive", 7, rh="b", rc=2), rec("delete", 7, rh="b")]
    (tmp_path / "a.jsonl").write_text("".join(json.dumps(r) + "\n" for r in truth[3:]))
    (tmp_path / "b.jsonl").write_text("".join(json.dumps(r) + "\n" for r in truth[:3]))
    got = check.load_truth(str(tmp_path))[MID]
    assert [(r["op"], r.get("rh")) for r in got] == [(r["op"], r.get("rh")) for r in truth]


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

def with_order(tl, edges, groups=None, facts=True):
    """Give the observed events IDs e1, e2, ... (in listing order) and the store's proven ``order.edges``,
    given as (from index, to index, sources) over the observed events. ``groups`` overrides their groups.

    With ``facts`` (the default), the events also get the facts that make each claimed source hold, as a
    real store's would (the checker recomputes every source): a clock gap, call-order ticks in one scope
    (in listing order), or SQS timestamps a millisecond apart. ``facts=False`` claims the edges bare."""
    obs = [e for e in tl["events"] if not e["inferred"]]
    for i, e in enumerate(obs):
        e["id"] = f"e{i + 1}"
        if groups is not None:
            e["group"] = groups[i]
    for a, b, src in edges if facts else ():
        if "clock" in src:
            obs[a]["end_unix_nano"] = str(int(obs[b]["start_unix_nano"]) + 40)
            obs[b]["start_unix_nano"] = str(int(obs[b]["start_unix_nano"]) + 60)
        if "process_order" in src:
            for i, e in enumerate(obs):
                e["process_order"] = {"scope": "s1", "issued": 2 * i + 1, "returned": 2 * i + 2}
        if "sqs_timestamps" in src:
            obs[b].update(sent_timestamp_ms=1000, first_receive_timestamp_ms=1001)
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


# -- every edge is checked, not trusted (QV-32 section 5, QV-34) --------------------------------------------

def concurrent_cross_process_timeline():
    """What the honest store gives on the emulator for a message deleted by another process: the receive's
    long poll was open before the send (the clocks can't order them), SQS stamped SentTimestamp and
    ApproximateFirstReceiveTimestamp in the same millisecond, and the delete was made in the deleter process
    (another call-order scope), with its interval overlapping the receive's."""
    tl = one_group(simple_timeline())
    sent, rcv, dele = [e for e in tl["events"] if not e["inferred"]]
    for e in (sent, rcv, dele):
        e["queue"] = "q"
    sent["process_order"] = {"scope": "producer", "issued": 1, "returned": 2}
    rcv["process_order"] = {"scope": "consumer", "issued": 7, "returned": 8}
    dele["process_order"] = {"scope": "deleter", "issued": 3, "returned": 4}
    rcv.update(sent_timestamp_ms=5000, first_receive_timestamp_ms=5000)
    return tl


def test_a_store_that_orders_by_rule_fails_even_with_real_source_labels():
    """The reviewer's fake store (QV-32 section 5): on the honest data, it adds 'send -> receive' labelled
    sqs_timestamps and 'receive -> its delete' labelled process_order, with no facts behind them. Before
    QV-34 the checker took the labels on trust and this passed 200/200 and the floor. Now neither recomputes."""
    honest = [with_order(concurrent_cross_process_timeline(), []) for _ in range(3)]
    s = summary_of(honest)
    assert (s["passed"], s["floor"]["result"], s["result"]) == (3, "FAIL", "FAIL")  # honest: concurrent

    fake = [with_order(concurrent_cross_process_timeline(), [(0, 1, ["sqs_timestamps"]), (1, 2, ["process_order"])],
                       groups=[0, 1, 2], facts=False) for _ in range(3)]
    r = check.evaluate([dict(x, id=MID) for x in SIMPLE_TRUTH], fake[0])
    assert r["in_order"]  # the pair checks alone can't tell: every edge agrees with what caused what
    assert not r["edges_verified"] and not r["passed"]
    assert [(u["from"], u["to"], u["not_recomputed"]) for u in r["edges"]["unverified"]] == [
        ("e1", "e2", ["sqs_timestamps"]), ("e2", "e3", ["process_order"])]
    s = summary_of(fake)
    assert s["passed"] == 0 and s["edges_verified"] == 0 and s["result"] == "FAIL"
    assert (s["edges_checked"], s["edges_unverified"]) == (6, 6)
    # The floor alone would have passed: the labels resolve every pair.
    assert s["floor"]["result"] == "PASS"


# The truth as the drivers write it since QV-38: each call in its own process (producer, consumer, deleter)
# and SQS's own timestamps on the receive, as the honest emulator run gives them (the same millisecond).
XPROC_TRUTH = [rec("send", 1, host="producer", pid=7),
               rec("receive", 2, rh="a", rc=1, host="consumer", pid=8, sent_ms=5000, first_receive_ms=5000),
               rec("delete", 3, rh="a", host="deleter", pid=9, by="deleter")]


def fake2(tl):
    """The reviewer's fake2 (QV-35 section 1): the QV-32 rule edges (send -> receive labelled sqs_timestamps,
    receive -> its delete labelled process_order), plus made-up facts so that both labels recompute:
    ApproximateFirstReceiveTimestamp one millisecond after SentTimestamp, and call-order ticks for the
    receive and the delete in one scope although the deleter is another process."""
    tl = with_order(tl, [(0, 1, ["sqs_timestamps"]), (1, 2, ["process_order"])], groups=[0, 1, 2], facts=False)
    sent, rcv, dele = [e for e in tl["events"] if not e["inferred"]]
    rcv["first_receive_timestamp_ms"] = rcv["sent_timestamp_ms"] + 1
    rcv["process_order"] = {"scope": "fake", "issued": 1, "returned": 2}
    dele["process_order"] = {"scope": "fake", "issued": 3, "returned": 4}
    return tl


def xproc_summary(timelines):
    truth, results = {}, {}
    for i, tl in enumerate(timelines):
        mid = f"m-{i}"
        truth[mid] = [dict(r, id=mid) for r in XPROC_TRUTH]
        results[mid] = check.evaluate(truth[mid], tl)
    return check.summarize(truth, results, len(timelines))


def test_fake2_with_made_up_facts_fails_against_the_truth():
    """QV-35 section 1: fake2 passed 200/200 and the gating floor, because every source recomputed from the
    store's own (made-up) facts. Now the truth records SQS's facts and each call's process, so it fails."""
    honest = [with_order(concurrent_cross_process_timeline(), []) for _ in range(3)]
    s = xproc_summary(honest)
    assert (s["passed"], s["sqs_facts"], s["receives_sqs_facts_compared"]) == (3, 3, 3)
    assert (s["floor"]["result"], s["result"]) == ("FAIL", "FAIL")  # honest: concurrent, the floor not met

    fakes = [fake2(concurrent_cross_process_timeline()) for _ in range(3)]
    r = check.evaluate([dict(x, id=MID) for x in XPROC_TRUTH], fakes[0])
    assert r["in_order"] and r["joined"] and r["receive_count"]  # the checks before QV-38 can't tell
    assert not r["sqs_facts"] and not r["passed"]
    assert r["facts"]["sqs_facts_wrong"] == [{"event": "e2", "delivery": 1,
                                             "store": {"sent_ms": 5000, "first_receive_ms": 5001},
                                             "truth": {"sent_ms": 5000, "first_receive_ms": 5000}}]
    # The made-up ticks recompute, but the truth shows the receive and the delete in two processes.
    assert not r["edges_verified"]
    assert [(u["from"], u["to"], u["not_recomputed"]) for u in r["edges"]["unverified"]] == [
        ("e2", "e3", ["process_order"])]
    s = xproc_summary(fakes)
    assert (s["passed"], s["sqs_facts"], s["receives_sqs_facts_wrong"], s["result"]) == (0, 0, 3, "FAIL")
    assert s["floor"]["result"] == "PASS"  # the floor alone would still pass: the labels resolve every pair


@pytest.mark.parametrize("store", [{"first_receive_timestamp_ms": None}, {"sent_timestamp_ms": 4999},
                                   {"first_receive_timestamp_ms": 5000, "sent_timestamp_ms": 5000}])
def test_a_receive_must_show_the_sqs_facts_the_driver_saw(store):
    tl = concurrent_cross_process_timeline()
    rcv = tl["events"][1]
    for k, v in store.items():
        if v is None:
            rcv.pop(k)
        else:
            rcv[k] = v
    r = check.evaluate(XPROC_TRUTH, tl)
    ok = store == {"first_receive_timestamp_ms": 5000, "sent_timestamp_ms": 5000}
    assert r["sqs_facts"] is ok and r["passed"] is ok


def test_truth_from_before_qv38_has_no_sqs_facts_to_compare():
    tl = concurrent_cross_process_timeline()
    tl["events"][1]["first_receive_timestamp_ms"] = 1  # can't be caught without the facts in the truth
    r = check.evaluate(SIMPLE_TRUTH, tl)
    assert r["sqs_facts"] and r["facts"]["receives_compared"] == 0


def test_process_order_between_two_processes_fails_even_if_the_ticks_recompute():
    tl = with_order(simple_timeline(), [(0, 1, ["clock"]), (1, 2, ["process_order"])])
    assert check.evaluate(SIMPLE_TRUTH, tl)["edges_verified"]  # one process (pid 1 on host h): holds
    r = check.evaluate([dict(x, pid=2) if x["op"] == "delete" else x for x in SIMPLE_TRUTH], tl)
    assert not r["edges_verified"] and r["edges"]["unverified"][0]["not_recomputed"] == ["process_order"]


def bracketed(truth, same_clock=True, slack=1):
    """The truth with brackets around ev()'s default interval [10 t, 10 t + 5]."""
    return [dict(r, t_start_ns=10 * r["t_ns"] - slack, t_end_ns=10 * r["t_ns"] + 5 + slack,
                 **({"same_clock_as_spans": True} if same_clock else {})) for r in truth]


def test_python_events_must_lie_within_their_calls_brackets():
    r = check.evaluate(bracketed(SIMPLE_TRUTH), simple_timeline())
    assert r["passed"], r
    assert r["facts"]["events_in_brackets_checked"] == 3
    r = check.evaluate(bracketed(SIMPLE_TRUTH, slack=0), simple_timeline())
    assert r["in_brackets"]  # the bracket's ends are inclusive
    tl = simple_timeline()
    tl["events"][1]["end_unix_nano"] = str(27)  # the receive "returned" 1 ns after the driver saw it return
    r = check.evaluate(bracketed(SIMPLE_TRUTH), tl)
    assert not r["in_brackets"] and not r["passed"]
    assert r["facts"]["outside_brackets"] == [{"event": None, "op": "receive", "store": [20, 27], "truth": [19, 26]}]


def test_truth_on_another_clock_than_the_spans_is_not_bracket_checked():
    """Truth that doesn't say ``same_clock_as_spans`` (Node.js before QV-40) is not compared with the spans."""
    tl = simple_timeline()
    tl["events"][1]["end_unix_nano"] = str(27)
    r = check.evaluate(bracketed(SIMPLE_TRUTH, same_clock=False), tl)
    assert r["in_brackets"] and r["facts"]["events_in_brackets_checked"] == 0


# -- times bounded, not only contained (QV-40) -------------------------------------------------------------

MS = 1_000_000


def receive_offset_by(start_ns, end_ns):
    """SIMPLE_TRUTH bracketed widely (a long poll), with the store's receive starting ``start_ns`` after its
    truth start and ending ``end_ns`` before its truth end; the other calls tightly bracketed."""
    truth = [dict(r, t_start_ns=10 * r["t_ns"], t_end_ns=10 * r["t_ns"] + 5, same_clock_as_spans=True)
             for r in SIMPLE_TRUTH]
    truth[1].update(t_start_ns=1000, t_end_ns=1000 * MS)  # opened after the send returned (ns 15)
    tl = simple_timeline()
    tl["events"][1].update(start_unix_nano=str(1000 + start_ns), end_unix_nano=str(1000 * MS - end_ns))
    for e in tl["events"][2:]:  # the delete after the receive returned
        e.update(start_unix_nano=str(1000 * MS + 1), end_unix_nano=str(1000 * MS + 2))
    truth[2].update(t_start_ns=1000 * MS + 1, t_end_ns=1000 * MS + 2)
    return check.evaluate(truth, tl)


@pytest.mark.parametrize("start_ns,end_ns,ok,tight", [
    (1, 0, True, 0), (MS, MS, True, 0), (26 * MS, 2 * MS, True, 1), (100 * MS, 100 * MS, True, 1),
    (100 * MS + 1, 0, False, 1), (0, 100 * MS + 1, False, 1), (714 * MS, 0, False, 1)])
def test_times_may_lie_at_most_100_ms_inside_their_brackets(start_ns, end_ns, ok, tight):
    """Start at most 100 ms after the truth start, end at most 100 ms before the truth end (QV-40). The honest gap
    is scheduling: under 1 ms on an idle machine, up to 31 ms on a busy CI runner. QV-39's fake3 moved starts by
    1.5-714 ms. Events more than 1 ms inside are counted, not failed."""
    r = receive_offset_by(start_ns, end_ns)
    assert r["in_brackets"] is ok and r["passed"] is ok
    assert r["facts"]["bracket_offsets_max_ns"] == {"start": start_ns, "end": end_ns}
    assert r["facts"]["more_than_1ms_inside"] == tight


def real_run(lang):
    """Nine messages (three of each kind) of a real ``demo/check.sh`` run on the emulator, unedited: their truth
    lines and the store's timelines (tests/fixtures, QV-40)."""
    with open(os.path.join(os.path.dirname(__file__), "fixtures", f"real-{lang}.json"), encoding="utf-8") as f:
        return json.load(f)


def fake3(tl, b=False):
    """The reviewer's fake3 (QV-39 section 2) on one timeline: the receive of delivery 1 now starts 1 ns after the
    send's end, where that is later than its real start and still before its end (so inside its bracket), and
    the store claims the clock edge send -> receive, which now recomputes. No rule labels, no SQS facts changed.
    fake3b (``b``) first pulls the send's end back to 2 ns before the receive's end where it was later, inside
    the send's bracket too. Returns (timeline, whether anything moved)."""
    tl = json.loads(json.dumps(tl))
    evs = [e for e in tl["events"] if not e.get("inferred")]
    send = next(e for e in evs if e["type"] == "sent")
    rcv = next(e for e in evs if e["type"] == "received" and e["delivery"] == 1)
    s_start, s_end, r_start, r_end = (int(send["start_unix_nano"]), int(send["end_unix_nano"]),
                                      int(rcv["start_unix_nano"]), int(rcv["end_unix_nano"]))
    if b and s_end + 1 >= r_end and r_end - 2 >= s_start:
        s_end = r_end - 2
        send["end_unix_nano"] = send["time_unix_nano"] = str(s_end)
    if not r_start <= s_end + 1 < r_end:
        return tl, False
    rcv["start_unix_nano"] = str(s_end + 1)
    edges = tl.setdefault("order", {}).setdefault("edges", [])
    edge = next((e for e in edges if (e["from"], e["to"]) == (send["id"], rcv["id"])), None)
    if edge is None:
        edges.append({"from": send["id"], "to": rcv["id"], "sources": ["clock"]})
    elif "clock" not in edge["sources"]:
        edge["sources"].append("clock")
    return tl, True


@pytest.mark.parametrize("lang", ["python", "node"])
def test_a_real_run_passes_with_every_time_bracket_checked(lang):
    run = real_run(lang)
    for mid, truth in run["truth"].items():
        r = check.evaluate(truth, run["timelines"][mid])
        assert r["passed"], (mid, r)
        assert r["facts"]["events_in_brackets_checked"] == len(truth)


@pytest.mark.parametrize("lang", ["python", "node"])
@pytest.mark.parametrize("b", [False, True], ids=["fake3", "fake3b"])
def test_fake3_and_fake3b_on_real_runs_pass_containment_but_fail_the_time_bound(lang, b, monkeypatch):
    """QV-39 S1: on real data, fake3 and fake3b passed every check and the gating floor, because a time was only
    checked to lie inside its call's bracket. With the bound, every message whose times the fake moved by more
    than 100 ms fails, on in_brackets alone, so the run fails. A move under 100 ms still passes (the gate's
    limit) but is counted as more than 1 ms inside."""
    run = real_run(lang)
    fakes = {mid: fake3(tl, b) for mid, tl in run["timelines"].items()}
    moved = {mid for mid, (_, m) in fakes.items() if m}

    def summary():
        results = {mid: check.evaluate(run["truth"][mid], fakes[mid][0]) for mid in run["truth"]}
        return results, check.summarize(run["truth"], results, len(results))

    # Containment alone (the checker before QV-40): the fake passes, with send -> receive resolved by the clock.
    monkeypatch.setattr(check, "BRACKET_BOUND_NS", 10 ** 18)
    results, s = summary()
    assert s["passed"] == len(results) and s["edges_verified"] == len(results)
    assert all(results[mid]["resolution"]["send->receive"]["resolved"] for mid in moved)
    far = {mid for mid in moved if max(results[mid]["facts"]["bracket_offsets_max_ns"].values()) > 100 * MS}
    assert len(far) >= 3  # in these runs: the long polls opened about 450 ms before their send
    monkeypatch.undo()

    results, s = summary()
    failed = {mid for mid, r in results.items() if not r["passed"]}
    assert failed == far and s["result"] == "FAIL"
    assert all([c for c in check.CHECKS if not results[mid][c]] == ["in_brackets"] for mid in failed)
    assert s["events_more_than_1ms_inside_brackets"] >= len(moved)


def test_an_edge_whose_sources_recompute_passes():
    tl = with_order(concurrent_cross_process_timeline(), [(0, 1, ["sqs_timestamps"])], groups=[0, 1, 1])
    tl["events"][1].update(first_receive_timestamp_ms=5001)  # SQS's stamps differ: the source holds
    r = check.evaluate(SIMPLE_TRUTH, tl)
    assert r["passed"], r
    assert (r["edges"]["edges"], r["edges"]["implied"]) == (1, 1)


@pytest.mark.parametrize("sources", [[], ["causality"], ["clock", "receive_count"]])
def test_an_edge_with_no_source_an_unknown_one_or_one_that_does_not_hold_fails(sources):
    """receive_count can't order a send before a receive; "causality" is a rule, not a source."""
    tl = with_order(simple_timeline(), [(0, 1, sources), (1, 2, ["clock"])])
    r = check.evaluate(SIMPLE_TRUTH, tl)
    assert not r["edges_verified"] and not r["passed"]
    assert r["edges"]["unverified"][0]["from"] == "e1"


def test_an_edge_to_an_event_that_is_not_an_observed_step_fails():
    tl = with_order(simple_timeline(), [(0, 1, ["clock"]), (1, 2, ["clock"])])
    tl["order"]["edges"].append({"from": "e1", "to": "e99", "sources": ["clock"]})
    r = check.evaluate(SIMPLE_TRUTH, tl)
    assert not r["edges_verified"]


SLOW_TRUTH = [  # a slow consumer: its delete of delivery 1 comes after the redelivery (a stale handle)
    rec("send", 10, t_start_ns=10, t_end_ns=11),
    rec("receive", 20, rh="a", rc=1, t_start_ns=5, t_end_ns=20),
    rec("receive", 40, rh="b", rc=2, t_start_ns=30, t_end_ns=40),
    rec("delete", 50, rh="a", t_start_ns=50, t_end_ns=51),
    rec("delete", 60, rh="b", t_start_ns=60, t_end_ns=61),
]


def slow_timeline():
    return {"state": "deleted", "events": [
        ev("sent", 1), ev("received", 2, delivery=1, receive_count=1), ev("received", 3, delivery=2, receive_count=2),
        ev("deleted", 4, delivery=1, result="ok", status="stale_handle"),
        ev("deleted", 5, delivery=2, result="ok", status="final")]}


def test_an_implied_order_that_contradicts_the_truth_fails():
    """No cause/effect pair joins the stale delete of delivery 1 and receive 2, so the pair checks can't see
    this. The store's facts are made up to agree with each other (ticks in one scope), but the truth shows
    receive 2 returned before the delete of delivery 1 was made, so 'delete 1 -> receive 2' is false, and
    so is everything it implies."""
    tl = slow_timeline()
    obs = [e for e in tl["events"] if not e["inferred"]]
    for i, e in enumerate(obs):
        e["id"] = f"e{i + 1}"
        e["group"] = 0
    ticks = {"e1": (1, 2), "e2": (3, 4), "e4": (5, 6), "e3": (7, 8), "e5": (9, 10)}  # delete 1 before receive 2
    for e in obs:
        e["process_order"] = {"scope": "s", "issued": ticks[e["id"]][0], "returned": ticks[e["id"]][1]}
    tl["order"] = {"edges": [{"from": "e4", "to": "e3", "sources": ["process_order"]}]}
    r = check.evaluate(SLOW_TRUTH, tl)
    assert r["edges"]["unverified"] == []  # the label recomputes from the facts given...
    assert [(c["from"], c["to"]) for c in r["edges"]["contradicted"]] == [("e4", "e3")]  # ...but it's false
    assert not r["edges_verified"] and not r["passed"]
    # The true direction passes.
    tl["order"]["edges"] = [{"from": "e3", "to": "e4", "sources": ["clock"]}]
    assert check.evaluate(SLOW_TRUTH, tl)["edges_verified"]


def test_truth_without_brackets_is_judged_by_the_bound_it_has():
    """Before QV-34 a truth line had only t_ns: before a send or settle, after a receive."""
    assert check.truth_bounds({"op": "send", "t_ns": 5}) == (5, None)
    assert check.truth_bounds({"op": "receive", "t_ns": 7}) == (None, 7)
    assert check.truth_bounds({"op": "delete", "t_ns": 9, "t_start_ns": 9, "t_end_ns": 12}) == (9, 12)


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
    assert s["result"] == "FAIL"  # the run fails on the floor alone
    assert s["resolution"]["send->receive"]["resolved_share"] == 0.0
    assert s["floor"] == {"min_resolved_share": 0.9, "kinds": ["send->receive", "receive->delete"],
                          "failed": ["send->receive", "receive->delete"], "result": "FAIL", "gates": True}


def test_floor_counts_the_share_per_kind():
    """9 of 10 send->receive resolved passes 90%; 8 of 10 receive->delete fails."""
    tls = []
    for i in range(10):
        edges = ([(0, 1, ["sqs_timestamps"])] if i < 9 else []) + ([(1, 2, ["process_order"])] if i < 8 else [])
        tls.append(with_order(one_group(simple_timeline()), edges,
                              groups=[0, 0, 0]))
    s = summary_of(tls)
    assert (s["resolution"]["send->receive"]["resolved"], s["resolution"]["receive->delete"]["resolved"]) == (9, 8)
    assert s["floor"]["failed"] == ["receive->delete"] and s["result"] == "FAIL"
    assert s["resolution"]["receive->delete"]["by_source"]["process_order"] == 8
    assert s["resolution"]["release->redelivery"]["pairs"] == 0  # no floor, reported anyway


def test_floor_report_only_reports_the_floor_but_does_not_fail_the_run():
    """The emulator (QV-34): messages still gate, the floor is computed at 90% and shown, but not gated."""
    truth = {f"m-{i}": [dict(r, id=f"m-{i}") for r in SIMPLE_TRUTH] for i in range(3)}
    results = {m: check.evaluate(t, with_order(one_group(simple_timeline()), [])) for m, t in truth.items()}
    s = check.summarize(truth, results, 3, floor_gates=False)
    assert s["floor"]["result"] == "FAIL" and s["floor"]["gates"] is False and s["floor"]["min_resolved_share"] == 0.9
    assert s["result"] == "PASS"
    results["m-0"] = check.evaluate(truth["m-0"], None)  # a message that fails still fails the run
    assert check.summarize(truth, results, 3, floor_gates=False)["result"] == "FAIL"


def test_floor_with_no_pairs_fails():
    assert check.floor_check(check.resolution_table({}))["result"] == "FAIL"


# -- the move to a dead-letter queue (QV-33, contract 3.1) ---------------------------------------------

DLQ_TRUTH = [
    rec("send", 1, kind="poison_fast"),
    rec("receive", 2, rh="a", rc=1, queue="q"),
    rec("change_visibility", 3, rh="a"),
    rec("receive", 4, rh="b", rc=2, queue="q-dlq"),
    rec("delete", 5, rh="b", queue="q-dlq"),
]


def dlq_timeline(move):
    events = [ev("sent", 1), ev("received", 2, delivery=1, receive_count=1),
              ev("visibility_changed", 3, delivery=1, result="ok", visibility_timeout_s=0)]
    if move is not None:
        events.append(move)
    events += [ev("received", 4, delivery=2, receive_count=2),
               ev("deleted", 5, delivery=2, result="ok", status="final")]
    return {"message_id": MID, "state": "deleted", "events": events, "notices": []}


OBSERVED_MOVE = {"type": "moved_to_dead_letter_queue", "inferred": False, "source": "sqs_dlq_source", "time": None,
                 "group": None, "from_queue": "q", "to_queue": "q-dlq"}


def test_observed_dlq_move_passes_and_is_counted():
    r = check.evaluate(DLQ_TRUTH, dlq_timeline(OBSERVED_MOVE))
    assert r["passed"] and r["dlq_move"], r
    assert r["dlq_moves"] == {"expected": [("q", "q-dlq")], "observed": [("q", "q-dlq")], "inferred": 0, "ok": True}
    s = check.summarize({MID: DLQ_TRUTH}, {MID: r}, 1, floor=0)
    assert (s["dlq_messages"], s["dlq_moves_observed"], s["dlq_moves_inferred"]) == (1, 1, 0)


def test_inferred_dlq_move_fails():
    inferred = {**OBSERVED_MOVE, "inferred": True, "source": None, "window": {}, "reason": "guessed"}
    r = check.evaluate(DLQ_TRUTH, dlq_timeline(inferred))
    assert not r["dlq_move"] and not r["passed"]
    assert r["complete"] and r["joined"] and r["receive_count"] and r["final"]  # only the move is wrong
    s = check.summarize({MID: DLQ_TRUTH}, {MID: r}, 1, floor=0)
    assert (s["dlq_moves_observed"], s["dlq_moves_inferred"], s["result"]) == (0, 1, "FAIL")


def test_missing_or_wrong_dlq_move_fails():
    assert not check.evaluate(DLQ_TRUTH, dlq_timeline(None))["dlq_move"]
    wrong = {**OBSERVED_MOVE, "from_queue": "other"}
    assert not check.evaluate(DLQ_TRUTH, dlq_timeline(wrong))["dlq_move"]
    no_source = {k: v for k, v in OBSERVED_MOVE.items() if k != "source"}
    assert not check.evaluate(DLQ_TRUTH, dlq_timeline(no_source))["dlq_move"]


def test_move_without_one_in_the_truth_fails():
    truth = [dict(r, queue="q") if r["op"] == "receive" else r for r in DLQ_TRUTH]
    assert not check.evaluate(truth, dlq_timeline(OBSERVED_MOVE))["dlq_move"]
    assert check.evaluate(RETRY_TRUTH, retry_timeline())["dlq_moves"]["expected"] == []
