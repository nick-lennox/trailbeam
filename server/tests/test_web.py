"""The web screen over HTTP: lookup, timeline with inferred steps marked, duplicates, stuck messages."""
import json
import re
from html import unescape
import urllib.error
import urllib.parse
import urllib.request

import pytest

from spans import SEC, T0, Consumer, to_proto

MID = "5fea7756-0ea4-451a-a703-a558b933e274"
POISON = "poison-1"
ODD = '<b>"id"/&?</b>'


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *a, **k):
        return None


def get(url, path, follow=True):
    opener = urllib.request.build_opener() if follow else urllib.request.build_opener(NoRedirect)
    try:
        with opener.open(url + path, timeout=5) as r:
            return r.status, r.headers, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.headers, e.read().decode()


def post(url, spans):
    req = urllib.request.Request(url + "/v1/traces", data=to_proto(spans).SerializeToString(), method="POST",
                                 headers={"Content-Type": "application/x-protobuf"})
    with urllib.request.urlopen(req, timeout=5) as r:
        assert r.status == 200


def page(mid):
    return "/message?" + urllib.parse.urlencode({"id": mid})


def events(html):
    """(type, inferred) of each timeline entry, in page order."""
    return [(t, i == "true") for t, i in re.findall(r'<li class="[^"]*" data-type="([^"]+)" data-inferred="(\w+)"', html)]


@pytest.fixture
def site(server):
    a, b = Consumer("consumer-a"), Consumer("consumer-b")
    p = Consumer("producer", service="orders-api")
    dlq = Consumer("consumer-a", queue="orders-dlq")
    post(server, [
        # Redelivered after a visibility timeout, then deleted.
        p.send(T0, MID), a.receive(T0 + SEC, MID, "rh-1"),
        b.receive(T0 + 40 * SEC, MID, "rh-2", count=2), b.delete(T0 + 41 * SEC, "rh-2"),
        # Fails twice, then moves to the dead-letter queue, where nobody deletes it.
        p.send(T0, POISON), a.receive(T0 + SEC, POISON, "p-1"),
        a.change_visibility(T0 + 2 * SEC, "p-1", 0), a.receive(T0 + 3 * SEC, POISON, "p-2", count=2),
        dlq.receive(T0 + 100 * SEC, POISON, "p-3", count=3, dlq_source="orders"),
        # A message ID with HTML in it, sent and never received.
        p.send(T0, ODD),
    ])
    return server


def test_index_lists_recent_messages_and_has_lookup(site):
    status, headers, html = get(site, "/")
    assert status == 200 and headers["Content-Type"] == "text/html; charset=utf-8"
    assert '<form method="get" action="/message">' in html
    assert f'href="{page(MID)}"' in html and f'href="{page(POISON)}"' in html


def test_timeline_in_order_with_inferred_steps_marked(site):
    status, _, html = get(site, page(MID))
    assert status == 200
    assert events(html) == [("sent", False), ("received", False), ("visibility_timeout_expired", True),
                            ("received", False), ("deleted", False)]
    inferred = re.search(r'<li class="event inferred"[^>]*>(.*?)</li>', html).group(1)
    assert '<span class="badge">inferred</span>' in inferred
    assert "between" in inferred and "SQS delivered it again" in inferred  # the window and the reason
    assert "2025-11-14 00:26:40.000000Z" in html  # observed times are shown
    assert "consumer-b" in html and "orders-api" in html
    assert "<strong>final</strong>" in html


def test_dead_letter_queue_timeline(site):
    _, _, html = get(site, page(POISON))
    types = events(html)
    assert ("moved_to_dead_letter_queue", False) in types and ("visibility_changed", False) in types
    assert ("moved_to_dead_letter_queue", True) not in types  # contract 3.1: never inferred
    assert types.index(("moved_to_dead_letter_queue", False)) == len(types) - 2  # just before the DLQ receive
    move = re.search(r'<li [^>]*data-type="moved_to_dead_letter_queue"[^>]*>(.*?)</li>', html)
    assert 'data-source="sqs_dlq_source"' in move.group(0)
    assert "reported by SQS (DeadLetterQueueSourceArn on the DLQ receive)" in move.group(1)
    assert '<span class="badge">inferred</span>' not in move.group(1)
    assert "between" in move.group(1)  # SQS gives no time for the move: a window, from its anchors
    assert re.search(r'data-sources="sqs_dlq_source">After receive 2: ', move.group(1))
    assert re.search(r'data-sources="sqs_dlq_source">Before receive 3: SQS reported the move', move.group(1))
    assert "(dead-letter queue)" in html and "orders-dlq" in html
    assert "moved, reported by SQS" in html


def test_not_seen_on_the_dead_letter_queue_yet(store):
    """maxReceiveCount configured for orders; the receive count reached it, and no DLQ receive reported a move."""
    import threading

    from qv2_store.server import make_server

    httpd = make_server(store, "127.0.0.1", 0, max_receive_counts={"orders": 2})
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{httpd.server_address[1]}"
    try:
        a = Consumer("consumer-a")
        post(base, [a.send(T0, MID), a.receive(T0 + SEC, MID, "rh-1"),
                    a.receive(T0 + 40 * SEC, MID, "rh-2", count=2)])
        _, _, html = get(base, page(MID))
        assert "moved_to_dead_letter_queue" not in html
        assert 'data-code="dlq_move_not_seen"' in html
        assert "Receive count exceeded maxReceiveCount; not seen on the DLQ yet." in html
        _, _, html = get(base, "/")
        assert "(maxReceiveCount reached; not seen on the DLQ yet)" in html
        assert json.loads(get(base, "/api/health")[2])["config"]["max_receive_counts"] == {"orders": 2}
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_message_id_is_escaped_and_round_trips(site):
    status, _, html = get(site, page(ODD))
    assert status == 200
    assert ODD not in html and "&lt;b&gt;&quot;id&quot;/&amp;?&lt;/b&gt;" in html
    assert events(html) == [("sent", False)]
    assert "send_not_observed" not in html


def test_unknown_message_is_404_page(site):
    status, headers, html = get(site, page("nope"))
    assert status == 404 and headers["Content-Type"].startswith("text/html")
    assert "Message not found" in html and 'value="nope"' in html


def test_empty_lookup_redirects_home(site):
    status, headers, _ = get(site, "/message?id=", follow=False)
    assert status == 303 and headers["Location"] == "/"


def test_duplicates_page(site):
    _, _, html = get(site, "/duplicates")
    assert f'href="{page(MID)}"' in html and f'href="{page(POISON)}"' in html
    assert "received more than once" in html
    _, _, html = get(site, "/duplicates?reason=succeeded_more_than_once")
    assert "No duplicate deliveries." in html
    assert '<option value="succeeded_more_than_once" selected>' in html


def test_stuck_page(site):
    _, _, html = get(site, "/stuck?older_than_s=60")
    assert f'href="{page(POISON)}"' in html and f'href="{page(ODD)}"' in html  # never received
    assert f'href="{page(MID)}"' not in html  # deleted, so not stuck
    assert 'name="older_than_s" value="60"' in html and "days" in html
    assert "(DLQ, from orders)" in html


def test_queue_filter_and_empty_lists(site):
    _, _, html = get(site, "/stuck?queue=payments")
    assert "No stuck messages." in html
    _, _, html = get(site, "/?queue=&limit=")  # empty form fields mean the defaults
    assert f'href="{page(MID)}"' in html


def test_bad_parameter_is_400_page(site):
    status, _, html = get(site, "/stuck?older_than_s=soon")
    assert status == 400 and "older_than_s must be an integer" in html


def test_unknown_page_is_404(site):
    assert get(site, "/nope")[0] == 404
    assert get(site, "/api/health")[0] == 200  # the JSON API still answers


def test_overlapping_steps_are_shown_as_concurrent_bars_on_one_scale(server):
    p, c = Consumer("producer"), Consumer("consumer-a")
    ms = 1_000_000
    post(server, [p.send(T0 + 30 * ms, "conc-1", start=T0),
                  c.receive(T0 + 20 * ms, "conc-1", "rh-c", start=T0 - 900 * ms),   # long poll, overlaps
                  c.delete(T0 + 60 * ms, "rh-c", start=T0 + 50 * ms)])
    status, _, html = get(server, page("conc-1"))
    assert status == 200
    groups = re.findall(r'<li class="group concurrent" data-group="(\d+)">(.*?)</ul></li>', html)
    assert len(groups) == 1 and groups[0][0] == "0"
    inside = groups[0][1]
    assert "concurrent, order unknown" in inside
    # A waterfall, not a row of cards: rows by when each request was sent. The long poll was sent 900 ms
    # before the send, so it is listed first; the bars overlap from T0 to T0 + 20 ms.
    assert '<ul class="waterfall">' in inside and "which is not the order SQS acted in" in inside
    assert events(inside) == [("received", False), ("sent", False)]
    assert re.findall(r'data-type="(\w+)".*?style="left:([\d.]+)%;width:([\d.]+)%"', inside) == [
        ("received", "0.00", "98.92"), ("sent", "96.77", "3.23")]  # scale T0 - 900 ms to T0 + 30 ms
    assert events(html) == [("received", False), ("sent", False), ("deleted", False)]
    # The delete is after the group and outside it.
    assert html.index('data-type="deleted"') > html.index("</ul></li>")
    # Intervals are shown as start to end.
    assert "2025-11-14 00:26:40.000000Z</time> to <time" in html and "(30.0 ms)" in html
    assert "order is unknown" in html  # the explanation above the timeline
    # The delete says why it is after the box: the clocks.
    d = re.search(r'<li class="event observed" data-type="deleted"[^>]*>(.*?)</li>', html).group(1)
    assert "After receive 1: Receive 1 ended" in d and 'data-sources="clock"' in d


def test_rows_in_a_box_follow_the_request_times_not_the_response_times(server):
    """QV-32 section 7.2: the box listed its steps by end time, so a receive whose response came back first
    was drawn before the send (Node.js: 351 of 581 boxes). Rows now follow when each request was sent, and
    each step's bar shows where it sits on the box's one time scale."""
    p, c = Consumer("producer"), Consumer("consumer-a")
    ms = 1_000_000
    post(server, [p.send(T0 + 30 * ms, "wf-1", start=T0),
                  c.receive(T0 + 20 * ms, "wf-1", "rh-wf", start=T0 + 10 * ms),  # sent after, back first
                  c.delete(T0 + 60 * ms, "rh-wf", start=T0 + 50 * ms)])
    _, _, html = get(server, page("wf-1"))
    inside = re.findall(r'<li class="group concurrent" data-group="(\d+)">(.*?)</ul></li>', html)[0][1]
    assert events(inside) == [("sent", False), ("received", False)]
    assert re.findall(r'data-type="(\w+)".*?style="left:([\d.]+)%;width:([\d.]+)%"', inside) == [
        ("sent", "0.00", "100.00"), ("received", "33.33", "33.33")]
    assert "The scale runs from" in inside and "(30.0 ms)" in inside


def test_a_step_whose_start_is_unknown_is_drawn_open_from_the_left_edge(server):
    """A Lambda receive (start not observed, no SQS timestamps) and its send: nothing orders them. The
    receive's bar runs from the box's left edge and fades in there, instead of inventing a start, and it is
    listed first because it may have started any time before."""
    p, fn = Consumer("producer"), Consumer("env-1", source="lambda")
    post(server, [p.send(T0 + 5 * SEC, "lam-wf", start=T0 + 5 * SEC - 1_000_000),
                  fn.receive(T0 + 10 * SEC, "lam-wf", "rh-l1", lambda_inv="inv-1")])
    _, _, html = get(server, page("lam-wf"))
    inside = re.findall(r'<li class="group concurrent" data-group="(\d+)">(.*?)</ul></li>', html)[0][1]
    got = re.findall(r'data-type="(\w+)".*?<span class="([^"]+)" style="left:([\d.]+)%;width:([\d.]+)%"', inside)
    # The inferred end of the invocation (no outcome within the 900 s Lambda timeout) is in the box too,
    # hatched over its window, which sets the scale's right end.
    assert got == [("received", "bar open-start", "0.00", "0.55"), ("sent", "bar", "0.00", "0.00"),
                   ("lambda_invocation_ended_without_outcome", "bar window", "0.55", "99.45")]


def test_a_receive_with_an_unknown_start_shows_only_its_upper_bound(server):
    fn = Consumer("env-1", source="lambda")
    post(server, [fn.receive(T0, "lam-1", "rh-l", lambda_inv="inv-1"), fn.outcome(T0 + SEC, "lam-1", "success", "inv-1")])
    _, _, html = get(server, page("lam-1"))
    assert "no later than <time" in html


def test_a_chained_concurrent_box_does_not_claim_every_step_overlapped(server):
    # The receive overlaps the send, the send overlaps the delete, but the receive ended before the delete
    # started: one group through the chain, and the box must not say all three overlapped.
    p, c = Consumer("producer"), Consumer("consumer-a")
    ms = 1_000_000
    post(server, [c.receive(T0 + 20 * ms, "chain-1", "rh-ch", start=T0 - 900 * ms),
                  p.send(T0 + 30 * ms, "chain-1", start=T0),
                  c.delete(T0 + 60 * ms, "rh-ch", start=T0 + 25 * ms)])
    _, _, html = get(server, page("chain-1"))
    groups = re.findall(r'<li class="group concurrent" data-group="(\d+)">(.*?)</ul></li>', html)
    assert len(groups) == 1
    head = re.search(r'<div class="group-head">(.*?)</div>', groups[0][1]).group(1)
    text = re.sub(r"<[^>]+>", "", head)
    assert text == ("concurrentconcurrent, order unknown: the store can't put these 3 steps in one order; "
                    "no observed fact orders some of them.")
    # The one pair the clocks do order inside the box says so, on the later step.
    d = re.search(r'<li class="event observed" data-type="deleted"[^>]*>(.*?)</li>', html).group(1)
    assert 'data-sources="clock">After receive 1: Receive 1 ended (' in d
    assert "overlapped in time, so" not in html and "cannot tell which happened first" not in html


def test_a_box_counts_the_inferred_step_it_holds(server):
    # A rejected delete inside the expiry's window: one box with the delete and the expiry.
    c = Consumer("consumer-a")
    post(server, [c.send(T0 + 19 * SEC, "slow-1", start=T0 + 19 * SEC - 2_000_000),
                  c.receive(T0 + 20 * SEC, "slow-1", "rh-s1", start=T0 + 20 * SEC - 1_000_000),
                  c.delete(T0 + 32 * SEC, "rh-s1", ok=False, start=T0 + 32 * SEC - 1_000_000),
                  c.receive(T0 + 35 * SEC, "slow-1", "rh-s2", count=2, start=T0 + 35 * SEC - 1_000_000)])
    _, _, html = get(server, page("slow-1"))
    groups = re.findall(r'<li class="group concurrent" data-group="(\d+)">(.*?)</ul></li>', html)
    assert len(groups) == 1
    assert "these 2 steps in one order" in groups[0][1]
    # The expiry's window opens at receive 1, before the delete's request was sent, so its row is first.
    assert events(groups[0][1]) == [("visibility_timeout_expired", True), ("deleted", False)]
    assert 'class="bar window"' in groups[0][1]


def test_each_step_says_which_observed_fact_puts_it_after_another(server):
    """The QV-31 sources on the screen, in plain words."""
    p, c = Consumer("producer"), Consumer("consumer-a", scope="proc-a")
    ms = 1_000_000
    sent = T0 // ms
    post(server, [p.send(T0 + 30 * ms, "why-1", start=T0),
                  c.receive(T0 + 20 * ms, "why-1", "rh-w1", start=T0 - 900 * ms, sent_ms=sent + 4, first_ms=sent + 6,
                            order=(1, 2)),
                  c.receive(T0 + 25 * ms, "why-1", "rh-w2", count=2, start=T0 + 15 * ms),
                  c.delete(T0 + 40 * ms, "rh-w1", start=T0 + 20 * ms, order=(3, 4))])
    _, _, html = get(server, page("why-1"))
    whys = [unescape(re.sub(r"<[^>]+>", "", w)) for w in re.findall(r'<div class="why"[^>]*>(.*?)</div>', html)]
    assert ("After the send: SQS's timestamps show this message was sent (2025-11-14T00:26:40.004000Z) before "
            "its first receive (2025-11-14T00:26:40.006000Z).") in whys
    assert ("After receive 1: SQS's receive count shows receive 1 (receive count 1) came before this one "
            "(receive count 2).") in whys
    assert "After receive 1: Same process: receive 1 returned before this call was issued." in whys
    assert "Placed after receive 1 and before receive 2, the steps it was inferred from." in whys


CONTROL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


def test_no_page_carries_control_characters(site, server):
    """QV-32 section 7: the CSS arrow was written "\\2191" in a plain string, which Python read as the
    octal escape \\21 (U+0011) followed by "91", so every reason line started with a stray glyph and "91"."""
    test_each_step_says_which_observed_fact_puts_it_after_another(server)
    for path in ("/", page(MID), page(POISON), page(ODD), page("why-1"), "/duplicates", "/stuck?older_than_s=0"):
        status, _, html = get(site, path)
        assert status == 200, path
        assert not CONTROL.search(html), (path, CONTROL.search(html))
    assert '.why:before{content:"\\2191  "' in html  # the CSS escape reaches the browser intact
