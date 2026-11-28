"""The web screen over HTTP: lookup, timeline with inferred steps marked, duplicates, stuck messages."""
import json
from pathlib import Path
import re
from html import escape, unescape
import urllib.error
import urllib.parse
import urllib.request

import pytest

from trailbeam.server import web
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


def test_wording_shared_with_the_ui_package_matches_it():
    """QV-247: the server-rendered pages say what @trailbeam/ui says, word for word."""
    ui = Path(__file__).resolve().parents[3] / "ui" / "src"
    tenants = (ui / "tenants" / "Tenants.tsx").read_text()
    p95 = re.search(r'export const P95_NOTE = ((?:"[^"]*"\s*\+?\s*)+);', tenants).group(1)
    assert "".join(re.findall(r'"([^"]*)"', p95)) == web.P95_NOTE
    messages = (ui / "messages" / "Messages.tsx").read_text()
    title = re.search(r'export const RECEIVES_TITLE = ((?:"[^"]*"\s*\+?\s*)+);', messages).group(1)
    assert "".join(re.findall(r'"([^"]*)"', title)) == web.RECEIVES_TITLE
    for state, label in web.STATE_LABELS.items():
        assert f'{state}: "{label}"' in messages
    title = re.search(r'export const POSSIBLY_PURGED_TITLE = ((?:"[^"]*"\s*\+?\s*)+);', messages).group(1)
    assert "".join(re.findall(r'"([^"]*)"', title)) == web.POSSIBLY_PURGED_TITLE
    stuck = (ui.parent / "shell" / "stuck.tsx").read_text()  # the Stuck page (QV-367)
    plain = lambda h: unescape(re.sub(r"<[^>]+>", "", h.replace("&rsquo;", "'")))  # noqa: E731
    assert plain(web.STUCK_LEAVES_SQS_NOTE) in plain(stuck)


def test_receives_column_marks_a_message_received_more_than_once(site):
    for path in ("/", "/duplicates", "/stuck?older_than_s=60"):
        _, _, html = get(site, path)
        assert ('<th title="Receives Trailbeam observed, on every queue, the receive from a dead-letter queue '
                "included. SQS&#x27;s own receive count can be higher") in html
        assert ">Receives</th>" in html and "Deliveries" not in html and " ok)" not in html
    _, _, html = get(site, "/")
    assert '<span class="repeat">2</span> <span class="muted">\u00b7 1 handled</span>' in html  # MID
    assert '0 <span class="muted">\u00b7 0 handled</span>' in html  # ODD, never received: no mark
    _, _, html = get(site, page(MID))
    assert "<dt>Receives</dt><dd>2 (1 handled)</dd>" in html


def test_timeline_in_order_with_inferred_steps_marked(site):
    status, _, html = get(site, page(MID))
    assert status == 200
    assert events(html) == [("sent", False), ("received", False), ("visibility_timeout_expired", True),
                            ("received", False), ("deleted", False)]
    inferred = re.search(r'<li class="event inferred"[^>]*>(.*?)</li>', html).group(1)
    assert '<span class="badge">inferred</span>' in inferred
    assert "between" in inferred and "so the message was received again" in inferred  # the window and the reason
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


def test_not_seen_on_the_dead_letter_queue_yet(db):
    """maxReceiveCount configured for orders; the receive count reached it, and no DLQ receive reported a move."""
    import threading

    from trailbeam.server.server import make_server

    httpd = make_server(db, "127.0.0.1", 0, max_receive_counts={"orders": 2}, ui_dir=None)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{httpd.server_address[1]}"
    try:
        a = Consumer("consumer-a")
        post(base, [a.send(T0, MID), a.receive(T0 + SEC, MID, "rh-1"),
                    a.receive(T0 + 40 * SEC, MID, "rh-2", count=2)])
        _, _, html = get(base, page(MID))
        assert "moved_to_dead_letter_queue" not in html
        assert 'data-code="dlq_move_not_seen"' in html
        assert "Receive count reached maxReceiveCount; not seen on the DLQ yet." in html
        assert "This is inferred from the configured value, and that receive may still be in progress." in html
        assert "receive count reached maxReceiveCount, so a move is inferred; not seen on the DLQ yet" in html
        _, _, html = get(base, "/")
        assert "(maxReceiveCount reached; DLQ move inferred, not seen yet)" in html
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
    assert "A second receive is expected after your own <code>ChangeMessageVisibility</code>" in html
    assert 'received <span class="mono">2</span> times' in html
    assert '<option value="succeeded_more_than_once">handled more than once</option>' in html
    _, _, html = get(site, "/duplicates?reason=succeeded_more_than_once")
    assert "No duplicates." in html
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
    assert text == ("concurrentconcurrent, order unknown: Trailbeam can't put these 3 steps in one order; "
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


def test_first_seen_says_which_clock_it_is_on(server):
    """QV-32 section 4: the send's time on the host's clock when the send was observed; SQS's
    SentTimestamp only when it wasn't, labelled as SQS's, on the timeline and on the stuck list."""
    p, c = Consumer("producer"), Consumer("consumer-a")
    post(server, [p.send(T0, "fs-host"),
                  c.receive(T0 + SEC, "fs-host", "rh-fh", sent_ms=T0 // 1_000_000 - 2000),  # SQS 2 s behind
                  c.receive(T0 + SEC, "fs-sqs", "rh-fs", sent_ms=T0 // 1_000_000 - 2000)])
    _, _, html = get(server, page("fs-host"))
    seen = re.search(r"<dt>First seen</dt><dd>(.*?)</dd>", html).group(1)
    assert "2025-11-14 00:26:40.000000Z" in seen and "the send, on the host's clock" in seen
    _, _, html = get(server, page("fs-sqs"))
    seen = re.search(r"<dt>First seen</dt><dd>(.*?)</dd>", html).group(1)
    assert "2025-11-14 00:26:38.000000Z" in seen and "SQS's SentTimestamp, on SQS's clock" in seen
    _, _, html = get(server, "/stuck?older_than_s=0")
    row = re.search(r'<tr><td><a [^>]*>fs-sqs</a>.*?</tr>', html).group(0)
    assert "SQS's clock" in row and "this server's clock minus SQS's SentTimestamp: two clocks" in row
    row = re.search(r'<tr><td><a [^>]*>fs-host</a>.*?</tr>', html).group(0)
    assert "SQS's clock" not in row and "two clocks" not in row



def test_an_inferred_window_that_ties_with_an_observed_start_is_listed_after_it(server):
    """QV-35 section 3: an inferred visibility-timeout expiry's window starts at its anchor's request start
    (here receive 1, since the change isn't proven before receive 2), so the two tie, and the box fell back
    to Trailbeam's member order: in the reviewer's screenshots the expiry was listed above the receive it is
    placed after. On a tie an observed step now comes first, whatever the member order; otherwise rows stay
    by request start, never by kind."""
    from trailbeam.server import web

    p, c, d = Consumer("producer"), Consumer("consumer-a"), Consumer("consumer-b")
    ms = 1_000_000
    post(server, [p.send(T0 + 30 * ms, "tie-1", start=T0),
                  c.receive(T0 + 20 * ms, "tie-1", "rh-t1", start=T0 - 900 * ms),  # long poll, overlaps the send
                  c.change_visibility(T0 + 28 * ms, "rh-t1", 0, start=T0 + 25 * ms),
                  d.receive(T0 + 40 * ms, "tie-1", "rh-t2", count=2, start=T0 - 500 * ms),
                  d.delete(T0 + 60 * ms, "rh-t2", start=T0 + 50 * ms)])
    tl = json.loads(get(server, "/api/messages/tie-1")[2])
    evs = tl["events"]
    expiry = next(e for e in evs if e["type"] == "visibility_timeout_expired")
    rcv1 = next(e for e in evs if e["type"] == "received" and e["delivery"] == 1)
    send = next(e for e in evs if e["type"] == "sent")
    assert web._span_of(expiry)[0] == web._span_of(rcv1)[0]  # the tie
    by_id = {e["id"]: e for e in evs if e.get("id")}
    for members in ([expiry, rcv1, send], [rcv1, expiry, send], [send, expiry, rcv1]):
        assert events(web._waterfall(members, by_id)) == [
            ("received", False), ("visibility_timeout_expired", True), ("sent", False)]
    # Without a tie, a window that starts first is listed first: rows are by start, not by kind.
    early = dict(expiry, window=dict(expiry["window"], **{"from": "2025-11-14T00:26:39Z"}))
    assert events(web._waterfall([rcv1, early], by_id))[0] == ("visibility_timeout_expired", True)


def test_the_tie_holds_for_nanosecond_starts_as_python_spans_have(server):
    """QV-39 S2: Python's spans start in nanoseconds (time.time_ns()), and the expiry's window ``from`` is an ISO
    time to the microsecond, so the receive started a few hundred ns after its window's ``from`` and the
    exact-tie rule never fired: the expiry was listed above its receive in 87 of 87 Python windows. The times
    here are shaped like real ones (QV-39: from ...45.115712Z, receive start ...45.115712133 ns)."""
    from trailbeam.server import web

    p, c, d = Consumer("producer"), Consumer("consumer-a"), Consumer("consumer-b")
    ms = 1_000_000
    t = T0 + 115_712_133  # a receive start with sub-microsecond digits, as Python's time.time_ns() gives
    post(server, [p.send(t + 190 * ms + 873, "tie-ns", start=t + 189 * ms + 4_211),
                  c.receive(t + 191 * ms + 371, "tie-ns", "rh-n1", start=t),  # a long poll open before the send
                  c.change_visibility(t + 194 * ms + 59, "rh-n1", 0, start=t + 191 * ms + 902),
                  d.receive(t + 196 * ms + 618, "tie-ns", "rh-n2", count=2, start=t + 100 * ms + 77),
                  d.delete(t + 199 * ms + 286, "rh-n2", start=t + 197 * ms + 15)])
    tl = json.loads(get(server, "/api/messages/tie-ns")[2])
    evs = tl["events"]
    expiry = next(e for e in evs if e["type"] == "visibility_timeout_expired")
    rcv1 = next(e for e in evs if e["type"] == "received" and e["delivery"] == 1)
    send = next(e for e in evs if e["type"] == "sent")
    w_from, r_start = web._span_of(expiry)[0], web._span_of(rcv1)[0]
    assert r_start - w_from == 133 and w_from // 1000 == r_start // 1000  # not an exact tie: the same µs
    by_id = {e["id"]: e for e in evs if e.get("id")}
    for members in ([expiry, rcv1, send], [rcv1, expiry, send], [send, expiry, rcv1]):
        assert events(web._waterfall(members, by_id))[:2] == [("received", False), ("visibility_timeout_expired", True)]
    # A step that starts a microsecond after the window's from is listed after it: rows stay by start.
    later = dict(rcv1, start_unix_nano=str(w_from + 1000))
    assert events(web._waterfall([later, expiry], by_id))[0] == ("visibility_timeout_expired", True)


def test_an_empty_store_says_nothing_is_connected_yet(server):
    """QV-147: on a store with no span, an empty list means no service has sent anything, not that all is well."""
    for path in ("/", "/stuck", "/duplicates"):
        _, _, html = get(server, path)
        assert "No messages yet. No service has sent spans here" in html, path
        assert "No stuck messages." not in html and "No duplicates." not in html, path
    _, _, html = get(server, "/tenants")
    assert "No tenants yet" in html and "15.2" not in html and "revision 3.2" not in html
    post(server, [Consumer("producer").send(T0, "first-one")])
    _, _, html = get(server, "/stuck?queue=payments")
    assert "No stuck messages." in html and "No messages yet." not in html


def test_stuck_intro_says_the_threshold_in_words_and_how_to_change_it(site):
    """QV-216: the intro leads with what stuck means and the settings; the clock note is in the footer."""
    _, _, html = get(site, "/stuck?older_than_s=300")
    assert "Messages first seen more than 5 min ago that no consumer has deleted yet, oldest first." in html
    assert "<code>--stuck-after</code> (<code>TRAILBEAM_STUCK_AFTER_S</code>)" in html
    assert 'name="older_than_s" value="300"' in html  # the field stays in seconds
    foot = ("Times are in UTC. Age is this server's clock now minus the send's time on the sender's clock, so "
            "skew between the two clocks shows up in it.")
    assert html.index("</table>") < html.index(foot)
    assert "producer host" not in html
    _, _, html = get(site, "/stuck?older_than_s=90")
    assert "more than 90 s ago" in html


def test_threshold_in_words():
    from trailbeam.server.web import _threshold
    assert [_threshold(s) for s in (300, 60, 90, 3600, 7200, 86_400, 172_800, 0)] == [
        "5 min", "1 min", "90 s", "1 h", "2 h", "1 day", "2 days", "0 s"]


def test_purged_and_possibly_purged_messages(server):
    """Contract section 18: a message a recorded PurgeQueue removed is "purged (observed: PurgeQueue at ...)" and off
    Stuck; one whose last step was in the purge's window stays on Stuck, marked possibly purged."""
    p = Consumer("producer", service="orders-api")
    post(server, [p.send(T0, "gone"), p.send(T0 + 9 * SEC + 1, "during"),
                  p.purge(T0 + 10 * SEC, start=T0 + 9 * SEC)])
    _, _, html = get(server, "/stuck?older_than_s=60")
    assert f'href="{page("gone")}"' not in html and f'href="{page("during")}"' in html
    assert 'data-purge="possibly_purged"' in html and "possibly purged (PurgeQueue at" in html
    assert "MessageRetentionPeriod" in html and "cannot see it" in html
    _, _, html = get(server, "/")
    assert '<span class="state state-purged">purged</span>' in html and "(observed: PurgeQueue at" in html
    _, _, html = get(server, page("gone"))
    assert ("queue_purged", False) in events(html)
    assert "Queue purged (PurgeQueue)" in html and "deletes every message in the queue" in html
    _, _, html = get(server, page("during"))
    assert ("queue_purged", False) in events(html) and "can&#x27;t tell whether this one was" in html


def test_sns_fan_out_shows_the_publish_and_the_arrival(server):
    """Contract section 21.5: a publish is a recorded step on each message it reached, with its topic, how it joined,
    the SNS delivery time when joined through the envelope, and the other messages it reached as links. A receive
    whose envelope names a topic with no recorded publish gets an "Arrived via SNS topic" step, with no time."""
    p, c = Consumer("producer", service="orders-api"), Consumer("orders-worker")
    t0_ms = T0 // 10**6
    post(server, [p.publish(T0 + SEC, "sns-1", publish_id="pub-1", start=T0),
                  c.sns_receive(T0 + 3 * SEC, "via-env", "rh-1", sns_id="sns-1", sns_ts_ms=t0_ms + 1000,
                                sent_ms=t0_ms + 1040, start=T0 + 2 * SEC),
                  c.sns_receive(T0 + 3 * SEC, "via-raw", "rh-2", publish_id="pub-1", queue="billing",
                                sent_ms=t0_ms + 1050, start=T0 + 2 * SEC),
                  c.sns_receive(T0 + 9 * SEC, "no-pub", "rh-3", sns_id="sns-2", topic="audit-topic",
                                sns_ts_ms=t0_ms + 7000, sent_ms=t0_ms + 7025, start=T0 + 8 * SEC)])
    _, _, html = get(server, page("via-env"))
    assert events(html)[:2] == [("published", False), ("received", False)]
    assert "Published to SNS topic <code>orders-topic</code>" in html
    assert "SNS delivery ms 40" in html and "joined by the SNS envelope&#x27;s MessageId (raw delivery off)" in html
    assert f'<a class="mono" href="{escape(page("via-raw"))}">via-raw</a>' in html
    assert '<span class="mono">via-env</span> (this message)' in html
    assert "orders-api" in html
    _, _, html = get(server, page("via-raw"))
    assert "joined by the trailbeam.publish_id attribute (raw delivery on)" in html and "SNS delivery ms" not in html
    _, _, html = get(server, page("no-pub"))
    assert events(html)[:2] == [("arrived_via_sns", False), ("received", False)]
    assert "Arrived via SNS topic <code>audit-topic</code>" in html and "SNS delivery ms 25" in html
    assert "publisher runs no Trailbeam package" in html and 'class="reported-by"' not in html
    _, _, html = get(server, "/")
    assert "sns-1" not in html and "orders-topic" not in html  # a publish is the topic's step, not a message


def test_a_resent_sns_envelope_does_not_claim_sns_delivered_it(server):
    """QV-514, contract section 21.5: a message with a recorded send whose body looks like an SNS envelope gets no
    "Arrived via SNS" step, as EventBridge and S3 bodies get none (23.3): the send put it on the queue."""
    p, c = Consumer("producer", service="orders-api"), Consumer("orders-worker")
    post(server, [p.send(T0, "resent"),
                  c.sns_receive(T0 + 2 * SEC, "resent", "rh-1", sns_id="abc", topic="t", start=T0 + SEC)])
    _, _, html = get(server, page("resent"))
    assert [t for t, _ in events(html)] == ["sent", "received"]
    assert "Arrived via SNS" not in html and "SNS delivered" not in html


def test_a_deduplicated_fifo_send_is_shown_as_such_with_its_reason(server):
    """QV-246: SQS returns the first send's MessageId to a FIFO send it deduplicates and enqueues nothing."""
    from trailbeam.server.timeline import DEDUPLICATED_REASON
    p = Consumer("producer", service="orders-api", queue="py-fifo.fifo")
    post(server, [p.send(T0, "fifo-1"), p.send(T0 + SEC, "fifo-1")])
    _, _, html = get(server, page("fifo-1"))
    assert [t for t, _ in events(html)] == ["sent", "sent"]
    assert html.count('<span class="etype">Sent</span>') == 1
    assert '<span class="etype">Sent again, deduplicated by SQS</span>' in html
    assert f'<div class="reason">{DEDUPLICATED_REASON}</div>' in html


def test_eventbridge_and_s3_origins_are_inferred_first_steps(db, server):
    """Contract section 23.3: a receive whose body is an EventBridge event or an S3 notification gets an inferred
    first step, with no time, naming the event's source and detail-type, or the S3 event, bucket and key."""
    c = Consumer("orders-worker")
    t0_ms = T0 // 10**6
    db.store_bodies = frozenset({"orders"})  # the object key is stored only on an opted-in queue (QV-564)
    post(server, [
        c.receive_with(T0 + 3 * SEC, "via-eb", "rh-1", {
            "trailbeam.eventbridge.event_id": "6a7e8feb-b491-4cf7-a9f1-bf3703467718",
            "trailbeam.eventbridge.source": "com.example.orders", "trailbeam.eventbridge.detail_type": "OrderPlaced",
            "trailbeam.eventbridge.account": "111122223333", "trailbeam.eventbridge.time_ms": t0_ms},
            sent_ms=t0_ms + 300, start=T0 + 2 * SEC),
        c.receive_with(T0 + 3 * SEC, "via-s3", "rh-2", {
            "trailbeam.s3.event_name": "ObjectCreated:Put", "trailbeam.s3.bucket": "uploads",
            "trailbeam.s3.object_key": "red+flower.jpg", "trailbeam.s3.sequencer": "0055AED6DCD90281E5"},
            start=T0 + 2 * SEC),
        c.receive_with(T0 + 3 * SEC, "s3-test", "rh-3", {
            "trailbeam.s3.event_name": "s3:TestEvent", "trailbeam.s3.bucket": "uploads"}, start=T0 + 2 * SEC)])
    _, _, html = get(server, page("via-eb"))
    assert events(html)[:2] == [("arrived_via_eventbridge", True), ("received", False)]
    assert "Arrived via an EventBridge rule" in html and "source <code>com.example.orders</code>" in html
    assert "detail-type <code>OrderPlaced</code>" in html and "delivery ms 300" in html
    assert "The event does not name the rule" in html
    _, _, html = get(server, page("via-s3"))
    assert events(html)[:2] == [("s3_event", True), ("received", False)]
    assert "S3 <code>ObjectCreated:Put</code>" in html and "key <code>red+flower.jpg</code>" in html
    _, _, html = get(server, page("s3-test"))
    assert "S3 test message" in html and "notification configuration on bucket uploads" in html


def test_sqs_steps_use_sqs_words_and_rabbitmq_steps_keep_rabbitmq_words():
    """QV-588: an SQS step is a receive ("Receives not observed", "the delete of receive 2"); RabbitMQ's own term
    is a delivery, so its steps keep it, inferred gaps included (they carry no AMQP operation)."""
    gap = {"type": "deliveries_not_observed", "inferred": True, "time": None, "count": 2,
           "window": {"from": None, "to": None}, "reason": "r"}
    assert web._label(gap) == "Receives not observed"
    assert "Receives not observed" in web._event(gap, self_id="orders-1")
    assert "Deliveries not observed" in web._event(gap, self_id="~rabbitmq~orders-1")
    assert web._ref({"type": "deleted", "delivery": 2}) == "the delete of receive 2"
    assert web._ref({"type": "lambda_outcome", "delivery": 1}) == "the Lambda outcome of receive 1"
    assert web._ref({"type": "acked", "delivery": 2}) == "the ack of delivery 2"
