"""The web screen over HTTP: lookup, timeline with inferred steps marked, duplicates, stuck messages."""
import re
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
        dlq.receive(T0 + 100 * SEC, POISON, "p-3", count=3),
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
    assert ("moved_to_dead_letter_queue", True) in types and ("visibility_changed", False) in types
    assert types.index(("moved_to_dead_letter_queue", True)) == len(types) - 2  # just before the DLQ receive
    assert "(dead-letter queue)" in html and "orders-dlq" in html


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


def test_overlapping_steps_are_shown_side_by_side_as_concurrent(server):
    p, c = Consumer("producer"), Consumer("consumer-a")
    ms = 1_000_000
    post(server, [p.send(T0 + 30 * ms, "conc-1", start=T0),
                  c.receive(T0 + 20 * ms, "conc-1", "rh-c", start=T0 - 900 * ms),   # long poll, overlaps
                  c.delete(T0 + 60 * ms, "rh-c", start=T0 + 50 * ms)])
    status, _, html = get(server, page("conc-1"))
    assert status == 200
    groups = re.findall(r'<li class="group concurrent" data-group="(\d+)">(.*?)</ol></li>', html)
    assert len(groups) == 1 and groups[0][0] == "0"
    inside = groups[0][1]
    assert "concurrent, order unknown" in inside
    assert events(inside) == [("received", False), ("sent", False)]
    assert events(html) == [("received", False), ("sent", False), ("deleted", False)]
    # The delete is after the group and outside it.
    assert html.index('data-type="deleted"') > html.index("</ol></li>")
    # Intervals are shown as start to end.
    assert "2025-11-14 00:26:40.000000Z</time> to <time" in html and "(30.0 ms)" in html
    assert "order is unknown" in html  # the explanation above the timeline


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
    groups = re.findall(r'<li class="group concurrent" data-group="(\d+)">(.*?)</ol></li>', html)
    assert len(groups) == 1
    head = re.search(r'<div class="group-head">(.*?)</div>', groups[0][1]).group(1)
    text = re.sub(r"<[^>]+>", "", head)
    assert text == ("concurrentconcurrent, order unknown: the store can't put these 3 steps in one order; "
                    "some of them overlapped in time.")
    assert "overlapped in time, so" not in html and "cannot tell which happened first" not in html


def test_a_box_counts_the_inferred_step_it_holds(server):
    # A rejected delete inside the expiry's window: one box with the delete and the expiry.
    c = Consumer("consumer-a")
    post(server, [c.send(T0 + 19 * SEC, "slow-1", start=T0 + 19 * SEC - 2_000_000),
                  c.receive(T0 + 20 * SEC, "slow-1", "rh-s1", start=T0 + 20 * SEC - 1_000_000),
                  c.delete(T0 + 32 * SEC, "rh-s1", ok=False, start=T0 + 32 * SEC - 1_000_000),
                  c.receive(T0 + 35 * SEC, "slow-1", "rh-s2", count=2, start=T0 + 35 * SEC - 1_000_000)])
    _, _, html = get(server, page("slow-1"))
    groups = re.findall(r'<li class="group concurrent" data-group="(\d+)">(.*?)</ol></li>', html)
    assert len(groups) == 1
    assert "these 2 steps in one order" in groups[0][1]
    assert events(groups[0][1]) == [("deleted", False), ("visibility_timeout_expired", True)]
