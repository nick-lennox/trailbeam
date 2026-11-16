"""The web screen: server-rendered HTML pages built from the JSON API's responses (https://docs.trailbeam.app/reference/api).

Pages:

- ``/``: look up a message by ID, and the most recent messages, or with ``body=`` those whose kept body
  matches body.* terms (bodies.py).
- ``/message?id=...``: one message's timeline. Each observed step shows its interval (request sent to
  response received) and, in plain words, the observed fact that puts it after the step before it
  (the clocks, the call order in one process, SQS's receive count or SQS's timestamps). Steps that no
  fact orders are drawn in one box labelled "concurrent, order unknown", as a waterfall: each step is a
  bar on a time scale shared by the box, from request sent to response received, and the rows are
  listed by when each request was sent (an observed time, never the kind of step or what caused what),
  so the overlap, not the position, says what is known. Inferred steps
  are drawn differently and labelled "inferred". A move to a dead-letter queue says "reported by SQS"
  and which attribute reported it; it is never inferred.
- ``/duplicates``: duplicate deliveries.
- ``/stuck``: stuck messages.
- ``/tenants``: per tenant, sends, share, p95 time in queue and dead-lettered messages (contract/tenants.md).
  ``/?tenant=...`` lists one tenant's messages.

Each page calls the same ``App.api`` the JSON endpoints use, so the screen shows exactly what the API
returns. No JavaScript, no external assets: everything is in one response.
"""
from __future__ import annotations

from datetime import datetime
from html import escape
from typing import Callable, Iterable, Optional
from urllib.parse import quote, urlencode

from . import __version__
from .timeline import DLQ_SOURCE, iso

Response = tuple[int, dict, bytes]

STATE_LABELS = {
    "deleted": "deleted",
    "deleted_inferred": "deleted by Lambda (inferred)",
    "processing": "processing",
    "not_deleted": "not deleted",
}

EVENT_LABELS = {
    "sent": "Sent",
    "received": "Received",
    "deleted": "Delete",
    "visibility_changed": "Visibility changed",
    "lambda_outcome": "Lambda outcome",
    "visibility_timeout_expired": "Visibility timeout expired",
    "deliveries_not_observed": "Deliveries not observed",
    "moved_to_dead_letter_queue": "Moved to dead-letter queue",
    "deleted_by_lambda": "Deleted by Lambda",
    "lambda_invocation_ended_without_outcome": "Lambda invocation ended without an outcome",
}

DLQ_MOVE_LABELS = {
    "observed": "moved, reported by SQS (DeadLetterQueueSourceArn on the DLQ receive)",
    "not_seen_on_dlq_yet": "receive count exceeded maxReceiveCount; not seen on the DLQ yet",
}

DUPLICATE_REASONS = {
    "received_more_than_once": "received more than once",
    "succeeded_more_than_once": "succeeded more than once",
}

# A raw string: the CSS has its own escapes (\2191 is the up arrow), which Python must not read as octal.
CSS = r"""
body{font-family:system-ui,-apple-system,"Segoe UI",sans-serif;margin:0;color:#1d2330;background:#f7f8fa}
header{background:#1d2330;color:#fff;padding:.6rem 1.2rem;display:flex;gap:1.5rem;align-items:center}
header a{color:#fff;text-decoration:none}header a.brand{font-weight:700}
header form{margin-left:auto;display:flex;gap:.4rem}
main{padding:1.2rem;max-width:72rem}
input[type=text],input[type=number],select{padding:.3rem;border:1px solid #b8bfcc;border-radius:3px}
input[type=text]{min-width:22rem}input[type=number]{width:6rem}button{padding:.3rem .8rem}
table{border-collapse:collapse;width:100%;background:#fff}
th,td{text-align:left;padding:.35rem .6rem;border-bottom:1px solid #e3e6ec;vertical-align:top}
th{background:#eef0f4;font-weight:600}
code,.mono{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:.9em}
.state{padding:.05rem .4rem;border-radius:3px;font-size:.85em;white-space:nowrap}
.state-deleted{background:#d8f0dd}.state-deleted_inferred{background:#e4efd8}
.state-processing{background:#dbe7f7}.state-not_deleted{background:#f7e2cf}
.summary dt{font-weight:600;float:left;clear:left;width:13rem}.summary dd{margin:0 0 .2rem 13rem}
ol.timeline{list-style:none;padding:0;margin:1rem 0}
ol.timeline li{background:#fff;border:1px solid #d5dae3;border-left:4px solid #3b6fd6;border-radius:3px;
  margin:.4rem 0;padding:.5rem .8rem}
ol.timeline li.inferred{background:repeating-linear-gradient(135deg,#fffaf0,#fffaf0 8px,#fdf3e1 8px,#fdf3e1 16px);
  border:1px dashed #c98a1b;border-left:4px dashed #c98a1b}
ol.timeline li.failed{border-left-color:#c43d3d}
ol.timeline li.reported{border-left:4px double #3b6fd6}.reported-by{font-size:.9em;margin-top:.2rem}
.badge{display:inline-block;font-size:.75em;font-weight:700;text-transform:uppercase;letter-spacing:.04em;
  padding:.05rem .4rem;border-radius:3px;background:#c98a1b;color:#fff;margin-right:.4rem}
.when{color:#5b6475;font-family:ui-monospace,Menlo,Consolas,monospace;font-size:.85em;margin-right:.6rem}
.etype{font-weight:600}.detail{color:#3c4352;font-size:.9em;margin-top:.2rem}
.reason{font-style:italic;margin-top:.2rem}
.why{font-size:.85em;color:#2f5a2f;margin-top:.2rem}.why:before{content:"\2191  ";color:#5b8a5b}
ol.timeline li.group{background:none;border:2px dashed #8a94a8;border-left:2px dashed #8a94a8;padding:.4rem .6rem}
.group-head{font-size:.85em;color:#3c4352;margin-bottom:.2rem}
.badge-concurrent{background:#5b6475}
ul.waterfall{list-style:none;padding:0;margin:0}
ul.waterfall>li{margin:.3rem 0 !important}
.group-note{font-size:.8em;color:#5b6475;margin:.1rem 0 .3rem}
.track{position:relative;height:.7rem;margin:.35rem 0 .1rem;background:#eef0f4;border-radius:2px}
.track .bar{position:absolute;top:0;bottom:0;min-width:3px;background:#3b6fd6;border-radius:2px}
.track .bar.open-start{border-top-left-radius:0;border-bottom-left-radius:0;
  background:linear-gradient(90deg,rgba(59,111,214,.15),#3b6fd6 2rem)}
.track .bar.open-end{background:linear-gradient(270deg,rgba(59,111,214,.15),#3b6fd6 2rem)}
.track .bar.window{background:repeating-linear-gradient(135deg,#e7b866,#e7b866 4px,#f6dfb4 4px,#f6dfb4 8px)}
.notice{background:#fff4d6;border:1px solid #e6c25c;padding:.5rem .8rem;margin:.4rem 0;border-radius:3px}
.error{background:#fde2e2;border:1px solid #d98c8c;padding:.6rem .9rem;border-radius:3px}
.muted{color:#6b7385}form.filters{margin:.6rem 0 1rem;display:flex;gap:.6rem;align-items:center;flex-wrap:wrap}
footer{color:#6b7385;font-size:.8em;padding:1rem 1.2rem}
"""


def handle(api: Callable[[str, dict], object], api_error: type, path: str, query: dict) -> Optional[Response]:
    """Render the page for ``path``; None if ``path`` is not a page. ``api`` is ``App.api``."""
    pages = {"/": _index, "/message": _message, "/duplicates": _duplicates, "/stuck": _stuck, "/tenants": _tenants}
    page = pages.get(path)
    if page is None:
        return None
    try:
        return page(api, query)
    except api_error as e:  # bad parameter or unknown message
        title = "Message not found" if e.status == 404 else "Bad request"
        return _page(e.status, title, f'<h1>{escape(title)}</h1><p class="error">{escape(e.message)}</p>'
                     + _lookup_form(query.get("id", "")))


# ---- pages ------------------------------------------------------------------------------------


def _index(api, q: dict) -> Response:
    data = api("messages", _pick(q, "limit", "queue", "tenant", "body"))
    searching = "body_search" in data
    note = ""
    if searching:
        note = ('<p class="muted body-note">Searches only the bodies this server keeps: those of queues in '
                "<code>TRAILBEAM_STORE_BODIES</code>, as the package redacted them. Trailbeam has no logins, so "
                "anyone who can reach this server can read them.</p>")
        if not data["body_search"]["stores_bodies"]:
            note += ('<p class="notice">This server keeps no bodies: <code>TRAILBEAM_STORE_BODIES</code> is not '
                     "set, so a body search finds nothing.</p>")
    body = ("<h1>Look up a message</h1>" + _lookup_form("")
            + ("<h2>Messages whose body matches</h2>" if searching else "<h2>Recent messages</h2>")
            + _filters("/", q, [("body", "Body", "text"), ("queue", "Queue", "text"),
                                 ("tenant", "Tenant", "text"), ("limit", "Limit", "number")])
            + note
            + _summary_table(data["messages"], empty="No message body matches." if searching else "No messages stored yet."))
    return _page(200, "Trailbeam", body)


def _message(api, q: dict) -> Response:
    mid = q.get("id", "").strip()
    if not mid:
        return 303, {"Location": "/"}, b""
    extra = _pick(q, "lambda_timeout_s")
    t = api("messages/" + _quote_segment(mid), extra)
    parts = [f'<h1>Message <code>{escape(t["message_id"])}</code></h1>']
    for n in t.get("notices", []):
        parts.append(f'<div class="notice" data-code="{escape(n["code"])}"><strong>{escape(n["code"])}</strong>: '
                     f'{escape(n["message"])}</div>')
    parts.append('<dl class="summary">')
    parts.append(_dd("State", _state(t["state"])))
    parts.append(_dd("Source queue", escape(t.get("source_queue") or "?")))
    parts.append(_dd("Current queue", escape(t.get("current_queue") or "?")
                     + (" <strong>(dead-letter queue)</strong>" if t.get("in_dead_letter_queue") else "")))
    if t.get("dead_letter_queue_move"):
        parts.append(_dd("Dead-letter queue", escape(DLQ_MOVE_LABELS.get(t["dead_letter_queue_move"],
                                                                         t["dead_letter_queue_move"]))))
    parts.append(_dd("Send observed", "yes" if t.get("send_observed") else "no"))
    parts.append(_dd("Deliveries", f'{t["deliveries"]} ({t["successful_deliveries"]} successful)'))
    parts.append(_dd("First seen", _first_seen(t)))
    parts.append(_dd("Last seen", _time(t.get("last_seen"))))
    parts.append("</dl>")
    parts.append('<h2>Timeline</h2><p class="muted">Each step shows when its SQS request was sent and when the '
                 "response came back; SQS acted somewhere in between. A step is shown above another only if an "
                 "observed fact proves it came first, and the step says which: it ended before the other "
                 "started (the hosts' clocks), the same process issued it after the other returned, SQS's "
                 "receive count went up, or SQS's own timestamps show the message was sent before its first "
                 "receive. Nothing is ordered by what usually causes what. Steps in a box marked "
                 '<span class="badge badge-concurrent">concurrent</span> can\'t be put in one order: no '
                 "observed fact orders some of them (they overlapped in time, or a start was not observed), so "
                 "their order is unknown. Each is drawn as a bar on one time scale for the box; the rows are "
                 "listed by when each request was sent, which is not the order SQS acted in. Where a pair "
                 "inside a box is ordered, the later step says so. Steps marked "
                 '<span class="badge">inferred</span> were deduced by Trailbeam; no span recorded them, so they '
                 "have a time window instead of a time. A move to a dead-letter queue is shown only when SQS "
                 "reports it: the receive from the dead-letter queue carries DeadLetterQueueSourceArn naming the "
                 "queue it came from. SQS gives the move no time, so it has a window too, between the last "
                 "receive on that queue and the receive that reports it; it is never inferred.</p>")
    parts.append('<ol class="timeline">')
    parts.extend(_timeline_items(t))
    parts.append("</ol>")
    body_url = "/api/messages/" + _quote_segment(t["message_id"]) + "/body"
    parts.append(f'<p class="muted">The message body, or why there is none: <a href="{escape(body_url)}">'
                 f"{escape(body_url)}</a> (the Body tab, with scripts on).</p>")
    return _page(200, f'Message {t["message_id"]}', "".join(parts))


def _duplicates(api, q: dict) -> Response:
    params = _pick(q, "limit", "queue", "reason")
    data = api("duplicates", params)
    reason_opts = [("", "any reason")] + list(DUPLICATE_REASONS.items())
    body = ("<h1>Duplicate deliveries</h1>"
            '<p class="muted">Messages received more than once, or processed successfully more than once.</p>'
            + _filters("/duplicates", q, [("reason", "Reason", reason_opts), ("queue", "Queue", "text"),
                                          ("limit", "Limit", "number")])
            + _summary_table(data["messages"], extra=("Why", lambda m: ", ".join(
                escape(DUPLICATE_REASONS.get(r, r)) for r in m.get("reasons", []))),
                empty="No duplicate deliveries."))
    return _page(200, "Duplicate deliveries", body)


def _stuck(api, q: dict) -> Response:
    data = api("stuck", _pick(q, "older_than_s", "limit", "queue"))
    body = ("<h1>Stuck messages</h1>"
            f'<p class="muted">First seen more than {int(data["older_than_s"])} s ago, with no final delete. '
            "Oldest first. First seen is the send, on the producer host's clock. Only when the send was not "
            "observed is it SQS's SentTimestamp, on SQS's clock; that message's age then compares SQS's clock "
            "with this server's, and says so.</p>"
            + _filters("/stuck", {**q, "older_than_s": str(data["older_than_s"])},
                       [("older_than_s", "Older than (s)", "number"), ("queue", "Queue", "text"),
                        ("limit", "Limit", "number")])
            + _summary_table(data["messages"], extra=("Age", _stuck_age), empty="No stuck messages."))
    return _page(200, "Stuck messages", body)


TENANT_RANGES = [("3600", "Last hour"), ("86400", "Last 24 hours"), ("604800", "Last 7 days")]
TENANT_SETUP = ("A tenant comes from a Trailbeam package at contract revision 3.2 with a tenant field set: "
                "<code>TRAILBEAM_TENANT_ATTRIBUTE</code> (a message attribute) or "
                "<code>TRAILBEAM_TENANT_BODY_PATH</code> (a dotted path in a JSON body), or the package's "
                "<code>tenant</code> option. Set it in each service that sends or receives. See the span "
                "contract, section 15.1.")


def _tenants(api, q: dict) -> Response:
    since = q.get("since_s") if q.get("since_s") in dict(TENANT_RANGES) else "86400"
    d = api("tenants", {"since_s": since})
    label = dict(TENANT_RANGES)[since]
    head = ("<h1>Tenants</h1><p class=\"muted\">Which of your customers are sending the traffic, and who is "
            "waiting longest.</p>" + _filters("/tenants", {"since_s": since}, [("since_s", "Range", TENANT_RANGES)]))
    if not d["rows"]:
        seen = f'{d["total_sent"]:,} sends were seen, none with a tenant. ' if d["total_sent"] else ""
        return _page(200, "Tenants", head + f'<p class="notice"><strong>No tenants in this range.</strong> {seen}'
                     + TENANT_SETUP + "</p>")
    rows = []
    for r in d["rows"]:
        share = "&lt;1" if 0 < r["share"] < 0.01 else str(round(r["share"] * 100))
        p95 = ('<span class="muted">-</span>' if r["p95_in_queue_ms"] is None
               else escape(_took(r["p95_in_queue_ms"] * 1_000_000)))
        rows.append(f'<tr><td><a href="/?{escape(urlencode({"tenant": r["tenant"]}))}">{escape(r["tenant"])}</a></td>'
                    f'<td class="mono">{r["sent"]:,}</td><td class="mono">{share}%</td>'
                    f'<td class="mono" title="over {r["first_deliveries"]} first deliveries">{p95}</td>'
                    f'<td class="mono">{r["dead_lettered"]:,}</td></tr>')
    untagged = f', {d["untagged_sent"]:,} of them without a tenant' if d["untagged_sent"] else ""
    note = (f'<p class="muted">{escape(label)}, from {"the most recent 2,000 messages" if d["capped"] else "every message"} '
            f'in the range. Share is of all {d["total_sent"]:,} observed sends{untagged}. p95 in queue is from the '
            "send to the first receive, over first deliveries only. Dead-lettered counts messages SQS was seen "
            "moving to a dead-letter queue. " + TENANT_SETUP + "</p>")
    table = ("<table><tr><th>Tenant</th><th>Messages sent</th><th>Share</th><th>p95 in queue</th>"
             "<th>Dead-lettered</th></tr>" + "".join(rows) + "</table>")
    return _page(200, "Tenants", head + table + note)


# ---- pieces -----------------------------------------------------------------------------------


def _timeline_items(t: dict) -> list[str]:
    """One <li> per step, except that each concurrent group is one <li> holding its steps as a waterfall
    (:func:`_waterfall`)."""
    concurrent = {g["group"]: g for g in t.get("groups", []) if g.get("concurrent")}
    out: list[str] = []
    evs = t["events"]
    by_id = {e["id"]: e for e in evs if e.get("id")}
    i = 0
    while i < len(evs):
        g = evs[i].get("group")
        if g not in concurrent:
            out.append(_event(evs[i], by_id))
            i += 1
            continue
        j = i
        while j < len(evs) and evs[j].get("group") == g:
            j += 1
        info = concurrent[g]
        n = info.get("observed_events", 0) + info.get("inferred_events", 0) or j - i
        out.append(f'<li class="group concurrent" data-group="{int(g)}">'
                   f'<div class="group-head"><span class="badge badge-concurrent">concurrent</span>'
                   f'<strong>{escape(info.get("label") or "concurrent, order unknown")}</strong>: Trailbeam can\'t put '
                   f"these {int(n)} steps in one order; no observed fact orders some of them.</div>"
                   + _waterfall(evs[i:j], by_id) + "</li>")
        i = j
    return out


def _ns_of_iso(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    return (int(dt.replace(microsecond=0).timestamp()) * 1_000_000 + dt.microsecond) * 1000


def _span_of(e: dict) -> tuple[Optional[int], Optional[int], bool]:
    """(from, to, is_window) in ns on the hosts' clocks: an observed step's interval, or the window of an
    inferred step or of a move to a dead-letter queue (which SQS gives no time). Either end may be unknown."""
    if e.get("inferred") or e.get("source") == DLQ_SOURCE:
        w = e.get("window") or {}
        return _ns_of_iso(w.get("from")), _ns_of_iso(w.get("to")), True
    start = e.get("start_unix_nano")
    end = e.get("end_unix_nano") or e.get("time_unix_nano")
    return (None if start is None else int(start)), (None if end is None else int(end)), False


def _waterfall(members: list[dict], by_id: dict) -> str:
    """A concurrent box as a waterfall, so that its steps don't read as a sequence (QV-32 section 7.2).

    Each step gets a bar on a time scale shared by the box: from its request sent to its response received
    (an inferred step or a move: its window, hatched; an unknown end fades out). The rows are listed by when
    each request was sent, an observed time; a step whose start is unknown is drawn from the left edge and
    listed first, since it may have started at any time before. When a window (an inferred step or a move)
    starts at the same time as an observed step, the observed step is listed first (QV-35 section 3): a
    window starts at its anchor's request, so it ties with that step, and listing it above the step it is
    placed after read as the wrong order. The tie is taken at microseconds (QV-40): a window's ``from`` is
    an ISO time to the microsecond, while an observed start is in nanoseconds (Python's spans), so the two
    never tie exactly. Otherwise rows are never ordered by the kind of step or by what caused what, and the
    note under the header says the rows are not the order SQS acted in."""
    spans = [_span_of(e) for e in members]
    known = [x for a, b, _ in spans for x in (a, b) if x is not None]
    lo, hi = (min(known), max(known)) if known else (0, 0)
    width = hi - lo

    def pct(x: int) -> float:
        return 0.0 if width <= 0 else (x - lo) * 100.0 / width

    rows = []
    for k, (e, (a, b, window)) in enumerate(zip(members, spans)):
        left = pct(a) if a is not None else 0.0
        right = pct(b) if b is not None else 100.0
        cls = ["bar"] + (["window"] if window else []) + (["open-start"] if a is None else []) \
            + (["open-end"] if b is None else [])
        bar = (f'<div class="track"><span class="{" ".join(cls)}" '
               f'style="left:{left:.2f}%;width:{max(right - left, 0.0):.2f}%"></span></div>')
        rows.append(((lo // 1000 - 1) if a is None else a // 1000, window, k, _event(e, by_id, bar)))
    rows.sort(key=lambda r: r[:3])  # by start (µs); on a tie, observed before a window; then Trailbeam's order
    scale = "" if width <= 0 else (f" The scale runs from {_time(iso(lo))} to {_time(iso(hi))}"
                                   f" ({_took(width)}).")
    note = ('<p class="group-note">Each bar is one step on the same time scale: from its request sent to its '
            "response received (hatched: a window, for a step with no time of its own). Rows are listed by when "
            "each request was sent, which is not the order SQS acted in: where bars overlap, SQS may have acted "
            f"on either first.{scale}</p>")
    return note + '<ul class="waterfall">' + "".join(r[3] for r in rows) + "</ul>"


def _took(ns: int) -> str:
    ms = ns / 1e6
    return f"{ms:.1f} ms" if ms < 10_000 else f"{ms / 1000:.1f} s"


def _ref(e: Optional[dict]) -> str:
    """A step as a reader names it: "the send", "receive 2", "the delete of delivery 1"."""
    if not e:
        return "an earlier step"
    t, d = e["type"], e.get("delivery")
    of = f" of delivery {d}" if d is not None else ""
    names = {"sent": "the send", "received": f"receive {d}" if d is not None else "a receive",
             "deleted": "the delete" + of, "visibility_changed": "the visibility change" + of,
             "lambda_outcome": "the Lambda outcome" + of}
    return names.get(t, EVENT_LABELS.get(t, t).lower())


def _why(e: dict, by_id: dict) -> list[str]:
    """One line per step this one is shown after, saying which observed fact proves it."""
    out = []
    entries = list(e.get("after") or [])
    if e.get("source") == DLQ_SOURCE and e.get("before"):
        entries.append(e["before"])
    for a in entries:
        src = a.get("sources") or [a.get("source")]
        if e.get("inferred"):
            continue  # its anchors are in the reason and the window
        rel = "Before" if a is e.get("before") else "After"
        out.append(f'<div class="why" data-after="{escape(str(a.get("event")))}" '
                   f'data-sources="{escape(" ".join(map(str, src)))}">{rel} {escape(_ref(by_id.get(a.get("event"))))}: '
                   f'{escape(a.get("why") or "")}</div>')
    if e.get("inferred"):
        after = [by_id.get(a.get("event")) for a in e.get("after") or []]
        before = by_id.get((e.get("before") or {}).get("event"))
        names = [f"after {_ref(x)}" for x in after if x] + ([f"before {_ref(before)}"] if before else [])
        if names:
            out.append(f'<div class="why" data-sources="inferred_from">Placed {escape(" and ".join(names))}, the '
                       f'step{"s" if len(names) > 1 else ""} it was inferred from.</div>')
    return out


def _event(e: dict, by_id: Optional[dict] = None, bar: str = "") -> str:
    inferred = bool(e.get("inferred"))
    label = escape(EVENT_LABELS.get(e["type"], e["type"]))
    failed = e.get("result") == "failed" or (e["type"] == "lambda_outcome" and e.get("outcome") != "success")
    cls = ["event", "inferred" if inferred else "observed"] + (["failed"] if failed else [])
    head = []
    reported = e.get("source") == DLQ_SOURCE  # observed, but SQS gives no time for it (contract 9b)
    if reported:
        cls.append("reported")
    if inferred:
        head.append('<span class="badge">inferred</span>')
    if inferred or reported:
        w = e.get("window") or {}
        head.append(f'<span class="when">between {_time(w.get("from"))} and {_time(w.get("to"))}</span>')
    else:
        head.append(f'<span class="when">{_interval(e)}</span>')
    head.append(f'<span class="etype">{label}</span>')
    details = _details(e)
    out = [f'<li class="{" ".join(cls)}" data-type="{escape(e["type"])}" data-inferred="{str(inferred).lower()}"'
           + (f' data-source="{escape(str(e["source"]))}"' if e.get("source") else "")
           + (f' data-id="{escape(str(e["id"]))}"' if e.get("id") else "") + ">",
           "".join(head), bar]
    if reported:
        out.append(f'<div class="reported-by"><strong>{escape(e.get("reported_by") or "")}</strong></div>')
    if details:
        out.append(f'<div class="detail">{" · ".join(details)}</div>')
    if (inferred or reported) and e.get("reason"):
        out.append(f'<div class="reason">{escape(e["reason"])}</div>')
    out.extend(_why(e, by_id or {}))
    out.append("</li>")
    return "".join(out)


def _details(e: dict) -> list[str]:
    d: list[str] = []

    def add(name: str, key: str, fmt: Callable[[object], str] = lambda v: escape(str(v))):
        if e.get(key) is not None:
            d.append(f"{escape(name)} {fmt(e[key])}")

    if e.get("from_queue") or e.get("to_queue"):
        d.append(f'from <code>{escape(str(e.get("from_queue")))}</code> to <code>{escape(str(e.get("to_queue")))}</code>')
    elif e.get("queue"):
        d.append(f'queue <code>{escape(e["queue"])}</code>')
    add("delivery", "delivery")
    add("receive count", "receive_count")
    add("after delivery", "after_delivery")
    add("before delivery", "before_delivery")
    add("count", "count")
    add("result", "result")
    add("status", "status", lambda v: f"<strong>{escape(str(v))}</strong>")
    add("error", "error_code", lambda v: f"<code>{escape(str(v))}</code>")
    add("visibility timeout", "visibility_timeout_s", lambda v: f"{escape(str(v))} s")
    add("outcome", "outcome", lambda v: f"<strong>{escape(str(v))}</strong>")
    add("error type", "error_type", lambda v: f"<code>{escape(str(v))}</code>")
    add("error text", "error_message", lambda v: f"<code>{escape(str(v))}</code>")
    add("batch entry", "batch_entry_id", lambda v: f"<code>{escape(str(v))}</code>")
    add("invocation", "faas_invocation_id", lambda v: f"<code>{escape(str(v))}</code>")
    c = e.get("consumer")
    if c:
        d.append("by " + _consumer(c))
    s = e.get("span")
    if s:
        d.append(f'span <code title="trace {escape(str(s.get("trace_id")))}">{escape(str(s.get("span_id")))}</code>')
    return d


def _consumer(c: dict) -> str:
    who = c.get("service_name") or c.get("faas_name") or "?"
    bits = [f"<strong>{escape(str(who))}</strong>"]
    for key, name in (("client_id", "client"), ("source", "source"), ("faas_name", "function"),
                      ("faas_invocation_id", "invocation"), ("pid", "pid"), ("thread_name", "thread")):
        if c.get(key) is not None and not (key == "faas_name" and c[key] == who):
            bits.append(f"{name} {escape(str(c[key]))}")
    return " ".join(bits[:1]) + (f' ({", ".join(bits[1:])})' if len(bits) > 1 else "")


def _summary_table(messages: list[dict], *, empty: str, extra: Optional[tuple] = None) -> str:
    if not messages:
        return f'<p class="muted empty">{escape(empty)}</p>'
    cols = ["Message ID", "State", "Queue", "Deliveries", "Consumers", "First seen", "Last seen"]
    if extra:
        cols.append(extra[0])
    rows = []
    for m in messages:
        queue = escape(m.get("current_queue") or m.get("source_queue") or "?")
        if m.get("in_dead_letter_queue"):
            queue += f' <span class="muted">(DLQ, from {escape(m.get("source_queue") or "?")})</span>'
        elif m.get("dead_letter_queue_move") == "not_seen_on_dlq_yet":
            queue += ' <span class="muted">(maxReceiveCount reached; not seen on the DLQ yet)</span>'
        consumers = ", ".join(escape(str(c.get("service_name") or "?"))
                              + (f' <span class="muted">{escape(str(c["client_id"]))}</span>' if c.get("client_id") else "")
                              for c in m.get("consumers", []))
        cells = [f'<a class="mono" href="/message?{escape(urlencode({"id": m["message_id"]}))}">{escape(m["message_id"])}</a>',
                 _state(m["state"]), queue, f'{m["deliveries"]} ({m["successful_deliveries"]} ok)', consumers,
                 _first_seen(m, short=True), _time(m.get("last_seen"))]
        if extra:
            cells.append(extra[1](m))
        rows.append("<tr>" + "".join(f"<td>{c}</td>" for c in cells) + "</tr>")
    return ("<table><thead><tr>" + "".join(f"<th>{escape(c)}</th>" for c in cols) + "</tr></thead><tbody>"
            + "".join(rows) + "</tbody></table>")


def _filters(action: str, q: dict, fields: Iterable[tuple]) -> str:
    out = [f'<form class="filters" method="get" action="{escape(action)}">']
    for name, label, kind in fields:
        val = q.get(name, "")
        if isinstance(kind, list):
            opts = "".join(f'<option value="{escape(v)}"{" selected" if v == val else ""}>{escape(t)}</option>'
                           for v, t in kind)
            out.append(f'<label>{escape(label)} <select name="{name}">{opts}</select></label>')
        else:
            out.append(f'<label>{escape(label)} <input type="{kind}" name="{name}" value="{escape(val)}"'
                       f'{" size=8" if kind == "number" else ""}></label>')
    out.append("<button>Apply</button></form>")
    return "".join(out)


def _lookup_form(value: str) -> str:
    return ('<form method="get" action="/message"><input type="text" name="id" placeholder="SQS message ID" '
            f'value="{escape(value)}" required autofocus> <button>Show timeline</button></form>')


def _page(status: int, title: str, body: str) -> Response:
    html = ("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">"
            "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">"
            f"<title>{escape(title)} - Trailbeam</title><style>{CSS}</style></head><body>"
            '<header><a class="brand" href="/">Trailbeam</a><a href="/">Messages</a>'
            '<a href="/duplicates">Duplicates</a><a href="/stuck">Stuck</a><a href="/tenants">Tenants</a>'
            '<form method="get" action="/message"><input type="text" name="id" placeholder="Message ID">'
            "<button>Go</button></form></header>"
            f"<main>{body}</main><footer>Trailbeam {escape(__version__)} · "
            '<a href="/api/health">health</a></footer></body></html>')
    return status, {"Content-Type": "text/html; charset=utf-8"}, html.encode()


def _dd(name: str, value: str) -> str:
    return f"<dt>{escape(name)}</dt><dd>{value}</dd>"


def _state(s: str) -> str:
    return f'<span class="state state-{escape(s)}">{escape(STATE_LABELS.get(s, s))}</span>'


def _time(t: Optional[str]) -> str:
    if not t:
        return '<span class="muted">unknown</span>'
    return f'<time datetime="{escape(t)}">{escape(t.replace("T", " "))}</time>'


def _interval(e: dict) -> str:
    """An observed step's interval: start to end, or only "no later than" the end if the start is unknown."""
    end = e.get("end") or e.get("time")
    if not e.get("start"):
        return f"no later than {_time(end)}"
    try:
        took = f" ({_took(int(e['end_unix_nano']) - int(e['start_unix_nano']))})"
    except (KeyError, TypeError, ValueError):
        took = ""
    return f"{_time(e['start'])} to {_time(end)}{escape(took)}"


def _first_seen(m: dict, short: bool = False) -> str:
    """First seen, saying which clock it is on when it is SQS's (QV-32 section 4)."""
    t = _time(m.get("first_seen"))
    if m.get("first_seen_clock") != "sqs":
        if short:
            return t
        what = "the send" if m.get("send_observed") else "the earliest step"
        return t + f' <span class="muted">({what}, on the host\'s clock that recorded it)</span>'
    if short:
        return t + ' <span class="muted clock-sqs">SQS\'s clock</span>'
    return (t + ' <span class="muted clock-sqs">(SQS\'s SentTimestamp, on SQS\'s clock: the send was not '
            "observed; not comparable with the host times below)</span>")


def _stuck_age(m: dict) -> str:
    age = escape(_age(m.get("age_s")))
    if m.get("age_across_clocks"):
        age += ' <span class="muted clock-sqs">(this server\'s clock minus SQS\'s SentTimestamp: two clocks)</span>'
    return age


def _age(s) -> str:
    if s is None:
        return "?"
    s = float(s)
    if s < 120:
        return f"{s:.0f} s"
    if s < 7200:
        return f"{s / 60:.0f} min"
    if s < 172800:
        return f"{s / 3600:.1f} h"
    return f"{s / 86400:.1f} days"


def _pick(q: dict, *names: str) -> dict:
    """Pass only the named, non-empty parameters on to the API (empty form fields mean "default")."""
    return {k: q[k] for k in names if q.get(k, "") != ""}


def _quote_segment(s: str) -> str:
    return quote(s, safe="")
