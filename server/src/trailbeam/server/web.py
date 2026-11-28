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

  A RabbitMQ message (contract/span-contract-amqp.md; QV-307) has RabbitMQ's words instead: published to an
  exchange, routed, delivered, acked, requeued, refused, returned to the queue with its cause, and a move to a
  dead-letter queue reported by RabbitMQ's ``x-death`` header (``_amqp``).
- ``/duplicates``: duplicates (messages received or handled more than once).
- ``/stuck``: stuck messages. ``/issues`` is the same page without the shared UI's build (with it, Issues: QV-708).
- ``/tenants``: per tenant, sends, share, p95 time in queue and dead-lettered messages (contract/tenants.md).
  ``/?tenant=...`` lists one tenant's messages.

Each page calls the same ``App.api`` the JSON endpoints use, so the screen shows exactly what the API
returns. No JavaScript, no external assets: everything is in one response.
"""
from __future__ import annotations

from datetime import datetime, timezone
from html import escape
from typing import Callable, Iterable, Optional
from urllib.parse import quote, urlencode

from . import __version__, sky, systems
from .rabbitmq import DEAD_LETTER_REASONS, DLQ_SOURCE as AMQP_DLQ_SOURCE
from .timeline import DLQ_SOURCE, SNS_ENVELOPE_SOURCE, iso

REPORTED_SOURCES = (DLQ_SOURCE, AMQP_DLQ_SOURCE, SNS_ENVELOPE_SOURCE)  # observed steps with no time of their own

Response = tuple[int, dict, bytes]

STATE_LABELS = {
    "deleted": "deleted",
    "deleted_inferred": "deleted by Lambda (inferred)",
    "processing": "being handled (inferred)",
    "not_deleted": "no delete seen",
    "purged": "purged",
}

# Shared with @trailbeam/ui word for word (test_web.py checks): Messages.tsx and the Stuck page's foot (lists.tsx).
POSSIBLY_PURGED_TITLE = ("The last observed step ended while a purge of the queue was running, or within 60 s after it. "
                         "SQS may or may not have deleted this message.")
STUCK_LEAVES_SQS_NOTE = (
    "A message that a recorded PurgeQueue call removed is not listed here; one whose last step was during a purge "
    "(or within 60 s after it) is listed, marked possibly purged. A message can also leave SQS without a delete when "
    "its queue's retention period (<code>MessageRetentionPeriod</code>) ends; SQS reports nothing when that happens, "
    "so Trailbeam cannot see it and the message stays here until Trailbeam's own retention removes it.")

EVENT_LABELS = {
    "sent": "Sent",
    "received": "Received",
    "deleted": "Delete",
    "visibility_changed": "Visibility changed",
    "lambda_outcome": "Lambda outcome",
    "visibility_timeout_expired": "Visibility timeout expired",
    "deliveries_not_observed": "Receives not observed",
    "moved_to_dead_letter_queue": "Moved to dead-letter queue",
    "deleted_by_lambda": "Deleted by Lambda",
    "lambda_invocation_ended_without_outcome": "Lambda invocation ended without an outcome",
    "queue_purged": "Queue purged (PurgeQueue)",
    "published": "Published to SNS topic",
    "arrived_via_sns": "Arrived via SNS topic",
    "arrived_via_eventbridge": "Arrived via an EventBridge rule",
    "put_event": "Put on EventBridge bus",
    "s3_upload": "Uploaded to S3 bucket",
    "s3_event": "S3",
}

DLQ_MOVE_LABELS = {
    "observed": "moved, reported by SQS (DeadLetterQueueSourceArn on the DLQ receive)",
    "not_seen_on_dlq_yet": "receive count reached maxReceiveCount, so a move is inferred; not seen on the DLQ yet",
}

# A RabbitMQ message's words (QV-307): its states, steps and dead-letter moves, in RabbitMQ's terms.
AMQP_STATE_LABELS = {
    "deleted": "done",
    "processing": "being handled (inferred)",
    "not_deleted": "no ack seen",
}
AMQP_STATE_TITLE = ("Done: every queue's copy was acked, refused or moved to a dead-letter queue, and the copy "
                    "on the dead-letter queue, if any, was acked or refused.")

AMQP_EVENT_LABELS = {
    "sent": "Published",
    "routed": "Routed",
    "received": "Delivered",
    "acked": "Acked (basic.ack)",
    "requeued": "Requeued",
    "rejected": "Refused, not requeued",
    "channel_closed": "Channel closed",
    "returned": "Returned to the publisher (basic.return)",
    "returned_to_queue": "Returned to the queue",
    "moved_to_dead_letter_queue": "Dead-lettered",
    "deliveries_not_observed": "Deliveries not observed",
}
AMQP_ONLY_TYPES = {"routed", "acked", "requeued", "rejected", "channel_closed", "returned", "returned_to_queue"}

RETURN_CAUSES = {
    "requeued": "requeued by the consumer",
    "channel_closed": "its channel closed before an ack",
    "consumer_timeout": "consumer timeout (consumer_timeout)",
    "consumer_cancelled": "the broker cancelled the consumer",
    "not_observed": "not observed",
}

AMQP_DLQ_MOVE_LABELS = {
    "observed": "moved, reported by RabbitMQ (the x-death header on the delivery from the dead-letter queue)",
    "not_seen_on_dlq_yet": "x-delivery-limit passed, so a move is inferred; not seen on a dead-letter queue yet",
}

AMQP_TIMELINE_NOTE = (
    "Each step shows when its call was made and when it returned; RabbitMQ acted somewhere in between. A step is "
    "shown above another only if an observed fact proves it came first, and the step says which: it ended before "
    "the other started (the hosts' clocks), the same process issued it after the other returned, or the "
    "delivery's count went up (x-delivery-count, x-acquired-count or redelivered). Nothing is ordered by what "
    "usually causes what. Steps in a box marked <span class=\"badge badge-concurrent\">concurrent</span> can't be "
    "put in one order: no observed fact orders some of them. Steps marked <span class=\"badge\">inferred</span> "
    "were deduced by Trailbeam; no span recorded them, so they have a time window instead of a time. Routed lists "
    "the queues the message was delivered on, so a queue no consumer with Trailbeam reads is missing from it. Each "
    "queue holds its own copy of the message, which is not a duplicate. A return to the queue says what caused "
    "it: a requeue, a closed channel, a consumer timeout or a broker cancel, or not observed when no span shows "
    "one. A move to a dead-letter queue is shown only when RabbitMQ reports it: the delivery from the dead-letter "
    "queue carries an x-death header naming the queue it came from and the reason.")

AMQP_DUPLICATES_NOTE = (
    "RabbitMQ messages are counted per queue copy: a message an exchange routed to two queues has two copies, and "
    "one delivery of each is not a duplicate. A second delivery on the same queue follows a requeue, a closed "
    "channel or a consumer timeout, and the message's timeline says which. A message published twice under the "
    "same message ID is flagged on its timeline.")

AMQP_STUCK_NOTE = (
    "A RabbitMQ message is listed while a queue's copy of it has no basic.ack, no refusal (requeue = false) and no "
    "observed move to a dead-letter queue. An unroutable message (basic.return) is not listed: no queue holds it. "
    "A copy can also leave its queue when its time to live (TTL) runs out or the queue drops it at its length "
    "limit with no dead-letter exchange; RabbitMQ reports nothing then, so it stays here until Trailbeam's own "
    "retention removes it.")


def _is_amqp(m: dict) -> bool:
    """Whether a timeline or a list row is a RabbitMQ message's."""
    return m.get("system") == systems.RABBITMQ or systems.system_of(str(m.get("message_id") or "")) == systems.RABBITMQ


def _is_amqp_event(e: dict) -> bool:
    """Whether a step is a RabbitMQ message's: its type, the x-death source, or its AMQP operation."""
    op = str(e.get("operation") or "")
    return (e["type"] in AMQP_ONLY_TYPES or e.get("source") == AMQP_DLQ_SOURCE
            or op.startswith(("basic.", "channel.", "connection.")))


def _label(e: dict, amqp_message: bool = False) -> str:
    """A step's heading. ``amqp_message``: the step is a RabbitMQ message's even if nothing on the step says so (an
    inferred gap carries no AMQP operation), so it keeps RabbitMQ's words ("Deliveries not observed")."""
    labels = AMQP_EVENT_LABELS if amqp_message or _is_amqp_event(e) else EVENT_LABELS
    return labels.get(e["type"], EVENT_LABELS.get(e["type"], e["type"]))


def _shown_id(t: dict) -> str:
    """The message ID as its application set it: a RabbitMQ key without its system mark."""
    return systems.split(t["message_id"])[1]


DUPLICATE_REASONS = {
    "received_more_than_once": "received more than once",
    "succeeded_more_than_once": "handled more than once",
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
a.button{display:inline-block;padding:.45rem 1rem;border:1px solid #1d2330;border-radius:4px;color:#1d2330;text-decoration:none;background:#fff}
table{border-collapse:collapse;width:100%;background:#fff}
th,td{text-align:left;padding:.35rem .6rem;border-bottom:1px solid #e3e6ec;vertical-align:top}
th{background:#eef0f4;font-weight:600}
code,.mono{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:.9em}
.state{padding:.05rem .4rem;border-radius:3px;font-size:.85em;white-space:nowrap}
.state-deleted{background:#d8f0dd}.state-deleted_inferred{background:#e4efd8}
.state-processing{background:#dbe7f7}.state-not_deleted{background:#f7e2cf}.state-purged{background:#e6e1ef}
.possibly-purged{padding:.05rem .4rem;border-radius:3px;font-size:.85em;white-space:nowrap;background:#f3ecc8}
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
.muted{color:#6b7385}.repeat{color:#a35a00;font-weight:600}form.filters{margin:.6rem 0 1rem;display:flex;gap:.6rem;align-items:center;flex-wrap:wrap}
footer{color:#6b7385;font-size:.8em;padding:1rem 1.2rem}
"""


def handle(api: Callable[[str, dict], object], api_error: type, path: str, query: dict) -> Optional[Response]:
    """Render the page for ``path``; None if ``path`` is not a page. ``api`` is ``App.api``."""
    pages = {"/": _index, "/message": _message, "/duplicates": _duplicates, "/stuck": _stuck, "/issues": _stuck, "/tenants": _tenants,
             "/people": _people, "/tokens": _tokens, "/account": _account}
    page = pages.get(path)
    if page is None:
        if path in SHELL_ONLY:
            return _shell_only(*SHELL_ONLY[path])
        from .ui_files import is_issue_page
        if is_issue_page(path):
            return _shell_only("Issue", "<code>/api/causes?cause=</code> with the issue's ID, and the "
                               '<a href="/stuck">stuck messages</a> page')
        return None
    try:
        return page(api, query)
    except api_error as e:  # bad parameter or unknown message
        title = "Message not found" if e.status == 404 else "Bad request"
        return _page(e.status, title, f'<h1>{escape(title)}</h1><p class="error">{escape(e.message)}</p>'
                     + _lookup_form(query.get("id", "")))


# ---- pages ------------------------------------------------------------------------------------


# The shell's pages that have no server-rendered version (ui_files.SHELL_PAGES): without the shared UI's build, each
# says so, as /people does, and where its data is in the API, instead of a bare 404 (QV-769).
SHELL_ONLY = {
    "/overview": ("Overview", "<code>/api/flows</code> and <code>/api/causes</code>"),
    "/queues": ("Queues", "<code>/api/queues</code>"),
    "/queue": ("Queue", "<code>/api/queues/{queue}/now</code> and <code>/api/queues/{queue}/series</code>"),
    "/events": ("Events", "<code>/api/events</code>"),
    "/dashboards": ("Dashboards", "<code>/api/dashboards</code>"),
    "/alerts": ("Alerts", "<code>/api/alerts/rules</code> and <code>/api/alerts/channels</code>"),
    "/settings": ("Settings", "<code>/api/settings/aws</code> (an admin can turn the AWS reads on there with a "
                  "<code>PATCH</code>)"),
}


def _shell_only(title: str, api_hint: str) -> Response:
    return _page(200, title, f"<h1>{escape(title)}</h1><p>The {escape(title)} page is part of the web screen's build, "
                 "which this install doesn't have (build it with <code>npm run build</code> in <code>ui/</code>). "
                 f"Without it, its data is in the API: {api_hint}.</p>")


def _people(api, q: dict) -> Response:
    """``/people`` without the shared UI's build (an editable install from a clone that was never built): the
    People page is drawn only by the shell, so this says how to manage people without it."""
    return _page(200, "People", "<h1>People</h1><p>The People page is part of the web screen's build, which this "
                 "install doesn't have (build it with <code>npm run build</code> in <code>ui/</code>). Without it, "
                 "manage people from a shell with <code>trailbeam users</code>, or with the API: "
                 "<code>/api/users</code>.</p>")


def _tokens(api, q: dict) -> Response:
    """``/tokens`` without the shared UI's build: as ``/people``, it says how to manage tokens without it."""
    return _page(200, "Tokens", "<h1>Tokens</h1><p>The Tokens page is part of the web screen's build, which this "
                 "install doesn't have (build it with <code>npm run build</code> in <code>ui/</code>). Without it, "
                 "manage tokens from a shell with <code>trailbeam tokens</code>, or with the API: "
                 "<code>/api/me/tokens</code> for your personal API tokens and, for admins, "
                 "<code>/api/tokens/ingest</code>.</p>")


def _index(api, q: dict) -> Response:
    searching = bool(q.get("body", "").strip())
    data = api("messages", _pick(q, "limit", "queue", "tenant", "body", "cursor",
                                 *(("since_s", "from_ms", "to_ms") if searching else ())))
    note, nxt = "", q
    if searching:
        bs = data["body_search"]
        note = ('<p class="muted body-note">Searches only the bodies this server keeps: those of queues in '
                "<code>TRAILBEAM_STORE_BODIES</code>, as the package redacted them. Anyone who can open this "
                "page can read them.</p>")
        if not bs["stores_bodies"]:
            note += ('<p class="notice">This server keeps no message bodies, so there&rsquo;s nothing to search. To '
                     "keep them, set <code>TRAILBEAM_STORE_BODIES</code> on the server and "
                     "<code>TRAILBEAM_CAPTURE_BODY_QUEUES</code> in the package. "
                     '<a href="https://github.com/nick-lennox/trailbeam#message-bodies">Message bodies</a></p>')
        elif bs["scanned"]:
            note += (f'<p class="muted">Read the bodies of {bs["scanned"]:,} messages last seen between '
                     f'{escape(_ms(bs["searched_from_ms"]))} and {escape(_ms(bs["to_ms"]))}'
                     + ("." if bs["complete"] else f'; the next page reads older ones, back to {escape(_ms(bs["from_ms"]))}.')
                     + "</p>")
        nxt = {**q, "from_ms": str(bs["from_ms"]), "to_ms": str(bs["to_ms"])}
        nxt.pop("since_s", None)
    fields = [("body", "Body", "text"), ("queue", "Queue", "text"), ("tenant", "Tenant", "text"),
              ("limit", "Limit", "number")]
    if searching:
        fields.insert(2, ("since_s", "Last seen within", WINDOWS))
    body = ("<h1>Look up a message</h1>" + _lookup_form("")
            + ("<h2>Messages whose body matches</h2>" if searching else "<h2>Recent messages</h2>")
            + _filters("/", {"since_s": "86400", **q} if searching else q, fields)
            + note
            + _summary_table(data["messages"], empty="No message body matches." if searching else (
                NO_SPANS if not data["messages"] and _store_empty(api) else "No messages stored yet."))
            + _next("/", nxt, data))
    return _page(200, "Trailbeam", body)


def _message(api, q: dict) -> Response:
    mid = q.get("id", "").strip()
    if not mid:
        return 303, {"Location": "/"}, b""
    extra = _pick(q, "lambda_timeout_s")
    t = api("messages/" + _quote_segment(mid), extra)
    amqp = _is_amqp(t)
    parts = [f'<h1>Message <code>{escape(_shown_id(t))}</code>'
             + (' <span class="muted">RabbitMQ</span>' if amqp else "") + "</h1>"]
    for n in t.get("notices", []):
        parts.append(f'<div class="notice" data-code="{escape(n["code"])}"><strong>{escape(n["code"])}</strong>: '
                     f'{escape(n["message"])}</div>')
    parts.append('<dl class="summary">')
    parts.append(_dd("State", _state(t["state"], t.get("purge"), amqp)))
    parts.append(_dd("Source queue", escape(t.get("source_queue") or "?")))
    parts.append(_dd("Current queue", escape(t.get("current_queue") or "?")
                     + (" <strong>(dead-letter queue)</strong>" if t.get("in_dead_letter_queue") else "")))
    if t.get("dead_letter_queue_move"):
        labels = AMQP_DLQ_MOVE_LABELS if amqp else DLQ_MOVE_LABELS
        parts.append(_dd("Dead-letter queue", escape(labels.get(t["dead_letter_queue_move"],
                                                                t["dead_letter_queue_move"]))))
    if amqp:
        parts.append(_dd("Publish observed", "yes" if t.get("send_observed") else "no"))
        parts.append(_dd("Deliveries", f'{t["deliveries"]} ({t["successful_deliveries"]} acked)'))
        if t.get("copies"):
            # a queue is (virtual host, queue): name the vhost when the copies are in more than one
            vhosts = len({c.get("vhost") for c in t["copies"]}) > 1
            parts.append(_dd("Copies", "; ".join(
                f'<code>{escape(str(c["queue"]))}</code>'
                + (f' (vhost <code>{escape(str(c.get("vhost")))}</code>)' if vhosts else "")
                + f': {int(c["deliveries"])} '
                f'deliver{"y" if c["deliveries"] == 1 else "ies"}, {escape(str(c.get("ended") or "not settled"))}'
                for c in t["copies"])))
    else:
        parts.append(_dd("Send observed", "yes" if t.get("send_observed") else "no"))
        parts.append(_dd("Receives", f'{t["deliveries"]} ({t["successful_deliveries"]} handled)'))
    parts.append(_dd("First seen", _first_seen(t)))
    parts.append(_dd("Last seen", _time(t.get("last_seen"))))
    parts.append("</dl>")
    parts.append('<h2>Timeline</h2><p class="muted">' + AMQP_TIMELINE_NOTE + "</p>" if amqp else
                 '<h2>Timeline</h2><p class="muted">Each step shows when its SQS request was sent and when the '
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
    return _page(200, f'Message {_shown_id(t)}', "".join(parts))


def _duplicate_why(m: dict) -> str:
    """Each reason with its count from the row: receives, or successful handlings."""
    counts = {"received_more_than_once": ("received", m.get("deliveries")),
              "succeeded_more_than_once": ("handled", m.get("successful_deliveries"))}
    out = []
    for r in m.get("reasons", []):
        verb, n = counts.get(r, (None, None))
        out.append(f'{verb} <span class="mono">{int(n)}</span> times' if verb and n is not None
                   else escape(DUPLICATE_REASONS.get(r, r)))
    return ", ".join(out)


def _duplicates(api, q: dict) -> Response:
    params = _pick(q, "limit", "queue", "reason", "cursor")
    data = api("duplicates", params)
    reason_opts = [("", "any reason")] + list(DUPLICATE_REASONS.items())
    body = ("<h1>Duplicates</h1>"
            '<p class="muted">Messages SQS returned more than once, or that more than one consumer handled. '
            "A second receive is expected after your own <code>ChangeMessageVisibility</code> or after a "
            "consumer failed to handle it. It is worth a look when the visibility timeout expired while a consumer was still "
            "working, or when a message was handled twice.</p>"
            f'<p class="muted">{AMQP_DUPLICATES_NOTE}</p>'
            + _filters("/duplicates", q, [("reason", "Reason", reason_opts), ("queue", "Queue", "text"),
                                          ("limit", "Limit", "number")])
            + _summary_table(data["messages"], extra=("Why", _duplicate_why),
                empty=NO_SPANS if not data["messages"] and _store_empty(api) else "No duplicates.")
            + _next("/duplicates", q, data))
    return _page(200, "Duplicates", body)


def _stuck(api, q: dict) -> Response:
    data = api("stuck", _pick(q, "older_than_s", "limit", "queue", "cursor"))
    body = ("<h1>Stuck messages</h1>"
            f'<p class="muted">Messages first seen more than {_threshold(int(data["older_than_s"]))} ago that no '
            "consumer has deleted yet, oldest first. Change the age with Older than, or the server default with "
            "<code>--stuck-after</code> (<code>TRAILBEAM_STUCK_AFTER_S</code>).</p>"
            + _filters("/stuck", {**q, "older_than_s": str(data["older_than_s"])},
                       [("older_than_s", "Older than (s)", "number"), ("queue", "Queue", "text"),
                        ("limit", "Limit", "number")])
            + _summary_table(data["messages"], extra=("Age", _stuck_age),
                             empty=NO_SPANS if not data["messages"] and _store_empty(api) else "No stuck messages.")
            + _next("/stuck", q, data)
            + f'<p class="muted">{STUCK_LEAVES_SQS_NOTE}</p>'
            + f'<p class="muted">{AMQP_STUCK_NOTE}</p>'
            + '<p class="muted">Times are in UTC. Age is this server\'s clock now minus the send\'s time on the '
            "sender's clock, so skew between the two clocks shows up in it. If Trailbeam never saw the send, it "
            "counts from SQS's <code>SentTimestamp</code>, and the age says so; with neither, from the first "
            "receive, on the consumer's clock.</p>")
    return _page(200, "Stuck messages", body)


def _threshold(s: int) -> str:
    """A threshold in seconds, in words when it is a whole number of days, hours or minutes: 300 is "5 min"."""
    for size, one, many in ((86_400, "day", "days"), (3600, "h", "h"), (60, "min", "min")):
        if s > 0 and s % size == 0:
            n = s // size
            return f"{n} {one if n == 1 else many}"
    return f"{s} s"


TENANT_RANGES = [("3600", "Last hour"), ("86400", "Last 24 hours"), ("604800", "Last 7 days")]
TENANT_IS = "A tenant is your own customer or account ID, read from each message."
TENANTS_README = '<a href="https://github.com/nick-lennox/trailbeam#tenants">Tenants in the README</a>'
# ui/src/tenants/Tenants.tsx P95_NOTE: what p95 in queue is over, and how close it is (QV-247)
P95_NOTE = ("p95 in queue is from the send to the first receive, over first receives only. It compares two "
            "clocks: the sender's host and the consumer's, or SQS's SentTimestamp when the send wasn't observed "
            "(those first receives count too, so a tenant can have more first receives than sends). Clock skew "
            "shows up in it, and a time below 0 counts as 0. It is estimated, within 1% of the exact p95.")
TENANT_ENV = ("export TRAILBEAM_TENANT_ATTRIBUTE=tenant        # a message attribute\n"
              "export TRAILBEAM_TENANT_BODY_PATH=customer.id   # or a path in a JSON body")


def _no_tenants(total_sent: int, label: str) -> str:
    """The Tenants page with no tenant in its range (QV-217): what a tenant is, and how to set one."""
    when = "in the " + label.replace("Last ", "last ", 1)
    if not total_sent:
        seen = f"No messages were sent {when}."
    else:
        seen = f'{total_sent:,} {"message was" if total_sent == 1 else "messages were"} sent {when}, none with one.'
    return (f'<div class="notice"><p><strong>No tenants yet</strong></p><p>{TENANT_IS} {seen} To split traffic by '
            f"customer, set one of these in every service that sends or receives:</p>"
            f"<pre><code>{escape(TENANT_ENV)}</code></pre><p>{TENANTS_README}</p></div>")


def _tenants(api, q: dict) -> Response:
    since = q.get("since_s") if q.get("since_s") in dict(TENANT_RANGES) else "86400"
    d = api("tenants", {"since_s": since})
    label = dict(TENANT_RANGES)[since]
    head = ("<h1>Tenants</h1><p class=\"muted\">Which of your customers are sending the traffic, and who is "
            "waiting longest.</p>" + _filters("/tenants", {"since_s": since}, [("since_s", "Range", TENANT_RANGES)]))
    if not d["rows"]:
        return _page(200, "Tenants", head + _no_tenants(d["total_sent"], label))
    rows = []
    for r in d["rows"]:
        share = "&lt;1" if 0 < r["share"] < 0.01 else str(round(r["share"] * 100))
        p95 = ('<span class="muted">-</span>' if r["p95_in_queue_ms"] is None
               else escape(_took(r["p95_in_queue_ms"] * 1_000_000)))
        rows.append(f'<tr><td><a href="/?{escape(urlencode({"tenant": r["tenant"]}))}">{escape(r["tenant"])}</a></td>'
                    f'<td class="mono">{r["sent"]:,}</td><td class="mono">{share}%</td>'
                    f'<td class="mono" title="over {r["first_deliveries"]} first receives">{p95}</td>'
                    f'<td class="mono">{r["dead_lettered"]:,}</td></tr>')
    untagged = f', {d["untagged_sent"]:,} of them without a tenant' if d["untagged_sent"] else ""
    note = (f'<p class="muted">{escape(label)}, from every message in the range. Share is of all '
            f'{d["total_sent"]:,} observed sends{untagged}. {escape(P95_NOTE)} Dead-lettered counts messages SQS '
            f"was seen moving to a dead-letter queue. {TENANT_IS} How to set one: {TENANTS_README}.</p>")
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
            out.append(_event(evs[i], by_id, self_id=t.get("message_id")))
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
                   + _waterfall(evs[i:j], by_id, t.get("message_id")) + "</li>")
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
    if e.get("inferred") or e.get("source") in REPORTED_SOURCES:
        w = e.get("window") or {}
        return _ns_of_iso(w.get("from")), _ns_of_iso(w.get("to")), True
    start = e.get("start_unix_nano")
    end = e.get("end_unix_nano") or e.get("time_unix_nano")
    return (None if start is None else int(start)), (None if end is None else int(end)), False


def _waterfall(members: list[dict], by_id: dict, self_id: Optional[str] = None) -> str:
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
        rows.append(((lo // 1000 - 1) if a is None else a // 1000, window, k, _event(e, by_id, bar, self_id)))
    rows.sort(key=lambda r: r[:3])  # by start (µs); on a tie, observed before a window; then Trailbeam's order
    scale = "" if width <= 0 else (f" The scale runs from {_time(iso(lo))} to {_time(iso(hi))}"
                                   f" ({_took(width)}).")
    broker = "RabbitMQ" if any(_is_amqp_event(m) for m in members) else "SQS"
    note = ('<p class="group-note">Each bar is one step on the same time scale: from its request sent to its '
            "response received (hatched: a window, for a step with no time of its own). Rows are listed by when "
            f"each request was sent, which is not the order {broker} acted in: where bars overlap, {broker} may "
            f"have acted on either first.{scale}</p>")
    return note + '<ul class="waterfall">' + "".join(r[3] for r in rows) + "</ul>"


def _took(ns: int) -> str:
    ms = ns / 1e6
    return f"{ms:.1f} ms" if ms < 10_000 else f"{ms / 1000:.1f} s"


def _ref(e: Optional[dict]) -> str:
    """A step as a reader names it: "the send", "receive 2", "the delete of receive 1" ("of delivery 1" on RabbitMQ)."""
    if not e:
        return "an earlier step"
    t, d = e["type"], e.get("delivery")
    if _is_amqp_event(e):
        of = f" of delivery {d}" if d is not None else ""
        names = {"sent": "the publish", "received": f"delivery {d}" if d is not None else "a delivery",
                 "acked": "the ack" + of, "requeued": "the requeue" + of, "rejected": "the refusal" + of,
                 "channel_closed": "the channel close" + of, "returned": "the basic.return",
                 "returned_to_queue": "the return to the queue" + of, "routed": "the routing",
                 "moved_to_dead_letter_queue": "the move to the dead-letter queue"}
        return names.get(t, _label(e).lower())
    of = f" of receive {d}" if d is not None else ""
    names = {"sent": "the send", "received": f"receive {d}" if d is not None else "a receive",
             "deleted": "the delete" + of, "visibility_changed": "the visibility change" + of,
             "lambda_outcome": "the Lambda outcome" + of}
    return names.get(t, EVENT_LABELS.get(t, t).lower())


def _why(e: dict, by_id: dict) -> list[str]:
    """One line per step this one is shown after, saying which observed fact proves it."""
    out = []
    entries = list(e.get("after") or [])
    if e.get("source") in REPORTED_SOURCES and e.get("before"):
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


def _event(e: dict, by_id: Optional[dict] = None, bar: str = "", self_id: Optional[str] = None) -> str:
    inferred = bool(e.get("inferred"))
    amqp = _is_amqp_event(e)
    label = escape(_label(e, systems.system_of(str(self_id or "")) == systems.RABBITMQ))
    dedup = e["type"] == "sent" and bool(e.get("deduplicated"))  # a FIFO send SQS deduplicated (QV-246)
    if dedup:
        label = "Sent again, deduplicated by SQS"
    if e["type"] in ("published", "arrived_via_sns") and e.get("topic"):
        label += f' <code>{escape(str(e["topic"]))}</code>'
    elif e["type"] == "put_event" and e.get("event_bus"):  # contract 25.3
        label += f' <code>{escape(str(e["event_bus"]))}</code>'
    elif e["type"] == "s3_upload" and e.get("bucket"):  # contract 27.3
        label += f' <code>{escape(str(e["bucket"]))}</code> by {escape(str(e.get("operation")))}'
    elif e["type"] == "s3_event":  # contract 23.3: "S3 ObjectCreated:Put", or the test message
        label = ("S3 test message" if e.get("test_event")
                 else f'S3 <code>{escape(str(e.get("s3_event_name")))}</code>')
    failed = e.get("result") == "failed" or (e["type"] == "lambda_outcome" and e.get("outcome") != "success")
    cls = ["event", "inferred" if inferred else "observed"] + (["failed"] if failed else [])
    head = []
    # observed, but with no time of its own: the DLQ move SQS reports (contract 9b), the SNS delivery an envelope
    # reports (21.5)
    reported = e.get("source") in REPORTED_SOURCES
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
    if reported and e.get("reported_by"):
        out.append(f'<div class="reported-by"><strong>{escape(e["reported_by"])}</strong></div>')
    if details:
        out.append(f'<div class="detail">{" · ".join(details)}</div>')
    if (inferred or reported or dedup or amqp or e["type"] in ("queue_purged", "published", "put_event", "s3_upload")) and e.get("reason"):
        out.append(f'<div class="reason">{escape(e["reason"])}</div>')
    if e["type"] in ("published", "put_event", "s3_upload"):
        out.append(_reached(e, self_id))
    out.extend(_why(e, by_id or {}))
    out.append("</li>")
    return "".join(out)


JOINED_BY = {"envelope_message_id": "the SNS envelope's MessageId (raw delivery off)",
             "publish_id": "the trailbeam.publish_id attribute (raw delivery on)",
             "event_id": "the EventBridge event's id in the receive's body",
             "bucket_key_time": "the bucket, object key and event time of the S3 notification in the receive's body"}


def _reached(e: dict, self_id: Optional[str]) -> str:
    """The SQS messages an SNS publish reached, by queue (contract section 21.5), each a link but this one."""
    parts = []
    for r in e.get("reached") or []:
        ids = []
        for m in r["message_ids"]:
            ids.append(f'<span class="mono">{escape(m)}</span> (this message)' if m == self_id
                       else f'<a class="mono" href="/message?{escape(urlencode({"id": m}))}">{escape(m)}</a>')
        parts.append(f'<code>{escape(r["queue"])}</code>: {", ".join(ids)}')
    more = " · and more (the first 100 receives are listed)" if e.get("reached_truncated") else ""
    return f'<div class="detail reached">reached {" · ".join(parts)}{more}</div>'


def _details(e: dict) -> list[str]:
    d: list[str] = []

    def add(name: str, key: str, fmt: Callable[[object], str] = lambda v: escape(str(v))):
        if e.get(key) is not None:
            d.append(f"{escape(name)} {fmt(e[key])}")

    if e.get("from_queue") or e.get("to_queue"):
        d.append(f'from <code>{escape(str(e.get("from_queue")))}</code> to <code>{escape(str(e.get("to_queue")))}</code>')
    elif e.get("topic"):
        d.append(f'topic <code>{escape(str(e["topic"]))}</code>')
    elif e.get("exchange") and e["type"] in ("sent", "returned", "routed"):
        d.append(f'exchange <code>{escape(str(e["exchange"]) or "(default)")}</code>')
    elif e.get("queue"):
        d.append(f'queue <code>{escape(e["queue"])}</code>')
    if e.get("queues") and e["type"] == "routed":
        d.append("to " + ", ".join(f"<code>{escape(str(q))}</code>" for q in e["queues"]))
    add("routing key", "routing_key", lambda v: f"<code>{escape(str(v))}</code>")
    add("routing key", "routing_key_hmac",
        lambda v: f'<span title="Hashed in your process: body capture is off for this queue">hashed</span> '
                  f'<code>{escape(str(v))}</code>')
    if e.get("cause"):
        d.append(f'cause <strong>{escape(RETURN_CAUSES.get(e["cause"], str(e["cause"])))}</strong>')
    if e.get("dead_letter_reason"):
        r = str(e["dead_letter_reason"])
        d.append(f"x-death reason <code>{escape(r)}</code>"
                 + (f" ({escape(DEAD_LETTER_REASONS[r])})" if r in DEAD_LETTER_REASONS else ""))
    add("reply code", "reply_code")
    add("reply text", "reply_text", lambda v: f"<code>{escape(str(v))}</code>")
    add("SNS delivery ms", "sns_delivery_ms")
    if e.get("joined_by"):
        by = [e["joined_by"]] if isinstance(e["joined_by"], str) else e["joined_by"]  # put_event: one string (25.3)
        d.append("joined by " + " and ".join(escape(JOINED_BY.get(j, j)) for j in by))
    add("SNS message ID", "sns_message_id", lambda v: f"<code>{escape(str(v))}</code>")
    code = lambda v: f"<code>{escape(str(v))}</code>"  # noqa: E731
    # EventBridge and S3 origins (contract 23.3)
    add("source", "eventbridge_source", code)
    add("detail-type", "detail_type", code)
    add("event ID", "event_id", code)
    add("account", "account", code)
    add("bucket", "bucket", code)
    add("key", "object_key", code)
    add("sequencer", "sequencer", code)
    add("configuration", "configuration_id", code)
    add("records in the message", "record_count")
    add("event time", "event_time")
    add("delivery ms", "delivery_ms")
    add("EventBridge delivery ms", "eventbridge_delivery_ms")
    add("S3 delivery ms", "s3_delivery_ms")
    add("ETag", "etag", code)
    add("version ID", "version_id", code)
    if e.get("event_time_given"):
        d.append("event time given by the sender (PutEvents Time), not EventBridge's clock")
    add("entry", "entry_index")
    add("delivery", "delivery")
    if e.get("receive_count_source") and e.get("receive_count_source") != "first_delivery":
        d.append(f'receive count {escape(str(e.get("receive_count")))} (from {escape(str(e["receive_count_source"]))})')
    else:
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
        if s.get("trace_url"):  # --trace-link (trace_links.py): an http(s) URL the server built
            d.append(f'<a href="{escape(str(s["trace_url"]), quote=True)}" target="_blank" rel="noopener noreferrer">'
                     'Open trace</a>')
    return d


def _consumer(c: dict) -> str:
    who = c.get("service_name") or c.get("faas_name") or "?"
    bits = [f"<strong>{escape(str(who))}</strong>"
            + (f" {escape(str(c['service_version']))}" if c.get("service_version") else "")]
    for key, name in (("client_id", "client"), ("source", "source"), ("faas_name", "function"),
                      ("faas_invocation_id", "invocation"), ("pid", "pid"), ("thread_name", "thread")):
        if c.get(key) is not None and not (key == "faas_name" and c[key] == who):
            bits.append(f"{name} {escape(str(c[key]))}")
    return " ".join(bits[:1]) + (f' ({", ".join(bits[1:])})' if len(bits) > 1 else "")


RECEIVES_TITLE = ("Receives Trailbeam observed, on every queue, the receive from a dead-letter queue included. "
                  "SQS's own receive count can be higher: it counts receives by consumers Trailbeam doesn't see. "
                  "Handled: a receive that ended in a delete or a successful Lambda record.")


def _receives(m: dict) -> str:
    """``2 · 1 handled``; the count is marked when the message was received more than once."""
    n = m["deliveries"]
    count = f'<span class="repeat">{n}</span>' if n > 1 else str(n)
    return f'{count} <span class="muted">· {m["successful_deliveries"]} handled</span>'


def _summary_table(messages: list[dict], *, empty: str, extra: Optional[tuple] = None) -> str:
    if not messages:
        return f'<p class="muted empty">{escape(empty)}</p>'
    cols = ["Message ID", "State", "Queue", "Receives", "Consumers", "First seen", "Last seen"]
    if extra:
        cols.append(extra[0])
    rows = []
    for m in messages:
        amqp = _is_amqp(m)
        queue = escape(m.get("current_queue") or m.get("source_queue") or "?")
        if m.get("in_dead_letter_queue"):
            queue += f' <span class="muted">(DLQ, from {escape(m.get("source_queue") or "?")})</span>'
        elif m.get("dead_letter_queue_move") == "not_seen_on_dlq_yet":
            queue += (' <span class="muted">(x-delivery-limit passed; DLQ move inferred, not seen yet)</span>' if amqp
                      else ' <span class="muted">(maxReceiveCount reached; DLQ move inferred, not seen yet)</span>')
        consumers = ", ".join(escape(str(c.get("service_name") or "?"))
                              + (f' <span class="muted">{escape(str(c["client_id"]))}</span>' if c.get("client_id") else "")
                              for c in m.get("consumers", []))
        cells = [f'<a class="mono" href="/message?{escape(urlencode({"id": m["message_id"]}))}">'
                 f'{escape(systems.split(m["message_id"])[1])}</a>'
                 + (' <span class="muted">RabbitMQ</span>' if amqp else ""),
                 _state(m["state"], m.get("purge"), amqp), queue, _receives(m), consumers,
                 _first_seen(m, short=True), _time(m.get("last_seen"))]
        if extra:
            cells.append(extra[1](m))
        rows.append("<tr>" + "".join(f"<td>{c}</td>" for c in cells) + "</tr>")
    return ("<table><thead><tr>" + "".join(
        f'<th title="{escape(RECEIVES_TITLE)}">{escape(c)}</th>' if c == "Receives" else f"<th>{escape(c)}</th>" for c in cols) + "</tr></thead><tbody>"
            + "".join(rows) + "</tbody></table>")


WINDOWS = [("3600", "an hour"), ("86400", "a day"), ("604800", "a week"), ("2592000", "30 days")]  # body search


def _ms(ms: Optional[int]) -> str:
    return "-" if ms is None else datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _next(action: str, q: dict, data: dict) -> str:
    """A link to the next page, with the same filters, when the API returned a cursor for one."""
    if not data.get("next_cursor"):
        return ""
    params = {**{k: v for k, v in q.items() if v != "" and k != "cursor"}, "cursor": data["next_cursor"]}
    return f'<p class="pager"><a href="{escape(action)}?{escape(urlencode(params))}">Next page</a></p>'


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


def _account(api, q: dict) -> Response:
    """``/account`` without the shared UI's build: as ``/people``, it says how to manage passkeys without it."""
    return _page(200, "Account", "<h1>Account</h1><p>The Account page is part of the web screen's build, which this "
                 "install doesn't have (build it with <code>npm run build</code> in <code>ui/</code>). Without it, "
                 "your passkeys are listed by the API at <code>/api/me/passkeys</code>, and can be removed there. "
                 "Adding one needs the page: the browser makes it.</p>")


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


def _sso_button(next_path: str, sso) -> str:
    """The sign-in buttons, for the ways in that are on: ``sso`` is a list of ``(path, label)`` (single sign-on,
    oidc.py: ``/login/oidc``; GitHub, github.py: ``/login/github``), or one label for single sign-on alone."""
    if not sso:
        return ""
    if isinstance(sso, str):
        sso = [("/login/oidc", sso)]
    return "".join(f'<p><a class="quiet" href="{escape(path + "?" + urlencode({"next": next_path}))}">'
                   + (sky.GITHUB_MARK if path == "/login/github" else "")
                   + f"Sign in with {escape(label)}</a></p>" for path, label in sso)


def _ways(next_path: str, sso, passkey: bool) -> str:
    """The buttons above the form, as the hosted sign-in has them: single sign-on, GitHub and a passkey."""
    inner = _sso_button(next_path, sso) + (_passkey_button(next_path) if passkey else "")
    return f'<div class="ways">{inner}</div>' if inner else ""


def _hidden(name: str, value: str) -> str:
    return f'<input type="hidden" name="{name}" value="{escape(value)}">'


def login_page(next_path: str, error: bool = False, status: int = 401, sso=None) -> Response:
    """The page a browser gets without the session cookie when the server has a token and no admin (auth.py)."""
    body = ("<h1>Sign in</h1>"
            + '<p class="lead">This Trailbeam server needs its access token (<code>TRAILBEAM_TOKEN</code>).</p>'
            + (sky.notice("That is not the token.") if error else "")
            + _ways(next_path, sso, False) + ('<p class="divider">or with the token</p>' if sso else "")
            + f'<form class="auth-form{"" if sso else " spaced"}" method="post" action="/login">'
            + sky.field("Access token", '<input type="password" id="f-token" name="token" class="mono" '
                        'autocomplete="current-password" required autofocus>')
            + _hidden("next", next_path) + '<div class="actions"><button class="btn">Sign in</button></div></form>')
    return sky.page(status, "Sign in", body)


SIGN_IN_NOTICES = {
    "wrong": "That email and password don't match an account.",
    "locked": "Too many failed sign-ins: this account is locked for now. Try again in {minutes}.",
    "slow_down": "Too many failed sign-ins from here. Try again in {minutes}.",
    "busy": "The server is busy. Try again in a moment.",
}


def _minutes(seconds: Optional[int]) -> str:
    m = max(1, ((seconds or 60) + 59) // 60)
    return "1 minute" if m == 1 else f"{m} minutes"


minutes = _minutes

# Sign in with a passkey (passkeys.py): a challenge from /login/passkey/options, navigator.credentials.get, and the
# signed answer to /login/passkey, which sets the session cookie and says where to go. The binary fields travel as
# base64url. Without scripts or WebAuthn, the button stays hidden and the password form is there as before.
_PASSKEY_SCRIPT = (
    "<script>(function(){var b=document.getElementById('passkey'),m=document.getElementById('passkey-note');"
    "if(!b||!window.PublicKeyCredential||!navigator.credentials)return;b.parentNode.hidden=false;"
    "function u(s){s=s.replace(/-/g,'+').replace(/_/g,'/');while(s.length%4)s+='=';var r=atob(s),a=new Uint8Array(r.length);"
    "for(var i=0;i<r.length;i++)a[i]=r.charCodeAt(i);return a.buffer;}"
    "function e(x){var a=new Uint8Array(x),s='';for(var i=0;i<a.length;i++)s+=String.fromCharCode(a[i]);"
    "return btoa(s).replace(/[+]/g,'-').replace(/[/]/g,'_').replace(/=+$/,'');}"
    "function post(p,d){return fetch(p,{method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json'},"
    "body:JSON.stringify(d)}).then(function(r){return r.json().catch(function(){return {};}).then(function(j){"
    "if(!r.ok)throw new Error(j.error||'');return j;});});}"
    "b.addEventListener('click',function(){b.disabled=true;m.hidden=true;"
    "post('/login/passkey/options',{}).then(function(j){var o=j.options;o.challenge=u(o.challenge);"
    "(o.allowCredentials||[]).forEach(function(c){c.id=u(c.id);});return navigator.credentials.get({publicKey:o});})"
    ".then(function(c){var r=c.response;return post('/login/passkey',{next:b.getAttribute('data-next'),credential:{"
    "id:c.id,rawId:e(c.rawId),type:c.type,response:{clientDataJSON:e(r.clientDataJSON),"
    "authenticatorData:e(r.authenticatorData),signature:e(r.signature),userHandle:r.userHandle?e(r.userHandle):null},"
    "clientExtensionResults:c.getClientExtensionResults?c.getClientExtensionResults():{}}});})"
    ".then(function(j){location.assign(j.next||'/');}).catch(function(x){"
    "m.textContent=x&&x.name==='NotAllowedError'?'The passkey sign-in was canceled or ran out of time.':"
    "(x&&x.message)||\"That passkey didn't work. Try another way to sign in.\";m.hidden=false;b.disabled=false;});});"
    "})();</script>")


def _passkey_button(next_path: str) -> str:
    return (f'<p hidden><button type="button" class="quiet" id="passkey" data-next="{escape(next_path)}">'
            f'{sky.KEY_ICON}Sign in with a passkey</button></p>' + sky.notice("", "passkey-note", hidden=True)
            + _PASSKEY_SCRIPT)


def sign_in_page(next_path: str, email: str = "", outcome: Optional[str] = None, retry_after_s: Optional[int] = None,
                 status: int = 401, sso=None, password: bool = True, passkey: bool = False) -> Response:
    """The page a browser gets without a session once the server has an admin (access.py): email and password, and
    the single sign-on and GitHub buttons when they are on (``sso``, as _sso_button). ``password=False``: those buttons
    only (no admin yet). ``passkey``: passkeys are on (passkeys.py), so it has Sign in with a passkey too."""
    notice = SIGN_IN_NOTICES.get(outcome or "")
    head = ("<h1>Sign in</h1>" + (sky.notice(notice.format(minutes=_minutes(retry_after_s))) if notice else "")
            + _ways(next_path, sso, passkey and password))
    if not password:
        return sky.page(status, "Sign in", head)
    body = (head + ('<p class="divider">Or sign in with your Trailbeam email and password.</p>' if sso or passkey
                    else '<p class="lead">Sign in with your Trailbeam account.</p>')
            + f'<form class="auth-form{"" if sso or passkey else " spaced"}" method="post" action="/login">'
            + sky.field("Email", f'<input type="email" id="f-email" name="email" value="{escape(email)}" '
                        'placeholder="you@example.com" autocomplete="username" required autofocus>')
            + sky.field("Password", '<input type="password" id="f-password" name="password" '
                        'autocomplete="current-password" required>')
            + _hidden("next", next_path) + '<div class="actions"><button class="btn">Sign in</button></div></form>')
    return sky.page(status, "Sign in", body)


SETUP_NOTICES = {
    "bad_code": "This setup link has been used, is out of date (links last an hour), or isn't the latest one. "
                "Run trailbeam start again for a new link, or create the admin with trailbeam users create.",
    "mismatch": "The two passwords are not the same.",
    "busy": "The server is busy. Try again in a moment.",
    "slow_down": "Too many tries from here. Try again in {minutes}.",
}
# The code is in the URL fragment, which the browser never sends: this moves it into the form and out of the
# address bar. Without scripts, the code (after "#code=" in the link) can be pasted into the field.
_SETUP_SCRIPT = (
    "<script>(function(){var m=/[#&]code=([A-Za-z0-9_-]+)/.exec(location.hash),f=document.getElementById('code');"
    "if(m&&f&&!f.value){f.value=m[1];}if(f&&f.value){document.getElementById('code-row').hidden=true;}"
    "if(location.hash){history.replaceState(null,'',location.pathname);}})();</script>")


def _new_password_fields(autofocus: bool) -> str:
    return (sky.field("Password", '<input type="password" id="f-password" name="password" autocomplete="new-password" '
                      f'minlength="10" required{" autofocus" if autofocus else ""}>', hint="At least 10 characters.")
            + sky.field("Password again", '<input type="password" id="f-password2" name="password2" '
                        'autocomplete="new-password" minlength="10" required>'))


def _code_field(label: str, code: str) -> str:
    # id="code" and the row's id="code-row" are _SETUP_SCRIPT's: it fills the field from the link and hides the row
    return sky.field(label, f'<input type="text" id="code" name="code" class="mono" value="{escape(code)}" '
                     'autocomplete="off" required>', row_id="code-row")


def setup_page(code: str = "", email: str = "", name: str = "", notice: Optional[str] = None, message: str = "",
               retry_after_s: Optional[int] = None, status: int = 200) -> Response:
    """``/setup``: the first admin's form, opened from the link trailbeam start prints (first_admin.py)."""
    text = message or SETUP_NOTICES.get(notice or "", "")
    body = ("<h1>Create your admin account</h1><p class=\"lead\">This account manages the people and tokens of this "
            "Trailbeam. It becomes the first admin, and you are signed in with it.</p>"
            + (sky.notice(text.format(minutes=_minutes(retry_after_s))) if text else "")
            + '<form class="auth-form spaced" method="post" action="/setup">'
            + _code_field("Setup code", code)
            + sky.field("Email", f'<input type="email" id="f-email" name="email" value="{escape(email)}" '
                        'autocomplete="username" required autofocus>')
            + sky.field("Name", f'<input type="text" id="f-name" name="name" value="{escape(name)}" autocomplete="name" '
                        "required>")
            + _new_password_fields(False)
            + '<div class="actions"><button class="btn">Create admin account</button></div></form>' + _SETUP_SCRIPT)
    st, headers, html = sky.page(status, "Create your admin account", body)
    return st, {**headers, "Referrer-Policy": "no-referrer", "Cache-Control": "no-store"}, html


def sso_error_page(message: str, status: int = 400) -> Response:
    """Single sign-on didn't work (oidc.OidcError): why, in words for the person, and a way back."""
    st, headers, html = sky.page(status, "Sign-in failed", f'<h1>Sign-in failed</h1><p class="lead">{escape(message)}</p>'
                                 '<p class="links"><a href="/login">Back to sign in</a></p>')
    return st, {**headers, "Cache-Control": "no-store", "Referrer-Policy": "no-referrer"}, html


PASSWORD_NOTICES = {
    "bad_code": "This link has been used, is out of date (invite links last 7 days, reset links 24 hours), or "
                "isn't the latest one made for you. Ask an admin for a new link.",
    "mismatch": "The two passwords are not the same.",
    "busy": "The server is busy. Try again in a moment.",
    "slow_down": "Too many tries from here. Try again in {minutes}.",
}


def password_page(code: str = "", notice: Optional[str] = None, message: str = "", retry_after_s: Optional[int] = None,
                  email: str = "", status: int = 200) -> Response:
    """``/set-password``: choose a password, from an invite or reset link an admin made on the People page
    (people_api.py). The code travels in the URL fragment, as on /setup."""
    text = message or PASSWORD_NOTICES.get(notice or "", "")
    body = ("<h1>Choose your password</h1><p class=\"lead\">Choose the password you sign in to this Trailbeam with. If "
            "you had one, it stops working, and so do your other sign-ins and your passkeys: add a passkey again on "
            "the Account page if you want one.</p>"
            + (sky.notice(text.format(minutes=_minutes(retry_after_s))) if text else "")
            + '<form class="auth-form spaced" method="post" action="/set-password">'
            + _code_field("Link code", code)
            + (f'<input type="hidden" name="email" value="{escape(email)}" autocomplete="username">' if email else "")
            + _new_password_fields(True)
            + '<div class="actions"><button class="btn">Set password and sign in</button></div></form>' + _SETUP_SCRIPT)
    st, headers, html = sky.page(status, "Choose your password", body)
    return st, {**headers, "Referrer-Policy": "no-referrer", "Cache-Control": "no-store"}, html


def forbidden_page(message: str) -> Response:
    """Signed in, but this page needs more (an admin's, say): why, and the ways on, which the old page's header had."""
    return sky.page(403, "Not allowed", f'<h1>Not allowed</h1><p class="lead">{escape(message)}.</p>'
                    '<p class="links"><a href="/">Messages</a><a href="/account">Your account</a>'
                    '<a href="/logout">Sign out</a></p>')


def _dd(name: str, value: str) -> str:
    return f"<dt>{escape(name)}</dt><dd>{value}</dd>"


def _state(s: str, purge: Optional[dict] = None, amqp: bool = False) -> str:
    """A message's state, and what a recorded PurgeQueue call says about it (contract section 18): "purged
    (observed: PurgeQueue at ...)", or the state with "possibly purged" after it."""
    if amqp:
        title = f' title="{escape(AMQP_STATE_TITLE)}"' if s == "deleted" else ""
        return f'<span class="state state-{escape(s)}"{title}>{escape(AMQP_STATE_LABELS.get(s, s))}</span>'
    out = f'<span class="state state-{escape(s)}">{escape(STATE_LABELS.get(s, s))}</span>'
    if not purge:
        return out
    at = f'PurgeQueue at {_time(purge.get("start"))}'
    if purge.get("status") == "purged":
        return out + f' <span class="muted purge" data-purge="purged">(observed: {at})</span>'
    return (out + f' <span class="purge possibly-purged" data-purge="possibly_purged" '
            f'title="{escape(POSSIBLY_PURGED_TITLE)}">possibly purged ({at})</span>')


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
        what = ("the publish" if _is_amqp(m) else "the send") if m.get("send_observed") else "the earliest step"
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


NO_SPANS = ("No messages yet. No service has sent spans here: the packages send them to /v1/traces on this "
            "server (https://github.com/nick-lennox/trailbeam#quickstart).")


def _store_empty(api) -> bool:
    """Whether the store holds no span at all (/api/health data.last_row is 0), so an empty list means nothing is
    connected yet rather than nothing is wrong."""
    try:
        return api("health", {})["data"]["last_row"] == 0
    except Exception:  # noqa: BLE001 - a page never fails over its empty-state wording
        return False


def _pick(q: dict, *names: str) -> dict:
    """Pass only the named, non-empty parameters on to the API (empty form fields mean "default")."""
    return {k: q[k] for k in names if q.get(k, "") != ""}


def _quote_segment(s: str) -> str:
    return quote(s, safe="")
