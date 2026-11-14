"""The web screen: server-rendered HTML pages built from the JSON API's responses (docs/store-api.md).

Pages:

- ``/``: look up a message by ID, and the most recent messages.
- ``/message?id=...``: one message's timeline. Each observed step shows its interval (request sent to
  response received). Steps whose intervals overlap are drawn side by side in one box labelled
  "concurrent, order unknown". Inferred steps are drawn differently and labelled "inferred".
- ``/duplicates``: duplicate deliveries.
- ``/stuck``: stuck messages.

Each page calls the same ``App.api`` the JSON endpoints use, so the screen shows exactly what the API
returns. No JavaScript, no external assets: everything is in one response.
"""
from __future__ import annotations

from html import escape
from typing import Callable, Iterable, Optional
from urllib.parse import quote, urlencode

from . import __version__

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
    "moved_back_to_source_queue": "Moved back to source queue",
    "deleted_by_lambda": "Deleted by Lambda",
    "lambda_invocation_ended_without_outcome": "Lambda invocation ended without an outcome",
}

DUPLICATE_REASONS = {
    "received_more_than_once": "received more than once",
    "succeeded_more_than_once": "succeeded more than once",
}

CSS = """
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
.badge{display:inline-block;font-size:.75em;font-weight:700;text-transform:uppercase;letter-spacing:.04em;
  padding:.05rem .4rem;border-radius:3px;background:#c98a1b;color:#fff;margin-right:.4rem}
.when{color:#5b6475;font-family:ui-monospace,Menlo,Consolas,monospace;font-size:.85em;margin-right:.6rem}
.etype{font-weight:600}.detail{color:#3c4352;font-size:.9em;margin-top:.2rem}
.reason{font-style:italic;margin-top:.2rem}
ol.timeline li.group{background:none;border:2px dashed #8a94a8;border-left:2px dashed #8a94a8;padding:.4rem .6rem}
.group-head{font-size:.85em;color:#3c4352;margin-bottom:.2rem}
.badge-concurrent{background:#5b6475}
ol.side{list-style:none;padding:0;margin:0;display:flex;flex-wrap:wrap;gap:.5rem}
ol.side>li{flex:1 1 18rem;margin:0 !important}
.notice{background:#fff4d6;border:1px solid #e6c25c;padding:.5rem .8rem;margin:.4rem 0;border-radius:3px}
.error{background:#fde2e2;border:1px solid #d98c8c;padding:.6rem .9rem;border-radius:3px}
.muted{color:#6b7385}form.filters{margin:.6rem 0 1rem;display:flex;gap:.6rem;align-items:center;flex-wrap:wrap}
footer{color:#6b7385;font-size:.8em;padding:1rem 1.2rem}
"""


def handle(api: Callable[[str, dict], object], api_error: type, path: str, query: dict) -> Optional[Response]:
    """Render the page for ``path``; None if ``path`` is not a page. ``api`` is ``App.api``."""
    pages = {"/": _index, "/message": _message, "/duplicates": _duplicates, "/stuck": _stuck}
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
    data = api("messages", _pick(q, "limit", "queue"))
    body = ("<h1>Look up a message</h1>" + _lookup_form("")
            + "<h2>Recent messages</h2>" + _filters("/", q, [("queue", "Queue", "text"), ("limit", "Limit", "number")])
            + _summary_table(data["messages"], empty="No messages stored yet."))
    return _page(200, "qv2", body)


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
    parts.append(_dd("Send observed", "yes" if t.get("send_observed") else "no"))
    parts.append(_dd("Deliveries", f'{t["deliveries"]} ({t["successful_deliveries"]} successful)'))
    parts.append(_dd("First seen", _time(t.get("first_seen"))))
    parts.append(_dd("Last seen", _time(t.get("last_seen"))))
    parts.append("</dl>")
    parts.append('<h2>Timeline</h2><p class="muted">Each step shows when its SQS request was sent and when the '
                 "response came back; SQS acted somewhere in between. A step is shown above another only if it "
                 "ended before the other started. Steps in a box marked "
                 '<span class="badge badge-concurrent">concurrent</span> can\'t be put in one order, because '
                 "some of them overlapped in time (each step overlaps at least one other in the box, not "
                 "necessarily all of them), so their order is unknown: they are side by side, in no particular "
                 "order. Steps marked "
                 '<span class="badge">inferred</span> were deduced by the store; no span recorded them, so they '
                 "have a time window instead of a time.</p>")
    parts.append('<ol class="timeline">')
    parts.extend(_timeline_items(t))
    parts.append("</ol>")
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
            "Oldest first.</p>"
            + _filters("/stuck", {**q, "older_than_s": str(data["older_than_s"])},
                       [("older_than_s", "Older than (s)", "number"), ("queue", "Queue", "text"),
                        ("limit", "Limit", "number")])
            + _summary_table(data["messages"], extra=("Age", lambda m: escape(_age(m.get("age_s")))),
                             empty="No stuck messages."))
    return _page(200, "Stuck messages", body)


# ---- pieces -----------------------------------------------------------------------------------


def _timeline_items(t: dict) -> list[str]:
    """One <li> per step, except that each concurrent group is one <li> holding its steps side by side."""
    concurrent = {g["group"]: g for g in t.get("groups", []) if g.get("concurrent")}
    out: list[str] = []
    evs = t["events"]
    i = 0
    while i < len(evs):
        g = evs[i].get("group")
        if g not in concurrent:
            out.append(_event(evs[i]))
            i += 1
            continue
        j = i
        while j < len(evs) and evs[j].get("group") == g:
            j += 1
        info = concurrent[g]
        n = info.get("observed_events", 0) + info.get("inferred_events", 0) or j - i
        out.append(f'<li class="group concurrent" data-group="{int(g)}">'
                   f'<div class="group-head"><span class="badge badge-concurrent">concurrent</span>'
                   f'<strong>{escape(info.get("label") or "concurrent, order unknown")}</strong>: the store can\'t put '
                   f"these {int(n)} steps in one order; some of them overlapped in time.</div>"
                   '<ol class="side">' + "".join(_event(e) for e in evs[i:j]) + "</ol></li>")
        i = j
    return out


def _event(e: dict) -> str:
    inferred = bool(e.get("inferred"))
    label = escape(EVENT_LABELS.get(e["type"], e["type"]))
    failed = e.get("result") == "failed" or (e["type"] == "lambda_outcome" and e.get("outcome") != "success")
    cls = ["event", "inferred" if inferred else "observed"] + (["failed"] if failed else [])
    head = []
    if inferred:
        head.append('<span class="badge">inferred</span>')
        w = e.get("window") or {}
        head.append(f'<span class="when">between {_time(w.get("from"))} and {_time(w.get("to"))}</span>')
    else:
        head.append(f'<span class="when">{_interval(e)}</span>')
    head.append(f'<span class="etype">{label}</span>')
    details = _details(e)
    out = [f'<li class="{" ".join(cls)}" data-type="{escape(e["type"])}" data-inferred="{str(inferred).lower()}">',
           "".join(head)]
    if details:
        out.append(f'<div class="detail">{" · ".join(details)}</div>')
    if inferred and e.get("reason"):
        out.append(f'<div class="reason">{escape(e["reason"])}</div>')
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
        consumers = ", ".join(escape(str(c.get("service_name") or "?"))
                              + (f' <span class="muted">{escape(str(c["client_id"]))}</span>' if c.get("client_id") else "")
                              for c in m.get("consumers", []))
        cells = [f'<a class="mono" href="/message?{escape(urlencode({"id": m["message_id"]}))}">{escape(m["message_id"])}</a>',
                 _state(m["state"]), queue, f'{m["deliveries"]} ({m["successful_deliveries"]} ok)', consumers,
                 _time(m.get("first_seen")), _time(m.get("last_seen"))]
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
            f"<title>{escape(title)} - qv2</title><style>{CSS}</style></head><body>"
            '<header><a class="brand" href="/">qv2</a><a href="/">Messages</a>'
            '<a href="/duplicates">Duplicates</a><a href="/stuck">Stuck</a>'
            '<form method="get" action="/message"><input type="text" name="id" placeholder="Message ID">'
            "<button>Go</button></form></header>"
            f"<main>{body}</main><footer>qv2 store {escape(__version__)} · "
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
        dur = (int(e["end_unix_nano"]) - int(e["start_unix_nano"])) / 1e6
        took = f" ({dur:.1f} ms)" if dur < 10_000 else f" ({dur / 1000:.1f} s)"
    except (KeyError, TypeError, ValueError):
        took = ""
    return f"{_time(e['start'])} to {_time(end)}{escape(took)}"


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
