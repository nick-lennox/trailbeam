"""A message's Body tab, and ``body.*`` search, from the spans the server stored (contract section 15).

Both follow ``contract/body-view.md``. The hosted app implements the same rules in TypeScript (the
``bodyView`` and ``bodyHit`` functions of ``@trailbeam/ui``), and both are checked against the same
expected outputs in ``contract/fixtures/``.

Bodies are only there for queues in ``TRAILBEAM_STORE_BODIES`` (the Store drops the rest before writing).
There are no logins: anyone who can reach the server can read what this module returns.
"""
from __future__ import annotations

import json
import math
import re
from typing import Iterable, NamedTuple, Optional

from .store import Row

# Capture statuses (contract 15.4) that are also a reason for no body. "captured" and "offloaded_to_s3"
# come with a body, so they are not here.
STATUS_REASONS = ("off", "md5_mismatch", "redaction_failed", "exceeds_capture_limit")
REASONS = ("not_opted_in", "producer_not_instrumented", "package_too_old") + STATUS_REASONS + ("unknown",)

CARRIERS = ("send", "receive")  # the span kinds that carry a body (contract 15.2)


def _attrs(r: Row) -> dict:
    return json.loads(r["attributes"])


def _str(v) -> Optional[str]:
    return v if isinstance(v, str) and v != "" else None


def _int(v) -> Optional[int]:
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float) and v.is_integer():
        return int(v)
    if isinstance(v, str) and re.fullmatch(r"-?[0-9]+", v):
        return int(v)
    return None


def _has_body(r: Row) -> bool:
    return r["kind"] in CARRIERS and isinstance(_attrs(r).get("trailbeam.body"), str)


def _served(queue: Optional[str], store_bodies: frozenset) -> bool:
    """Bodies are served only for queues in the server's list now. A queue taken off the list keeps the bodies
    already in the SQLite file (nothing is deleted), but they are no longer shown or searched."""
    return "*" in store_bodies or queue in store_bodies


def kept_body(rows: Iterable[Row], store_bodies: frozenset) -> Optional[Row]:
    """The one body kept for a message: the first send's that arrived, else the first receive's (contract 15.4)."""
    best = None
    for r in rows:
        if not _has_body(r) or not _served(r["queue"], store_bodies):
            continue
        if best is None or (best["kind"] != "send" and r["kind"] == "send") or (
                best["kind"] == r["kind"] and r["row"] < best["row"]):
            best = r
    return best


def body_view(message_id: str, rows: list[Row], store_bodies: frozenset) -> dict:
    """The Body tab of one message (contract/body-view.md): its kept body, or the reason there is none.

    ``rows``: every row of the message, in time order (Store.message_rows). ``store_bodies``: the
    server's TRAILBEAM_STORE_BODIES."""
    carriers = [r for r in rows if r["kind"] in CARRIERS + ("outcome",)]
    ordered = [r for r in carriers if r["kind"] == "send"] + [r for r in carriers if r["kind"] != "send"]

    def first(key: str, read=_str):
        for r in ordered:
            v = read(_attrs(r).get(key))
            if v is not None:
                return r, v
        return None, None

    size = first("messaging.message.body.size", _int)[1]
    offloaded = first("trailbeam.body.offloaded_size", _int)[1]
    kept = kept_body(rows, store_bodies)
    if kept is not None:
        a = _attrs(kept)
        t_row, tenant = first("trailbeam.tenant")
        return {
            "message_id": message_id,
            "kind": "body",
            "body": {
                "body": a["trailbeam.body"],
                "format": _str(a.get("trailbeam.body.format")),
                "redacted_count": _int(a.get("trailbeam.body.redacted_count")),
                "envelope": _str(a.get("trailbeam.body.envelope")),
                "capture_status": _str(a.get("trailbeam.body.capture_status")),
                "from_kind": kept["kind"],
                "service": kept["service_name"],
                "queue": kept["queue"],
                "trace_id": kept["trace_id"],
                "span_id": kept["span_id"],
                "time_unix_nano": str(kept["time_ns"]),
            },
            "size": own if (own := _int(a.get("messaging.message.body.size"))) is not None else size,
            "offloaded_size": (own_off if (own_off := _int(a.get("trailbeam.body.offloaded_size"))) is not None
                               else offloaded),
            "tenant": tenant[:256] if tenant else None,
            "tenant_source": _str(_attrs(t_row).get("trailbeam.tenant.source")) if t_row is not None else None,
        }
    sends = [r for r in rows if r["kind"] == "send"]
    receives = [r for r in rows if r["kind"] == "receive"]
    send_status = next((s for s in (_str(_attrs(r).get("trailbeam.body.capture_status")) for r in sends) if s), None)
    receive_statuses = [s for s in (_str(_attrs(r).get("trailbeam.body.capture_status")) for r in receives) if s]
    if any(r["body_dropped"] or (_has_body(r) and not _served(r["queue"], store_bodies)) for r in rows):
        reason = "not_opted_in"
    elif sends:
        reason = "package_too_old" if not send_status else send_status if send_status in STATUS_REASONS else "unknown"
    else:
        reason = "producer_not_instrumented"
    return {
        "message_id": message_id,
        "kind": "none",
        "reason": reason,
        "size": size,
        "sends": len(sends),
        "receives": len(receives),
        "send_status": send_status,
        "receive_status": receive_statuses[-1] if receive_statuses else None,
    }


# ---- body.* search ------------------------------------------------------------------------------
#
# The body part of the explorer's query language: body:text finds text anywhere in the body, and
# body.<path>:<value> compares the values at a JSON path (body.order.qty:>=2, body.Records.eventName:Object*).
# A path through an array matches if any element does; a number in the path picks one element. Terms on the
# same field are alternatives, different fields must all match, and a leading minus excludes.


class QueryError(ValueError):
    pass


_TOKEN = re.compile(r'(?:[^\s"]+|"[^"]*"?)+')
_FIELD = re.compile(r"^body(\.|\[|$)", re.I)
_UNIT = {"": 1, "ms": 1, "s": 1000, "m": 60_000, "b": 1, "kb": 1024, "mb": 1024 ** 2}
_NUM = re.compile(r"^(>=|<=|>|<)?(\d+(?:\.\d+)?)(ms|s|m|b|kb|mb)?$", re.I)
_RANGE = re.compile(r"^(\d+(?:\.\d+)?)(ms|s|m|b|kb|mb)?\.\.(\d+(?:\.\d+)?)(ms|s|m|b|kb|mb)?$", re.I)


def parse_query(text: str) -> list[dict]:
    """``body.order.sku:A-100 -body.order.qty:1`` -> terms {field, path, value, neg}. Only body terms."""
    terms = []
    for m in _TOKEN.finditer(text or ""):
        raw = m.group(0)
        neg = raw.startswith("-") and len(raw) > 1
        term = raw[1:] if neg else raw
        i = term.find(":")
        name = term[:i] if i > 0 and not term.startswith('"') else ""
        if not _FIELD.match(name):
            raise QueryError(f"{raw!r}: a body search takes body:<text> or body.<path>:<value> terms only")
        field = "body" + name[4:]
        terms.append({"field": field, "path": body_path(field), "value": term[i + 1:].replace('"', ""), "neg": neg})
    if not terms:
        raise QueryError("the body search is empty")
    return terms


def body_path(field: str) -> list[str]:
    """``body.Records[0].key`` -> ["Records", "0", "key"]."""
    return [p for p in re.sub(r"\[(\d+)\]", r".\1", field[4:]).split(".") if p]


def _is_num(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def body_values(node, path: list[str]) -> list:
    """Every scalar at the path; an array on the way is searched element by element."""
    if node is None:
        return []
    if isinstance(node, list):
        if path and re.fullmatch(r"[0-9]+", path[0]):
            k = int(path[0])
            return body_values(node[k], path[1:]) if k < len(node) else []
        return [v for x in node for v in body_values(x, path)]
    if not path:
        return [] if isinstance(node, dict) else [node]
    return body_values(node.get(path[0]), path[1:]) if isinstance(node, dict) else []


def _js_string(v) -> str:
    """String(v) as JavaScript writes it, so both implementations compare the same text."""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, float):
        if v.is_integer() and abs(v) < 1e21:
            return str(int(v))
        if math.isfinite(v):
            return repr(v)
    return str(v)


def _num_hit(x: float, value: str) -> Optional[bool]:
    r = _RANGE.match(value)
    if r:
        return float(r[1]) * _UNIT[(r[2] or "").lower()] <= x < float(r[3]) * _UNIT[(r[4] or "").lower()]
    m = _NUM.match(value)
    if not m:
        return None
    n = float(m[2]) * _UNIT[(m[3] or "").lower()]
    return {">": x > n, ">=": x >= n, "<": x < n, "<=": x <= n}.get(m[1], x == n)


def _text_hit(v, value: str) -> bool:
    s, want = _js_string(v).lower(), value.lower()
    if "*" in want:
        return re.fullmatch(".*".join(re.escape(p) for p in want.split("*")), s) is not None
    return s == want


def parsed_body(body: str, fmt: Optional[str]):
    """A stored body as search reads it: parsed JSON, or the text itself."""
    if fmt != "text":
        try:
            return json.loads(body)
        except ValueError:
            pass
    return body


def term_hit(body, t: dict) -> bool:
    if not t["value"]:
        return True
    if not t["path"]:
        whole = json.dumps(body, ensure_ascii=False, separators=(",", ":"))
        return t["value"].lower() in whole.lower()
    for v in body_values(body, t["path"]):
        n = _num_hit(v, t["value"]) if _is_num(v) else None
        if n if n is not None else _text_hit(v, t["value"]):
            return True
    return False


def matches(body, terms: list[dict]) -> bool:
    by_field: dict[str, list[dict]] = {}
    for t in terms:
        if t["neg"]:
            if term_hit(body, t):
                return False
            continue
        by_field.setdefault(t["field"], []).append(t)
    return all(any(term_hit(body, t) for t in ts) for ts in by_field.values())


class SearchResult(NamedTuple):
    mids: list            # the matching messages, most recently seen first
    after: Optional[tuple]  # (last_ns, mid) of the last message read, to go on from; None: nothing left to read
    scanned: int          # messages read
    oldest_ns: Optional[int]  # last_ns of the oldest message read: the search covered [oldest_ns, to_ns)


SCAN = 5000  # messages a search reads at most; the next page goes on where it stopped
_CHUNK = 500


def _kept(rows: list, store_bodies: frozenset):
    """kept_body over Store.body_spans rows: the first send's body that arrived, else the first receive's."""
    best = None
    for r in rows:
        if not _served(r["queue"], store_bodies):
            continue
        if best is None or (best["kind"] != "send" and r["kind"] == "send") or (
                best["kind"] == r["kind"] and r["row"] < best["row"]):
            best = r
    return best


def search(db, terms: list[dict], store_bodies: frozenset, *, limit: int = 100, queue: Optional[str] = None,
           from_ns: Optional[int] = None, to_ns: Optional[int] = None, after: Optional[tuple] = None,
           scan: int = SCAN) -> SearchResult:
    """The messages whose kept body matches, most recently seen first. ``db``: a Store.

    There is no index of body contents, so a search reads bodies, and it is bounded: it walks the messages last
    seen in [from_ns, to_ns) (on ``queue``, if given), newest first and from ``after`` on, the way the Messages
    page lists them, and reads the kept body of each (Store.body_spans, an index lookup). It stops at ``limit``
    matches or after ``scan`` messages, whichever comes first, and says where (``SearchResult.after``), so the
    caller can go on from there. The cost is ``scan`` messages at most, however many bodies are stored."""
    if not store_bodies:
        return SearchResult([], None, 0, None)
    hits: list = []
    scanned, pos, oldest = 0, after, None
    while scanned < scan:
        want = min(_CHUNK, scan - scanned)
        chunk = db.newest_messages(want, queue, from_ns=from_ns, to_ns=to_ns, after=pos)
        by_mid: dict = {}
        for r in db.body_spans([m for _, m in chunk]) if chunk else ():
            by_mid.setdefault(r["mid"], []).append(r)
        for last, mid in chunk:
            scanned, pos, oldest = scanned + 1, (last, mid), last
            kept = _kept(by_mid.get(mid, ()), store_bodies)
            if kept is not None and matches(parsed_body(kept["body"], _str(kept["format"])), terms):
                hits.append(mid)
                if len(hits) == limit:
                    return SearchResult(hits, pos, scanned, oldest)
        if len(chunk) < want:
            return SearchResult(hits, None, scanned, oldest)  # the window is read to its end
    return SearchResult(hits, pos, scanned, oldest)
