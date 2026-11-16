"""The queries behind the screens: one message's timeline, duplicate deliveries, stuck messages, tenants."""
from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone
from typing import Optional

from . import bodies, timeline
from .db import Database

MAX_LIMIT = 1000


def message(db: Database, message_id: str, now_ns: int, lambda_timeout_s: int,
            max_receive_counts: Optional[dict] = None) -> Optional[dict]:
    """``max_receive_counts``: queue name -> maxReceiveCount, as configured (``--max-receive-count``)."""
    rows = db.message_rows(message_id)
    return timeline.build(rows, now_ns, lambda_timeout_s, max_receive_counts) if rows else None


def _summary(db: Database, mid: str, now_ns: int, lambda_timeout_s: int, mrc: Optional[dict] = None) -> dict:
    t = message(db, mid, now_ns, lambda_timeout_s, mrc)
    consumers = sorted({(e["consumer"].get("service_name"), e["consumer"].get("client_id"))
                        for e in t["events"] if e["type"] == "received"}, key=str)
    return {
        "message_id": mid,
        "state": t["state"],
        "source_queue": t["source_queue"],
        "current_queue": t["current_queue"],
        "in_dead_letter_queue": t["in_dead_letter_queue"],
        "dead_letter_queue_move": t["dead_letter_queue_move"],
        "deliveries": t["deliveries"],
        "successful_deliveries": t["successful_deliveries"],
        "consumers": [{"service_name": s, "client_id": c} for s, c in consumers],
        "first_seen": t["first_seen"],
        "first_seen_clock": t["first_seen_clock"],
        "last_seen": t["last_seen"],
    }


def _queue_filter(queue: Optional[str]) -> tuple[str, tuple]:
    if not queue:
        return "", ()
    return " AND EXISTS (SELECT 1 FROM spans q WHERE q.mid = s.mid AND q.queue = ?)", (queue,)


def recent(db: Database, now_ns: int, lambda_timeout_s: int, limit: int, queue: Optional[str] = None,
           mrc: Optional[dict] = None, tenant: Optional[str] = None) -> list[dict]:
    """The most recently active messages; ``tenant``: only those with a span naming it (contract/tenants.md)."""
    qsql, qp = _queue_filter(queue)
    tsql, tp = tenant_filter_sql(tenant)
    qsql, qp = qsql + tsql, qp + tp
    mids = db.query(
        f"""SELECT s.mid, MAX(s.time_ns) AS last FROM spans s WHERE s.mid IS NOT NULL {qsql}
            GROUP BY s.mid ORDER BY last DESC LIMIT ?""", qp + (limit,))
    return [_summary(db, r["mid"], now_ns, lambda_timeout_s, mrc) for r in mids]


def body_search(db: Database, now_ns: int, lambda_timeout_s: int, limit: int, queue: Optional[str],
                terms: list[dict], mrc: Optional[dict] = None) -> list[dict]:
    """Messages whose kept body matches the body.* terms (bodies.py), most recently seen first."""
    mids = bodies.search(db, terms, db.store_bodies)
    if queue:
        inq = {r["mid"] for r in db.query("SELECT DISTINCT mid FROM spans WHERE queue = ? AND mid IS NOT NULL", (queue,))}
        mids = [m for m in mids if m in inq]
    return [_summary(db, m, now_ns, lambda_timeout_s, mrc) for m in mids[:limit]]


def duplicates(db: Database, now_ns: int, lambda_timeout_s: int, limit: int,
               queue: Optional[str] = None, reason: Optional[str] = None, mrc: Optional[dict] = None) -> list[dict]:
    """Messages received more than once, or that succeeded more than once.

    A success is a delivery that ended in an accepted delete (counted once per delivery, however many
    times its handle was deleted) or a Lambda ``success`` outcome.
    """
    qsql, qp = _queue_filter(queue)
    having = {"received_more_than_once": "receives > 1", "succeeded_more_than_once": "successes > 1",
              None: "receives > 1 OR successes > 1"}[reason]
    mids = db.query(
        f"""SELECT s.mid,
                   SUM(s.kind = 'receive') AS receives,
                   COUNT(DISTINCT CASE WHEN s.kind = 'delete' AND s.result = 'ok' THEN s.receive_row END)
                   + COUNT(DISTINCT CASE WHEN s.kind = 'outcome' AND s.outcome = 'success'
                                         THEN s.faas_invocation_id END) AS successes,
                   MAX(s.time_ns) AS last
            FROM spans s WHERE s.mid IS NOT NULL {qsql}
            GROUP BY s.mid HAVING {having}
            ORDER BY last DESC LIMIT ?""", qp + (limit,))
    out = []
    for r in mids:
        item = _summary(db, r["mid"], now_ns, lambda_timeout_s, mrc)
        item["reasons"] = ([ "received_more_than_once"] if item["deliveries"] > 1 else []) + \
                          (["succeeded_more_than_once"] if item["successful_deliveries"] > 1 else [])
        out.append(item)
    return out


def stuck(db: Database, now_ns: int, lambda_timeout_s: int, older_than_s: int, limit: int,
          queue: Optional[str] = None, mrc: Optional[dict] = None) -> list[dict]:
    """Messages first seen more than ``older_than_s`` ago that have no final delete.

    Final means an accepted delete whose receipt handle is the one from the message's latest receive
    (span contract section 4.3), or a Lambda ``success`` outcome for the latest receive. First seen is
    as in :func:`timeline.first_seen_ns`: the send time on the producer host's clock when the send was
    observed, and only otherwise SQS's SentTimestamp, on SQS's clock. For those messages the cutoff and
    ``age_s`` compare SQS's clock with this server's, and each such item says so (``age_across_clocks``).
    """
    cutoff = now_ns - older_than_s * 1_000_000_000
    qsql, qp = _queue_filter(queue)
    rows = db.query(
        f"""WITH latest AS (
                SELECT mid, row AS rrow, faas_invocation_id AS inv FROM (
                    SELECT mid, row, faas_invocation_id,
                           ROW_NUMBER() OVER (PARTITION BY mid ORDER BY time_ns DESC, row DESC) AS rn
                    FROM spans WHERE kind = 'receive')
                WHERE rn = 1),
            seen AS (
                SELECT s.mid, MIN(CASE WHEN s.kind = 'send' THEN s.time_ns END) AS send_ns,
                       MIN(CASE WHEN s.kind = 'receive' THEN s.sent_ts_ms END) * 1000000 AS sqs_ns,
                       MIN(s.time_ns) AS any_ns
                FROM spans s WHERE s.mid IS NOT NULL AND s.kind IN ('send', 'receive') {qsql}
                GROUP BY s.mid),
            first AS (
                SELECT mid, COALESCE(send_ns, sqs_ns, any_ns) AS first_ns,
                       send_ns IS NULL AND sqs_ns IS NOT NULL AS on_sqs_clock
                FROM seen)
            SELECT f.mid, f.first_ns, f.on_sqs_clock FROM first f LEFT JOIN latest l ON l.mid = f.mid
            WHERE f.first_ns < ?
              AND NOT EXISTS (SELECT 1 FROM spans d WHERE d.kind = 'delete' AND d.result = 'ok'
                              AND d.receive_row = l.rrow)
              AND NOT EXISTS (SELECT 1 FROM spans o WHERE o.kind = 'outcome' AND o.outcome = 'success'
                              AND o.mid = f.mid AND o.faas_invocation_id = l.inv)
            ORDER BY f.first_ns LIMIT ?""", qp + (cutoff, limit))
    out = []
    for r in rows:
        item = _summary(db, r["mid"], now_ns, lambda_timeout_s, mrc)
        item["age_s"] = round((now_ns - r["first_ns"]) / 1e9, 3)
        # True when first seen is SQS's SentTimestamp: the age is this server's clock minus SQS's, so skew
        # between the two clocks is in it.
        item["age_across_clocks"] = bool(r["on_sqs_clock"])
        out.append(item)
    return out


# ---- tenants (contract/tenants.md) -------------------------------------------------------------

TENANT_ATTR = "trailbeam.tenant"
TENANT_KINDS = ("send", "receive", "outcome")
TENANT_MAX_CHARS = 256
TENANT_MESSAGES = 2000  # the most messages the Tenants page reads (contract/tenants.md)
_NS_PER_MS = 1_000_000
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
# a span's tenant, in SQL: a non-empty string, cut to 256 characters
_TENANT_SQL = (f"""CASE WHEN json_type(attributes, '$."{TENANT_ATTR}"') = 'text'
                         AND json_extract(attributes, '$."{TENANT_ATTR}"') <> ''
                       THEN substr(json_extract(attributes, '$."{TENANT_ATTR}"'), 1, {TENANT_MAX_CHARS}) END""")


def span_tenant(row) -> Optional[str]:
    """The tenant a stored span names, or None: only send, receive and outcome spans name one."""
    if row["kind"] not in TENANT_KINDS:
        return None
    v = json.loads(row["attributes"]).get(TENANT_ATTR)
    return v[:TENANT_MAX_CHARS] if isinstance(v, str) and v else None


def message_tenant(rows) -> Optional[str]:
    """A message's tenant: its earliest send's, else its earliest receive's or outcome's."""
    ordered = sorted(rows, key=lambda r: (r["kind"] != "send", r["time_ns"], r["trace_id"], r["span_id"]))
    return next((t for t in map(span_tenant, ordered) if t is not None), None)


def _ms(ns) -> int:
    return int(ns) // _NS_PER_MS


def _iso_ms(text: str) -> int:
    # timeline.iso: 2025-11-14T00:26:40.123456Z, always UTC with microseconds
    dt = datetime.strptime(text, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    return (dt - _EPOCH) // timedelta(milliseconds=1)  # exact: no float on the way


def p95(values: list) -> Optional[int]:
    """Nearest-rank p95: the value at position ceil(0.95 n), counting from 1. None for no values."""
    if not values:
        return None
    s = sorted(values)
    return s[max(0, math.ceil(0.95 * len(s)) - 1)]


def tenant_counts(timeline_events: list, tenant: Optional[str], from_ms: int, to_ms: int) -> dict:
    """What one message adds to the Tenants page: its sends, first deliveries' times in queue and
    whether it was dead-lettered, each only when its time is in [from_ms, to_ms]."""
    sends = [e for e in timeline_events if e["type"] == "sent"]
    first_send = _ms(sends[0]["end_unix_nano"]) if sends else None
    out = {"tenant": tenant, "sent": 0, "waits": [], "dead_lettered": False}
    for e in timeline_events:
        if e["type"] == "sent":
            if from_ms <= _ms(e["end_unix_nano"]) <= to_ms:
                out["sent"] += 1
        elif e["type"] == "received":
            at = _ms(e["end_unix_nano"])
            if e.get("delivery") != 1 or e.get("receive_count") != 1 or not from_ms <= at <= to_ms:
                continue
            sent_at = first_send if first_send is not None else e.get("sent_timestamp_ms")
            if isinstance(sent_at, int):
                out["waits"].append(max(0, at - sent_at))
        elif e["type"] == "moved_to_dead_letter_queue":
            to = (e.get("window") or {}).get("to")
            if to and from_ms <= _iso_ms(to) <= to_ms:
                out["dead_lettered"] = True
    return out


def tenant_stats(per_message: list, capped: bool = False) -> dict:
    """The Tenants page from each message's tenant_counts (contract/tenants.md)."""
    by: dict = {}
    total = untagged = 0
    for m in per_message:
        total += m["sent"]
        if m["tenant"] is None:
            untagged += m["sent"]
            continue
        if not (m["sent"] or m["waits"] or m["dead_lettered"]):
            continue
        g = by.setdefault(m["tenant"], {"sent": 0, "waits": [], "dead_lettered": 0})
        g["sent"] += m["sent"]
        g["waits"] += m["waits"]
        g["dead_lettered"] += 1 if m["dead_lettered"] else 0
    rows = [{"tenant": t, "sent": g["sent"], "share": g["sent"] / total if total else 0,
             "p95_in_queue_ms": p95(g["waits"]), "first_deliveries": len(g["waits"]),
             "dead_lettered": g["dead_lettered"]} for t, g in by.items()]
    rows.sort(key=lambda r: (-r["sent"], -r["dead_lettered"], r["tenant"]))
    return {"total_sent": total, "untagged_sent": untagged, "capped": capped, "rows": rows}


def tenants(db: Database, now_ns: int, lambda_timeout_s: int, from_ms: int, to_ms: int,
            mrc: Optional[dict] = None, max_messages: int = TENANT_MESSAGES) -> dict:
    """The Tenants page for [from_ms, to_ms], both included (contract/tenants.md)."""
    mids = db.query(
        """SELECT mid, MAX(time_ns) AS last FROM spans WHERE mid IS NOT NULL AND time_ns >= ? AND time_ns < ?
           GROUP BY mid ORDER BY last DESC, mid LIMIT ?""",
        (from_ms * _NS_PER_MS, (to_ms + 1) * _NS_PER_MS, max_messages + 1))
    per_message = []
    for r in mids[:max_messages]:
        rows = db.message_rows(r["mid"])
        t = timeline.build(rows, now_ns, lambda_timeout_s, mrc)
        per_message.append(tenant_counts(t["events"], message_tenant(rows), from_ms, to_ms))
    out = tenant_stats(per_message, capped=len(mids) > max_messages)
    return {"from_ms": from_ms, "to_ms": to_ms, **out}


def tenant_filter_sql(tenant: Optional[str]) -> tuple[str, tuple]:
    """A WHERE fragment for spans aliased s: the message has a span naming the tenant."""
    if not tenant:
        return "", ()
    return (f" AND EXISTS (SELECT 1 FROM spans tn WHERE tn.mid = s.mid AND tn.kind IN {TENANT_KINDS!r}"
            f" AND {_TENANT_SQL.replace('attributes', 'tn.attributes')} = ?)", (tenant[:TENANT_MAX_CHARS],))


def find_tenants(db: Database, text: str, limit: int = 8) -> list[str]:
    """Tenants whose name starts with the text, ignoring ASCII case; an exact match first (contract/tenants.md)."""
    if not text:
        return []
    like = "".join("\\" + c if c in "\\%_" else c for c in _ascii_lower(text)) + "%"
    rows = db.query(
        f"""SELECT tenant, MAX(time_ns) AS last FROM (
                SELECT {_TENANT_SQL} AS tenant, time_ns FROM spans WHERE kind IN {TENANT_KINDS!r})
            WHERE tenant IS NOT NULL AND LOWER(tenant) LIKE ? ESCAPE '\\'
            GROUP BY tenant ORDER BY LOWER(tenant) = ? DESC, last DESC, tenant LIMIT ?""",
        (like, _ascii_lower(text), limit))
    return [r["tenant"] for r in rows]


def _ascii_lower(s: str) -> str:
    return s.translate(_ASCII_LOWER)


_ASCII_LOWER = str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz")
