"""The queries behind the screens: one message's timeline, duplicate deliveries, stuck messages."""
from __future__ import annotations

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
           mrc: Optional[dict] = None) -> list[dict]:
    qsql, qp = _queue_filter(queue)
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
