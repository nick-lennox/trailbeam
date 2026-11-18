"""The SQLite summary tables: one row per message (summary.py) and the lists the list pages search by.

- ``messages``: the summary, one row per message (``mid``).
- ``message_queues``: (queue, mid) for every queue the message has a span on, with the message's ``last_ns``,
  so one queue's messages are a range scan by newest span.
- ``message_tenants``: (tenant, mid) for every tenant a span of the message names, with the newest time a span
  named it (``named_ns``) and the message's ``last_ns``: the tenant filter and the tenant search.
- ``tenant_events``: what each message adds to the Tenants page, by time (summary.py).

``refresh`` rewrites these rows for the messages given, from their spans. SqliteStore.insert calls it, in the
insert's transaction, for every message a stored span or a join touched; retention calls ``forget``. So the
tables always say what the spans say, whatever order spans arrive in. ``backfill`` builds them for a file
that has spans but no summaries (migration 7).
"""
from __future__ import annotations

import sqlite3
from typing import Iterable

from . import summary
from .summary import FIELDS

CREATE = """
CREATE TABLE IF NOT EXISTS messages (
    mid TEXT PRIMARY KEY,
    first_ns INTEGER NOT NULL,           -- first seen (timeline.first_seen_ns), on first_clock
    first_clock TEXT NOT NULL,           -- host | sqs
    last_ns INTEGER NOT NULL,            -- the newest span's time_ns
    source_queue TEXT NOT NULL,
    current_queue TEXT NOT NULL,
    in_dlq INTEGER NOT NULL,
    moved INTEGER NOT NULL,              -- an observed move to a dead-letter queue (contract 9b)
    source_receive_count INTEGER NOT NULL,
    deliveries INTEGER NOT NULL,
    successes INTEGER NOT NULL,
    consumers TEXT NOT NULL,             -- JSON [[service_name, client_id], ...]
    final_delete INTEGER NOT NULL,
    lambda_ok INTEGER NOT NULL,
    lambda_wait_ns INTEGER,              -- the latest receive is a Lambda's with no outcome: its time_ns
    tenant TEXT,                         -- the message's tenant (contract/tenants.md)
    dup_received INTEGER NOT NULL,
    dup_succeeded INTEGER NOT NULL,
    stuck_first_ns INTEGER,              -- NULL: no send and no receive, never stuck
    stuck_sqs_clock INTEGER NOT NULL,
    stuck_open INTEGER NOT NULL          -- no final delete, no success for the latest receive
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS messages_last ON messages (last_ns DESC, mid);
CREATE INDEX IF NOT EXISTS messages_duplicates ON messages (last_ns DESC, mid) WHERE dup_received OR dup_succeeded;
CREATE INDEX IF NOT EXISTS messages_stuck ON messages (stuck_first_ns, mid)
    WHERE stuck_open AND stuck_first_ns IS NOT NULL;
CREATE TABLE IF NOT EXISTS message_queues (
    mid TEXT NOT NULL,
    queue TEXT NOT NULL,
    last_ns INTEGER NOT NULL,            -- the message's, as in messages
    PRIMARY KEY (mid, queue)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS message_queues_last ON message_queues (queue, last_ns DESC, mid);
CREATE TABLE IF NOT EXISTS message_tenants (
    mid TEXT NOT NULL,
    tenant TEXT NOT NULL,
    tenant_lower TEXT NOT NULL,          -- ASCII letters lowered, for the search
    named_ns INTEGER NOT NULL,           -- the newest span of the message naming the tenant
    last_ns INTEGER NOT NULL,            -- the message's, as in messages
    PRIMARY KEY (mid, tenant)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS message_tenants_last ON message_tenants (tenant, last_ns DESC, mid);
CREATE INDEX IF NOT EXISTS message_tenants_search ON message_tenants (tenant_lower, tenant, named_ns);
CREATE TABLE IF NOT EXISTS tenant_events (
    at_ms INTEGER NOT NULL,              -- the event's time, in whole milliseconds
    kind TEXT NOT NULL,                  -- sent | first_delivery | dead_lettered
    mid TEXT NOT NULL,
    tenant TEXT,                         -- the message's tenant, NULL if none
    wait_ms INTEGER                      -- first_delivery: its time in queue
);
CREATE INDEX IF NOT EXISTS tenant_events_at ON tenant_events (at_ms);
CREATE INDEX IF NOT EXISTS tenant_events_mid ON tenant_events (mid);
"""

TABLES = ("messages", "message_queues", "message_tenants", "tenant_events")

_UPSERT = (f"INSERT OR REPLACE INTO messages ({', '.join(FIELDS)}) VALUES ({', '.join('?' * len(FIELDS))})")
_ROWS = "SELECT * FROM spans WHERE mid = ? ORDER BY time_ns, row"


def ascii_lower(s: str) -> str:
    return s.translate(_ASCII_LOWER)


_ASCII_LOWER = str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz")


def forget(conn: sqlite3.Connection, mids: Iterable[str]) -> None:
    """Remove the messages' rows from every summary table."""
    ids = [(m,) for m in mids]
    for t in TABLES:
        conn.executemany(f"DELETE FROM {t} WHERE mid = ?", ids)


def refresh(conn: sqlite3.Connection, mids: Iterable[str]) -> None:
    """Rewrite the summary rows of these messages from their spans; a message with no spans left loses them.
    Runs in the caller's transaction. Only rows that changed are written: most spans change a message's
    ``last_ns`` and little else, and every row rewritten is pages of index to write at commit."""
    for mid in {m for m in mids if m is not None}:
        new = _summarize(conn, mid)
        if new is None:
            forget(conn, [mid])
            continue
        cur = conn.cursor()
        cur.row_factory = sqlite3.Row
        old = cur.execute("SELECT * FROM messages WHERE mid = ?", (mid,)).fetchone()
        if old is None:
            write(conn, [new])
            continue
        values = [new[f] for f in FIELDS]
        changed = [f for f, v in zip(FIELDS, values) if old[f] != v]
        if changed:
            conn.execute(f"UPDATE messages SET {', '.join(f + ' = ?' for f in changed)} WHERE mid = ?",
                         [new[f] for f in changed] + [mid])
        last = new["last_ns"]
        _sync(conn, "message_queues", "queue", mid, {q: (last,) for q in new["queues"]}, ("last_ns",))
        _sync(conn, "message_tenants", "tenant", mid,
              {t: (ascii_lower(t), ns, last) for t, ns in new["tenants"].items()}, ("tenant_lower", "named_ns", "last_ns"))
        events = sorted(new["tenant_events"])
        if sorted(tuple(r) for r in conn.execute("SELECT at_ms, kind, wait_ms FROM tenant_events WHERE mid = ?", (mid,))) != events \
                or (events and old["tenant"] != new["tenant"]):
            conn.execute("DELETE FROM tenant_events WHERE mid = ?", (mid,))
            conn.executemany("INSERT INTO tenant_events (at_ms, kind, mid, tenant, wait_ms) VALUES (?, ?, ?, ?, ?)",
                             [(at, kind, mid, new["tenant"], wait) for at, kind, wait in events])


def _sync(conn: sqlite3.Connection, table: str, key: str, mid: str, want: dict, cols: tuple) -> None:
    """Make the message's rows of ``table`` (one per ``key``) hold ``want``: {key: values of cols}."""
    have = {r[0]: tuple(r)[1:] for r in conn.execute(f"SELECT {key}, {', '.join(cols)} FROM {table} WHERE mid = ?", (mid,))}
    for k in have.keys() - want.keys():
        conn.execute(f"DELETE FROM {table} WHERE mid = ? AND {key} = ?", (mid, k))
    for k, v in want.items():
        if k not in have:
            conn.execute(f"INSERT INTO {table} (mid, {key}, {', '.join(cols)}) VALUES (?, ?{', ?' * len(cols)})",
                         (mid, k) + v)
        elif have[k] != v:
            diff = [(c, x) for c, x, y in zip(cols, v, have[k]) if x != y]
            conn.execute(f"UPDATE {table} SET {', '.join(c + ' = ?' for c, _ in diff)} WHERE mid = ? AND {key} = ?",
                         [x for _, x in diff] + [mid, k])


def _summarize(conn: sqlite3.Connection, mid: str):
    cur = conn.cursor()
    cur.row_factory = sqlite3.Row
    rows = cur.execute(_ROWS, (mid,)).fetchall()
    return summary.summarize(rows) if rows else None


def write(conn: sqlite3.Connection, sums: list) -> None:
    """Insert summaries of messages that have no summary rows (``forget`` them first)."""
    conn.executemany(_UPSERT, [[s[f] for f in FIELDS] for s in sums])
    conn.executemany("INSERT INTO message_queues (mid, queue, last_ns) VALUES (?, ?, ?)",
                     [(s["mid"], q, s["last_ns"]) for s in sums for q in s["queues"]])
    conn.executemany("INSERT INTO message_tenants (mid, tenant, tenant_lower, named_ns, last_ns) VALUES (?, ?, ?, ?, ?)",
                     [(s["mid"], t, ascii_lower(t), ns, s["last_ns"]) for s in sums for t, ns in s["tenants"].items()])
    conn.executemany("INSERT INTO tenant_events (at_ms, kind, mid, tenant, wait_ms) VALUES (?, ?, ?, ?, ?)",
                     [(at, kind, s["mid"], s["tenant"], wait) for s in sums for at, kind, wait in s["tenant_events"]])


def backfill(conn: sqlite3.Connection, chunk: int = 5000) -> int:
    """Build the summary rows of every message, in the caller's transaction: one pass over the spans in
    ``mid`` order (the spans_mid index). Returns the number of messages."""
    for t in TABLES:
        conn.execute(f"DELETE FROM {t}")
    cur = conn.cursor()
    cur.row_factory = sqlite3.Row
    cur.execute("SELECT * FROM spans INDEXED BY spans_mid WHERE mid IS NOT NULL ORDER BY mid, time_ns, row")
    n, batch, rows, mid = 0, [], [], None
    for r in cur:
        if r["mid"] != mid and rows:
            batch.append(summary.summarize(rows))
            rows = []
            if len(batch) >= chunk:
                n += len(batch)
                write(conn, batch)
                batch = []
        mid = r["mid"]
        rows.append(r)
    if rows:
        batch.append(summary.summarize(rows))
    write(conn, batch)
    return n + len(batch)
