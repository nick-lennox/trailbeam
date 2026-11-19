"""The SQLite summary tables: one row per message (summary.py) and the lists the list pages search by.

- ``messages``: the summary, one row per message (``mid``), with the message's lists as JSON
  (``queues_json``, ``tenants_json``, ``events_json``). Those say which rows of the other tables are the
  message's, so they are found by key, not by ``mid``. ``rows_json`` lists the row IDs of the message's
  spans: its spans are read by primary key, and spans has no index on ``mid`` (schema 11, QV-78). It is a
  rowid table with a unique index on ``mid``, not keyed by ``mid``: a message's row is written again whenever
  it gains a span, nearly always within seconds of the last time, so rows in arrival order keep those writes
  on the last pages of the B-tree. Keyed by the random ``mid``, nearly every write was to a different page.
- ``message_queues``: (queue, last_ns, mid) for every queue the message has a span on, ``last_ns`` the
  message's newest span: one queue's messages, newest first, are a range of the primary key.
- ``message_tenants``: (tenant, last_ns, mid) for every tenant a span of the message names, with the newest
  time a span named it (``named_ns``): the tenant filter, and by ``message_tenants_search`` the tenant search.
- ``tenant_events``: what each message adds to the Tenants page, by time (summary.py), keyed (at_ms, mid, seq).

None of the last three has an index on ``mid``. Every one would be a B-tree with one random key per message,
and a page of it rewritten for nearly every message an export touches: at 1M spans the three were 670 of the
2,320 pages one 512-span export wrote (QV-73). A filter on queue reads ``messages.queues_json`` instead.

``refresh`` rewrites these rows for the messages given, from their spans. SqliteStore.insert calls it, in the
insert's transaction, for every message a stored span or a join touched, with the rows each gained and lost
(``Changes``); retention calls ``forget``. So the tables always say what the spans say, whatever order spans
arrive in: ``rows_json`` is every row whose ``mid`` is the message (tests/server/test_summary.py checks it).
``backfill`` builds them for a file that has spans but no summaries (migration 11).
"""
from __future__ import annotations

import json
import sqlite3
from typing import Iterable, Optional

from . import summary
from .summary import FIELDS

CREATE = """
CREATE TABLE IF NOT EXISTS messages (
    mid TEXT NOT NULL PRIMARY KEY,       -- a unique index: the rows themselves are in arrival order (rowid)
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
    stuck_open INTEGER NOT NULL,         -- no final delete, no success for the latest receive
    queues_json TEXT NOT NULL,           -- JSON [queue, ...]: its message_queues rows
    tenants_json TEXT NOT NULL,          -- JSON {tenant: named_ns}: its message_tenants rows
    events_json TEXT NOT NULL,           -- JSON [[at_ms, kind, wait_ms], ...]: its tenant_events rows, seq = index
    rows_json TEXT NOT NULL              -- JSON [row, ...]: its spans (spans.row, ascending)
);
CREATE INDEX IF NOT EXISTS messages_last ON messages (last_ns DESC, mid);
CREATE INDEX IF NOT EXISTS messages_duplicates ON messages (last_ns DESC, mid) WHERE dup_received OR dup_succeeded;
CREATE INDEX IF NOT EXISTS messages_stuck ON messages (stuck_first_ns, mid)
    WHERE stuck_open AND stuck_first_ns IS NOT NULL;
CREATE TABLE IF NOT EXISTS message_queues (
    queue TEXT NOT NULL,
    last_ns INTEGER NOT NULL,            -- the message's, as in messages
    mid TEXT NOT NULL,
    PRIMARY KEY (queue, last_ns DESC, mid)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS message_tenants (
    tenant TEXT NOT NULL,
    last_ns INTEGER NOT NULL,            -- the message's, as in messages
    mid TEXT NOT NULL,
    tenant_lower TEXT NOT NULL,          -- ASCII letters lowered, for the search
    named_ns INTEGER NOT NULL,           -- the newest span of the message naming the tenant
    PRIMARY KEY (tenant, last_ns DESC, mid)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS message_tenants_search ON message_tenants (tenant_lower, tenant, named_ns);
CREATE TABLE IF NOT EXISTS tenant_events (
    at_ms INTEGER NOT NULL,              -- the event's time, in whole milliseconds
    mid TEXT NOT NULL,
    seq INTEGER NOT NULL,                -- its place in the message's events_json
    kind TEXT NOT NULL,                  -- sent | first_delivery | dead_lettered
    tenant TEXT,                         -- the message's tenant, NULL if none
    wait_ms INTEGER,                     -- first_delivery: its time in queue
    PRIMARY KEY (at_ms, mid, seq)
) WITHOUT ROWID;
"""

TABLES = ("messages", "message_queues", "message_tenants", "tenant_events")
LISTS = ("queues_json", "tenants_json", "events_json")  # the lists that key the other tables' rows
COLUMNS = FIELDS + LISTS  # the summary columns both stores keep (postgres.py imports these)
_SQLITE_COLUMNS = COLUMNS + ("rows_json",)  # SQLite also lists a message's span rows: it has no spans_mid index

_UPSERT = f"INSERT INTO messages ({', '.join(_SQLITE_COLUMNS)}) VALUES ({', '.join('?' * len(_SQLITE_COLUMNS))})"
_ROWS = "SELECT * FROM spans WHERE row IN (SELECT value FROM json_each(?))"
_OLD = f"SELECT mid, last_ns, tenant, {', '.join(LISTS)} FROM messages WHERE mid = ?"
_DEL_QUEUE = "DELETE FROM message_queues WHERE queue = ? AND last_ns = ? AND mid = ?"
_ADD_QUEUE = "INSERT INTO message_queues (queue, last_ns, mid) VALUES (?, ?, ?)"
_DEL_TENANT = "DELETE FROM message_tenants WHERE tenant = ? AND last_ns = ? AND mid = ?"
_ADD_TENANT = "INSERT INTO message_tenants (tenant, last_ns, mid, tenant_lower, named_ns) VALUES (?, ?, ?, ?, ?)"
_DEL_EVENT = "DELETE FROM tenant_events WHERE at_ms = ? AND mid = ? AND seq = ?"
_ADD_EVENT = "INSERT INTO tenant_events (at_ms, mid, seq, kind, tenant, wait_ms) VALUES (?, ?, ?, ?, ?, ?)"


def ascii_lower(s: str) -> str:
    return s.translate(_ASCII_LOWER)


_ASCII_LOWER = str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz")


class Changes:
    """The rows each message gained and lost in a transaction, for ``refresh``. A row joined to another message
    (the receive join) is ``move``d: lost by the message it was in, if any, gained by the new one."""

    def __init__(self):
        self.added: dict = {}
        self.removed: dict = {}

    def add(self, mid, row: int) -> None:
        if mid is not None:
            self.added.setdefault(mid, set()).add(row)
            self.removed.get(mid, set()).discard(row)

    def move(self, row: int, old, new) -> None:
        if old == new:
            return
        if old is not None:
            self.removed.setdefault(old, set()).add(row)
            self.added.get(old, set()).discard(row)
        self.add(new, row)

    def __iter__(self):
        return iter(self.added.keys() | self.removed.keys())

    def __bool__(self):
        return bool(self.added or self.removed)


def rows_of(conn: sqlite3.Connection, mid: str, select: Optional[str] = None) -> list:
    """Every span of a message, from its summary's rows_json, in time order; [] if it has none. ``select``: a
    SELECT of spans ``s`` to read them with (body_table.rows_sql, which puts each body back), else the rows."""
    got = conn.execute("SELECT rows_json FROM messages WHERE mid = ?", (mid,)).fetchone()
    if got is None:
        return []
    cur = conn.cursor()
    cur.row_factory = sqlite3.Row
    sql = _ROWS if select is None else f"{select} WHERE s.row IN (SELECT value FROM json_each(?))"
    return sorted(cur.execute(sql, (got[0],)).fetchall(), key=lambda r: (r["time_ns"], r["row"]))


def _lists(s) -> tuple:
    """A summary's three lists, as stored in messages."""
    return (json.dumps(s["queues"]), json.dumps(s["tenants"], sort_keys=True),
            json.dumps(sorted(list(e) for e in s["tenant_events"])))


def _keys(old) -> tuple:
    """A stored message's rows of the other tables, from its messages row (last_ns, tenant and the lists)."""
    queues = [(q, old["last_ns"], old["mid"]) for q in json.loads(old["queues_json"])]
    return queues, json.loads(old["tenants_json"]), json.loads(old["events_json"])


def forget(conn: sqlite3.Connection, mids: Iterable[str]) -> None:
    """Remove the messages' rows from every summary table."""
    cur = conn.cursor()
    cur.row_factory = sqlite3.Row
    for mid in mids:
        old = cur.execute(_OLD, (mid,)).fetchone()
        if old is not None:
            _drop(conn, old)
            conn.execute("DELETE FROM messages WHERE mid = ?", (mid,))


def prefetch(conn: sqlite3.Connection, mids: Iterable[str]) -> None:
    """Read, and so bring into the page cache, every index entry ``forget`` would change for these messages.
    For retention: it runs on a read connection without the writer lock, so that the write that follows finds
    those pages in memory instead of reading them from disk while ingest waits (QV-72: steps of seconds at
    1M spans, on pages of old messages no page had read for days). Changes nothing."""
    cur = conn.cursor()
    cur.row_factory = sqlite3.Row
    for mid in mids:
        old = cur.execute(_OLD, (mid,)).fetchone()
        if old is None:
            continue
        queues, tenants, events = _keys(old)
        last = old["last_ns"]
        conn.execute("SELECT 1 FROM messages INDEXED BY messages_last WHERE last_ns = ? AND mid = ?", (last, mid)).fetchall()
        conn.execute("SELECT 1 FROM messages INDEXED BY messages_stuck WHERE stuck_open AND stuck_first_ns IS NOT NULL "
                     "AND stuck_first_ns = (SELECT stuck_first_ns FROM messages WHERE mid = ?) AND mid = ?", (mid, mid)).fetchall()
        for q in queues:
            conn.execute("SELECT 1 FROM message_queues WHERE queue = ? AND last_ns = ? AND mid = ?", q).fetchall()
        for t, named in tenants.items():
            conn.execute("SELECT 1 FROM message_tenants WHERE tenant = ? AND last_ns = ? AND mid = ?", (t, last, mid)).fetchall()
            conn.execute("SELECT 1 FROM message_tenants INDEXED BY message_tenants_search "
                         "WHERE tenant_lower = ? AND tenant = ? AND named_ns = ?", (ascii_lower(t), t, named)).fetchall()
        for i, e in enumerate(events):
            conn.execute("SELECT 1 FROM tenant_events WHERE at_ms = ? AND mid = ? AND seq = ?", (e[0], mid, i)).fetchall()


def _drop(conn: sqlite3.Connection, old) -> None:
    queues, tenants, events = _keys(old)
    last, mid = old["last_ns"], old["mid"]
    conn.executemany(_DEL_QUEUE, queues)
    conn.executemany(_DEL_TENANT, [(t, last, mid) for t in tenants])
    conn.executemany(_DEL_EVENT, [(e[0], mid, i) for i, e in enumerate(events)])


def refresh(conn: sqlite3.Connection, changes: Changes) -> None:
    """Rewrite the summary rows of the messages in ``changes`` from their spans: the rows in their rows_json,
    plus those they gained, less those they lost. A message with no spans left loses its summary rows. Runs in
    the caller's transaction. Only rows that changed are written: most spans change a message's ``last_ns`` and
    little else, and every row rewritten is pages of index to write at commit."""
    cur = conn.cursor()
    cur.row_factory = sqlite3.Row
    for mid in changes:
        old = cur.execute("SELECT * FROM messages WHERE mid = ?", (mid,)).fetchone()
        rows = set(json.loads(old["rows_json"])) if old is not None else set()
        rows = (rows | changes.added.get(mid, set())) - changes.removed.get(mid, set())
        spans = cur.execute(_ROWS, (json.dumps(sorted(rows)),)).fetchall() if rows else []
        new = summary.summarize(spans) if spans else None
        if new is not None:
            new["rows_json"] = json.dumps(sorted(r["row"] for r in spans))
        if new is None:
            if old is not None:
                _drop(conn, old)
                conn.execute("DELETE FROM messages WHERE mid = ?", (mid,))
            continue
        if old is None:
            write(conn, [new])
            continue
        lists = _lists(new)
        values = [new[f] for f in FIELDS] + list(lists) + [new["rows_json"]]
        changed = [(c, v) for c, v in zip(_SQLITE_COLUMNS, values) if old[c] != v]
        if not changed:
            continue
        conn.execute(f"UPDATE messages SET {', '.join(c + ' = ?' for c, _ in changed)} WHERE mid = ?",
                     [v for _, v in changed] + [mid])
        last, was_last = new["last_ns"], old["last_ns"]
        old_queues, old_tenants, old_events = _keys(old)
        if last != was_last or lists[0] != old["queues_json"]:
            conn.executemany(_DEL_QUEUE, old_queues)
            conn.executemany(_ADD_QUEUE, [(q, last, mid) for q in new["queues"]])
        if last != was_last:
            conn.executemany(_DEL_TENANT, [(t, was_last, mid) for t in old_tenants])
            conn.executemany(_ADD_TENANT, [(t, last, mid, ascii_lower(t), ns) for t, ns in new["tenants"].items()])
        elif lists[1] != old["tenants_json"]:
            want = new["tenants"]
            conn.executemany(_DEL_TENANT, [(t, last, mid) for t in old_tenants if t not in want])
            conn.executemany(_ADD_TENANT, [(t, last, mid, ascii_lower(t), ns) for t, ns in want.items()
                                           if t not in old_tenants])
            conn.executemany("UPDATE message_tenants SET named_ns = ? WHERE tenant = ? AND last_ns = ? AND mid = ?",
                             [(ns, t, last, mid) for t, ns in want.items() if t in old_tenants and old_tenants[t] != ns])
        if lists[2] != old["events_json"] or (old_events and old["tenant"] != new["tenant"]):
            conn.executemany(_DEL_EVENT, [(e[0], mid, i) for i, e in enumerate(old_events)])
            conn.executemany(_ADD_EVENT, _events(new, json.loads(lists[2])))


def _events(s, events: list) -> list:
    return [(at, s["mid"], i, kind, s["tenant"], wait) for i, (at, kind, wait) in enumerate(events)]


def write(conn: sqlite3.Connection, sums: list) -> None:
    """Insert summaries of messages that have no summary rows (``forget`` them first). Each has ``rows_json``."""
    rows = []
    for s in sums:
        lists = _lists(s)
        rows.append(([s[f] for f in FIELDS] + list(lists) + [s["rows_json"]], json.loads(lists[2])))
    conn.executemany(_UPSERT, [r for r, _ in rows])
    conn.executemany(_ADD_QUEUE, [(q, s["last_ns"], s["mid"]) for s in sums for q in s["queues"]])
    conn.executemany(_ADD_TENANT, [(t, s["last_ns"], s["mid"], ascii_lower(t), ns)
                                   for s in sums for t, ns in s["tenants"].items()])
    conn.executemany(_ADD_EVENT, [e for s, (_, ev) in zip(sums, rows) for e in _events(s, ev)])


def backfill(conn: sqlite3.Connection, chunk: int = 5000) -> int:
    """Build the summary rows of every message, in the caller's transaction: one pass over the spans in
    ``mid`` order: by the spans_mid index of schema 9 (migration 11 runs it before it rebuilds spans without that
    index), or on a file without it by a sort of every span (tests). Returns the number of messages."""
    for t in TABLES:
        conn.execute(f"DELETE FROM {t}")
    cur = conn.cursor()
    cur.row_factory = sqlite3.Row
    by = "INDEXED BY spans_mid" if conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'spans_mid'").fetchone() else ""
    cur.execute(f"SELECT * FROM spans {by} WHERE mid IS NOT NULL ORDER BY mid, time_ns, row")
    n, batch, rows, mid = 0, [], [], None
    def done(rows):
        s = summary.summarize(rows)
        s["rows_json"] = json.dumps(sorted(r["row"] for r in rows))
        return s

    for r in cur:
        if r["mid"] != mid and rows:
            batch.append(done(rows))
            rows = []
            if len(batch) >= chunk:
                n += len(batch)
                write(conn, batch)
                batch = []
        mid = r["mid"]
        rows.append(r)
    if rows:
        batch.append(done(rows))
    write(conn, batch)
    return n + len(batch)
