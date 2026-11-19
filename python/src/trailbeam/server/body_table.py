"""The SQLite ``bodies`` table: each stored message body once, referenced from the spans that carried it.

A captured body travels on the send span and again on every receive span of the message (contract 15.2), so
storing it in each span's ``attributes`` wrote the same text two or more times. Instead (schema 10):

- ``bodies`` has one row per distinct body of a message: ``(mid, sha)`` is unique, ``sha`` the SHA-256 of the
  text. The send's body and a receive's identical copy are one row; a receive that carried a different text
  (another redaction, an envelope) gets its own row, so nothing a span carried is lost.
- the span keeps every other attribute, without ``trailbeam.body``, and ``spans.body_id`` points at its row.
  ``spans_body`` (a partial index on ``mid``) finds a message's body spans without reading its other spans.

``ROWS_SQL`` reads a message's spans with the body put back into ``attributes``, so every reader (timeline,
Body tab, search) sees the spans as they arrived. Retention deletes a message's bodies with its spans.

Only send and receive spans with a message ID and a text ``trailbeam.body`` are split this way; anything
else is stored as it came, so a reader still finds a body in ``attributes`` if one is there.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import Optional

CREATE = """
CREATE TABLE IF NOT EXISTS bodies (
    id INTEGER PRIMARY KEY,
    mid TEXT NOT NULL,                   -- the message (spans.mid of the send and receive spans)
    sha BLOB NOT NULL,                   -- SHA-256 of body
    body TEXT NOT NULL,                  -- trailbeam.body as the span carried it
    UNIQUE (mid, sha)
);
CREATE INDEX IF NOT EXISTS spans_body ON spans (mid) WHERE body_id IS NOT NULL;
"""

BODY = "trailbeam.body"
CARRIERS = ("send", "receive")
_PUT = "INSERT INTO bodies (mid, sha, body) VALUES (?, ?, ?) ON CONFLICT (mid, sha) DO NOTHING"
_ID = "SELECT id FROM bodies WHERE mid = ? AND sha = ?"


def split(kind: str, mid: Optional[str], attributes: dict) -> tuple[dict, Optional[str]]:
    """The attributes to store in the span, and the body to store once (None: nothing to split off)."""
    body = attributes.get(BODY)
    if kind not in CARRIERS or not mid or not isinstance(body, str):
        return attributes, None
    return {k: v for k, v in attributes.items() if k != BODY}, body


def put(conn: sqlite3.Connection, mid: str, body: str) -> int:
    """The id of the message's row for this body, written if it is new. In the caller's transaction."""
    sha = hashlib.sha256(body.encode("utf-8", "surrogatepass")).digest()
    cur = conn.execute(_PUT, (mid, sha, body))
    if cur.rowcount:
        return cur.lastrowid
    return conn.execute(_ID, (mid, sha)).fetchone()[0]


def attributes_json(attributes: dict) -> str:
    """How a span's attributes are written (db.py and the benchmark's seeder)."""
    return json.dumps(attributes, sort_keys=True, default=str)


def rows_sql(span_columns: list[str]) -> str:
    """SELECT of spans ``s`` with the body put back into ``attributes``: add ``WHERE ...``."""
    cols = ", ".join(f"s.{c}" for c in span_columns if c != "attributes")
    return (f"""SELECT {cols}, CASE WHEN s.body_id IS NULL THEN s.attributes
                    ELSE json_set(s.attributes, '$."{BODY}"', b.body) END AS attributes
                FROM spans s LEFT JOIN bodies b ON b.id = s.body_id""")


def span_columns(conn: sqlite3.Connection) -> list[str]:
    return [r[1] for r in conn.execute("PRAGMA table_info(spans)")]


def move_inline(conn: sqlite3.Connection) -> int:
    """Migration 10: take the bodies out of every stored span's attributes into ``bodies``. One pass over
    the send and receive spans (spans_kind_time); returns the spans changed."""
    n = 0
    cur = conn.execute("""SELECT row, kind, mid, attributes FROM spans INDEXED BY spans_kind_time
                          WHERE kind IN ('send', 'receive') AND mid IS NOT NULL
                            AND json_type(attributes, '$."trailbeam.body"') = 'text'""")
    for row, kind, mid, raw in cur.fetchall():
        attrs, body = split(kind, mid, json.loads(raw))
        if body is None:
            continue
        conn.execute("UPDATE spans SET attributes = ?, body_id = ? WHERE row = ?",
                     (attributes_json(attrs), put(conn, mid, body), row))
        n += 1
    return n


def forget(conn: sqlite3.Connection, mids: list) -> int:
    """Delete these messages' bodies (retention, after their spans). Returns the rows deleted."""
    return conn.execute("DELETE FROM bodies WHERE mid IN (SELECT value FROM json_each(?))",
                        (json.dumps(mids),)).rowcount


def prefetch(conn: sqlite3.Connection, mids: list) -> None:
    """Read the pages ``forget`` will change (see db._prefetch_delete). Changes nothing."""
    conn.execute("SELECT id, length(body) FROM bodies WHERE mid IN (SELECT value FROM json_each(?))",
                 (json.dumps(mids),)).fetchall()
