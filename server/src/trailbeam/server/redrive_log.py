"""The redrive log (QV-590): who asked to redrive which dead-letter queue, when, and how it went. SQLite schema 33,
Postgres schema 27. The hosted app's ``tb_redrive_log`` (trailbeam-app lib/app/workspace-data.ts), for one server.

One row per request, written when it is made, whether SQS started the move (``started``), Trailbeam refused it
before asking SQS (``refused``: a move was already running) or SQS refused it (``failed``). A started row then
follows the move: ``status``, ``moved`` and ``toMove`` as ListMessageMoveTasks gives them, ``statusAt`` when the
status last changed, and ``left``, the dead-letter queue's depth read at ``leftAt`` a minute after SQS called the move
finished (redrive.next_check), because SQS can say COMPLETED before it has moved everything.

The log is an audit log: rows are never deleted, by retention, ``--vacuum`` or anything else. ``LogTables(store)``
reads and writes it on a SqliteStore or a PostgresStore, as aws_settings.SettingsTables does; ``MemoryLog`` keeps it
in memory (the tests, a store without the table). Entries are in the shape @trailbeam/ui's redrive views read
(``RedriveEntry``, ui/src/redrive/types.ts).
"""
from __future__ import annotations

import copy
import json
import threading
import uuid
from typing import Any, Optional

SQLITE_CREATE = """
CREATE TABLE IF NOT EXISTS redrive_log (
    id TEXT PRIMARY KEY,
    dlq_name TEXT NOT NULL,
    dlq_arn TEXT NOT NULL,
    account_id TEXT NOT NULL,
    region TEXT NOT NULL,
    message_ids_json TEXT NOT NULL,      -- the messages picked when it was asked for; SQS moves the whole queue
    requested_by TEXT NOT NULL,          -- the person's name, else email, else how the request was let in
    requested_by_email TEXT,
    requested_at_ms INTEGER NOT NULL,
    outcome TEXT NOT NULL,               -- started, refused or failed
    detail TEXT,                         -- why it was refused or failed
    task_handle TEXT,                    -- SQS's handle for the move, while it runs
    status TEXT,                         -- RUNNING, COMPLETED, CANCELLING, CANCELLED or FAILED, as SQS last said
    moved INTEGER,
    to_move INTEGER,
    failure TEXT,                        -- SQS's FailureReason, or who asked to cancel
    status_at_ms INTEGER,
    left_count INTEGER,                  -- the dead-letter queue's depth a minute after the move finished
    left_at_ms INTEGER
);
CREATE INDEX IF NOT EXISTS redrive_log_by_time ON redrive_log (requested_at_ms);
"""

PG_CREATE = SQLITE_CREATE.replace(" INTEGER", " BIGINT")

#: a status change may set these, by the entry's names
STATUS_FIELDS = {"status": "status", "moved": "moved", "toMove": "to_move", "failure": "failure",
                 "statusAt": "status_at_ms", "left": "left_count", "leftAt": "left_at_ms"}
OUTCOMES = ("started", "refused", "failed")
MAX_LIST = 200

_COLUMNS = ("id, dlq_name, dlq_arn, account_id, region, message_ids_json, requested_by, requested_by_email, "
            "requested_at_ms, outcome, detail, task_handle, status, moved, to_move, failure, status_at_ms, "
            "left_count, left_at_ms")


def entry_of(r: dict) -> dict:
    """A row as the views read it (RedriveEntry)."""
    return {"id": r["id"], "dlqName": r["dlq_name"], "dlqArn": r["dlq_arn"], "accountId": r["account_id"],
            "region": r["region"], "messageIds": json.loads(r["message_ids_json"] or "[]"),
            "requestedBy": r["requested_by"], "requestedByEmail": r["requested_by_email"],
            "requestedAt": r["requested_at_ms"], "outcome": r["outcome"], "detail": r["detail"],
            "taskHandle": r["task_handle"], "status": r["status"], "moved": r["moved"], "toMove": r["to_move"],
            "failure": r["failure"], "statusAt": r["status_at_ms"], "left": r["left_count"],
            "leftAt": r["left_at_ms"]}


def new_row(e: dict) -> dict:
    """A new row from a request: ``dlqName, dlqArn, accountId, region, messageIds, requestedBy, requestedByEmail,
    requestedAt, outcome, detail, taskHandle`` and, for a started one, its first status."""
    if e["outcome"] not in OUTCOMES:
        raise ValueError(f"outcome is one of {', '.join(OUTCOMES)}")
    return {"id": uuid.uuid4().hex, "dlq_name": e["dlqName"], "dlq_arn": e["dlqArn"], "account_id": e["accountId"],
            "region": e["region"], "message_ids_json": json.dumps(list(e.get("messageIds") or [])[:1000]),
            "requested_by": e["requestedBy"], "requested_by_email": e.get("requestedByEmail"),
            "requested_at_ms": int(e["requestedAt"]), "outcome": e["outcome"], "detail": e.get("detail"),
            "task_handle": e.get("taskHandle"), "status": e.get("status"), "moved": e.get("moved"),
            "to_move": e.get("toMove"), "failure": e.get("failure"), "status_at_ms": e.get("statusAt"),
            "left_count": e.get("left"), "left_at_ms": e.get("leftAt")}


class MemoryLog:
    """The log in memory: the tests, and a store without the table (a test double; lost on a restart)."""

    def __init__(self):
        self._rows: list[dict] = []
        self._lock = threading.Lock()

    def add(self, e: dict) -> str:
        r = new_row(e)
        with self._lock:
            self._rows.append(r)
        return r["id"]

    def list(self, limit: int = 50) -> list:
        with self._lock:
            rows = sorted(self._rows, key=lambda r: (-r["requested_at_ms"], r["id"]))[:max(0, min(limit, MAX_LIST))]
            return [entry_of(copy.deepcopy(r)) for r in rows]

    def get(self, id_: str) -> Optional[dict]:
        with self._lock:
            r = next((r for r in self._rows if r["id"] == id_), None)
            return entry_of(copy.deepcopy(r)) if r else None

    def set_status(self, id_: str, change: dict) -> None:
        with self._lock:
            for r in self._rows:
                if r["id"] == id_:
                    r.update({STATUS_FIELDS[k]: v for k, v in change.items() if k in STATUS_FIELDS})


class LogTables:
    """``redrive_log`` on a SqliteStore (db.Database) or a PostgresStore: reads on a read connection, each write one
    statement on the writer. Thread-safe."""

    def __init__(self, store):
        self.store = store
        self.postgres = hasattr(store, "reads") and hasattr(store, "ingest")

    def _read(self, q: str, args: tuple) -> list:
        if self.postgres:
            with self.store.reads.connection() as c:
                return [dict(r) for r in c.execute(q.replace("?", "%s"), args).fetchall()]
        with self.store.reader() as c:
            return [dict(r) for r in c.execute(q, args).fetchall()]

    def _write(self, q: str, args: tuple) -> None:
        if self.postgres:
            with self.store.ingest.connection() as c, c.transaction():
                c.execute(q.replace("?", "%s"), args)
            return
        s = self.store
        with s.lock:
            s.conn.execute("BEGIN IMMEDIATE")
            try:
                s.conn.execute(q, args)
                s.conn.execute("COMMIT")
            except BaseException:
                s.conn.execute("ROLLBACK")
                raise

    def add(self, e: dict) -> str:
        r = new_row(e)
        cols = _COLUMNS.split(", ")
        self._write(f"INSERT INTO redrive_log ({_COLUMNS}) VALUES ({', '.join('?' for _ in cols)})",
                    tuple(r[c] for c in cols))
        return r["id"]

    def list(self, limit: int = 50) -> list:
        n = max(0, min(int(limit), MAX_LIST))
        return [entry_of(r) for r in self._read(
            f"SELECT {_COLUMNS} FROM redrive_log ORDER BY requested_at_ms DESC, id LIMIT ?", (n,))]

    def get(self, id_: str) -> Optional[dict]:
        r = self._read(f"SELECT {_COLUMNS} FROM redrive_log WHERE id = ?", (id_,))
        return entry_of(r[0]) if r else None

    def set_status(self, id_: str, change: dict) -> None:
        sets = [(STATUS_FIELDS[k], v) for k, v in change.items() if k in STATUS_FIELDS]
        if not sets:
            return
        self._write(f"UPDATE redrive_log SET {', '.join(f'{c} = ?' for c, _ in sets)} WHERE id = ?",
                    tuple(v for _, v in sets) + (id_,))


def tables_for(db) -> Any:
    """The log of a store, or MemoryLog for a store without one (a test double)."""
    return LogTables(db) if hasattr(db, "reader") or hasattr(db, "reads") else MemoryLog()
