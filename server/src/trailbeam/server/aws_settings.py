"""The AWS settings (QV-589): whether this server reads what AWS says about its queues, and which queues. SQLite schema
31, Postgres schema 25.

**Off by default.** A new server, or one upgraded to this schema, has AWS reads off: it makes no AWS call of any kind,
imports no boto3, looks for no credentials, and the queue pages keep their "What Trailbeam can't see here" note. An
admin turns the reads on in Settings, under AWS, by naming the queues to read (exact names, or patterns with ``*``).
Reads are on only when they are switched on *and* at least one queue is named.

The credentials are never kept here. boto3 finds them the usual way (environment variables, a profile, an instance or
task role), and they need only the three read-only calls aws_reads.py makes. What is kept:

- ``enabled``: the switch. Switching off keeps the list, so switching back on needs no retyping.
- ``queues``: the names or patterns. A queue that none of them names is never read (``no_account``, with a sentence).
- ``region``: the home region, for a queue whose spans don't say its region (boto3's own default when not set).
- ``regions``: the other regions a queue may be read in. A queue in a region that is neither is never read: its
  spans can name any region, and the server's credentials do the reading (QV-740).
- ``accountId``: the one AWS account queues are read in. When not set, it is the credentials' own account
  (sts:GetCallerIdentity, which needs no permission). A queue whose spans name another account is never read, and
  every queue's URL is built from this account, its region and its name, never taken from its spans (QV-740).
- ``redrive`` (QV-590): the dead-letter queues this server may redrive (names or patterns, as ``queues``). Empty by
  default, so redrive is off until an admin names a queue here, even with reads on: redrive is the one thing
  Trailbeam does that changes a queue (redrive.py), and its credentials then need SQS's move-task permissions too.

The table, ``server_settings``, is a small key-value table so later settings can go in it too: one row per key
(``aws`` here), its JSON, and who changed it when. Postgres has no ``meta`` table for this, and SQLite's ``meta`` is
the schema's own. ``SettingsTables(store)`` reads and writes it on a SqliteStore or a PostgresStore, as
dashboards_store.DashboardTables does; ``MemorySettings`` keeps it in memory (the tests, a store without the table).
Retention, ``--vacuum`` and the page queries never touch it.
"""
from __future__ import annotations

import copy
import importlib.util
import json
import re
import threading
import time
from typing import Any, Callable, Optional

SQLITE_CREATE = """
CREATE TABLE IF NOT EXISTS server_settings (
    key TEXT PRIMARY KEY,                -- which settings: 'aws'
    value_json TEXT NOT NULL,            -- the settings, as JSON
    updated_at_ms INTEGER NOT NULL,
    updated_by TEXT NOT NULL             -- the person's ID, or how the request was let in
);
"""

PG_CREATE = SQLITE_CREATE.replace(" INTEGER", " BIGINT")

KEY = "aws"

#: what a server has before anyone changes anything: off
DEFAULT = {"enabled": False, "queues": [], "redrive": [], "region": None, "regions": [], "accountId": None,
           "updatedAt": None, "updatedBy": None}

#: the fields a change may set
FIELDS = ("enabled", "queues", "redrive", "region", "regions", "accountId")

MAX_QUEUES = 200
MAX_REGIONS = 40
#: an SQS queue name (1 to 80 letters, digits, - and _, .fifo at the end of a FIFO queue), with * allowed in a pattern
_QUEUE_PATTERN = re.compile(r"^[A-Za-z0-9_*-]{1,80}(\.fifo)?$")
#: us-east-1, eu-central-2, us-gov-west-1, cn-north-1, ap-southeast-5, ...
_REGION = re.compile(r"^[a-z]{2}(-[a-z]+)+-\d{1,2}$")
#: a queue's name, as SQS allows it: 1 to 80 letters, digits, - and _, of which the last five may be .fifo
_QUEUE_NAME = re.compile(r"^(?=.{1,80}$)[A-Za-z0-9_-]+(\.fifo)?$")
_ACCOUNT = re.compile(r"^\d{12}$")


class SettingsProblem(ValueError):
    """A change that can't be saved; the message is a sentence for the settings screen."""


def _names(qs: Any, field: str) -> list:
    """A list of queue names or patterns, one per line when typed: trimmed, empty lines and repeats dropped."""
    if isinstance(qs, str):  # one name per line, as typed
        qs = qs.splitlines()
    if not isinstance(qs, list) or not all(isinstance(q, str) for q in qs):
        raise SettingsProblem(f"{field} is a list of queue names.")
    names = list(dict.fromkeys(q.strip() for q in qs if q.strip()))
    bad = [q for q in names if not _QUEUE_PATTERN.match(q)]
    if bad:
        raise SettingsProblem(f"{bad[0]} is not a queue name. A queue name has up to 80 letters, digits, hyphens "
                              "and underscores, and a FIFO queue's ends in .fifo. Use * to match any part of a "
                              "name, as in orders-*.")
    if len(names) > MAX_QUEUES:
        raise SettingsProblem(f"Name at most {MAX_QUEUES} queues. Use * to match many with one pattern.")
    return names


def check_change(body: Any) -> dict:
    """The fields of a change (FIELDS), checked and tidied: queue names trimmed, empty lines and repeats dropped, an
    empty region or account ID as None. Raises SettingsProblem."""
    if not isinstance(body, dict):
        raise SettingsProblem("Send the settings as a JSON object.")
    unknown = sorted(set(body) - set(FIELDS) - {"updatedAt", "updatedBy"})
    if unknown:
        raise SettingsProblem(f"These are not AWS settings: {', '.join(unknown)}.")
    out: dict = {}
    if "enabled" in body:
        if not isinstance(body["enabled"], bool):
            raise SettingsProblem("enabled is true or false.")
        out["enabled"] = body["enabled"]
    for field in ("queues", "redrive"):
        if field in body:
            out[field] = _names(body[field], field)
    if "regions" in body:
        rs = body["regions"]
        if rs is None:
            rs = []
        if isinstance(rs, str):  # one per line, or with commas, as typed
            rs = rs.replace(",", "\n").splitlines()
        if not isinstance(rs, list) or not all(isinstance(r, str) for r in rs):
            raise SettingsProblem("regions is a list of AWS regions.")
        regions = list(dict.fromkeys(r.strip() for r in rs if r.strip()))
        bad = [r for r in regions if not _REGION.match(r)]
        if bad:
            raise SettingsProblem(f"{bad[0][:40]} is not an AWS region, such as eu-west-1.")
        if len(regions) > MAX_REGIONS:
            raise SettingsProblem(f"Name at most {MAX_REGIONS} regions.")
        out["regions"] = regions
    for field, pattern, example in (("region", _REGION, "eu-west-1"), ("accountId", _ACCOUNT, "123456789012")):
        if field not in body:
            continue
        v = body[field]
        if v is None or (isinstance(v, str) and not v.strip()):
            out[field] = None
            continue
        if not isinstance(v, str) or not pattern.match(v.strip()):
            what = "an AWS region" if field == "region" else "a 12-digit AWS account ID"
            raise SettingsProblem(f"{str(v).strip()[:40]} is not {what}, such as {example}.")
        out[field] = v.strip()
    return out


def is_on(s: dict) -> bool:
    """AWS reads are on: switched on, with at least one queue named."""
    return bool(s.get("enabled")) and bool(s.get("queues"))


def is_queue_name(name: object) -> bool:
    """A name SQS would take for a queue. A queue key from spans can carry anything, and a pattern's * matches any
    character, so the reader checks this before reading (QV-740)."""
    return isinstance(name, str) and _QUEUE_NAME.match(name) is not None


def boto3_installed() -> bool:
    return importlib.util.find_spec("boto3") is not None


# ── the table ────────────────────────────────────────────────────────────────────────────────────────────────────

class MemorySettings:
    """The settings in memory: the tests, and a store without the table (a test double; lost on a restart)."""

    def __init__(self, rows: Optional[dict] = None):
        self._rows: dict = copy.deepcopy(rows or {})
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[dict]:
        with self._lock:
            r = self._rows.get(key)
            return copy.deepcopy(r) if r else None

    def put(self, key: str, value: dict, at_ms: int, by: str) -> None:
        with self._lock:
            self._rows[key] = {"value": copy.deepcopy(value), "updatedAt": at_ms, "updatedBy": by}


class SettingsTables:
    """``server_settings`` on a SqliteStore (db.Database) or a PostgresStore: reads on a read connection, each write
    one statement on the writer. Thread-safe."""

    def __init__(self, store):
        self.store = store
        self.postgres = hasattr(store, "reads") and hasattr(store, "ingest")

    def get(self, key: str) -> Optional[dict]:
        q = "SELECT value_json, updated_at_ms, updated_by FROM server_settings WHERE key = ?"
        if self.postgres:
            with self.store.reads.connection() as c:
                r = c.execute(q.replace("?", "%s"), (key,)).fetchone()
        else:
            with self.store.reader() as c:
                r = c.execute(q, (key,)).fetchone()
        if r is None:
            return None
        r = dict(r)
        return {"value": json.loads(r["value_json"]), "updatedAt": r["updated_at_ms"], "updatedBy": r["updated_by"]}

    def put(self, key: str, value: dict, at_ms: int, by: str) -> None:
        q = ("INSERT INTO server_settings (key, value_json, updated_at_ms, updated_by) VALUES (?, ?, ?, ?) "
             "ON CONFLICT (key) DO UPDATE SET value_json = excluded.value_json, "
             "updated_at_ms = excluded.updated_at_ms, updated_by = excluded.updated_by")
        args = (key, json.dumps(value, ensure_ascii=False, separators=(",", ":")), int(at_ms), by)
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


def tables_for(db) -> Any:
    """The settings table of a store, or MemorySettings for a store without one (a test double)."""
    return SettingsTables(db) if hasattr(db, "reader") or hasattr(db, "reads") else MemorySettings()


# ── the settings a server reads ──────────────────────────────────────────────────────────────────────────────────

class AwsSettings:
    """The AWS settings of one server, read from its store and kept for ``keep_s`` (every page with a queue on it
    asks), so a change made on another replica of the same store shows within that long. ``save`` is seen at once on
    the replica that made it."""

    KEEP_S = 5.0

    def __init__(self, table, clock: Callable[[], float] = time.monotonic, keep_s: float = KEEP_S):
        self.table = table
        self.clock = clock
        self.keep_s = keep_s
        self._lock = threading.Lock()
        self._kept: Optional[tuple[float, dict]] = None
        #: called after a save, with the settings saved (aws_reader.AwsReader drops its cached reads)
        self.on_change: list[Callable[[dict], None]] = []

    def current(self) -> dict:
        """The settings, DEFAULT when none were ever saved. Raises what the store raises when it is unreachable."""
        now = self.clock()
        with self._lock:
            if self._kept is not None and now - self._kept[0] < self.keep_s:
                return copy.deepcopy(self._kept[1])
        row = self.table.get(KEY)
        s = {**DEFAULT, **({k: row["value"].get(k, DEFAULT[k]) for k in FIELDS} if row else {}),
             **({"updatedAt": row["updatedAt"], "updatedBy": row["updatedBy"]} if row else {})}
        with self._lock:
            self._kept = (now, s)
        return copy.deepcopy(s)

    def on(self) -> bool:
        """Reads are on. False, never raising, when the store can't be read: a page then shows no AWS figure."""
        try:
            return is_on(self.current())
        except Exception:  # noqa: BLE001
            return False

    def save(self, change: dict, by: str, at_ms: int) -> dict:
        """Applies a change (check_change's fields) and stores the whole settings; the settings after."""
        before = self.current()
        after = {k: change.get(k, before[k]) for k in FIELDS}
        self.table.put(KEY, after, at_ms, by)
        s = {**after, "updatedAt": at_ms, "updatedBy": by}
        with self._lock:
            self._kept = (self.clock(), s)
        for f in self.on_change:
            f(copy.deepcopy(s))
        return copy.deepcopy(s)
