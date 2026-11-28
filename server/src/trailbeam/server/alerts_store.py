"""The alert tables (QV-650, the fifth part of QV-592: alerts in self-hosted Trailbeam): SQLite schema 26, Postgres
schema 20. ``AlertTables(store)`` is alerts_run.AlertStore, and the API's reads and writes (alerts_api.AlertConfigStore,
QV-642), on the tables of either store, the same way accounts.py keeps people: reads on the store's read connections,
each write a short transaction on its writer (SQLite: the writer connection under its lock; Postgres: a connection
from the ingest pool).

- ``alert_rules``: the "Alert me" rules (the hosted app's Rule records, alerts_api.add_rule) and the check's memory
  of each (seen, lastNewAt, wasMuted, wasHeld, regressedSeen: alerts_run.AlertStore.rules_with_memory). One rule per
  cause and workspace: a unique index on (ws, cause_key) for kind 'cause', so two people making it at once store one.
- ``alert_channels``: where alerts go. ``target`` (a Slack or webhook URL, email addresses) and ``secret`` (a
  webhook's signing secret) are stored as given, because delivery needs them: list_channels returns them, and the
  API (alerts_api.channel_view) leaves them out of every answer. They are in the file or database, and so in its
  backups, like the hashes of people's passwords. The SMTP server and its password are the operator's settings and
  are never stored here.
- ``alert_state``: each rule's state as the last check left it (alerts_run.STATE_FIELDS), dashboard rules under
  their dashboard and widget, "Alert me" rules under dashboard ``rule`` and the rule's ID.
- ``alert_checks``: per workspace (or alerts_run.span_key), when it was last checked and with what error, and its
  lease: who holds it until when.
- ``alert_outbox`` (QV-674; SQLite schema 30, Postgres schema 24): the changes a check won and is sending. Written
  just before delivery and deleted once it returns, so a check stopped in between (a shutdown, a crash, a delivery
  that raised) leaves its changes here, and the next check of the workspace sends them (alerts_run.run_alerts).

**Two checks at once.** Every write that decides whether a change is sent is one conditional statement whose row
count says whether it took effect (write_state, clear_was_muted, set_regressed_seen, add_rule), and the lease is
one upsert whose update only happens if the lease has ended and the workspace was not checked without an error
within the gap. On Postgres the upsert locks the row (a second one waits, then sees the first's lease), so of two
processes exactly one takes it; on SQLite the writer runs one statement at a time.

**NUL.** Postgres text columns refuse the NUL character (0x00), and a cause's label and key can hold one: they
carry its first error text (causes.cause_label, causes.fingerprint), and a consumer that reads binary data can raise
``ValueError: invalid literal for int(): '\x00\x01'``. So every string that goes in, as a value or in a WHERE, has
NUL stored as U+FFFF (a noncharacter, which no text holds), and every string that comes out has it turned back: a
label, a cause key or an error text round-trips as given, on SQLite and on Postgres alike, and a rule made on a
cause whose key holds NUL is found by that key (QV-658).

Times are milliseconds since the epoch. Nothing here holds spans: retention, ``--vacuum`` and the page queries
never touch these tables, and a ``--db :memory:`` store loses them on every restart.
"""
from __future__ import annotations

import contextlib
import json
from collections.abc import Iterator
from typing import Any

from . import alerts

TABLES = ("alert_rules", "alert_channels", "alert_state", "alert_checks", "alert_outbox")

SQLITE_CREATE = """
CREATE TABLE IF NOT EXISTS alert_rules (
    ws TEXT NOT NULL,                    -- the workspace
    id TEXT NOT NULL,
    kind TEXT NOT NULL,                  -- cause
    cause_key TEXT NOT NULL,             -- v1|queue|consumer|signature (causes.py)
    cause_id TEXT NOT NULL,
    label TEXT NOT NULL,
    queue TEXT NOT NULL,
    channels_json TEXT NOT NULL,         -- JSON [channel ID, ...]
    mode TEXT NOT NULL,                  -- quiet | every
    quiet_s INTEGER NOT NULL,
    muted_until_ms INTEGER,              -- NULL: not muted; alerts.FOREVER: until unmuted
    muted_by TEXT,
    muted_at_ms INTEGER,
    created_by TEXT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    -- what the check remembers of the rule (alerts_run)
    seen_json TEXT,                      -- JSON [message ID, ...]; NULL: never checked
    last_new_at_ms INTEGER,
    was_muted INTEGER NOT NULL DEFAULT 0,
    was_held INTEGER NOT NULL DEFAULT 0,
    regressed_seen_ms INTEGER,
    PRIMARY KEY (ws, id)
);
CREATE UNIQUE INDEX IF NOT EXISTS alert_rules_cause ON alert_rules (ws, cause_key) WHERE kind = 'cause';
CREATE TABLE IF NOT EXISTS alert_channels (
    ws TEXT NOT NULL,
    id TEXT NOT NULL,
    kind TEXT NOT NULL,                  -- webhook | slack | email
    name TEXT NOT NULL,
    target TEXT NOT NULL,                -- never answered by the API (alerts_api.channel_view)
    secret TEXT,                         -- a webhook's signing secret; answered once, when the channel is added
    min_tone TEXT NOT NULL,              -- warning | critical
    created_by TEXT,
    created_at_ms INTEGER NOT NULL,
    last_at_ms INTEGER,                  -- the last send, for Settings
    last_ok INTEGER,
    last_detail TEXT,
    PRIMARY KEY (ws, id)
);
CREATE TABLE IF NOT EXISTS alert_state (
    ws TEXT NOT NULL,
    dashboard_id TEXT NOT NULL,          -- 'rule' for an "Alert me" rule
    widget_id TEXT NOT NULL,             -- the rule's ID for an "Alert me" rule
    dashboard_name TEXT,
    title TEXT,
    tone TEXT NOT NULL,                  -- ok | warning | critical
    value TEXT,
    limit_text TEXT,
    queue TEXT,
    since_ms INTEGER,
    checked_at_ms INTEGER,
    PRIMARY KEY (ws, dashboard_id, widget_id)
);
CREATE TABLE IF NOT EXISTS alert_checks (
    key TEXT PRIMARY KEY,                -- a workspace ID, or alerts_run.span_key
    checked_at_ms INTEGER,               -- NULL: leased, never checked
    error TEXT,                          -- NULL: the last check went fine
    lease_owner TEXT NOT NULL DEFAULT '',
    lease_until_ms INTEGER NOT NULL DEFAULT 0
);
"""

SQLITE_OUTBOX = """
CREATE TABLE IF NOT EXISTS alert_outbox (
    ws TEXT NOT NULL,
    id TEXT NOT NULL,
    changes_json TEXT NOT NULL,          -- JSON [change, ...], as alerts_run.record and settle return them
    held_at_ms INTEGER NOT NULL,         -- the check that won them
    tries INTEGER NOT NULL,              -- deliveries started, the first one included
    PRIMARY KEY (ws, id)
);
"""

PG_OUTBOX = SQLITE_OUTBOX.replace(" INTEGER", " BIGINT")

PG_CREATE = (SQLITE_CREATE.replace(" INTEGER", " BIGINT")
             .replace("was_muted BIGINT", "was_muted SMALLINT").replace("was_held BIGINT", "was_held SMALLINT")
             .replace("last_ok BIGINT", "last_ok SMALLINT"))

# the hosted app's field names -> the columns; JSON: stored as JSON text
_RULE = (("id", "id"), ("kind", "kind"), ("causeKey", "cause_key"), ("causeId", "cause_id"), ("label", "label"),
         ("queue", "queue"), ("channels", "channels_json"), ("mode", "mode"), ("quietS", "quiet_s"),
         ("mutedUntil", "muted_until_ms"), ("mutedBy", "muted_by"), ("mutedAt", "muted_at_ms"),
         ("createdBy", "created_by"), ("createdAt", "created_at_ms"), ("updatedAt", "updated_at_ms"))
_RULE_COL = dict(_RULE)
_MEMORY = (("seen", "seen_json"), ("lastNewAt", "last_new_at_ms"), ("wasMuted", "was_muted"),
           ("wasHeld", "was_held"), ("regressedSeen", "regressed_seen_ms"))
_CHANNEL = (("id", "id"), ("kind", "kind"), ("name", "name"), ("target", "target"), ("secret", "secret"),
            ("minTone", "min_tone"), ("createdBy", "created_by"), ("createdAt", "created_at_ms"),
            ("lastAt", "last_at_ms"), ("lastOk", "last_ok"), ("lastDetail", "last_detail"))
_CHANNEL_COL = dict(_CHANNEL)
_STATE = (("dashboardId", "dashboard_id"), ("widgetId", "widget_id"), ("dashboardName", "dashboard_name"),
          ("title", "title"), ("tone", "tone"), ("value", "value"), ("limit", "limit_text"), ("queue", "queue"),
          ("since", "since_ms"), ("checkedAt", "checked_at_ms"))
_JSON = {"channels_json", "seen_json"}
_BOOL = {"was_muted", "was_held", "last_ok"}

_RULE_SELECT = ("SELECT " + ", ".join(c for _, c in _RULE + _MEMORY) + " FROM alert_rules")
_CHANNEL_SELECT = "SELECT " + ", ".join(c for _, c in _CHANNEL) + " FROM alert_channels"
_STATE_SELECT = "SELECT " + ", ".join(c for _, c in _STATE) + " FROM alert_state"


_NUL, _NUL_STORED = "\x00", "\uffff"


def _in(args: tuple) -> tuple:
    """The statement's arguments as stored: NUL in a string as U+FFFF, which Postgres takes."""
    return tuple(a.replace(_NUL, _NUL_STORED) if isinstance(a, str) and _NUL in a else a for a in args)


def _out(row) -> dict:
    """A row as read, its strings as they were given."""
    return {k: v.replace(_NUL_STORED, _NUL) if isinstance(v, str) and _NUL_STORED in v else v
            for k, v in dict(row).items()}


def _to_db(col: str, v: Any) -> Any:
    if col in _JSON:
        return None if v is None else json.dumps(list(v))
    if col in _BOOL:
        return None if v is None else int(bool(v))
    return v


def _from_db(col: str, v: Any) -> Any:
    if col in _JSON:
        return None if v is None else json.loads(v)
    if col in _BOOL:
        return None if v is None else bool(v)
    return v


def _record(row: dict, fields) -> dict:
    return {f: _from_db(c, row[c]) for f, c in fields}


def _columns(fields: dict, names: dict, what: str) -> list[tuple[str, Any]]:
    unknown = set(fields) - set(names)
    if unknown:
        raise ValueError(f"no such {what} field: {', '.join(sorted(unknown))}")
    return [(names[f], _to_db(names[f], v)) for f, v in fields.items()]


class AlertTables:
    """alerts_run.AlertStore and alerts_api.AlertConfigStore on a SqliteStore or a PostgresStore. Thread-safe: the
    API runs on request threads and the check on its own."""

    def __init__(self, store):
        self.store = store
        self.postgres = hasattr(store, "reads") and hasattr(store, "ingest")

    # ---- the two kinds of connection --------------------------------------------------------------------------

    def _read(self, q: str, args: tuple = ()) -> list[dict]:
        if self.postgres:
            with self.store.reads.connection() as c:
                return [_out(r) for r in c.execute(q.replace("?", "%s"), _in(args)).fetchall()]
        with self.store.reader() as c:
            return [_out(r) for r in c.execute(q, _in(args)).fetchall()]

    @contextlib.contextmanager
    def _write(self) -> Iterator[_Tx]:
        """One write transaction; commits when the block ends, rolls back if it raises."""
        if self.postgres:
            with self.store.ingest.connection() as c, c.transaction():
                yield _Tx(c, True)
            return
        s = self.store
        with s.lock:
            c = s.conn
            c.execute("BEGIN IMMEDIATE")
            try:
                yield _Tx(c, False)
                c.execute("COMMIT")
            except BaseException:
                c.execute("ROLLBACK")
                raise

    def _run(self, q: str, args: tuple = ()) -> int:
        """One statement in its own transaction; rows changed."""
        with self._write() as tx:
            return tx.run(q, args)

    # ---- checks and leases ------------------------------------------------------------------------------------

    def last_checks(self) -> dict[str, int]:
        return {r["key"]: r["checked_at_ms"] for r in self._read(
            "SELECT key, checked_at_ms FROM alert_checks WHERE checked_at_ms IS NOT NULL")}

    def last_check(self, key: str) -> int | None:
        rows = self._read("SELECT checked_at_ms FROM alert_checks WHERE key = ?", (key,))
        return rows[0]["checked_at_ms"] if rows else None

    def checked(self, key: str, at: int, error: str | None) -> None:
        self._run("INSERT INTO alert_checks (key, checked_at_ms, error) VALUES (?, ?, ?) "
                  "ON CONFLICT (key) DO UPDATE SET checked_at_ms = excluded.checked_at_ms, error = excluded.error",
                  (key, at, error))

    def lease(self, key: str, owner: str, at: int, ttl_ms: int, min_gap_ms: int) -> bool:
        # One statement: a new row is a lease nobody held; an existing one is taken only if its lease ended before
        # ``at`` and it was not checked without an error after ``at - min_gap_ms``. On Postgres a second upsert of
        # the row waits for the first's transaction and then reads its lease.
        return self._run(
            "INSERT INTO alert_checks (key, lease_owner, lease_until_ms) VALUES (?, ?, ?) "
            "ON CONFLICT (key) DO UPDATE SET lease_owner = excluded.lease_owner, "
            "lease_until_ms = excluded.lease_until_ms "
            "WHERE alert_checks.lease_until_ms < ? AND (alert_checks.checked_at_ms IS NULL "
            "OR alert_checks.error IS NOT NULL OR alert_checks.checked_at_ms <= ?)",
            (key, owner, at + ttl_ms, at, at - min_gap_ms)) == 1

    def release(self, key: str, owner: str) -> None:
        self._run("UPDATE alert_checks SET lease_until_ms = 0 WHERE key = ? AND lease_owner = ?", (key, owner))

    # ---- alert state ------------------------------------------------------------------------------------------

    def alert_states(self, ws: str) -> list[dict]:
        return [_record(r, _STATE) for r in self._read(
            _STATE_SELECT + " WHERE ws = ? ORDER BY dashboard_id, widget_id", (ws,))]

    def write_state(self, ws: str, state: dict, prev_tone: str | None) -> bool:
        cols = [c for _, c in _STATE]
        vals = tuple(state[f] for f, _ in _STATE)
        if prev_tone is None:
            return self._run(f"INSERT INTO alert_state (ws, {', '.join(cols)}) VALUES (?{', ?' * len(cols)}) "
                             "ON CONFLICT DO NOTHING", (ws,) + vals) == 1
        sets = ", ".join(f"{c} = ?" for c in cols[2:])
        return self._run(f"UPDATE alert_state SET {sets} WHERE ws = ? AND dashboard_id = ? AND widget_id = ? "
                         "AND tone = ?", vals[2:] + (ws, vals[0], vals[1], prev_tone)) == 1

    def drop_state(self, ws: str, dashboard_id: str, widget_id: str) -> bool:
        return self._run("DELETE FROM alert_state WHERE ws = ? AND dashboard_id = ? AND widget_id = ?",
                  (ws, dashboard_id, widget_id)) == 1

    # ---- rules and the check's memory of them -----------------------------------------------------------------

    def workspaces_with_rules(self) -> set[str]:
        return {r["ws"] for r in self._read("SELECT DISTINCT ws FROM alert_rules")}

    def rules_with_memory(self, ws: str) -> list[dict]:
        return [{"rule": _record(r, _RULE), "memory": _record(r, _MEMORY)} for r in self._read(
            _RULE_SELECT + " WHERE ws = ? ORDER BY created_at_ms, id", (ws,))]

    def remember(self, ws: str, rule_id: str, memory: dict, seen_as: dict | None) -> None:
        sets = [("seen_json", _to_db("seen_json", memory.get("seen") or [])),
                ("last_new_at_ms", memory.get("lastNewAt")),
                ("was_muted", int(bool(memory.get("wasMuted")))), ("was_held", int(bool(memory.get("wasHeld"))))]
        if seen_as:
            sets += [("label", seen_as["label"][:300]), ("queue", seen_as["queue"]), ("cause_id", seen_as["causeId"])]
        self._run(f"UPDATE alert_rules SET {', '.join(f'{c} = ?' for c, _ in sets)} WHERE ws = ? AND id = ?",
                  tuple(v for _, v in sets) + (ws, rule_id))

    def clear_was_muted(self, ws: str, rule_id: str) -> bool:
        return self._run("UPDATE alert_rules SET was_muted = 0 WHERE ws = ? AND id = ? AND was_muted <> 0",
                         (ws, rule_id)) == 1

    def set_regressed_seen(self, ws: str, rule_id: str, regressed_at: int) -> bool:
        return self._run("UPDATE alert_rules SET regressed_seen_ms = ? WHERE ws = ? AND id = ? "
                         "AND (regressed_seen_ms IS NULL OR regressed_seen_ms <> ?)",
                         (regressed_at, ws, rule_id, regressed_at)) == 1

    # ---- rules (the API) --------------------------------------------------------------------------------------

    def list_rules(self, ws: str) -> list[dict]:
        return [x["rule"] for x in self.rules_with_memory(ws)]

    def get_rule(self, ws: str, rule_id: str) -> dict | None:
        rows = self._read(_RULE_SELECT + " WHERE ws = ? AND id = ?", (ws, rule_id))
        return _record(rows[0], _RULE) if rows else None

    def rule_for_cause(self, ws: str, cause_key: str) -> dict | None:
        rows = self._read(_RULE_SELECT + " WHERE ws = ? AND kind = 'cause' AND cause_key = ?", (ws, cause_key))
        return _record(rows[0], _RULE) if rows else None

    def add_rule(self, ws: str, rule: dict) -> bool:
        cols = _columns(rule, _RULE_COL, "rule")
        return self._run(f"INSERT INTO alert_rules (ws, {', '.join(c for c, _ in cols)}) "
                         f"VALUES (?{', ?' * len(cols)}) ON CONFLICT DO NOTHING",
                         (ws,) + tuple(v for _, v in cols)) == 1

    def update_rule(self, ws: str, rule_id: str, fields: dict) -> dict | None:
        cols = _columns(fields, _RULE_COL, "rule")
        with self._write() as tx:
            if cols:
                tx.run(f"UPDATE alert_rules SET {', '.join(f'{c} = ?' for c, _ in cols)} WHERE ws = ? AND id = ?",
                       tuple(v for _, v in cols) + (ws, rule_id))
            r = tx.one(_RULE_SELECT + " WHERE ws = ? AND id = ?", (ws, rule_id))
        return _record(r, _RULE) if r else None

    def remove_rule(self, ws: str, rule_id: str) -> bool:
        with self._write() as tx:
            n = tx.run("DELETE FROM alert_rules WHERE ws = ? AND id = ?", (ws, rule_id))
            tx.run("DELETE FROM alert_state WHERE ws = ? AND dashboard_id = ? AND widget_id = ?",
                   (ws, alerts.RULE_DASHBOARD, rule_id))
        return n == 1

    # ---- channels (the API) -----------------------------------------------------------------------------------

    def list_channels(self, ws: str) -> list[dict]:
        """Oldest first, targets and secrets included: delivery needs them. The API never answers them."""
        return [_record(r, _CHANNEL) for r in self._read(_CHANNEL_SELECT + " WHERE ws = ? ORDER BY created_at_ms, id",
                                                         (ws,))]

    def get_channel(self, ws: str, channel_id: str) -> dict | None:
        rows = self._read(_CHANNEL_SELECT + " WHERE ws = ? AND id = ?", (ws, channel_id))
        return _record(rows[0], _CHANNEL) if rows else None

    def add_channel(self, ws: str, channel: dict) -> None:
        cols = _columns(channel, _CHANNEL_COL, "channel")
        self._run(f"INSERT INTO alert_channels (ws, {', '.join(c for c, _ in cols)}) VALUES (?{', ?' * len(cols)})",
                  (ws,) + tuple(v for _, v in cols))

    def update_channel(self, ws: str, channel_id: str, fields: dict) -> dict | None:
        cols = _columns(fields, _CHANNEL_COL, "channel")
        with self._write() as tx:
            if cols:
                tx.run(f"UPDATE alert_channels SET {', '.join(f'{c} = ?' for c, _ in cols)} WHERE ws = ? AND id = ?",
                       tuple(v for _, v in cols) + (ws, channel_id))
            r = tx.one(_CHANNEL_SELECT + " WHERE ws = ? AND id = ?", (ws, channel_id))
        return _record(r, _CHANNEL) if r else None

    def remove_channel(self, ws: str, channel_id: str) -> bool:
        return self._run("DELETE FROM alert_channels WHERE ws = ? AND id = ?", (ws, channel_id)) == 1

    def note_delivery(self, ws: str, channel_id: str, at: int, ok: bool, detail: str | None) -> None:
        self._run("UPDATE alert_channels SET last_at_ms = ?, last_ok = ?, last_detail = ? WHERE ws = ? AND id = ?",
                  (at, int(ok), detail[:300] if detail is not None else None, ws, channel_id))

    # ---- the outbox: changes won and not yet known to be sent (QV-674) ------------------------------------------

    def hold(self, ws: str, id_: str, changes: list, at: int) -> None:
        self._run("INSERT INTO alert_outbox (ws, id, changes_json, held_at_ms, tries) VALUES (?, ?, ?, ?, 1)",
                  (ws, id_, json.dumps(changes, separators=(",", ":")), at))

    def held(self, ws: str) -> list[dict]:
        return [{"id": r["id"], "changes": json.loads(r["changes_json"]), "heldAt": r["held_at_ms"],
                 "tries": r["tries"]} for r in self._read(
            "SELECT id, changes_json, held_at_ms, tries FROM alert_outbox WHERE ws = ? ORDER BY held_at_ms, id", (ws,))]

    def retry(self, ws: str, id_: str) -> None:
        self._run("UPDATE alert_outbox SET tries = tries + 1 WHERE ws = ? AND id = ?", (ws, id_))

    def sent(self, ws: str, id_: str) -> bool:
        return self._run("DELETE FROM alert_outbox WHERE ws = ? AND id = ?", (ws, id_)) == 1

    def workspaces_with_channels(self) -> set[str]:
        return {r["ws"] for r in self._read("SELECT DISTINCT ws FROM alert_channels")}


class _Tx:
    def __init__(self, conn, postgres: bool):
        self.c = conn
        self.postgres = postgres

    def _q(self, q: str) -> str:
        return q.replace("?", "%s") if self.postgres else q

    def run(self, q: str, args: tuple = ()) -> int:
        """Rows changed."""
        return self.c.execute(self._q(q), _in(args)).rowcount

    def one(self, q: str, args: tuple = ()) -> dict | None:
        r = self.c.execute(self._q(q), _in(args)).fetchone()
        return None if r is None else _out(r)
