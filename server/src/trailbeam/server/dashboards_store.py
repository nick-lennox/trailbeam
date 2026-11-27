"""The dashboard tables (QV-652, the second part of QV-591: dashboards in self-hosted Trailbeam): SQLite schema 27,
Postgres schema 21. ``DashboardStore`` is what the API (dashboards_api.py) and, later, the alert check read and write;
``MemoryDashboards`` keeps it in memory (the tests), ``DashboardTables(store)`` on the tables of a SqliteStore or a
PostgresStore, the same way alerts_store.AlertTables does: reads on the store's read connections, each write a short
transaction on its writer.

The records are the hosted app's (lib/dashboards/types.ts ``Dashboard``, data.ts), so the shared views can read
either backend: ``{id, name, def: {variables, sections, widgets}, starred, lockedBy, lockedById, createdBy,
updatedBy, updatedAt}``, plus what self-hosted Trailbeam keeps besides: ``time`` (the preset the dashboard opens at,
measures.PRESETS; None: the past hour), ``createdById`` (its owner's person ID, access.Who.user_id; None when it was
made before accounts, or by a token), ``createdAt`` and ``version`` (one more on every change, for the API's
read-change-write of one widget).

- ``dashboards``: one row per dashboard: its name, owner, workspace, time range, variables and sections.
- ``dashboard_widgets``: one row per widget: its definition (the widget's JSON, layout included, exactly as given)
  and its position in the definition's list of widgets. A definition is written whole: its widgets are replaced in
  the same transaction as the dashboard's row, so a reader never sees half of one.

**Locks and two writers.** ``update`` is one conditional UPDATE of the dashboard row (unlocked, and at the version
the caller read, when it says so) and the widgets' replacement in the same transaction: its row count says whether
it took effect, so of two changes made from the same read exactly one is stored. On Postgres the UPDATE locks the
row; on SQLite the writer runs one transaction at a time.

Times are milliseconds since the epoch. Nothing here holds spans: retention, ``--vacuum`` and the page queries never
touch these tables, and a ``--db :memory:`` store loses them on every restart.
"""
from __future__ import annotations

import contextlib
import copy
import json
import threading
from collections.abc import Iterator
from typing import Any, Literal, Protocol

TABLES = ("dashboards", "dashboard_widgets")

SQLITE_CREATE = """
CREATE TABLE IF NOT EXISTS dashboards (
    ws TEXT NOT NULL,                    -- the workspace
    id TEXT NOT NULL,
    name TEXT NOT NULL,
    variables_json TEXT NOT NULL,        -- JSON [{name, field, value}]
    sections_json TEXT NOT NULL,         -- JSON [{id, title, ...}]
    time_range TEXT,                     -- a preset ID (measures.PRESETS) it opens at; NULL: the past hour
    starred INTEGER NOT NULL DEFAULT 0,
    locked_by TEXT,                      -- the display name of who locked it, to show; NULL: not locked
    locked_by_id TEXT,                   -- their person ID, which decides who may unlock it
    created_by TEXT NOT NULL,            -- the owner's display name, or how the request was let in
    created_by_id TEXT,                  -- the owner's person ID; NULL before accounts, or by a token
    updated_by TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,  -- one more on every change
    PRIMARY KEY (ws, id)
);
CREATE TABLE IF NOT EXISTS dashboard_widgets (
    ws TEXT NOT NULL,
    dashboard_id TEXT NOT NULL,
    position INTEGER NOT NULL,           -- its place in the definition's list of widgets, from 0
    id TEXT NOT NULL,                    -- the widget's own "id" (the hosted app does not keep them unique either)
    def_json TEXT NOT NULL,              -- the widget's JSON, layout included, as given
    PRIMARY KEY (ws, dashboard_id, position)
);
"""

PG_CREATE = SQLITE_CREATE.replace(" INTEGER", " BIGINT").replace("starred BIGINT", "starred SMALLINT")

Outcome = Literal["ok", "locked", "missing", "conflict"]

#: what ``update`` may set, by the record's field name
FIELDS = ("name", "def", "time", "starred", "lockedBy", "lockedById", "updatedBy", "updatedAt")


def _def(d: dict) -> dict:
    return {"variables": list(d.get("variables") or []), "sections": list(d.get("sections") or []),
            "widgets": list(d.get("widgets") or [])}


class DashboardStore(Protocol):
    """The dashboards of each workspace, as the hosted app's Dashboard records (see the module's docstring)."""

    def list_dashboards(self, ws: str) -> list[dict]:
        """Starred first, then the latest changed (data.ts listDashboards), then by ID."""

    def all_dashboards(self) -> dict[str, list[dict]]:
        """Every workspace's dashboards, by workspace ID, for the alert check (data.ts allDashboards)."""

    def get_dashboard(self, ws: str, dashboard_id: str) -> dict | None:
        ...

    def add_dashboard(self, ws: str, dashboard: dict) -> bool:
        """Stores a new dashboard (a whole record, version 1). False if the workspace has one with its ID."""

    def update_dashboard(self, ws: str, dashboard_id: str, fields: dict, *, unlocked: bool = False,
                         version: int | None = None) -> tuple[Outcome, dict | None]:
        """Sets these fields (FIELDS; ``def`` replaces every widget) and adds one to the version, in one
        transaction: ("ok", the dashboard after). Not changed: ("missing", None) without the dashboard, ("locked",
        None) when ``unlocked`` and it is locked, ("conflict", None) when ``version`` is given and it has changed
        since. ``lockedBy`` given and None unlocks."""

    def lock_dashboard(self, ws: str, dashboard_id: str, by: str, by_id: str | None, at: int) -> dict | None:
        """Locks it unless it is locked already (no one takes over someone else's lock); the dashboard after, or
        None without it."""

    def remove_dashboard(self, ws: str, dashboard_id: str, *, unlocked: bool = False) -> Outcome:
        """Deletes it and its widgets: "ok", "missing", or "locked" (when ``unlocked`` and it is locked)."""


# ── in memory ────────────────────────────────────────────────────────────────────────────────────────────────────

def _order(ds: list[dict]) -> list[dict]:
    return sorted(ds, key=lambda d: (not d["starred"], -d["updatedAt"], d["id"]))


class MemoryDashboards:
    """DashboardStore in memory: the tests. Every method holds one lock (the API runs on request threads)."""

    def __init__(self, dashboards: dict | None = None):
        #: ws -> {dashboard ID: record}
        self.dashboards: dict[str, dict[str, dict]] = copy.deepcopy(dashboards or {})
        self._lock = threading.RLock()

    def list_dashboards(self, ws: str) -> list[dict]:
        with self._lock:
            return copy.deepcopy(_order(list(self.dashboards.get(ws, {}).values())))

    def all_dashboards(self) -> dict[str, list[dict]]:
        with self._lock:
            return {ws: self.list_dashboards(ws) for ws, ds in self.dashboards.items() if ds}

    def get_dashboard(self, ws: str, dashboard_id: str) -> dict | None:
        with self._lock:
            return copy.deepcopy(self.dashboards.get(ws, {}).get(dashboard_id))

    def add_dashboard(self, ws: str, dashboard: dict) -> bool:
        with self._lock:
            mine = self.dashboards.setdefault(ws, {})
            if dashboard["id"] in mine:
                return False
            mine[dashboard["id"]] = {**copy.deepcopy(dashboard), "def": _def(copy.deepcopy(dashboard["def"])),
                                     "version": 1}
            return True

    def update_dashboard(self, ws: str, dashboard_id: str, fields: dict, *, unlocked: bool = False,
                         version: int | None = None) -> tuple[Outcome, dict | None]:
        unknown = set(fields) - set(FIELDS)
        if unknown:
            raise ValueError(f"no such dashboard field: {', '.join(sorted(unknown))}")
        with self._lock:
            d = self.dashboards.get(ws, {}).get(dashboard_id)
            if d is None:
                return "missing", None
            if unlocked and d["lockedBy"] is not None:
                return "locked", None
            if version is not None and d["version"] != version:
                return "conflict", None
            new = copy.deepcopy(fields)
            if "def" in new:
                new["def"] = _def(new["def"])
            d.update(new, version=d["version"] + 1)
            return "ok", copy.deepcopy(d)

    def lock_dashboard(self, ws: str, dashboard_id: str, by: str, by_id: str | None, at: int) -> dict | None:
        with self._lock:
            d = self.dashboards.get(ws, {}).get(dashboard_id)
            if d is None:
                return None
            if d["lockedBy"] is None:
                d.update(lockedBy=by, lockedById=by_id, version=d["version"] + 1)
            return copy.deepcopy(d)

    def remove_dashboard(self, ws: str, dashboard_id: str, *, unlocked: bool = False) -> Outcome:
        with self._lock:
            mine = self.dashboards.get(ws, {})
            d = mine.get(dashboard_id)
            if d is None:
                return "missing"
            if unlocked and d["lockedBy"] is not None:
                return "locked"
            del mine[dashboard_id]
            return "ok"


# ── on the tables ────────────────────────────────────────────────────────────────────────────────────────────────

# the record's field names -> the dashboard row's columns
_COLS = (("id", "id"), ("name", "name"), ("time", "time_range"), ("starred", "starred"), ("lockedBy", "locked_by"),
         ("lockedById", "locked_by_id"), ("createdBy", "created_by"), ("createdById", "created_by_id"),
         ("updatedBy", "updated_by"), ("createdAt", "created_at_ms"), ("updatedAt", "updated_at_ms"),
         ("version", "version"))
_COL = dict(_COLS)
_SELECT = ("SELECT d.ws AS ws, " + ", ".join(f"d.{c} AS {c}" for _, c in _COLS)
           + ", d.variables_json AS variables_json, d.sections_json AS sections_json, "
             "w.def_json AS widget_json FROM dashboards d LEFT JOIN dashboard_widgets w "
             "ON w.ws = d.ws AND w.dashboard_id = d.id")
# starred first, then the latest changed; a dashboard's widgets in their order
_ORDER = " ORDER BY d.ws, d.starred DESC, d.updated_at_ms DESC, d.id, w.position"


def _records(rows: list[dict]) -> list[tuple[str, dict]]:
    """(workspace, dashboard) of the joined rows, in their order: one dashboard per run of rows."""
    out: list[tuple[str, dict]] = []
    for r in rows:
        if not out or out[-1][0] != r["ws"] or out[-1][1]["id"] != r["id"]:
            d = {f: r[c] for f, c in _COLS}
            d["starred"] = bool(d["starred"])
            d["def"] = {"variables": json.loads(r["variables_json"]), "sections": json.loads(r["sections_json"]),
                        "widgets": []}
            out.append((r["ws"], d))
        if r["widget_json"] is not None:
            out[-1][1]["def"]["widgets"].append(json.loads(r["widget_json"]))
    return out


def _json(v: Any) -> str:
    return json.dumps(v, ensure_ascii=False, separators=(",", ":"))


class DashboardTables:
    """DashboardStore on a SqliteStore (db.Database) or a PostgresStore. Thread-safe."""

    def __init__(self, store):
        self.store = store
        self.postgres = hasattr(store, "reads") and hasattr(store, "ingest")

    # ---- the two kinds of connection (as alerts_store.AlertTables) ----------------------------------------------

    def _read(self, q: str, args: tuple = ()) -> list[dict]:
        if self.postgres:
            with self.store.reads.connection() as c:
                return [dict(r) for r in c.execute(q.replace("?", "%s"), args).fetchall()]
        with self.store.reader() as c:
            return [dict(r) for r in c.execute(q, args).fetchall()]

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

    # ---- reads (one statement each, so one snapshot) ----------------------------------------------------------

    def list_dashboards(self, ws: str) -> list[dict]:
        return [d for _, d in _records(self._read(_SELECT + " WHERE d.ws = ?" + _ORDER, (ws,)))]

    def all_dashboards(self) -> dict[str, list[dict]]:
        out: dict[str, list[dict]] = {}
        for ws, d in _records(self._read(_SELECT + _ORDER)):
            out.setdefault(ws, []).append(d)
        return out

    def get_dashboard(self, ws: str, dashboard_id: str) -> dict | None:
        got = _records(self._read(_SELECT + " WHERE d.ws = ? AND d.id = ?" + _ORDER, (ws, dashboard_id)))
        return got[0][1] if got else None

    # ---- writes -----------------------------------------------------------------------------------------------

    @staticmethod
    def _put_widgets(tx: _Tx, ws: str, dashboard_id: str, widgets: list) -> None:
        tx.run("DELETE FROM dashboard_widgets WHERE ws = ? AND dashboard_id = ?", (ws, dashboard_id))
        for i, w in enumerate(widgets):
            tx.run("INSERT INTO dashboard_widgets (ws, dashboard_id, id, position, def_json) VALUES (?, ?, ?, ?, ?)",
                   (ws, dashboard_id, str(w["id"]), i, _json(w)))

    def add_dashboard(self, ws: str, dashboard: dict) -> bool:
        d = {**dashboard, "version": 1}
        cols = [(c, int(bool(d[f])) if f == "starred" else d.get(f)) for f, c in _COLS]
        dd = _def(d["def"])
        with self._write() as tx:
            n = tx.run(f"INSERT INTO dashboards (ws, {', '.join(c for c, _ in cols)}, variables_json, sections_json) "
                       f"VALUES (?{', ?' * len(cols)}, ?, ?) ON CONFLICT DO NOTHING",
                       (ws, *(v for _, v in cols), _json(dd["variables"]), _json(dd["sections"])))
            if n == 1:
                self._put_widgets(tx, ws, d["id"], dd["widgets"])
        return n == 1

    def update_dashboard(self, ws: str, dashboard_id: str, fields: dict, *, unlocked: bool = False,
                         version: int | None = None) -> tuple[Outcome, dict | None]:
        unknown = set(fields) - set(FIELDS)
        if unknown:
            raise ValueError(f"no such dashboard field: {', '.join(sorted(unknown))}")
        sets: list[tuple[str, Any]] = []
        for f, v in fields.items():
            if f == "def":
                dd = _def(v)
                sets += [("variables_json", _json(dd["variables"])), ("sections_json", _json(dd["sections"]))]
            else:
                sets.append((_COL[f], int(bool(v)) if f == "starred" else v))
        where = "ws = ? AND id = ?"
        args: list[Any] = [ws, dashboard_id]
        if unlocked:
            where += " AND locked_by IS NULL"
        if version is not None:
            where += " AND version = ?"
            args.append(version)
        r: dict | None = None
        with self._write() as tx:
            n = tx.run(f"UPDATE dashboards SET {''.join(f'{c} = ?, ' for c, _ in sets)}version = version + 1 "
                       f"WHERE {where}", (*(v for _, v in sets), *args))
            if n == 1 and "def" in fields:
                self._put_widgets(tx, ws, dashboard_id, _def(fields["def"])["widgets"])
            if n == 0:
                r = tx.one("SELECT locked_by, version FROM dashboards WHERE ws = ? AND id = ?", (ws, dashboard_id))
        if n == 1:
            return "ok", self.get_dashboard(ws, dashboard_id)
        if r is None:
            return "missing", None
        if unlocked and r["locked_by"] is not None:
            return "locked", None
        return "conflict", None

    def lock_dashboard(self, ws: str, dashboard_id: str, by: str, by_id: str | None, at: int) -> dict | None:
        with self._write() as tx:
            tx.run("UPDATE dashboards SET locked_by = ?, locked_by_id = ?, version = version + 1 "
                   "WHERE ws = ? AND id = ? AND locked_by IS NULL", (by, by_id, ws, dashboard_id))
        return self.get_dashboard(ws, dashboard_id)

    def remove_dashboard(self, ws: str, dashboard_id: str, *, unlocked: bool = False) -> Outcome:
        with self._write() as tx:
            n = tx.run("DELETE FROM dashboards WHERE ws = ? AND id = ?" + (" AND locked_by IS NULL" if unlocked else ""),
                       (ws, dashboard_id))
            if n == 1:
                tx.run("DELETE FROM dashboard_widgets WHERE ws = ? AND dashboard_id = ?", (ws, dashboard_id))
                return "ok"
            there = tx.one("SELECT 1 AS x FROM dashboards WHERE ws = ? AND id = ?", (ws, dashboard_id))
        return "locked" if there else "missing"


class _Tx:
    def __init__(self, conn, postgres: bool):
        self.c = conn
        self.postgres = postgres

    def _q(self, q: str) -> str:
        return q.replace("?", "%s") if self.postgres else q

    def run(self, q: str, args: tuple = ()) -> int:
        """Rows changed."""
        return self.c.execute(self._q(q), args).rowcount

    def one(self, q: str, args: tuple = ()) -> dict | None:
        r = self.c.execute(self._q(q), args).fetchone()
        return None if r is None else dict(r)
