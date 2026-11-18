"""Postgres storage for Trailbeam (``PostgresStore``): bring-your-own, selected by ``TRAILBEAM_STORE_URL=postgres://…``.

The same Store as SQLite (store.py; db.py is the reference), with the same answers: the conformance suite and
``contract/fixtures`` run against both. The driver (psycopg 3, ``pip install "trailbeam[postgres]"``) is
imported here only, and this module only when the store URL is a Postgres one, so a SQLite install needs
nothing new. The schema and its versions are pg_schema.py. Supported: Postgres 14 and later (CI: 14 and 16);
14 is the first with ``DETACH PARTITION … CONCURRENTLY``, which retention needs (below).

**Ingest** (``insert``): one transaction per export, its statements batched (arrays and pipelined
``executemany``), never a commit per statement and never a ``DELETE``. In it, as in SQLite: the spans (an
exporter's retry skipped by the unique key), the receipt-hash join in both directions, and the summary rows of
every message a span or a join touched (summary_table.py's tables, the same rules). Concurrent exports are
serialised only where they meet: a transaction-scoped advisory lock per receipt hash and per message, taken in
one sorted order, so the join and a message's summary see each other's spans. The join runs in this process;
across several replicas it is QV-81's. Exports and reads use separate connection pools.

**Placement.** Every table is partitioned by place, ``(rclass, bucket)`` (pg_schema.py), with buckets of
``bucket_ns`` (G, a day by default). A span is placed by its own time and its queue's retention class. A
message's summary rows and bodies are placed by its newest span's bucket and the longest retention class among
its queues, and move when that changes. Rows of a message in another place than its own (it crossed a bucket,
it moved to a dead-letter queue kept longer, a span came late) are recorded in ``strays``: rare, and small.

**Retention** (``purge_step``) removes whole places, never rows from ingest. A message expires once the bucket
of its newest span has ended before the cutoff of every queue it has a span on (``retention_granularity_ns``):
with daily buckets, ``7d`` keeps a message 7 to 8 days. One step takes one place, oldest bucket first:

1. *Drop*, when every queue in the place is past its cutoff. In short transactions: the place is marked
   ``dropping`` (ingest writes to the next bucket instead, and waits rather than change a row there); the
   place's stray rows move to their message's place; the rows other places hold of the place's messages are
   deleted (a message goes whole); then each table's partition is detached (``CONCURRENTLY``: no lock that
   stops exports or pages) and dropped.
2. *Sweep*, only for a place that mixes queues with different cutoffs (the retention changed after its spans
   were stored, or the store was given none): the expired messages are deleted in batches of ``limit``,
   outside ingest. A place is swept again only when more of its queues have expired.

With the retention given to the store (as the server does), every place has one retention class, so a pass is
drops only.
"""
from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from typing import Iterable, Optional

import psycopg
import psycopg_pool
from psycopg import errors, sql
from psycopg.rows import dict_row

from . import body_table, pg_schema, summary
from .db import gate_body
from .otlp import RECORD_FIELDS
from .store import (SETTLE_KINDS, TENANT_MAX_CHARS, Cutoffs, ExportAbandoned, PurgeStep, Store,
                    StuckCandidate, TenantActivity)
from .summary_table import COLUMNS, FIELDS, LISTS, ascii_lower

log = logging.getLogger("trailbeam.server.postgres")

DAY_NS = 86_400 * 10 ** 9
FOREVER_CLASS = 2 ** 62  # rclass of a queue kept forever; 0: no retention was given to the store
URL_SCHEMES = ("postgres://", "postgresql://")

# Every column of a span row, as SQLite's spans table has them (store.py "Rows").
SPAN_COLUMNS = (["row"] + [f for f in RECORD_FIELDS] +
                ["attributes", "mid", "mid_source", "receive_row", "body_dropped", "body_id"])
_INT = {"time_ns", "start_ns", "receive_count", "sent_ts_ms", "first_receive_ts_ms", "visibility_timeout", "pid",
        "order_issued", "order_returned", "body_id"}
_INSERT_COLUMNS = ["rclass", "bucket"] + list(RECORD_FIELDS) + ["attributes", "mid", "mid_source", "body_dropped", "body_id"]


def _q(c: str) -> str:
    return '"row"' if c == "row" else c


_SPAN_SELECT = ", ".join(f"s.{_q(c)}" for c in SPAN_COLUMNS)


class _NeedPlaces(Exception):
    def __init__(self, places):
        super().__init__(f"{len(places)} place(s) to create")
        self.places = set(places)


class _PlaceBusy(Exception):
    """A row the export must change is in a place retention is dropping: wait for the drop, then retry."""


def _key(text: str) -> int:
    """A 64-bit advisory lock key."""
    return int.from_bytes(hashlib.blake2b(text.encode("utf-8", "surrogatepass"), digest_size=8).digest(), "big",
                          signed=True)


def _body_bytes(text: str) -> bytes:
    return text.encode("utf-8", "surrogatepass")


class PostgresStore(Store):
    """Thread-safe: an ingest pool and a read pool (``ingest_connections``, ``read_connections``).

    ``retention``: retention.Retention (or anything with ``default`` and ``queues`` in seconds, None forever),
    the retention classes spans are placed by. None: one class; retention then sweeps places whose queues have
    different cutoffs instead of dropping them. ``bucket_ns``: the bucket width G (the granularity)."""

    INGEST_ATTEMPTS = 50
    cancels_abandoned_exports = True
    WATCH_INTERVAL_S = 0.2  # how often an export's client is checked (``insert(abandoned=...)``)

    def __init__(self, url: str, *, store_bodies: Iterable[str] = (), retention=None, bucket_ns: int = DAY_NS,
                 ingest_connections: int = 4, read_connections: int = 8, statement_timeout_ms: int = 60_000):
        if bucket_ns < 1:
            raise ValueError("bucket_ns must be at least 1")
        self.url = url
        self.store_bodies = frozenset(store_bodies)
        self.retention = retention
        self.bucket_ns = int(bucket_ns)
        self.retention_granularity_ns = self.bucket_ns
        with psycopg.connect(url, autocommit=True) as c:
            self.migrated = pg_schema.open_schema(c)
            self.server_version = c.info.server_version

        def ingest_conf(conn):
            conn.execute(f"SET statement_timeout = {int(statement_timeout_ms)}")
            conn.execute("SET lock_timeout = 0")

        def read_conf(conn):
            conn.execute(f"SET statement_timeout = {int(statement_timeout_ms)}")
            conn.execute("SET default_transaction_read_only = on")

        kw = {"autocommit": True, "row_factory": dict_row}
        self.ingest = psycopg_pool.ConnectionPool(url, min_size=1, max_size=ingest_connections, kwargs=kw,
                                                  configure=ingest_conf, name="trailbeam-ingest", open=True)
        self.reads = psycopg_pool.ConnectionPool(url, min_size=1, max_size=read_connections, kwargs=kw,
                                                 configure=read_conf, name="trailbeam-read", open=True)
        self._admin_lock = threading.Lock()
        self._known_queues: set = set()      # (rclass, bucket, queue) rows of place_queues written
        self._cache_lock = threading.Lock()
        self._tx = threading.local()         # per export: place_queues rows written, cached after the commit
        self._watched: dict = {}             # exports whose client is checked: key -> [conn, abandoned, cancelled]
        self._watch_lock = threading.Lock()
        self._stop = threading.Event()
        self._watcher = threading.Thread(target=self._watch_loop, name="trailbeam-pg-abandoned", daemon=True)
        self._watcher.start()

    def close(self) -> None:
        self._stop.set()
        self._watcher.join()
        self.ingest.close()
        self.reads.close()

    # ---- placement ----------------------------------------------------------------------------

    def queue_class(self, queue: Optional[str]) -> int:
        r = self.retention
        if r is None:
            return 0
        queues = getattr(r, "queues", None) or {}
        s = queues[queue] if queue in queues else r.default
        return FOREVER_CLASS if s is None else int(s)

    def bucket(self, time_ns: int) -> int:
        return int(time_ns) // self.bucket_ns

    def _ensure_places(self, places) -> None:
        """Create the partitions of these places, outside any export's transaction (DDL locks the parent)."""
        with self._admin_lock, psycopg.connect(self.url, autocommit=True) as c:
            for rclass, bucket in sorted(places):
                for ddl in pg_schema.place_ddl(rclass, bucket):
                    try:
                        c.execute(ddl)
                    except (errors.UniqueViolation, errors.DuplicateTable):
                        pass  # another server made it at the same moment
                c.execute("INSERT INTO places (rclass, bucket) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                          (rclass, bucket))

    @staticmethod
    def _lock_places(conn, places) -> dict:
        """Lock (FOR SHARE) and read the state of these places: {place: state}; a missing place is absent.
        Retention takes the rows FOR UPDATE to mark a place ``dropping``, so it waits for this transaction."""
        places = sorted(set(places))
        if not places:
            return {}
        rows = conn.execute(
            """SELECT rclass, bucket, state FROM places
               WHERE (rclass, bucket) IN (SELECT * FROM unnest(%s::bigint[], %s::bigint[]))
               ORDER BY rclass, bucket FOR SHARE""",
            ([p[0] for p in places], [p[1] for p in places])).fetchall()
        return {(r["rclass"], r["bucket"]): r["state"] for r in rows}

    def _resolve(self, conn, wanted: dict) -> dict:
        """``wanted``: {key: (rclass, bucket)} -> {key: live place}, locked. A bucket being dropped is replaced
        by the next one (a span late enough to land there is kept a bucket longer); a missing one raises
        _NeedPlaces."""
        out, todo = {}, dict(wanted)
        while todo:
            states = self._lock_places(conn, set(todo.values()))
            missing = {p for p in todo.values() if p not in states}
            if missing:
                raise _NeedPlaces(missing)
            nxt = {}
            for k, p in todo.items():
                if states[p] == "live":
                    out[k] = p
                else:
                    nxt[k] = (p[0], p[1] + 1)
            todo = nxt
        return out

    def _require_live(self, conn, places) -> None:
        """Places holding rows this export changes: all must be live (else _PlaceBusy)."""
        states = self._lock_places(conn, places)
        for p in set(places):
            if states.get(p) != "live":
                raise _PlaceBusy(p)

    def _note_queues(self, conn, pairs) -> None:
        """place_queues rows for these (rclass, bucket, queue), each written once per process."""
        with self._cache_lock:
            new = sorted(set(pairs) - self._known_queues)
        if not new:
            return
        conn.cursor().executemany(
            "INSERT INTO place_queues (rclass, bucket, queue) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", new)
        self._tx.noted += new  # cached once the export commits

    # ---- writes ---------------------------------------------------------------------------

    def _watch_loop(self) -> None:
        """Cancel the database work of exports whose client has gone (rule 5 of QV-69): the client will retry
        the export, so finishing it only adds load. One thread for every export."""
        while not self._stop.wait(self.WATCH_INTERVAL_S):
            with self._watch_lock:
                for w in self._watched.values():
                    if w[2]:
                        continue
                    try:
                        gone = w[1]()
                    except Exception:  # noqa: BLE001
                        gone = False
                    if gone:
                        w[2] = True
                        try:
                            (getattr(w[0], "cancel_safe", None) or w[0].cancel)()
                        except Exception as e:  # noqa: BLE001
                            log.debug("cancel failed: %s", e)

    def insert(self, records: Iterable[dict], *, abandoned=None) -> tuple[int, int]:
        """Store.insert. ``abandoned``: see Store.cancels_abandoned_exports; checked every WATCH_INTERVAL_S
        while the export's transaction runs, which is then cancelled (ExportAbandoned, nothing stored)."""
        records = list(records)  # an exporter's iterator that fails part way stores nothing
        if not records:
            return 0, 0
        prepared = []
        for r in records:
            own = r.get("message_id")
            attrs, dropped = gate_body(r.get("attributes", {}), r.get("queue"), self.store_bodies)
            attrs, body = body_table.split(r["kind"], own, attrs)
            prepared.append((r, own, body_table.attributes_json(attrs), dropped, body))
        delay = 0.01
        for attempt in range(self.INGEST_ATTEMPTS):
            if abandoned is not None and abandoned():
                raise ExportAbandoned()
            try:
                with self.ingest.connection() as conn:
                    watch = [conn, abandoned, False]
                    if abandoned is not None:
                        with self._watch_lock:
                            self._watched[id(watch)] = watch
                    try:
                        self._tx.noted = []
                        with conn.transaction():
                            result = self._insert(conn, prepared)
                    except psycopg.Error:
                        if watch[2]:
                            raise ExportAbandoned() from None
                        raise
                    finally:
                        if abandoned is not None:
                            with self._watch_lock:
                                del self._watched[id(watch)]
                            if watch[2]:
                                conn.close()  # a cancel may still be on its way: never reuse the connection
                    with self._cache_lock:
                        self._known_queues.update(self._tx.noted)
                    return result
            except _NeedPlaces as e:
                self._ensure_places(e.places)
            except _PlaceBusy:
                time.sleep(delay)
                delay = min(delay * 2, 0.5)
            except (errors.DeadlockDetected, errors.SerializationFailure):
                time.sleep(delay)
            except errors.CheckViolation:
                pass  # "no partition of relation found for row": its place was dropped meanwhile; _resolve sees it
        raise RuntimeError(f"insert did not complete after {self.INGEST_ATTEMPTS} attempts")

    def _insert(self, conn, prepared) -> tuple[int, int]:
        keys = set()
        for r, own, *_ in prepared:
            if own:
                keys.add(_key("m:" + own))
            if r["kind"] == "receive" or r["kind"] in SETTLE_KINDS:
                keys.add(_key(f"h:{r.get('receipt_hmac')}:{r.get('key_id')}"))
        conn.execute("SELECT pg_advisory_xact_lock(k) FROM unnest(%s::bigint[]) AS k", (sorted(keys),))

        want = {i: (self.queue_class(r.get("queue")), self.bucket(r["time_ns"])) for i, (r, *_) in enumerate(prepared)}
        places = self._resolve(conn, want)

        # bodies: one row per (message, text), wherever the message's other rows are (refresh moves it)
        body_ids: dict = {}
        wanted_bodies = {}
        for i, (r, own, _, _, body) in enumerate(prepared):
            if body is not None:
                wanted_bodies.setdefault((own, hashlib.sha256(_body_bytes(body)).digest()), (i, body))
        if wanted_bodies:
            ks = list(wanted_bodies)
            for row in conn.execute(
                    """SELECT mid, sha, id FROM bodies
                       WHERE (mid, sha) IN (SELECT * FROM unnest(%s::text[], %s::bytea[]))""",
                    ([k[0] for k in ks], [k[1] for k in ks])).fetchall():
                body_ids.setdefault((row["mid"], bytes(row["sha"])), row["id"])
            new = [k for k in ks if k not in body_ids]
            if new:
                ids = conn.execute("SELECT nextval('body_ids') AS id FROM generate_series(1, %s)", (len(new),)).fetchall()
                rows = []
                for k, idrow in zip(new, ids):
                    i, body = wanted_bodies[k]
                    body_ids[k] = idrow["id"]
                    rows.append((places[i][0], places[i][1], idrow["id"], k[0], k[1], _body_bytes(body)))
                conn.cursor().executemany(
                    "INSERT INTO bodies (rclass, bucket, id, mid, sha, body) VALUES (%s, %s, %s, %s, %s, %s)", rows)

        cols = {c: [] for c in _INSERT_COLUMNS}
        for i, (r, own, attrs, dropped, body) in enumerate(prepared):
            cols["rclass"].append(places[i][0])
            cols["bucket"].append(places[i][1])
            for f in RECORD_FIELDS:
                cols[f].append(r.get(f))
            cols["attributes"].append(attrs)
            cols["mid"].append(own)
            cols["mid_source"].append("span" if own else None)
            cols["body_dropped"].append(dropped)
            cols["body_id"].append(None if body is None else
                                   body_ids[(own, hashlib.sha256(_body_bytes(body)).digest())])
        types = ["bigint[]" if c in _INT or c in ("rclass", "bucket") else "text[]" for c in _INSERT_COLUMNS]
        names = ", ".join(_INSERT_COLUMNS)
        stmt = (f"INSERT INTO spans ({names}) SELECT {names} FROM unnest("
                + ", ".join(f"%s::{t}" for t in types)
                + f") WITH ORDINALITY AS u({names}, ord) ORDER BY ord "
                  "ON CONFLICT DO NOTHING RETURNING \"row\", trace_id, span_id")
        got = conn.execute(stmt, [cols[c] for c in _INSERT_COLUMNS]).fetchall()
        new_rows = {(g["trace_id"], g["span_id"]): g["row"] for g in got}
        inserted, dupes, order = 0, 0, []
        for i, (r, *_rest) in enumerate(prepared):
            k = (r["trace_id"], r["span_id"])
            if k in new_rows:
                order.append((i, new_rows.pop(k)))
                inserted += 1
            else:
                dupes += 1
        if not order:
            return 0, dupes

        touched, updates = self._join(conn, prepared, places, order)
        if updates:
            conn.execute(
                """UPDATE spans s SET mid = u.mid, mid_source = 'receipt_hash', receive_row = u.rr
                   FROM unnest(%s::bigint[], %s::bigint[], %s::bigint[], %s::text[], %s::bigint[])
                        AS u(rclass, bucket, "row", mid, rr)
                   WHERE s.rclass = u.rclass AND s.bucket = u.bucket AND s."row" = u."row" """,
                [[u[k] for u in updates] for k in range(5)])
        self._note_queues(conn, {(places[i][0], places[i][1], prepared[i][0]["queue"]) for i, _ in order})
        self._refresh(conn, touched)
        return inserted, dupes

    def _join(self, conn, prepared, places, order):
        """The receipt-hash join, in the export's order, exactly as SQLite runs it span by span (db.py). Returns
        the messages touched and the settle rows to update: (rclass, bucket, row, mid, receive_row)."""
        hashes = {(prepared[i][0].get("receipt_hmac"), prepared[i][0].get("key_id")) for i, _ in order
                  if prepared[i][0]["kind"] == "receive" or prepared[i][0]["kind"] in SETTLE_KINDS}
        hashes = sorted(h for h in hashes if h[0] is not None and h[1] is not None)
        mine = {row for _, row in order}
        receives: dict = {}
        unjoined: dict = {}
        if hashes:
            a, b = [h[0] for h in hashes], [h[1] for h in hashes]
            for x in conn.execute(
                    """SELECT "row", message_id, time_ns, receipt_hmac, key_id, kind, mid, rclass, bucket, receive_row
                       FROM spans WHERE (receipt_hmac, key_id) IN (SELECT * FROM unnest(%s::text[], %s::text[]))
                         AND (kind = 'receive' OR (kind IN ('delete', 'change_visibility') AND receive_row IS NULL))
                       ORDER BY "row" """, (a, b)).fetchall():
                if x["row"] in mine:
                    continue
                h = (x["receipt_hmac"], x["key_id"])
                if x["kind"] == "receive":
                    receives.setdefault(h, []).append((x["time_ns"], x["row"], x["message_id"]))
                else:
                    unjoined.setdefault(h, []).append((x["rclass"], x["bucket"], x["row"], x["mid"]))
        touched: set = set()
        updates: dict = {}  # row -> (rclass, bucket, row, mid, receive_row)
        busy = set()
        for i, row in order:
            r, own = prepared[i][0], prepared[i][1]
            h = (r.get("receipt_hmac"), r.get("key_id"))
            if r["kind"] in SETTLE_KINDS:
                cands = receives.get(h, []) if None not in h else []
                if cands:
                    t = r["time_ns"]
                    # SQLite: ORDER BY (time_ns <= t) DESC, CASE WHEN time_ns <= t THEN -time_ns ELSE time_ns END
                    best = min(cands, key=lambda c: (0 if c[0] <= t else 1, -c[0] if c[0] <= t else c[0], c[1]))
                    updates[row] = (places[i][0], places[i][1], row, best[2], best[1])
                    touched.add(best[2])
                else:
                    if None not in h:
                        unjoined.setdefault(h, []).append((places[i][0], places[i][1], row, own))
                    touched.add(own)
            elif r["kind"] == "receive":
                if None not in h:
                    for (rc, bk, srow, smid) in unjoined.pop(h, []):
                        if smid is not None:
                            touched.add(smid)
                        prev = updates.get(srow)
                        updates[srow] = (rc, bk, srow, r.get("message_id"), row)
                        if prev is None and srow not in mine:
                            busy.add((rc, bk))
                    receives.setdefault(h, []).append((r["time_ns"], row, r.get("message_id")))
                touched.add(own)
            else:
                touched.add(own)
        if busy:
            self._require_live(conn, busy)
        return {m for m in touched if m is not None}, list(updates.values())

    def _refresh(self, conn, mids) -> None:
        """Rewrite the summary rows of these messages from their spans (summary_table.refresh's rules), in the
        export's transaction; place them, and record their strays."""
        mids = sorted(set(mids))
        if not mids:
            return
        rows_by: dict = {}
        for r in conn.execute(
                f"""SELECT {_SPAN_SELECT}, s.rclass, s.bucket FROM spans s WHERE s.mid = ANY(%s)
                    ORDER BY s.mid, s.time_ns, s."row" """, (mids,)).fetchall():
            rows_by.setdefault(r["mid"], []).append(r)
        old_by = {r["mid"]: r for r in conn.execute(
            f"SELECT rclass, bucket, strays_json, {', '.join(COLUMNS)} FROM messages WHERE mid = ANY(%s)",
            (mids,)).fetchall()}
        # every place this changes rows in must be live
        self._require_live(conn, {(o["rclass"], o["bucket"]) for o in old_by.values()})
        news = {m: summary.summarize(rows_by[m]) for m in mids if m in rows_by}
        want = {m: (max(self.queue_class(q) for q in s["queues"]), self.bucket(s["last_ns"])) for m, s in news.items()}
        place = self._resolve(conn, want)

        cur = conn.cursor()
        ins_msg, del_rows, upd_cols = [], [], []
        add_q, add_t, add_e, upd_q, upd_t = [], [], [], [], []
        strays_new, bodies_move, noted = [], [], set()
        for m in mids:
            new, old = news.get(m), old_by.get(m)
            if new is None:
                continue  # not reachable from insert (a touched message has the span that touched it)
            pc, pb = place[m]
            lists = _lists(new)
            stray_places = sorted({(r["rclass"], r["bucket"]) for r in rows_by[m]} - {(pc, pb)})
            strays_json = json.dumps([list(p) for p in stray_places])
            noted.update((pc, pb, q) for q in new["queues"])
            if any(r["body_id"] is not None for r in rows_by[m]):
                bodies_move.append((pc, pb, m, pc, pb))
            values = [int(v) if isinstance(v, bool) else v for v in (new[f] for f in FIELDS)] + list(lists)
            if old is not None and (old["rclass"], old["bucket"]) != (pc, pb):
                del_rows.append((old["rclass"], old["bucket"], m))
                old = None
            if old is None:
                ins_msg.append([pc, pb] + values + [strays_json])
                add_q += [(pc, pb, q, new["last_ns"], m) for q in new["queues"]]
                add_t += [(pc, pb, t, new["last_ns"], m, ascii_lower(t), ns) for t, ns in new["tenants"].items()]
                add_e += [(pc, pb) + e for e in _events(new, json.loads(lists[2]))]
            else:
                changed = [(c, v) for c, v in zip(COLUMNS, values) if old[c] != v]
                if old["strays_json"] != strays_json:
                    changed.append(("strays_json", strays_json))
                if changed:
                    upd_cols.append((pc, pb, m, changed))
                last, was = new["last_ns"], old["last_ns"]
                if last != was or lists[0] != old["queues_json"]:
                    del_rows.append((pc, pb, m, "q"))
                    add_q += [(pc, pb, q, last, m) for q in new["queues"]]
                if last != was or lists[1] != old["tenants_json"]:
                    del_rows.append((pc, pb, m, "t"))
                    add_t += [(pc, pb, t, last, m, ascii_lower(t), ns) for t, ns in new["tenants"].items()]
                if lists[2] != old["events_json"] or old["tenant"] != new["tenant"]:
                    del_rows.append((pc, pb, m, "e"))
                    add_e += [(pc, pb) + e for e in _events(new, json.loads(lists[2]))]
            strays_new += [(sc, sb, m) for sc, sb in stray_places]

        # Rewriting a message's list rows replaces them (they are keyed by last_ns): the old ones go in the same
        # transaction. Summary upkeep, not retention; the rows are the message's own few.
        whole = [d for d in del_rows if len(d) == 3]
        for table in ("messages", "message_queues", "message_tenants", "tenant_events"):
            if whole:
                cur.executemany(f"DELETE FROM {table} WHERE rclass = %s AND bucket = %s AND mid = %s", whole)
        for code, table in (("q", "message_queues"), ("t", "message_tenants"), ("e", "tenant_events")):
            part = [d[:3] for d in del_rows if len(d) == 4 and d[3] == code]
            if part:
                cur.executemany(f"DELETE FROM {table} WHERE rclass = %s AND bucket = %s AND mid = %s", part)
        for pc, pb, m, changed in upd_cols:
            cur.execute(sql.SQL("UPDATE messages SET {} WHERE rclass = %s AND bucket = %s AND mid = %s").format(
                sql.SQL(", ").join(sql.SQL("{} = %s").format(sql.Identifier(c)) for c, _ in changed)),
                [v for _, v in changed] + [pc, pb, m])
        if ins_msg:
            mcols = ["rclass", "bucket"] + list(COLUMNS) + ["strays_json"]
            cur.executemany(f"INSERT INTO messages ({', '.join(mcols)}) VALUES ({', '.join(['%s'] * len(mcols))})",
                            ins_msg)
        if add_q:
            cur.executemany("INSERT INTO message_queues (rclass, bucket, queue, last_ns, mid) VALUES (%s, %s, %s, %s, %s)",
                            add_q)
        if add_t:
            cur.executemany("""INSERT INTO message_tenants (rclass, bucket, tenant, last_ns, mid, tenant_lower, named_ns)
                               VALUES (%s, %s, %s, %s, %s, %s, %s)""", add_t)
        if add_e:
            cur.executemany("""INSERT INTO tenant_events (rclass, bucket, at_ms, mid, seq, kind, tenant, wait_ms)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", add_e)
        if bodies_move:
            cur.executemany("""UPDATE bodies SET rclass = %s, bucket = %s
                               WHERE mid = %s AND (rclass, bucket) <> (%s, %s)""", bodies_move)
        if strays_new:
            cur.executemany("INSERT INTO strays (rclass, bucket, mid) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                            sorted(set(strays_new)))
        self._note_queues(conn, noted)

    # ---- retention ------------------------------------------------------------------------

    def purge_step(self, cutoffs: Cutoffs, resume: Optional[str] = None, limit: int = 100) -> PurgeStep:
        """Store.purge_step, by place (module docstring). ``resume``: ``<bucket>:<rclass>`` of the next place to
        look at, and for a sweep in progress ``:<last_ns>:<mid>`` of the last message it looked at."""
        if resume is None:
            self._precreate()
            start, sweep_after = (-(2 ** 63), -(2 ** 63)), None
        else:
            parts = resume.split(":", 3)
            start = (int(parts[0]), int(parts[1]))
            sweep_after = (int(parts[2]), parts[3]) if len(parts) == 4 else None
        with self.reads.connection() as c:
            places = c.execute(
                """SELECT rclass, bucket, state, swept FROM places WHERE (bucket, rclass) >= (%s, %s)
                   ORDER BY bucket, rclass LIMIT 1000""", start).fetchall()
            for p in places:
                pc, pb = p["rclass"], p["bucket"]
                end = (pb + 1) * self.bucket_ns
                if end > _latest(cutoffs):
                    return PurgeStep(0, 0, None)  # this and every later bucket ends after every cutoff
                queues = [r["queue"] for r in c.execute(
                    "SELECT queue FROM place_queues WHERE rclass = %s AND bucket = %s", (pc, pb)).fetchall()]
                expired = {q for q in queues if _expired(cutoffs.of(q), end)}
                if p["state"] == "dropping" or expired == set(queues):
                    n, examined, w = self._drop(pc, pb, expired)
                    return PurgeStep(n, examined, f"{pb}:{pc + 1}", w)
                swept = set(json.loads(p["swept"])) if p["swept"] else set()
                if expired and (sweep_after is not None or not expired <= swept):
                    n, examined, done_key, w = self._sweep(pc, pb, expired, sweep_after, limit)
                    if done_key is None:
                        with self.ingest.connection() as w_conn:
                            w_conn.execute("UPDATE places SET swept = %s WHERE rclass = %s AND bucket = %s",
                                           (json.dumps(sorted(expired)), pc, pb))
                        return PurgeStep(n, examined, f"{pb}:{pc + 1}", w)
                    return PurgeStep(n, examined, f"{pb}:{pc}:{done_key[0]}:{done_key[1]}", w)
                sweep_after = None
            if len(places) == 1000:
                last = places[-1]
                return PurgeStep(0, 0, f"{last['bucket']}:{last['rclass'] + 1}")
        return PurgeStep(0, 0, None)

    def _precreate(self) -> None:
        """Places for now and the next bucket, for every class the configuration names, made ahead of the spans
        so that ingest rarely creates a partition (DDL takes a lock on the parent table)."""
        now = self.bucket(time.time_ns())
        classes = {self.queue_class(None)}
        r = self.retention
        if r is not None:
            classes |= {self.queue_class(q) for q in (getattr(r, "queues", None) or {})}
        try:
            self._ensure_places({(c, b) for c in classes for b in (now, now + 1)})
        except psycopg.Error as e:  # e.g. no CREATE privilege: ingest makes them when spans come
            log.warning("could not create the next places ahead of time: %s", e)

    def _drop(self, pc: int, pb: int, expired: set) -> tuple[int, int, float]:
        """Remove one place whole. Returns (spans deleted, messages and unjoined settles examined, ms of writes)."""
        t0 = time.monotonic()
        with self.ingest.connection() as c:
            with c.transaction():
                st = c.execute("SELECT state FROM places WHERE rclass = %s AND bucket = %s FOR UPDATE",
                               (pc, pb)).fetchone()
                if st is None:
                    return 0, 0, 0.0
                c.execute("UPDATE places SET state = 'dropping' WHERE rclass = %s AND bucket = %s", (pc, pb))
            # From here ingest neither writes to the place nor changes a row in it (it waits: _PlaceBusy). The
            # messages changed below are locked as ingest locks them, so neither sees half of the other's change.
            with c.transaction():
                examined = c.execute(
                    "SELECT (SELECT COUNT(*) FROM messages WHERE rclass = %s AND bucket = %s) + "
                    "(SELECT COUNT(*) FROM spans WHERE rclass = %s AND bucket = %s AND mid IS NULL) AS n",
                    (pc, pb, pc, pb)).fetchone()["n"]
                strays = [x["mid"] for x in c.execute(
                    "SELECT mid FROM strays WHERE rclass = %s AND bucket = %s", (pc, pb)).fetchall()]
                spread = [x["mid"] for x in c.execute(
                    "SELECT mid FROM messages WHERE rclass = %s AND bucket = %s AND strays_json <> '[]'",
                    (pc, pb)).fetchall()]
                c.execute("SELECT pg_advisory_xact_lock(k) FROM unnest(%s::bigint[]) AS k",
                          (sorted({_key("m:" + m) for m in strays + spread}),))
                # 1. rows here of messages placed elsewhere: to their message's place
                for x in c.execute(
                        """SELECT mid, rclass, bucket, strays_json FROM messages
                           WHERE mid = ANY(%s) AND (rclass, bucket) <> (%s, %s)""", (strays, pc, pb)).fetchall():
                    c.execute("UPDATE spans SET rclass = %s, bucket = %s WHERE mid = %s AND rclass = %s AND bucket = %s",
                              (x["rclass"], x["bucket"], x["mid"], pc, pb))
                    c.execute("UPDATE messages SET strays_json = %s WHERE rclass = %s AND bucket = %s AND mid = %s",
                              (json.dumps([q for q in json.loads(x["strays_json"]) if q != [pc, pb]]),
                               x["rclass"], x["bucket"], x["mid"]))
                    c.execute("""INSERT INTO place_queues (rclass, bucket, queue)
                                 SELECT %s, %s, value FROM json_array_elements_text(
                                     (SELECT queues_json FROM messages WHERE rclass = %s AND bucket = %s AND mid = %s)::json)
                                 ON CONFLICT DO NOTHING""",
                              (x["rclass"], x["bucket"], x["rclass"], x["bucket"], x["mid"]))
                # 2. rows elsewhere of messages placed here go with them: a message goes whole
                n = 0
                for x in c.execute("SELECT mid, strays_json FROM messages WHERE rclass = %s AND bucket = %s "
                                   "AND mid = ANY(%s)", (pc, pb, spread)).fetchall():
                    for sc, sb in json.loads(x["strays_json"]):
                        if (sc, sb) == (pc, pb):
                            continue
                        n += c.execute("DELETE FROM spans WHERE rclass = %s AND bucket = %s AND mid = %s",
                                       (sc, sb, x["mid"])).rowcount
                        c.execute("DELETE FROM strays WHERE rclass = %s AND bucket = %s AND mid = %s", (sc, sb, x["mid"]))
                n += c.execute("SELECT COUNT(*) AS n FROM spans WHERE rclass = %s AND bucket = %s",
                               (pc, pb)).fetchone()["n"]
            # 3. the partitions themselves
            for t in pg_schema.PARTITIONED:
                self._detach_and_drop(c, t, pc, pb)
            with c.transaction():
                c.execute("DELETE FROM strays WHERE rclass = %s AND bucket = %s", (pc, pb))
                c.execute("DELETE FROM place_queues WHERE rclass = %s AND bucket = %s", (pc, pb))
                c.execute("DELETE FROM places WHERE rclass = %s AND bucket = %s", (pc, pb))
        with self._cache_lock:
            self._known_queues = {k for k in self._known_queues if (k[0], k[1]) != (pc, pb)}
        return n, examined, (time.monotonic() - t0) * 1000

    def _detach_and_drop(self, c, table: str, pc: int, pb: int) -> None:
        name = pg_schema.partition_name(table, pc, pb)
        attached = c.execute(
            """SELECT i.inhdetachpending AS pending FROM pg_inherits i
               JOIN pg_class ch ON ch.oid = i.inhrelid JOIN pg_class pa ON pa.oid = i.inhparent
               WHERE ch.relname = %s AND pa.relname = %s
                 AND ch.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema())""",
            (name, table)).fetchone()
        if attached is not None:
            how = "FINALIZE" if attached["pending"] else "CONCURRENTLY"
            c.execute(f"ALTER TABLE {table} DETACH PARTITION {name} {how}")
        c.execute(f"DROP TABLE IF EXISTS {name}")

    def _sweep(self, pc: int, pb: int, expired: set, after, limit: int):
        """Delete, in one short transaction, the expired messages among the next ``limit`` of a place that mixes
        queues with different cutoffs. Returns (spans deleted, examined, the last (last_ns, mid) looked at or None
        when the place is done, ms)."""
        t0 = time.monotonic()
        with self.ingest.connection() as c:
            with c.transaction():
                self._require_live(c, {(pc, pb)})
                c.execute("SELECT 1 FROM places WHERE rclass = %s AND bucket = %s FOR UPDATE", (pc, pb))
                p = [pc, pb]
                where = ""
                if after is not None:
                    where = " AND (last_ns, mid) > (%s, %s)"
                    p += list(after)
                msgs = c.execute(
                    f"""SELECT mid, last_ns, queues_json, strays_json FROM messages WHERE rclass = %s AND bucket = %s{where}
                        ORDER BY last_ns, mid LIMIT %s""", p + [limit]).fetchall()
                gone = [m for m in msgs if set(json.loads(m["queues_json"])) <= expired]
                n = 0
                if gone:
                    ids = [m["mid"] for m in gone]
                    other = {tuple(x) for m in gone for x in json.loads(m["strays_json"])} - {(pc, pb)}
                    self._require_live(c, other)
                    n += c.execute("DELETE FROM spans WHERE mid = ANY(%s)", (ids,)).rowcount
                    for t in ("messages", "message_queues", "message_tenants", "tenant_events", "bodies"):
                        c.execute(f"DELETE FROM {t} WHERE rclass = %s AND bucket = %s AND mid = ANY(%s)", (pc, pb, ids))
                    c.execute("DELETE FROM strays WHERE mid = ANY(%s)", (ids,))
                unjoined = 0
                if len(msgs) < limit:  # the messages are done: the place's unjoined settle spans, by their own queue
                    unjoined = c.execute(
                        """DELETE FROM spans WHERE rclass = %s AND bucket = %s AND mid IS NULL AND receive_row IS NULL
                           AND kind IN ('delete', 'change_visibility') AND queue = ANY(%s)""",
                        (pc, pb, sorted(expired))).rowcount
                    n += unjoined
        done = None if len(msgs) < limit else (msgs[-1]["last_ns"], msgs[-1]["mid"])
        return n, len(msgs) + unjoined, done, (time.monotonic() - t0) * 1000

    # ---- reads ----------------------------------------------------------------------------

    def _all(self, query, params=()) -> list:
        with self.reads.connection() as c:
            return c.execute(query, params).fetchall()

    def message_rows(self, message_id: str) -> list[dict]:
        out = []
        for r in self._all(f"""SELECT {_SPAN_SELECT}, b.body AS _body FROM spans s
                               LEFT JOIN bodies b ON b.mid = s.mid AND b.id = s.body_id
                               WHERE s.mid = %s ORDER BY s.time_ns, s."row" """, (message_id,)):
            body = r.pop("_body")
            if body is not None:
                attrs = json.loads(r["attributes"])
                attrs[body_table.BODY] = bytes(body).decode("utf-8", "surrogatepass")
                r["attributes"] = body_table.attributes_json(attrs)
            out.append(r)
        return out

    def _newest_first(self, src: str, p: list, limit: int, from_ns, to_ns, after) -> list:
        where, p = "", list(p)
        if from_ns is not None:
            where += " AND s.last_ns >= %s"
            p.append(from_ns)
        if to_ns is not None:
            where += " AND s.last_ns < %s"
            p.append(to_ns)
        if after is not None:
            where += " AND (s.last_ns < %s OR (s.last_ns = %s AND s.mid > %s))"
            p += [after[0], after[0], after[1]]
        table, _, rest = src.partition(" WHERE ")
        return self._all(f"SELECT s.last_ns, s.mid FROM {table} WHERE {rest}{where} "
                         "ORDER BY s.last_ns DESC, s.mid LIMIT %s", tuple(p) + (limit,))

    _ON_QUEUE = (" AND EXISTS (SELECT 1 FROM message_queues q WHERE q.queue = %s AND q.last_ns = s.last_ns"
                 " AND q.mid = s.mid)")

    def recent_messages(self, limit: int, queue: Optional[str] = None, tenant: Optional[str] = None, *,
                        from_ns: Optional[int] = None, to_ns: Optional[int] = None,
                        after: Optional[tuple] = None) -> list[str]:
        if tenant:
            src, p = "message_tenants s WHERE s.tenant = %s", [tenant[:TENANT_MAX_CHARS]]
            if queue:
                src += self._ON_QUEUE
                p.append(queue)
        elif queue:
            src, p = "message_queues s WHERE s.queue = %s", [queue]
        else:
            src, p = "messages s WHERE TRUE", []
        return [r["mid"] for r in self._newest_first(src, p, limit, from_ns, to_ns, after)]

    def duplicate_messages(self, limit: int, queue: Optional[str] = None, reason: Optional[str] = None, *,
                           from_ns: Optional[int] = None, to_ns: Optional[int] = None,
                           after: Optional[tuple] = None) -> list[str]:
        src = "messages s WHERE (s.dup_received <> 0 OR s.dup_succeeded <> 0)"
        src += {None: "", "received_more_than_once": " AND s.dup_received <> 0",
                "succeeded_more_than_once": " AND s.dup_succeeded <> 0"}[reason]
        p: list = []
        if queue:
            src += self._ON_QUEUE
            p.append(queue)
        return [r["mid"] for r in self._newest_first(src, p, limit, from_ns, to_ns, after)]

    def stuck_messages(self, cutoff_ns: int, limit: int, queue: Optional[str] = None, *,
                       from_ns: Optional[int] = None, after: Optional[tuple] = None) -> list[StuckCandidate]:
        where, p = "", [cutoff_ns]
        if from_ns is not None:
            where += " AND s.stuck_first_ns >= %s"
            p.append(from_ns)
        if after is not None:
            where += " AND (s.stuck_first_ns > %s OR (s.stuck_first_ns = %s AND s.mid > %s))"
            p += [after[0], after[0], after[1]]
        if queue:
            where += self._ON_QUEUE
            p.append(queue)
        rows = self._all(
            f"""SELECT s.mid, s.stuck_first_ns, s.stuck_sqs_clock FROM messages s
                WHERE s.stuck_open <> 0 AND s.stuck_first_ns IS NOT NULL AND s.stuck_first_ns < %s{where}
                ORDER BY s.stuck_first_ns, s.mid LIMIT %s""", tuple(p) + (limit,))
        return [StuckCandidate(r["mid"], r["stuck_first_ns"], bool(r["stuck_sqs_clock"])) for r in rows]

    def summaries(self, message_ids: list[str]) -> list[dict]:
        if not message_ids:
            return []
        out = {r["mid"]: r for r in self._all(f"SELECT {', '.join(COLUMNS)} FROM messages WHERE mid = ANY(%s)",
                                              (list(message_ids),))}
        return [out[m] for m in message_ids if m in out]

    def queue_messages(self, queue: str) -> set[str]:
        return {r["mid"] for r in self._all("SELECT mid FROM message_queues WHERE queue = %s", (queue,))}

    def tenant_activity(self, from_ms: int, to_ms: int) -> list[TenantActivity]:
        with self.reads.connection() as c:
            totals = c.execute(
                """SELECT tenant, SUM(CASE WHEN kind = 'sent' THEN 1 ELSE 0 END) AS sent,
                          COUNT(DISTINCT CASE WHEN kind = 'dead_lettered' THEN mid END) AS dead_lettered
                   FROM tenant_events WHERE at_ms BETWEEN %s AND %s GROUP BY tenant""", (from_ms, to_ms)).fetchall()
            waits: dict = {}
            for r in c.execute("""SELECT tenant, wait_ms FROM tenant_events
                                  WHERE at_ms BETWEEN %s AND %s AND kind = 'first_delivery'""", (from_ms, to_ms)):
                waits.setdefault(r["tenant"], []).append(r["wait_ms"])
        return [TenantActivity(r["tenant"], int(r["sent"]), int(r["dead_lettered"]), waits.get(r["tenant"], []))
                for r in totals]

    def find_tenants(self, text: str, limit: int, *, from_ns: Optional[int] = None, to_ns: Optional[int] = None,
                     queue: Optional[str] = None) -> list[str]:
        if not text:
            return []
        lo = ascii_lower(text)
        hi = lo[:-1] + chr(ord(lo[-1]) + 1) if ord(lo[-1]) < 0x10FFFF else None
        where, p = "", []
        if from_ns is not None:
            where += " AND named_ns >= %s"
            p.append(from_ns)
        if to_ns is not None:
            where += " AND named_ns < %s"
            p.append(to_ns)
        if queue:
            where += (" AND EXISTS (SELECT 1 FROM message_queues q WHERE q.queue = %s AND q.last_ns = t.last_ns"
                      " AND q.mid = t.mid)")
            p.append(queue)
        found = []
        with self.reads.connection() as c:
            key = (lo, "")
            while True:
                nxt = c.execute(
                    f"""SELECT tenant_lower, tenant FROM message_tenants
                        WHERE (tenant_lower, tenant) > (%s, %s) {"AND tenant_lower < %s" if hi else ""}
                        ORDER BY tenant_lower, tenant LIMIT 1""", key + ((hi,) if hi else ())).fetchone()
                if nxt is None or not nxt["tenant_lower"].startswith(lo):
                    break
                key = (nxt["tenant_lower"], nxt["tenant"])
                newest = c.execute(
                    f"""SELECT named_ns FROM message_tenants t WHERE tenant_lower = %s AND tenant = %s{where}
                        ORDER BY named_ns DESC LIMIT 1""", key + tuple(p)).fetchone()
                if newest is not None:
                    found.append((nxt["tenant_lower"] != lo, -newest["named_ns"], nxt["tenant"]))
        return [t for *_, t in sorted(found)[:limit]]

    def newest_messages(self, limit: int, queue: Optional[str] = None, *, from_ns: Optional[int] = None,
                        to_ns: Optional[int] = None, after: Optional[tuple] = None) -> list[tuple[int, str]]:
        if queue:
            src, p = "message_queues s WHERE s.queue = %s", [queue]
        else:
            src, p = "messages s WHERE TRUE", []
        return [(r["last_ns"], r["mid"]) for r in self._newest_first(src, p, limit, from_ns, to_ns, after)]

    def body_spans(self, message_ids: list[str]) -> list[dict]:
        if not message_ids:
            return []
        out = []
        for r in self._all("""SELECT s."row", s.kind, s.mid, s.queue, b.body, s.attributes FROM spans s
                              JOIN bodies b ON b.mid = s.mid AND b.id = s.body_id
                              WHERE s.mid = ANY(%s) AND s.body_id IS NOT NULL""", (list(message_ids),)):
            attrs = r.pop("attributes")
            r["body"] = bytes(r["body"]).decode("utf-8", "surrogatepass")
            r["format"] = json.loads(attrs).get("trailbeam.body.format")
            out.append(r)
        return out

    def last_row(self) -> int:
        return self._all('SELECT MAX("row") AS n FROM spans')[0]["n"] or 0

    def counts(self) -> dict:
        with self.reads.connection() as c:
            kinds = {r["kind"]: r["n"] for r in c.execute("SELECT kind, COUNT(*) AS n FROM spans GROUP BY kind")}
            unjoined = c.execute("""SELECT COUNT(*) AS n FROM spans
                                    WHERE kind IN ('delete', 'change_visibility') AND receive_row IS NULL""").fetchone()["n"]
            messages = c.execute("SELECT COUNT(*) AS n FROM messages").fetchone()["n"]
        return {"spans": kinds, "messages": messages, "unjoined_settles": unjoined}


def _lists(s) -> tuple:
    return (json.dumps(s["queues"]), json.dumps(s["tenants"], sort_keys=True),
            json.dumps(sorted(list(e) for e in s["tenant_events"])))


def _events(s, events: list) -> list:
    return [(at, s["mid"], i, kind, s["tenant"], wait) for i, (at, kind, wait) in enumerate(events)]


def _expired(cutoff: Optional[int], bucket_end: int) -> bool:
    return cutoff is not None and bucket_end <= cutoff


def _latest(cutoffs: Cutoffs) -> int:
    """The latest cutoff of any queue (a bucket ending after it has nothing expired); forever: below every time."""
    return max(-(2 ** 63) if c is None else c for c in (cutoffs.default, *cutoffs.queues.values()))


def is_postgres_url(url: Optional[str]) -> bool:
    return bool(url) and url.startswith(URL_SCHEMES)
