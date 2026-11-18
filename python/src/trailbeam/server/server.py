"""HTTP server: OTLP/HTTP trace ingest on /v1/traces, the JSON API under /api/ (https://docs.trailbeam.app/reference/api),
Prometheus metrics on /metrics (metrics.py), and the web screen (web.py) on every other GET path. An export
Trailbeam can't take now is answered 429 or 503 with Retry-After (backpressure.py)."""
from __future__ import annotations

import gzip
import json
import logging
import select
import socket
import threading
import time
import zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable, Optional
from urllib.parse import parse_qs, unquote, urlsplit

from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceResponse

from . import __version__
from . import auth as auth_mod, bodies, metrics, queries, ui_files, web
from .backpressure import GRPC_RESOURCE_EXHAUSTED, GRPC_UNAVAILABLE, IngestGate, Rejected, status_body
from .store import ExportAbandoned, Store, StoreUnavailable, WriterBusy
from .otlp import DecodeError, decode
from .timeline import DEFAULT_LAMBDA_TIMEOUT_S

log = logging.getLogger("trailbeam.server")

MAX_BODY = 32 * 1024 * 1024  # bytes, after decompression
DEFAULT_WINDOW_S = 86_400  # the time window of the Tenants page and the searches, unless given


class ApiError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


class App:
    """Request handling, independent of the HTTP server so tests can call it directly."""

    def __init__(self, db: Store, *, stuck_after_s: int = 300,
                 lambda_timeout_s: int = DEFAULT_LAMBDA_TIMEOUT_S, now: Callable[[], int] = time.time_ns,
                 max_receive_counts: Optional[dict] = None, ui_dir=ui_files.DEFAULT_UI_DIR, purger=None,
                 auth: Optional[auth_mod.Auth] = None, gate: Optional[IngestGate] = None):
        self.db = db
        self.auth = auth  # the shared token (auth.py); None: no token, every request is served (the default)
        self.purger = purger  # retention.Purger, for /api/health; None: no retention configured (tests)
        self.gate = gate or IngestGate()
        self.started = time.time()
        # the shared UI's build (ui_files.py); None serves only the server-rendered pages
        self.ui_dir = ui_dir
        self.max_receive_counts = dict(max_receive_counts or {})
        self.stuck_after_s = stuck_after_s
        self.lambda_timeout_s = lambda_timeout_s
        self.now = now
        self.stats_lock = threading.Lock()
        self.stats = {"requests": 0, "inserted": 0, "duplicates": 0, "ignored": 0, "rejected": 0}
        self.routes: dict[str, Callable[[dict, list[str]], object]] = {
            "health": self.health, "stats": self.stats_counts, "messages": self.messages, "duplicates": self.duplicates,
            "stuck": self.stuck, "tenants": self.tenants}
        self._init_metrics()

    # ---- /metrics -------------------------------------------------------------------------

    def _init_metrics(self) -> None:
        r = self.registry = metrics.Registry()
        r.gauge("trailbeam_build_info", "The Trailbeam version, as a label.", lambda: [((__version__,), 1)],
                ("version",))
        r.gauge("trailbeam_start_time_seconds", "When this server started, Unix time.", lambda: self.started)
        self.m_ingest_requests = r.counter(
            "trailbeam_ingest_requests_total", "OTLP export requests to /v1/traces, by response status.", ("code",))
        self.m_rejected = r.counter(
            "trailbeam_ingest_rejected_requests_total",
            "Exports answered 429 (queue_full), or 503 (writer_behind; store_unreachable: the database can't be "
            "reached) with Retry-After. Nothing in them was stored; OTLP exporters retry them.", ("reason",))
        self.m_spans = r.counter(
            "trailbeam_ingest_spans_total",
            "Trailbeam spans in accepted exports: inserted, duplicate (already stored, e.g. a retry), invalid "
            "(rejected by the span contract, reported as partial success), ignored (not Trailbeam spans).",
            ("result",))
        self.m_export = r.histogram(
            "trailbeam_ingest_request_duration_seconds",
            "Time to answer an export, from the whole body read to the response, by outcome "
            "(accepted, rejected, error; abandoned: the client went away first, nothing stored, no answer).", ("outcome",))
        self.m_wait = r.histogram("trailbeam_ingest_writer_wait_seconds",
                                  "Time an admitted export waited for the store's writer (SQLite: the writer lock; Postgres: a pooled "
                                  "ingest connection).")
        self.m_write = r.histogram("trailbeam_ingest_write_duration_seconds",
                                   "One export's write transaction, receipt-hash joins included.")
        self.m_join = r.histogram("trailbeam_join_duration_seconds",
                                  "One receipt-hash join at insert: settle (a delete or visibility change finds "
                                  "its receive) or receive (a receive finds settle spans that came first).",
                                  ("join",), buckets=metrics.JOIN_BUCKETS)
        self.db.on_join = lambda join, s: self.m_join.observe(s, join=join)
        r.gauge("trailbeam_ingest_queue_depth", "Exports in progress: decoding, waiting for the writer or writing.",
                lambda: self.gate.pending)
        r.gauge("trailbeam_ingest_queue_capacity",
                "Exports in progress at once before more are answered 429 (TRAILBEAM_INGEST_MAX_PENDING).",
                lambda: self.gate.max_pending)
        r.gauge("trailbeam_ingest_max_wait_seconds",
                "How long an export waits for the writer before it is answered 503 (TRAILBEAM_INGEST_MAX_WAIT_S).",
                lambda: self.gate.max_wait_s)
        r.gauge("trailbeam_ingest_retry_after_seconds", "The Retry-After a rejected export would get now.",
                self.gate.retry_after)
        self.m_http = r.histogram("trailbeam_http_request_duration_seconds",
                                  "GET requests (API, pages, /metrics), by route template.", ("route",))
        self.m_http_codes = r.counter("trailbeam_http_requests_total", "GET requests by route template and status.",
                                      ("route", "code"))
        r.gauge("trailbeam_db_file_bytes", "Size of the SQLite database file and its write-ahead log.",
                self._file_sizes, ("file",))
        r.gauge("trailbeam_db_last_row", "The highest span row ID stored, 0 if none (one index step, not a count).",
                self.db.last_row)

    def _file_sizes(self):
        sizes = self.db.file_sizes()
        return None if sizes is None else [((name,), n) for name, n in sizes.items()]

    def metrics_text(self) -> bytes:
        return self.registry.render().encode()

    # ---- OTLP ingest ----------------------------------------------------------------------

    def ingest(self, body: bytes, content_type: str,
               abandoned: Optional[Callable[[], bool]] = None) -> tuple[int, str, bytes, dict]:
        """Answer one export: (status, content type, body, extra headers). 200 only after the spans are
        committed; 429 or 503 (backpressure.py) with nothing written. ExportAbandoned (the client went away,
        nothing stored) is raised through: no answer is sent."""
        t0 = time.perf_counter()
        outcome = "error"
        status = 500
        try:
            status, ctype, out, headers = self._ingest(body, content_type, abandoned)
            outcome = "accepted" if status == 200 else "rejected" if status in (429, 503) else "error"
            return status, ctype, out, headers
        except ExportAbandoned:
            outcome, status = "abandoned", None
            raise
        finally:
            if status is not None:
                self.m_ingest_requests.inc(code=str(status))
            self.m_export.observe(time.perf_counter() - t0, outcome=outcome)

    def _ingest(self, body: bytes, content_type: str,
                abandoned: Optional[Callable[[], bool]]) -> tuple[int, str, bytes, dict]:
        json_mode = (content_type or "").split(";")[0].strip().lower() == "application/json"
        try:
            with self.gate.admit():
                try:
                    d = decode(body, content_type)
                except DecodeError as e:
                    return 400, "text/plain; charset=utf-8", str(e).encode(), {}
                timings: dict = {}
                kw = {"abandoned": abandoned} if abandoned is not None and self.db.cancels_abandoned_exports else {}
                try:
                    inserted, dupes = self.db.insert(d.records, wait_s=self.gate.max_wait_s, timings=timings, **kw)
                except WriterBusy:
                    self.m_wait.observe(self.gate.max_wait_s)
                    raise self.gate.writer_behind(self.gate.max_wait_s) from None
                except StoreUnavailable as e:
                    log.warning("ingest: %s", e)
                    raise self.gate.store_unreachable() from None
                self.m_wait.observe(timings["wait_s"])
                self.m_write.observe(timings["write_s"])
                self.gate.wrote(timings["write_s"])
        except Rejected as e:
            self.m_rejected.inc(reason=e.reason)
            log.info("export answered %d (%s): %s", e.status, e.reason, e.message)
            code = GRPC_RESOURCE_EXHAUSTED if e.status == 429 else GRPC_UNAVAILABLE
            ctype, out = status_body(code, e.message, json_mode)
            return e.status, ctype, out, {"Retry-After": str(e.retry_after_s)}
        with self.stats_lock:
            self.stats["requests"] += 1
            self.stats["inserted"] += inserted
            self.stats["duplicates"] += dupes
            self.stats["ignored"] += d.ignored
            self.stats["rejected"] += len(d.rejected)
        for result, n in (("inserted", inserted), ("duplicate", dupes), ("ignored", d.ignored),
                          ("invalid", len(d.rejected))):
            if n:
                self.m_spans.inc(n, result=result)
        resp = ExportTraceServiceResponse()
        msg = ""
        if d.rejected:
            reasons = sorted(set(d.rejected))
            msg = f"{len(d.rejected)} Trailbeam span(s) rejected: " + "; ".join(reasons[:5])
            log.warning(msg)
            resp.partial_success.rejected_spans = len(d.rejected)
            resp.partial_success.error_message = msg
        if json_mode:
            out = {"partialSuccess": {"rejectedSpans": str(len(d.rejected)), "errorMessage": msg}} if d.rejected else {}
            return 200, "application/json", json.dumps(out).encode(), {}
        return 200, "application/x-protobuf", resp.SerializeToString(), {}

    # ---- JSON API -------------------------------------------------------------------------

    def api(self, path: str, query: dict) -> object:
        parts = [unquote(p) for p in path.split("/") if p]
        if not parts or parts[0] not in self.routes:
            raise ApiError(404, f"no such endpoint: /api/{'/'.join(parts)}")
        try:
            return self.routes[parts[0]](query, parts[1:])
        except queries.CursorError as e:
            raise ApiError(400, str(e)) from None

    def health(self, query: dict, rest: list[str]) -> dict:
        """Cheap at any size: the ingest counters since start, the configuration, and one indexed read on a
        read connection (``last_row``), so it answers while an export is being written. The exact counts,
        which read the whole table, are ``/api/stats``."""
        if rest:
            raise ApiError(404, "no such endpoint")
        with self.stats_lock:
            ingest = dict(self.stats)
        # Liveness: this answers while the store is unreachable (a database outage must not get every replica
        # restarted); /api/ready is the check that fails then.
        try:
            self.db.ping()
            data = {"last_row": self.db.last_row()}
        except Exception as e:  # noqa: BLE001
            data = {"last_row": None, "store": "unreachable"}
            log.warning("health: the store is unreachable: %s", e)
        return {"status": "ok", "version": __version__, "ingest": ingest, "data": data,
                "config": {"auth": "token" if self.auth else "off",
                           "stuck_after_s": self.stuck_after_s, "lambda_timeout_s": self.lambda_timeout_s,
                           "max_receive_counts": self.max_receive_counts,
                           "store_bodies": sorted(self.db.store_bodies),
                           "retention": self.purger.status() if self.purger else None}}

    def ready(self) -> tuple[int, dict]:
        """``/api/ready``: 200 when the store answers, else 503. For a load balancer or a Kubernetes readiness
        probe: a replica that can't reach its database is taken out of rotation, not restarted. Like
        /api/health, answered without the token (it says nothing but ready or not)."""
        try:
            self.db.ping()
        except Exception as e:  # noqa: BLE001
            log.warning("ready: the store is unreachable: %s", e)
            return 503, {"status": "not ready", "error": "the store is unreachable"}
        return 200, {"status": "ready", "version": __version__}

    def stats_counts(self, query: dict, rest: list[str]) -> dict:
        """``/api/stats``: spans by kind, messages and settle spans not joined yet. Exact, so it reads the whole
        table (seconds at 10M spans), on a read connection: ingest and pages don't wait for it."""
        if rest:
            raise ApiError(404, "no such endpoint")
        return {"data": self.db.counts()}

    def _lambda_timeout(self, q: dict) -> int:
        return _int(q, "lambda_timeout_s", self.lambda_timeout_s, 1, 86_400)

    def messages(self, q: dict, rest: list[str]) -> dict:
        if len(rest) > 2 or (len(rest) == 2 and rest[1] != "body"):
            raise ApiError(404, "no such endpoint")
        if len(rest) == 2:
            # the Body tab (contract/body-view.md): anyone the server lets in (auth.py) can read it
            rows = self.db.message_rows(rest[0])
            if not rows:
                raise ApiError(404, f"no spans for message ID {rest[0]!r}")
            return bodies.body_view(rest[0], rows, self.db.store_bodies)
        if rest:
            t = queries.message(self.db, rest[0], self.now(), self._lambda_timeout(q), self.max_receive_counts)
            if t is None:
                raise ApiError(404, f"no spans for message ID {rest[0]!r}")
            return t
        if q.get("body", "").strip():
            try:
                terms = bodies.parse_query(q["body"])
            except bodies.QueryError as e:
                raise ApiError(400, str(e)) from None
            # bounded: the messages last seen in a window (default the last day), on a queue and naming a tenant if
            # given, and at most bodies.SCAN of them per page; next_cursor goes on from where this page stopped
            from_ms, to_ms = self._window(q)
            r = queries.body_search_page(self.db, self.now(), self._lambda_timeout(q), _limit(q), q.get("queue") or None,
                                         terms, mrc=self.max_receive_counts, from_ns=from_ms * 1_000_000,
                                         to_ns=(to_ms + 1) * 1_000_000, cursor=q.get("cursor"),
                                         tenant=q.get("tenant") or None)
            return {"messages": r.page.items, "next_cursor": r.page.next_cursor,
                    "body_search": {"stores_bodies": bool(self.db.store_bodies), "from_ms": from_ms, "to_ms": to_ms,
                                    "scanned": r.scanned,
                                    "searched_from_ms": None if r.oldest_ns is None else r.oldest_ns // 1_000_000,
                                    "complete": r.page.next_cursor is None}}
        frm, to = self._range(q)
        page = queries.recent_page(self.db, self.now(), self._lambda_timeout(q), _limit(q), q.get("queue"),
                                   mrc=self.max_receive_counts, tenant=q.get("tenant") or None,
                                   from_ns=frm, to_ns=to, cursor=q.get("cursor"))
        return {"messages": page.items, "next_cursor": page.next_cursor}

    def duplicates(self, q: dict, rest: list[str]) -> dict:
        if rest:
            raise ApiError(404, "no such endpoint")
        reason = q.get("reason")
        if reason not in (None, "received_more_than_once", "succeeded_more_than_once"):
            raise ApiError(400, "reason must be received_more_than_once or succeeded_more_than_once")
        frm, to = self._range(q)
        page = queries.duplicates_page(self.db, self.now(), self._lambda_timeout(q), _limit(q), q.get("queue"), reason,
                                       mrc=self.max_receive_counts, from_ns=frm, to_ns=to, cursor=q.get("cursor"))
        return {"messages": page.items, "next_cursor": page.next_cursor}

    def stuck(self, q: dict, rest: list[str]) -> dict:
        if rest:
            raise ApiError(404, "no such endpoint")
        older = _int(q, "older_than_s", self.stuck_after_s, 0, 10 ** 9)
        # only from_ms: the upper bound is now minus older_than_s (queries.stuck_page)
        frm = _int(q, "from_ms", -1, 0, 10 ** 15) * 1_000_000 if "from_ms" in q else None
        page = queries.stuck_page(self.db, self.now(), self._lambda_timeout(q), older, _limit(q), q.get("queue"),
                                  mrc=self.max_receive_counts, from_ns=frm, cursor=q.get("cursor"))
        return {"older_than_s": older, "messages": page.items, "next_cursor": page.next_cursor}

    def tenants(self, q: dict, rest: list[str]) -> dict:
        """``/api/tenants``: the Tenants page (contract/tenants.md) for [from_ms, to_ms], or the last
        ``since_s`` seconds (default a day). ``/api/tenants/search?q=``: tenant names for a search box."""
        if rest and rest != ["search"]:
            raise ApiError(404, "no such endpoint")
        now = self.now()
        from_ms, to_ms = self._window(q)
        if rest == ["search"]:
            # bounded by the page's range, so a search costs the same however much history is kept
            return {"tenants": queries.find_tenants(self.db, q.get("q", "").strip(), _int(q, "limit", 8, 1, 100),
                                                    from_ms=from_ms, to_ms=to_ms, queue=q.get("queue") or None),
                    "from_ms": from_ms, "to_ms": to_ms}
        # the server's Lambda timeout, not a lambda_timeout_s parameter: it doesn't change what the page counts
        return queries.tenants(self.db, now, self.lambda_timeout_s, from_ms, to_ms, self.max_receive_counts)

    def _window(self, q: dict) -> tuple:
        """[from_ms, to_ms], both included: given, or the last ``since_s`` seconds (default a day) to now. The
        Tenants page, tenant search and body search, which must not read all history, take this."""
        to_ms = _int(q, "to_ms", self.now() // 1_000_000, 0, 10 ** 15)
        since = _int(q, "since_s", DEFAULT_WINDOW_S, 1, 400 * 86_400)
        from_ms = _int(q, "from_ms", max(0, to_ms - since * 1000), 0, 10 ** 15)
        if from_ms > to_ms:
            raise ApiError(400, "from_ms must not be after to_ms")
        return from_ms, to_ms

    def _range(self, q: dict) -> tuple:
        """``from_ms``/``to_ms``, both optional and included, as [from_ns, to_ns)."""
        frm = _int(q, "from_ms", -1, 0, 10 ** 15) if "from_ms" in q else None
        to = _int(q, "to_ms", -1, 0, 10 ** 15) if "to_ms" in q else None
        if frm is not None and to is not None and frm > to:
            raise ApiError(400, "from_ms must not be after to_ms")
        return (None if frm is None else frm * 1_000_000), (None if to is None else (to + 1) * 1_000_000)


def _int(q: dict, name: str, default: int, lo: int, hi: int) -> int:
    if name not in q:
        return default
    try:
        v = int(q[name])
    except ValueError:
        raise ApiError(400, f"{name} must be an integer") from None
    if not lo <= v <= hi:
        raise ApiError(400, f"{name} must be between {lo} and {hi}")
    return v


def _limit(q: dict) -> int:
    return _int(q, "limit", 100, 1, queries.MAX_LIMIT)


def make_handler(app: App):
    class Handler(BaseHTTPRequestHandler):
        server_version = f"trailbeam/{__version__}"
        protocol_version = "HTTP/1.1"

        def _send(self, status: int, ctype: str, body: bytes, headers: Optional[dict] = None):
            if getattr(self, "_count_ingest_reject", False):
                # count a rejected export before the client can see the answer, so a scrape
                # that follows the response always includes it
                self._count_ingest_reject = False
                app.m_ingest_requests.inc(code=str(status))
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            for k, v in (headers or {}).items():
                self.send_header(k, v)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _json(self, status: int, obj):
            self._send(status, "application/json", json.dumps(obj, indent=None).encode())

        def _chunked(self) -> Optional[bytes]:
            """A ``Transfer-Encoding: chunked`` body, as the Node.js OTLP exporter sends it (no Content-Length).
            None after an error response. The connection is closed then: its stream position is lost."""
            parts, total = [], 0
            try:
                while True:
                    size = int(self.rfile.readline(1024).split(b";", 1)[0].strip(), 16)
                    if size < 0:
                        raise ValueError
                    total += size
                    if total > MAX_BODY:
                        self.close_connection = True
                        self._send(413, "text/plain", b"request body too large")
                        return None
                    if size == 0:
                        while self.rfile.readline(1024) not in (b"\r\n", b"\n", b""):
                            pass  # trailer fields, ignored
                        return b"".join(parts)
                    chunk = self.rfile.read(size)
                    if len(chunk) != size or self.rfile.readline(1024) not in (b"\r\n", b"\n"):
                        raise ValueError
                    parts.append(chunk)
            except ValueError:
                self.close_connection = True
                self._send(400, "text/plain", b"malformed chunked request body")
                return None

        def _body(self) -> Optional[bytes]:
            te = (self.headers.get("Transfer-Encoding") or "").lower()
            if te:
                if te.strip() != "chunked":  # other transfer codings are not supported
                    self.close_connection = True
                    self._send(501, "text/plain", f"unsupported Transfer-Encoding {te}".encode())
                    return None
                body = self._chunked()  # takes precedence over any Content-Length (RFC 9112 6.3)
                if body is None:
                    return None
            else:
                try:
                    n = int(self.headers.get("Content-Length") or 0)
                except ValueError:
                    n = -1
                if n < 0 or n > MAX_BODY:
                    self.close_connection = True
                    self._send(413, "text/plain", b"request body too large or without a valid Content-Length")
                    return None
                body = self.rfile.read(n)
            enc = (self.headers.get("Content-Encoding") or "identity").lower()
            try:
                if enc == "gzip":
                    body = _bounded(zlib.decompressobj(16 + zlib.MAX_WBITS), body)
                elif enc == "deflate":
                    body = _bounded(zlib.decompressobj(), body)
                elif enc != "identity":
                    self._send(415, "text/plain", f"unsupported Content-Encoding {enc}".encode())
                    return None
            except (zlib.error, EOFError, gzip.BadGzipFile):
                self._send(400, "text/plain", b"body does not match its Content-Encoding")
                return None
            except OverflowError:
                self._send(413, "text/plain", b"decompressed request body too large")
                return None
            return body

        def _unauthorized(self, ctype: str, body: bytes):
            self._send(401, ctype, body, {"WWW-Authenticate": 'Bearer realm="trailbeam"'})

        def do_POST(self):
            self._status = 500
            path = urlsplit(self.path).path
            if path == "/login" and app.auth is not None:
                self._login()
                return
            if path != "/v1/traces":
                self._send(404, "text/plain", b"not found")
                return
            self._count_ingest_reject = True  # a 401, 413, 415, 400 or 501 below is counted as it is sent
            if app.auth is not None and not app.auth.bearer_ok(self.headers.get("Authorization")):
                self.close_connection = True  # the request body is not read
                self._unauthorized("text/plain", b"this server needs Authorization: Bearer <TRAILBEAM_TOKEN>")
                return
            body = self._body()
            if body is None:  # 413, 415, 400 or 501, already sent and counted
                self._count_ingest_reject = False  # the handler is reused on a kept-alive connection
                return
            self._count_ingest_reject = False  # app.ingest counts the rest
            try:
                status, ctype, out, headers = app.ingest(body, self.headers.get("Content-Type", ""),
                                                         abandoned=lambda: _client_gone(self.connection))
            except ExportAbandoned:
                log.info("an export was abandoned by its client (it timed out or disconnected): not stored")
                self.close_connection = True
                return
            except Exception:
                log.exception("ingest failed")
                status, ctype, out, headers = 500, "text/plain", b"internal error", {}
            self._send(status, ctype, out, headers)

        def do_GET(self):
            u = urlsplit(self.path)
            t0 = time.perf_counter()
            self._status = 500
            try:
                self._get(u)
            finally:
                route = metrics.route_label(u.path, app.routes)
                app.m_http.observe(time.perf_counter() - t0, route=route)
                app.m_http_codes.inc(route=route, code=str(self._status))

        def send_response(self, code, message=None):
            self._status = code
            super().send_response(code, message)

        def _get(self, u):
            q = {k: v[-1] for k, v in parse_qs(u.query).items()}
            if u.path == "/api/ready":  # readiness probes: no token, and nothing but ready or not
                self._json(*app.ready())
                return
            if app.auth is not None and u.path in ("/login", "/logout"):
                if u.path == "/logout":
                    self._send(303, "text/plain", b"", {"Location": "/login", "Set-Cookie": auth_mod.clear_cookie()})
                else:
                    self._send_page(web.login_page(auth_mod.safe_next(q.get("next")), status=200))
                return
            if app.auth is not None and not app.auth.request_ok(self.headers):
                if u.path == "/api/health":  # health checks: alive, nothing more
                    self._json(200, {"status": "ok", "version": __version__})
                elif u.path.startswith("/api/"):
                    self._unauthorized("application/json", json.dumps(
                        {"error": "this server needs Authorization: Bearer <TRAILBEAM_TOKEN>"}).encode())
                else:
                    self._send_page(web.login_page(auth_mod.safe_next(self.path)))
                return
            if u.path == "/metrics":  # behind the token when one is set (Prometheus: authorization.credentials_file)
                try:
                    self._send(200, metrics.CONTENT_TYPE, app.metrics_text())
                except Exception:
                    log.exception("metrics failed")
                    self._send(500, "text/plain", b"internal error")
                return
            if not u.path.startswith("/api/"):
                self._page(u.path, q)
                return
            try:
                self._json(200, app.api(u.path[len("/api/"):], q))
            except ApiError as e:
                self._json(e.status, {"error": e.message})
            except Exception:
                log.exception("API request failed")
                self._json(500, {"error": "internal error"})

        def _login(self):
            """POST /login: the token from the form; right, the session cookie and back to the page."""
            n = self.headers.get("Content-Length") or ""
            if not n.isdigit() or int(n) > 8192:
                self.close_connection = True
                self._send(413, "text/plain", b"login form too large")
                return
            form = {k: v[-1] for k, v in parse_qs(self.rfile.read(int(n)).decode("utf-8", "replace")).items()}
            nxt = auth_mod.safe_next(form.get("next"))
            if not app.auth.token_ok(form.get("token", "")):
                log.warning("login with a wrong token from %s", self.address_string())
                self._send_page(web.login_page(nxt, error=True))
                return
            secure = (self.headers.get("X-Forwarded-Proto") or "").lower() == "https"
            self._send(303, "text/plain", b"", {"Location": nxt, "Set-Cookie": app.auth.set_cookie(secure)})

        def _send_page(self, r):
            status, headers, body = r
            headers = dict(headers)
            ctype = headers.pop("Content-Type", "text/plain")
            self._send(status, ctype, body, headers)

        def _page(self, path: str, q: dict):
            try:
                r = ui_files.handle(app.ui_dir, path) or web.handle(app.api, ApiError, path, q)
            except Exception:
                log.exception("page request failed")
                r = 500, {"Content-Type": "text/plain"}, b"internal error"
            if r is None:
                self._send(404, "text/plain", b"not found")
                return
            status, headers, body = r
            headers = dict(headers)
            ctype = headers.pop("Content-Type", "text/plain")
            self._send(status, ctype, body, headers)

        def log_message(self, fmt, *args):
            log.debug("%s %s", self.address_string(), fmt % args)

    return Handler


def _bounded(d, data: bytes) -> bytes:
    out = d.decompress(data, MAX_BODY + 1)
    if len(out) > MAX_BODY or d.unconsumed_tail:
        raise OverflowError
    if not d.eof:
        raise EOFError
    return out


def _client_gone(sock) -> bool:
    """The client closed its side of the connection (a peek reads end of file). Data waiting (a pipelined next
    request) is not a close."""
    try:
        ready, _, _ = select.select([sock], [], [], 0)
        if not ready:
            return False
        return sock.recv(1, socket.MSG_PEEK) == b""
    except (OSError, ValueError):
        return True


def make_server(db: Store, host: str = "127.0.0.1", port: int = 4318, **app_options) -> ThreadingHTTPServer:
    app = App(db, **app_options)
    httpd = ThreadingHTTPServer((host, port), make_handler(app))
    httpd.daemon_threads = True
    httpd.app = app
    return httpd
