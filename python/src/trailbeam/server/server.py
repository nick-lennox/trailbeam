"""HTTP server: OTLP/HTTP trace ingest on /v1/traces, the JSON API under /api/ (https://docs.trailbeam.app/reference/api), and the
web screen (web.py) on every other GET path."""
from __future__ import annotations

import gzip
import json
import logging
import threading
import time
import zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable, Optional
from urllib.parse import parse_qs, unquote, urlsplit

from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceResponse

from . import __version__
from . import bodies, queries, ui_files, web
from .db import Database
from .otlp import DecodeError, decode
from .timeline import DEFAULT_LAMBDA_TIMEOUT_S

log = logging.getLogger("trailbeam.server")

MAX_BODY = 32 * 1024 * 1024  # bytes, after decompression


class ApiError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


class App:
    """Request handling, independent of the HTTP server so tests can call it directly."""

    def __init__(self, db: Database, *, stuck_after_s: int = 300,
                 lambda_timeout_s: int = DEFAULT_LAMBDA_TIMEOUT_S, now: Callable[[], int] = time.time_ns,
                 max_receive_counts: Optional[dict] = None, ui_dir=ui_files.DEFAULT_UI_DIR):
        self.db = db
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

    # ---- OTLP ingest ----------------------------------------------------------------------

    def ingest(self, body: bytes, content_type: str) -> tuple[int, str, bytes]:
        json_mode = (content_type or "").split(";")[0].strip().lower() == "application/json"
        try:
            d = decode(body, content_type)
        except DecodeError as e:
            return 400, "text/plain; charset=utf-8", str(e).encode()
        inserted, dupes = self.db.insert(d.records)
        with self.stats_lock:
            self.stats["requests"] += 1
            self.stats["inserted"] += inserted
            self.stats["duplicates"] += dupes
            self.stats["ignored"] += d.ignored
            self.stats["rejected"] += len(d.rejected)
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
            return 200, "application/json", json.dumps(out).encode()
        return 200, "application/x-protobuf", resp.SerializeToString()

    # ---- JSON API -------------------------------------------------------------------------

    def api(self, path: str, query: dict) -> object:
        parts = [unquote(p) for p in path.split("/") if p]
        if not parts or parts[0] not in self.routes:
            raise ApiError(404, f"no such endpoint: /api/{'/'.join(parts)}")
        return self.routes[parts[0]](query, parts[1:])

    def health(self, query: dict, rest: list[str]) -> dict:
        """Cheap at any size: the ingest counters since start, the configuration, and one indexed read on a
        read connection (``last_row``), so it answers while an export is being written. The exact counts,
        which read the whole table, are ``/api/stats``."""
        if rest:
            raise ApiError(404, "no such endpoint")
        with self.stats_lock:
            ingest = dict(self.stats)
        return {"status": "ok", "version": __version__, "ingest": ingest, "data": {"last_row": self.db.last_row()},
                "config": {"stuck_after_s": self.stuck_after_s, "lambda_timeout_s": self.lambda_timeout_s,
                           "max_receive_counts": self.max_receive_counts,
                           "store_bodies": sorted(self.db.store_bodies)}}

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
            # the Body tab (contract/body-view.md): no logins, so anyone who can reach the server can read it
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
            items = queries.body_search(self.db, self.now(), self._lambda_timeout(q), _limit(q), q.get("queue"), terms,
                                        mrc=self.max_receive_counts)
            return {"messages": items, "body_search": {"stores_bodies": bool(self.db.store_bodies)}}
        items = queries.recent(self.db, self.now(), self._lambda_timeout(q), _limit(q), q.get("queue"),
                               mrc=self.max_receive_counts, tenant=q.get("tenant") or None)
        return {"messages": items}

    def duplicates(self, q: dict, rest: list[str]) -> dict:
        if rest:
            raise ApiError(404, "no such endpoint")
        reason = q.get("reason")
        if reason not in (None, "received_more_than_once", "succeeded_more_than_once"):
            raise ApiError(400, "reason must be received_more_than_once or succeeded_more_than_once")
        items = queries.duplicates(self.db, self.now(), self._lambda_timeout(q), _limit(q), q.get("queue"), reason,
                                   mrc=self.max_receive_counts)
        return {"messages": items}

    def stuck(self, q: dict, rest: list[str]) -> dict:
        if rest:
            raise ApiError(404, "no such endpoint")
        older = _int(q, "older_than_s", self.stuck_after_s, 0, 10 ** 9)
        items = queries.stuck(self.db, self.now(), self._lambda_timeout(q), older, _limit(q), q.get("queue"),
                              mrc=self.max_receive_counts)
        return {"older_than_s": older, "messages": items}

    def tenants(self, q: dict, rest: list[str]) -> dict:
        """``/api/tenants``: the Tenants page (contract/tenants.md) for [from_ms, to_ms], or the last
        ``since_s`` seconds (default a day). ``/api/tenants/search?q=``: tenant names for a search box."""
        if rest == ["search"]:
            return {"tenants": queries.find_tenants(self.db, q.get("q", "").strip(), _int(q, "limit", 8, 1, 100))}
        if rest:
            raise ApiError(404, "no such endpoint")
        now = self.now()
        to_ms = _int(q, "to_ms", now // 1_000_000, 0, 10 ** 15)
        since = _int(q, "since_s", 86_400, 1, 400 * 86_400)
        from_ms = _int(q, "from_ms", to_ms - since * 1000, 0, 10 ** 15)
        if from_ms > to_ms:
            raise ApiError(400, "from_ms must not be after to_ms")
        return queries.tenants(self.db, now, self._lambda_timeout(q), from_ms, to_ms, self.max_receive_counts)


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

        def do_POST(self):
            if urlsplit(self.path).path != "/v1/traces":
                self._send(404, "text/plain", b"not found")
                return
            body = self._body()
            if body is None:
                return
            try:
                status, ctype, out = app.ingest(body, self.headers.get("Content-Type", ""))
            except Exception:
                log.exception("ingest failed")
                status, ctype, out = 500, "text/plain", b"internal error"
            self._send(status, ctype, out)

        def do_GET(self):
            u = urlsplit(self.path)
            q = {k: v[-1] for k, v in parse_qs(u.query).items()}
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


def make_server(db: Database, host: str = "127.0.0.1", port: int = 4318, **app_options) -> ThreadingHTTPServer:
    httpd = ThreadingHTTPServer((host, port), make_handler(App(db, **app_options)))
    httpd.daemon_threads = True
    return httpd
