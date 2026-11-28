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
from . import github as github_mod
from . import queues as queues_mod
from . import queue_now as queue_now_mod
from . import alerts_api, aws_api, queue_data, dashboards_api, access as access_mod, auth as auth_mod, causes as causes_mod, proxy_auth as proxy_auth_mod, bodies, csrf, first_admin, \
    metrics, oidc as oidc_mod, passkeys as passkeys_mod, passwords, people_api, queries, \
    sessions as sessions_mod, trace_links, ui_files, web
from .accounts import AccountError, Accounts, AdminExists, BadSetupCode, check_email, check_name
from .backpressure import GRPC_RESOURCE_EXHAUSTED, GRPC_UNAVAILABLE, IngestGate, Rejected, status_body
from .sessions import Sessions
from .signin import AddressLimit, SignIn
from .tokens import Tokens
from . import events as events_mod
from . import timeline_events
from .store import ExportAbandoned, Store, StoreFull, StoreTimeout, StoreUnavailable, WriterBusy
from .proxy import TrustedProxies
from .otlp import PROBE_NOT_STORED, DecodeError, decode, valid_probe_id
from .timeline import DEFAULT_LAMBDA_TIMEOUT_S

log = logging.getLogger("trailbeam.server")
_UNSET = object()  # App.api: no route given

MAX_BODY = 32 * 1024 * 1024  # bytes, after decompression
DEFAULT_WINDOW_S = 86_400  # the time window of the Tenants page and the searches, unless given
STUCK_COUNT_CAP = 1000  # trailbeam_stuck_messages counts up to this many
DEAD_LETTER_WINDOW_S = 900  # trailbeam_dead_lettered_messages_recent: moves in the last 15 minutes
QUEUE_GAUGES_TTL_S = 10.0


class ApiError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


class App:
    """Request handling, independent of the HTTP server so tests can call it directly."""

    @property
    def sso_buttons(self) -> list:
        """The sign-in page's buttons: ``(path, label)`` for single sign-on (oidc.py) and GitHub (github.py), when
        they are on."""
        out = []
        if self.oidc is not None:
            out.append((oidc_mod.START_PATH, self.oidc.settings.label))
        if self.github is not None:
            out.append((github_mod.START_PATH, self.github.settings.label))
        return out

    def __init__(self, db: Store, *, stuck_after_s: int = 300,
                 lambda_timeout_s: int = DEFAULT_LAMBDA_TIMEOUT_S, now: Callable[[], int] = time.time_ns,
                 max_receive_counts: Optional[dict] = None, ui_dir=ui_files.DEFAULT_UI_DIR, purger=None,
                 auth: Optional[auth_mod.Auth] = None, gate: Optional[IngestGate] = None,
                 trusted_proxies: Optional[TrustedProxies] = None, trace_link: Optional[str] = None,
                 accounts="auto", proxy_auth: Optional[proxy_auth_mod.ProxyAuthConfig] = None,
                 oidc: Optional[oidc_mod.Settings] = None, oidc_provider=None,
                 github: Optional[github_mod.Settings] = None, github_provider=None, alerts="auto",
                 public_url: Optional[str] = None, mailer=None, dashboards="auto",
                 passkeys="auto", alert_check=None, aws="auto"):
        self.db = db
        # --public-url: where links in emails and invites point (else the request's address); None: not set
        self.public_url = public_url.rstrip("/") if public_url else None
        # the operator's mail server (mail.py, TRAILBEAM_SMTP_HOST): invite and reset emails; None: links only
        self.mailer = mailer
        # --trusted-proxy (proxy.py): whose X-Forwarded-For/-Proto/-Host is believed; empty: nobody's
        self.proxies = trusted_proxies or TrustedProxies()
        # --trace-link (trace_links.py): the URL template behind each step's "Open trace"; None: no links
        self.trace_link = trace_links.check(trace_link) if trace_link else None
        self.auth = auth  # the shared token (auth.py); None: no token
        # people and tokens in the store (accounts.py); None on a store without the account tables
        self.accounts = Accounts(db) if accounts == "auto" and has_account_tables(db) else (
            None if accounts == "auto" else accounts)
        self.sessions = Sessions(self.accounts) if self.accounts else None
        self.tokens = Tokens(self.accounts) if self.accounts else None
        self.signin = SignIn(self.accounts, self.sessions) if self.accounts else None
        self.people = people_api.PeopleApi(self)  # /api/users, /api/tokens/ingest, /api/me/tokens
        # sign-in through a proxy (proxy_auth.py, --proxy-auth-header); None: off
        self.proxy_auth = proxy_auth
        self.proxy_signin = (proxy_auth_mod.ProxySignIn(proxy_auth, self.accounts, self.proxies)
                             if proxy_auth is not None and self.accounts is not None else None)
        if proxy_auth is not None and self.accounts is None:
            log.warning("--proxy-auth-header is set, but this store has no tables of people: it is ignored")
        # who may do what (access.py): the shared token until an admin exists, then people
        self.access = access_mod.Access(auth, self.accounts, self.sessions, self.tokens, proxy=self.proxy_signin)
        # single sign-on (oidc.py); None: off
        if oidc is not None and self.accounts is None:
            raise ValueError("single sign-on needs a store with accounts (a SQLite file or Postgres)")
        self.oidc = oidc_mod.Oidc(oidc, self.accounts, self.sessions, oidc_provider) if oidc is not None else None
        # sign in with GitHub (github.py); None: off
        if github is not None and self.accounts is None:
            raise ValueError("GitHub sign-in needs a store with accounts (a SQLite file or Postgres)")
        self.github = (github_mod.GitHub(github, self.accounts, self.sessions, github_provider)
                       if github is not None else None)
        # sign-ins started per client address (each adds a row to oidc_logins until it runs out): approximate,
        # per replica, like the sign-in limit
        self.sso_starts = AddressLimit(max_failures=oidc_mod.STARTS_PER_ADDRESS, window_s=oidc_mod.STARTS_WINDOW_S)
        # passkeys (passkeys.py): on with the public URL and the webauthn extra, on a store with accounts; None: off
        self.passkeys = (passkeys_mod.Passkeys(passkeys_mod.settings(self.public_url), self.accounts, self.sessions)
                         if passkeys == "auto" and self.public_url and self.accounts is not None
                         and passkeys_mod.available() else (None if passkeys == "auto" else passkeys))
        self.passkey_starts = AddressLimit(max_failures=passkeys_mod.STARTS_PER_ADDRESS,
                                           window_s=passkeys_mod.STARTS_WINDOW_S)
        try:  # read now, so the first export doesn't wait for it (on a :memory: store, behind the writer)
            self.access.state.get()
        except access_mod.AccessUnavailable as e:
            log.warning("%s; asking again on the next request", e)
        self.purger = purger  # retention.Purger, for /api/health; None: no retention configured (tests)
        # the scheduled alert check (alerts_scheduler.AlertScheduler), for /api/health; None: not running here
        self.alert_check = alert_check
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
            "stuck": self.stuck, "causes": self.causes, "events": self.events, "tenants": self.tenants, "probes": self.probes,
            "me": self.me, "users": self._not_a_route, "tokens": self._not_a_route,
            "queues": self.queues, "flows": self.flows, "timeline-events": self.timeline_events}
        # /api/alerts (alerts_api.py): rules, channels and alert state; None (404) while the alert check is off
        self.alerts: alerts_api.AlertsApi | None = alerts_api.for_app(self) if alerts == "auto" else alerts
        # /api/dashboards (dashboards_api.py): dashboards, widgets and their measures; None (404) while
        # TRAILBEAM_DASHBOARDS is off
        self.dashboards: dashboards_api.DashboardsApi | None = \
            dashboards_api.for_app(self) if dashboards == "auto" else dashboards
        # /api/settings/aws and the AWS reads (aws_api.py, QV-589): always mounted; reads off until an admin names
        # queues, and with them off no AWS call is made
        self.aws: aws_api.ServerAws = aws_api.for_app(self) if aws == "auto" else aws
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
            "reached; store_full: its disk is full; store_timeout: a database statement timed out) with Retry-After. Nothing in them was stored; OTLP exporters retry them.", ("reason",))
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
        # What a team alerts on (QV-304). Read from the store, so every replica of one store serves the same
        # value: aggregate replicas with max, not sum. Cached for QUEUE_GAUGES_TTL_S, so a scrape every few
        # seconds, or several scrapers, read the store at most that often.
        self._queue_gauges_lock = threading.Lock()
        self._queue_gauges_at: Optional[float] = None
        self._queue_gauges_value: dict = {}
        r.gauge("trailbeam_stuck_messages",
                f"Messages stuck now, as the Stuck page lists them: first seen more than TRAILBEAM_STUCK_AFTER_S ago "
                f"and no final delete. Counted up to {STUCK_COUNT_CAP}: {STUCK_COUNT_CAP} means at least that many.",
                lambda: self._queue_gauges()["stuck"])
        r.gauge("trailbeam_stuck_oldest_age_seconds",
                "How long ago the oldest stuck message was first seen, 0 when none is stuck.",
                lambda: self._queue_gauges()["oldest_age_s"])
        r.gauge("trailbeam_dead_lettered_messages_recent",
                f"Messages moved to a dead-letter queue in the last {DEAD_LETTER_WINDOW_S // 60} minutes, by the time "
                f"of the receive on the dead-letter queue that reported the move.",
                lambda: self._queue_gauges()["dead_lettered"])
        r.gauge("trailbeam_stuck_after_seconds", "When a message without a final delete counts as stuck "
                "(TRAILBEAM_STUCK_AFTER_S).", lambda: self.stuck_after_s)

    def _queue_gauges(self) -> dict:
        """Stuck messages and recent dead-letter moves, from the store at most every QUEUE_GAUGES_TTL_S. A read
        that fails is not cached: the gauges are left out of that scrape (metrics.Gauge) and read again next time."""
        with self._queue_gauges_lock:
            t = time.monotonic()
            if self._queue_gauges_at is not None and t - self._queue_gauges_at < QUEUE_GAUGES_TTL_S:
                return self._queue_gauges_value
            now_ns = self.now()
            found = self.db.stuck_messages(now_ns - self.stuck_after_s * 1_000_000_000, STUCK_COUNT_CAP)
            oldest = max(0.0, round((now_ns - found[0].first_ns) / 1e9, 3)) if found else 0
            now_ms = now_ns // 1_000_000
            moved = sum(a.dead_lettered for a in self.db.tenant_activity(now_ms - DEAD_LETTER_WINDOW_S * 1000, now_ms))
            self._queue_gauges_value = {"stuck": len(found), "oldest_age_s": oldest, "dead_lettered": moved}
            self._queue_gauges_at = t
            return self._queue_gauges_value

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

    def _store_failed(self, e: StoreUnavailable, d, body: bytes) -> Rejected:
        """The 503 for a store failure, logged: nothing of the export was stored, and the exporter retries it."""
        if isinstance(e, StoreTimeout):  # QV-300: the size tells a lock pile-up from one export too big to write
            log.error("ingest: export of %d span(s), %d bytes not stored: %s", len(d.records),
                      len(body), e)
            return self.gate.store_timeout()
        if isinstance(e, StoreFull):
            log.error("ingest: %s", e)
            return self.gate.store_full()
        log.warning("ingest: %s", e)
        return self.gate.store_unreachable()

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
                    raise self._store_failed(e, d, body) from None
                self.m_wait.observe(timings["wait_s"])
                self.m_write.observe(timings["write_s"])
                write_s = timings["write_s"]
                if d.probes:  # trailbeam doctor's test spans: after the export's spans, one transaction on its writer
                    t = time.perf_counter()
                    try:
                        self.db.record_probes(d.probes, self.now(), wait_s=self.gate.max_wait_s)
                    except NotImplementedError as e:
                        d.rejected.extend([f"{PROBE_NOT_STORED} {e}"] * len(d.probes))
                    except WriterBusy:
                        raise self.gate.writer_behind(self.gate.max_wait_s) from None
                    except StoreUnavailable as e:
                        raise self._store_failed(e, d, body) from None
                    else:
                        write_s += time.perf_counter() - t
                        log.info("stored probe(s) %s (trailbeam doctor)", ", ".join(d.probes))
                self.gate.wrote(write_s)
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

    def api(self, path: str, query: dict, who: access_mod.Who = access_mod.NOBODY,
            route: object = _UNSET) -> object:
        """GET ``/api/{path}``. ``route``: the people-and-tokens route the access check decided on
        (access.api_route of the exact request path, or None); dispatched on as is, so the handler that runs
        is the one whose level was checked. Not given (tests, in-process callers): matched on the exact path,
        the same way. Never normalised here: /api/users/ or /api//users is no route (a 404), not /api/users."""
        parts = [unquote(p) for p in path.split("/") if p]
        mounted = self.mounted("/api/" + path)
        if mounted is not None:
            try:  # the alerts API reads no query; the dashboards API's measures do
                return mounted.call("GET", "/api/" + path, None if mounted is self.alerts else query, who)[1]
            except alerts_api.ApiProblem as e:
                raise ApiError(e.status, e.message) from None
        r = access_mod.api_route("GET", "/api/" + path) if route is _UNSET else route
        if r is not None:  # people and tokens (people_api.py); the level was checked with the request
            try:
                return self.people.call(r[1], r[2], None, who, "", self.now() // 1_000_000)[1]
            except people_api.ApiProblem as e:
                raise ApiError(e.status, e.message) from None
        if not parts or parts[0] not in self.routes:
            raise ApiError(404, f"no such endpoint: /api/{'/'.join(parts)}")
        try:
            if parts[0] == "me":
                return self.routes["me"](query, parts[1:], who)
            return self.routes[parts[0]](query, parts[1:])
        except queries.CursorError as e:
            raise ApiError(400, str(e)) from None

    def mounted(self, path: str, method: str = "GET"):
        """The JSON API mounted at this request path (alerts_api.py, dashboards_api.py or aws_api.py), when it is on,
        or None. Each answers ``call(method, path, body, who, base)`` and raise alerts_api.ApiProblem."""
        if self.alerts is not None and alerts_api.is_alerts_path(path):
            return self.alerts
        if self.dashboards is not None and dashboards_api.is_dashboards_path(path):
            return self.dashboards
        if self.aws is not None and aws_api.is_aws_path(path) and method in aws_api.METHODS:
            return self.aws
        return None

    def _not_a_route(self, query: dict, rest: list[str]) -> dict:
        """/api/users and /api/tokens paths that aren't one of access.API_ROUTES (matched in api)."""
        raise ApiError(404, "no such endpoint")

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
            data = {"last_row": self.db.last_row(), "last_span_ns": self.db.last_span_ns()}
        except Exception as e:  # noqa: BLE001
            data = {"last_row": None, "last_span_ns": None, "store": "unreachable"}
            log.warning("health: the store is unreachable: %s", e)
        # the alert check: a stuck or late one shows here (alerts_scheduler.AlertScheduler.health); None: not running
        alert_check = self.alert_check.health() if self.alert_check is not None else None
        return {"status": "ok", "version": __version__, "ingest": ingest, "data": data, "alert_check": alert_check,
                "config": {"auth": self._mode(), "ingest_auth": self._ingest_auth(),
                           "stuck_after_s": self.stuck_after_s, "lambda_timeout_s": self.lambda_timeout_s,
                           "max_receive_counts": self.max_receive_counts,
                           "store_bodies": sorted(self.db.store_bodies), "trace_link": self.trace_link,
                           "proxy_auth": self.proxy_signin is not None,
                           "passkeys": self.passkeys is not None,
                           # AWS reads (aws_api.py): {"on": False, ...} until an admin turns them on
                           "aws": self.aws.health() if self.aws is not None else None,
                           "retention": self.purger.status() if self.purger else None}}

    def _mode(self) -> str:
        try:
            return self.access.mode()
        except access_mod.AccessUnavailable:
            return "token" if self.auth else "off"

    def _ingest_auth(self) -> str:
        try:
            return "token" if self.access.ingest_closed() else "open"
        except access_mod.AccessUnavailable:
            return "token"

    def me(self, query: dict, rest: list[str], who: access_mod.Who = access_mod.NOBODY) -> dict:
        """``/api/me``: how this request was let in, and as whom."""
        if rest:
            raise ApiError(404, "no such endpoint")
        user = None if who.user_id is None else {"id": who.user_id, "email": who.email, "name": who.name,
                                                 "role": who.role}
        # whether Sign out (GET /logout) ends anything: a sign-in cookie, or the proxy's own session when
        # --proxy-auth-sign-out-url says where it ends
        sign_out = who.via in ("session", "shared_cookie") or (
            who.via == "proxy" and self.proxy_auth is not None and self.proxy_auth.sign_out_url is not None)
        out = {"auth": self._mode(), "via": who.via, "user": user, "sign_out": sign_out}
        ways = self._sign_in_ways(who.user_id)
        if ways is not None:
            out["sign_in"] = ways
        return out

    def _sign_in_ways(self, user_id: Optional[int]) -> Optional[dict]:
        """``/api/me``'s ``sign_in``, for the account page: whether the person has a password, and the accounts at an
        identity provider (single sign-on, oidc.py, or GitHub, github.py) that are them, each with whether this server
        still offers that way in (``on``). None without a person or accounts."""
        if user_id is None or self.accounts is None:
            return None
        u = self.accounts.user(user_id)
        if u is None:
            return None
        same = lambda a, b: a.rstrip("/") == b.rstrip("/")  # noqa: E731
        ways = []
        for i in self.accounts.oidc_identities(user_id):
            gh = self.github is not None and same(i["issuer"], self.github.settings.github_url)
            sso = self.oidc is not None and same(i["issuer"], self.oidc.settings.issuer)
            # a way this server no longer offers keeps its kind when the address says it (github.com)
            github = gh or (not sso and same(i["issuer"], github_mod.GITHUB_URL))
            label = self.github.settings.label if gh else self.oidc.settings.label if sso else None
            ways.append({"provider": "github" if github else "sso", "label": label, "on": gh or sso,
                         "email": i["email"], "last_sign_in_ms": i["last_sign_in_ms"]})
        return {"password": u["password_hash"] is not None, "identities": ways}

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

    def probes(self, query: dict, rest: list[str]) -> dict:
        """``/api/probes/{id}``: when this store took trailbeam doctor's test span of that ID (contract section 17),
        404 if it has not. Probes are kept a day."""
        if len(rest) != 1:
            raise ApiError(404, "no such endpoint")
        if not valid_probe_id(rest[0]):
            raise ApiError(400, "a probe ID is 8 to 64 letters, digits, - or _")
        received_ns = self.db.probe(rest[0])
        if received_ns is None:
            raise ApiError(404, f"no probe {rest[0]!r} stored")
        return {"data": {"id": rest[0], "received_ns": received_ns}}

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
            return trace_links.add_links(t, self.trace_link)
        if q.get("body", "").strip():
            if q.get("version"):
                raise ApiError(400, "version and body can't be given together: a body search does not filter by "
                                    "version")
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
                                   from_ns=frm, to_ns=to, cursor=q.get("cursor"), version=q.get("version") or None)
        return {"messages": page.items, "next_cursor": page.next_cursor}

    def duplicates(self, q: dict, rest: list[str]) -> dict:
        if rest:
            raise ApiError(404, "no such endpoint")
        reason = q.get("reason")
        if reason not in (None, "received_more_than_once", "succeeded_more_than_once"):
            raise ApiError(400, "reason must be received_more_than_once or succeeded_more_than_once")
        frm, to = self._range(q)
        page = queries.duplicates_page(self.db, self.now(), self._lambda_timeout(q), _limit(q), q.get("queue"), reason,
                                       mrc=self.max_receive_counts, from_ns=frm, to_ns=to, cursor=q.get("cursor"),
                                       version=q.get("version") or None)
        return {"messages": page.items, "next_cursor": page.next_cursor}

    def stuck(self, q: dict, rest: list[str]) -> dict:
        if rest:
            raise ApiError(404, "no such endpoint")
        older = _int(q, "older_than_s", self.stuck_after_s, 0, 10 ** 9)
        # only from_ms: the upper bound is now minus older_than_s (queries.stuck_page)
        frm = _int(q, "from_ms", -1, 0, 10 ** 15) * 1_000_000 if "from_ms" in q else None
        page = queries.stuck_page(self.db, self.now(), self._lambda_timeout(q), older, _limit(q), q.get("queue"),
                                  mrc=self.max_receive_counts, from_ns=frm, cursor=q.get("cursor"),
                                  version=q.get("version") or None)
        return {"older_than_s": older, "messages": page.items, "next_cursor": page.next_cursor}

    def events(self, q: dict, rest: list[str]) -> dict:
        """``/api/events``: the recorded steps in [from_ms, to_ms], or the last ``since_s`` seconds (default a day),
        newest first, and on the first page their counts per time bucket (events.py)."""
        if rest:
            raise ApiError(404, "no such endpoint")
        from_ms, to_ms = self._window(q)
        try:
            kinds = events_mod.parse_kinds(q.get("kind"))
        except events_mod.EventQueryError as e:
            raise ApiError(400, str(e)) from None
        return events_mod.page(self.db, from_ms, to_ms, _limit(q), queue=q.get("queue") or None,
                               service=q.get("service") or None, kinds=kinds,
                               message_id=q.get("message_id") or None, cursor=q.get("cursor"))

    def timeline_events(self, q: dict, rest: list[str]) -> dict:
        """``/api/timeline-events``: the Event explorer's events in [from_ms, to_ms], or the last ``since_s`` seconds
        (default a day), made from each message's timeline, inferred ones included (timeline_events.py, QV-580)."""
        if rest:
            raise ApiError(404, "no such endpoint")
        from_ms, to_ms = self._window(q)
        return timeline_events.page(self.db, from_ms, to_ms, self.now(), self._lambda_timeout(q), self.max_receive_counts,
                                    limit=_int(q, "limit", timeline_events.DEFAULT_MESSAGES, 1, timeline_events.MAX_MESSAGES))

    def causes(self, q: dict, rest: list[str]) -> dict:
        """``/api/causes``: the stuck messages grouped by why they didn't finish (causes.py, QV-367), the Needs
        attention page's groups. ``from_ms``/``to_ms`` narrow them to causes seen in that range; ``cause`` narrows
        the messages listed to one group's. The groups count every message read, the list holds ``limit``."""
        if rest:
            raise ApiError(404, "no such endpoint")
        older = _int(q, "older_than_s", self.stuck_after_s, 0, 10 ** 9)
        frm = _int(q, "from_ms", -1, 0, 10 ** 15) if "from_ms" in q else None
        to = _int(q, "to_ms", -1, 0, 10 ** 15) if "to_ms" in q else None
        if frm is not None and to is not None and frm > to:
            raise ApiError(400, "from_ms must not be after to_ms")
        read = causes_mod.read_unfinished(self.db, self.now(), self._lambda_timeout(q), self.max_receive_counts, older,
                                          q.get("queue") or None)
        assign: dict = {}
        groups = causes_mod.in_range(read["classified"], read["first_seen"], frm, to, assign)
        items = [{**i, "cause": assign[i["message_id"]]} for i in read["items"] if i["message_id"] in assign]
        wanted = q.get("cause") or None
        listed = [i for i in items if i["cause"] == wanted] if wanted and any(g["id"] == wanted for g in groups) else items
        return {"older_than_s": older, "from_ms": frm, "to_ms": to, "capped": read["capped"],
                "read_limit": causes_mod.UNFINISHED_LIMIT, "total": len(items),
                "in_dead_letter_queue": sum(1 for i in items if i["in_dead_letter_queue"]),
                "groups": [causes_mod.public(g) for g in groups],
                "dead_letter_queues": causes_mod.dlq_groups(items, read["urls"]),
                "cause": wanted, "messages": listed[:_limit(q)], "listed": len(listed)}

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

    def queues(self, q: dict, rest: list[str]) -> dict:
        """``/api/queues/{queue}/series``: the queue page's charts (QV-360 design 4.3, 9), per bucket of ``step_s``
        over [from_ms, to_ms] (default: the last day, and the page's step for the range). ``{queue}`` is a queue key
        (queues.key, URL-encoded) or a queue's name, when one queue has it. queues.py says what each figure counts."""
        if len(rest) != 2 or rest[1] not in ("series", "now"):
            raise ApiError(404, "no such endpoint")
        if rest[1] == "now":
            return self.queue_now(q, rest[0])
        from_ms, to_ms = self._window(q)
        # only when given: the default is the page's rule for the window (queues.default_step)
        step = (_int(q, "step_s", -1, queues_mod.STEPS_S[0], queues_mod.STEPS_S[-1]) if "step_s" in q
                else queues_mod.default_step(from_ms, to_ms))
        try:
            if "step_s" in q:  # the default is at most 6 h; only buckets with events are built, whatever the window
                queues_mod.check_step(step, from_ms, to_ms)
            keys = self.db.queue_keys()
        except ValueError as e:
            raise ApiError(400, str(e)) from None
        except NotImplementedError as e:
            raise ApiError(501, str(e)) from None
        split = q.get("split", "")
        if split not in ("", "version"):
            raise ApiError(400, "split must be version, or left out")
        want, place = self._queue_keys(rest[0], keys)
        now_ms = self.now() // 1_000_000
        # what AWS says about the queue over the page's range (queue_data.py); no AWS call while reads are off
        ref = queue_data.ref_of(want[0], place, _url_of(want[0]))
        aws = queue_data.read(self.aws.reader if self.aws is not None else None, [ref],
                              queue_data.page_window(from_ms, to_ms, step, now_ms), now_ms, known=keys)[ref["key"]]
        s = aws.get("settings")
        # the queue's own redrive policy, when read, over the server's --max-receive-count
        mrc = s.get("maxReceiveCount") if s is not None else self.max_receive_counts.get(place.name)
        rows = self.db.queue_events(want, from_ms, to_ms)
        return {"queue": {"key": want[0], "name": place.name, "account": place.account, "region": place.region,
                          "fifo": place.name.endswith(".fifo")},
                "from_ms": from_ms, "to_ms": to_ms, "step_s": step,
                "max_receive_count": mrc,
                "series": queues_mod.series(rows, step, mrc, by_version=split == "version"),
                "versions": queues_mod.versions(rows),
                "aws": aws}

    def queue_now(self, q: dict, asked: str) -> dict:
        """``/api/queues/{queue}/now``: "In ‹queue› now" (queue_now.py), the hosted app's ``InQueueView``, with
        ``aws`` (what AWS says about the queue: its settings, CloudWatch over the Overview's past ``minutes`` when
        asked, and its Lambda triggers) and ``live`` (its QueueLive). With AWS reads off, ``aws`` is
        ``not_configured``, no AWS call is made, and the view is from spans alone. ``{queue}`` is a queue key or name,
        as for ``series``."""
        try:
            keys = self.db.queue_keys()
        except NotImplementedError as e:
            raise ApiError(501, str(e)) from None
        want, place = self._queue_keys(asked, keys)
        now_ns = self.now()
        now_ms = now_ns // 1_000_000
        windows = queue_data.overview_windows(now_ms, _int(q, "minutes", 60, 1, 400 * 1440)) if "minutes" in q else []
        lambda_s = self._lambda_timeout(q)
        limit = _int(q, "limit", queue_now_mod.LIST_LIMIT, 1, queue_now_mod.LIST_LIMIT)

        def in_queue(ref: dict, settings, totals) -> dict:
            return queue_now_mod.view(self.db, want, place, now_ns, lambda_s, self.max_receive_counts, limit=limit,
                                      settings=settings, totals=totals)

        ref = queue_data.ref_of(want[0], place, _url_of(want[0]))
        d = queue_data.build(self.aws.reader if self.aws is not None else None, [ref], windows, now_ms,
                             in_queue, known=keys)[ref["key"]]
        return {"queue": {"key": want[0], "name": place.name, "account": place.account, "region": place.region,
                          "fifo": place.name.endswith(".fifo")},
                **d["inQueue"], "aws": d["aws"], "live": d["live"]}

    def flows(self, q: dict, rest: list[str]) -> dict:
        """``/api/flows``: the overview's flows over [from_ms, to_ms] (default: the last day): per queue, who sends to
        it and who receives from it, and its dead-letter queue (queues.flows). Each flow is the hosted app's
        ``Flow`` (trailbeam-app lib/app/flows.ts), field for field, so the shared overview reads either."""
        if rest:
            raise ApiError(404, "no such endpoint")
        from_ms, to_ms = self._window(q)
        try:
            keys, last, counts, links = self.db.queue_flows(from_ms, to_ms, self.now() // 1_000_000)
        except NotImplementedError as e:
            raise ApiError(501, str(e)) from None
        return {"from_ms": from_ms, "to_ms": to_ms, "flows": queues_mod.flows(keys, last, counts, links)}

    @staticmethod
    def _queue_keys(asked: str, keys: list[str]) -> tuple:
        """The keys whose events are the asked queue's, its own first, and its place (queues.resolve)."""
        try:
            return queues_mod.resolve(asked, keys)
        except ValueError as e:
            raise ApiError(400, str(e)) from None

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


def _url_of(key: str) -> Optional[str]:
    """A queue's URL when its key is one (queues.key: a URL with no account in it, as an emulator's can be); else
    None, and the AWS reader builds it from the queue's account and region."""
    return key if "://" in key else None


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
                for one in (v if isinstance(v, (list, tuple)) else (v,)):  # Set-Cookie can come more than once
                    self.send_header(k, one)
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

        def _peer(self) -> str:
            return self.client_address[0] if isinstance(self.client_address, tuple) else str(self.client_address)

        def _client(self) -> str:
            """The client's address: the TCP peer, or the client a --trusted-proxy names (proxy.py)."""
            return app.proxies.client_address(self._peer(), self.headers.get_all("X-Forwarded-For") or [])

        def _scheme(self) -> str:
            return app.proxies.scheme(self._peer(), self.headers.get("X-Forwarded-Proto"))

        def do_POST(self):
            self._status = 500
            path = urlsplit(self.path).path
            if path != "/v1/traces":  # everything but ingest: refused when another site's page sent it (csrf.py)
                why = csrf.check(self.headers, self._scheme(),
                                 app.proxies.host(self._peer(), self.headers.get("Host"),
                                                  self.headers.get("X-Forwarded-Host")))
                if why is not None:
                    log.warning("refused POST %s from %s: %s", path, self._client(), why)
                    self.close_connection = True  # the request body is not read
                    self._csrf_refused(path, why)
                    return
            if path == "/login":
                self._login()
                return
            if path == first_admin.PATH:
                self._setup_post()
                return
            if path == people_api.LINK_PATH:
                self._set_password_post()
                return
            if path in (passkeys_mod.OPTIONS_PATH, passkeys_mod.SIGN_IN_PATH):
                self._passkey_post(path)
                return
            if path.startswith("/api/"):
                self._api_write("POST", path)
                return
            if path != "/v1/traces":
                self._send(404, "text/plain", b"not found")
                return
            self._count_ingest_reject = True  # a 401, 413, 415, 400, 501 or 503 below is counted as it is sent
            try:  # x-trailbeam-key or Authorization: Bearer, before the body is read (access.py)
                d = app.access.check(self.headers, access_mod.INGEST, self._scheme())
            except access_mod.AccessUnavailable as e:
                log.warning("ingest: %s", e)
                self.close_connection = True  # the request body is not read
                r = app.gate.store_unreachable()
                json_mode = (self.headers.get("Content-Type") or "").split(";")[0].strip().lower() == "application/json"
                ctype, out = status_body(GRPC_UNAVAILABLE, r.message, json_mode)
                self._send(503, ctype, out, {"Retry-After": str(r.retry_after_s)})
                return
            if not d.ok:
                self.close_connection = True  # the request body is not read
                self._unauthorized("text/plain", d.message.encode())
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
            try:
                self._send(status, ctype, out, headers)
            except (BrokenPipeError, ConnectionResetError):
                # Its spans are committed (on 200), but the client won't know: it sends the export again, and
                # the copy is skipped as a duplicate. Not a traceback in the log.
                log.info("the client went away before the %d answer to its export", status)
                self.close_connection = True

        def do_PATCH(self):
            self._write_method("PATCH")

        def do_DELETE(self):
            self._write_method("DELETE")

        def _write_method(self, method: str):
            self._status = 500
            path = urlsplit(self.path).path
            why = csrf.check(self.headers, self._scheme(),
                             app.proxies.host(self._peer(), self.headers.get("Host"), self.headers.get("X-Forwarded-Host")))
            if why is not None:
                log.warning("refused %s %s from %s: %s", method, path, self._client(), why)
                self.close_connection = True
                self._csrf_refused(path, why)
                return
            if not path.startswith("/api/"):
                self.close_connection = True
                self._send(404, "text/plain", b"not found")
                return
            self._api_write(method, path)

        def _csrf_refused(self, path: str, why: str):
            if path.startswith("/api/"):
                self._json(403, {"error": f"forbidden: {why}", "reason": "csrf"})
            else:
                self._send(403, "text/plain; charset=utf-8", f"forbidden: {why}".encode())

        def _api_write(self, method: str, path: str):
            """POST, PATCH and DELETE on the people-and-tokens routes (people_api.py), after csrf.check. The route
            and the person are checked before the body is read."""
            r = access_mod.api_route(method, path)
            mounted = app.mounted(path, method) if r is None else None
            if r is None and mounted is None:
                self.close_connection = True
                self._json(404, {"error": f"no such endpoint: {method} {path}"})
                return
            # /api/alerts, /api/dashboards and /api/settings/aws: let in at MEMBER; what needs an admin is checked by the
            # handler
            level, handler, rid = r if r is not None else (access_mod.MEMBER, "", None)
            try:
                d = app.access.check(self.headers, level, self._scheme(), peer=self._peer())
            except access_mod.AccessUnavailable as e:
                log.warning("%s %s: %s", method, path, e)
                self.close_connection = True
                self._send(503, "application/json", json.dumps({"error": "the store is unreachable; try again"})
                           .encode(), {"Retry-After": "1"})
                return
            if not d.ok:
                self.close_connection = True
                out = json.dumps({"error": d.message}).encode()
                if d.status == 401:
                    self._unauthorized("application/json", out)
                else:
                    self._send(d.status, "application/json", out)
                return
            n = self.headers.get("Content-Length") or ("" if self.headers.get("Transfer-Encoding") else "0")
            if not n.isdigit() or int(n) > people_api.MAX_BODY:
                self.close_connection = True
                self._json(413, {"error": f"send a request body of at most {people_api.MAX_BODY} bytes, with a "
                                          "Content-Length"})
                return
            raw = self.rfile.read(int(n))
            host = app.proxies.host(self._peer(), self.headers.get("Host"), self.headers.get("X-Forwarded-Host"))
            base = app.public_url or f"{self._scheme()}://{host or 'localhost'}"
            try:
                body = None if method == "DELETE" else people_api.parse_body(self.headers.get("Content-Type"), raw)
                if mounted is not None:
                    status, obj = mounted.call(method, path, body, d.who, base)
                else:
                    status, obj = app.people.call(handler, rid, body, d.who, base, time.time_ns() // 1_000_000,
                                                  self._client())
            except people_api.ApiProblem as e:
                self._json(e.status, e.body())
                return
            except alerts_api.ApiProblem as e:
                self._json(e.status, {"error": e.message})
                return
            except access_mod.AccessUnavailable as e:
                log.warning("%s %s: %s", method, path, e)
                self._send(503, "application/json", json.dumps({"error": "the store is unreachable; try again"})
                           .encode(), {"Retry-After": "1"})
                return
            except Exception:  # noqa: BLE001
                log.exception("%s %s failed", method, path)
                self._json(500, {"error": "internal error"})
                return
            # a new token or link is in it: never kept by a browser or a proxy
            self._send(status, "application/json", json.dumps(obj).encode(), {"Cache-Control": "no-store"})

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
            path = u.path
            if path == first_admin.PATH:
                self._setup_get()
                return
            if path in (oidc_mod.START_PATH, oidc_mod.CALLBACK_PATH):
                self._sso(oidc_mod, app.oidc, "single sign-on", path, q)
                return
            if path in (github_mod.START_PATH, github_mod.CALLBACK_PATH):
                self._sso(github_mod, app.github, "GitHub sign-in", path, q)
                return
            if path == people_api.LINK_PATH:
                self._send_page(web.password_page() if app.accounts is not None else
                                (404, {"Content-Type": "text/plain"}, b"not found"))
                return
            api_r = None
            try:
                if path in ("/login", "/logout"):
                    mode = app.access.mode()
                    if mode != "off":
                        self._sign_in_out(path, mode, q)
                        return
                    if path == "/login" and app.sso_buttons:  # nobody has an account: single sign-on only
                        self._send_page(web.sign_in_page(auth_mod.safe_next(q.get("next")), status=200,
                                                         sso=app.sso_buttons, password=False))
                        return
                    d = access_mod.Decision(True)
                else:
                    # /api/health is open, but says more only to whoever may read the API
                    # One decision for the level and the handler: app.api dispatches on this same route.
                    api_r = access_mod.api_route("GET", path)
                    level = api_r[0] if api_r else access_mod.level_of("GET", path)
                    d = app.access.check(self.headers, access_mod.MEMBER if path == "/api/health"
                                         else level, self._scheme(), peer=self._peer())
            except access_mod.AccessUnavailable as e:
                log.warning("GET %s: %s", path, e)
                if path == "/api/health":  # liveness answers while the store is unreachable
                    self._json(200, {"status": "ok", "version": __version__})
                elif path.startswith("/api/"):
                    self._json(500, {"error": "internal error: the store is unreachable"})
                else:
                    self._send(500, "text/plain", b"internal error: the store is unreachable")
                return
            if not d.ok:
                if path == "/api/health":  # health checks: alive, nothing more
                    self._json(200, {"status": "ok", "version": __version__})
                elif path.startswith("/api/") or path == "/metrics":
                    body = json.dumps({"error": d.message}).encode() if path != "/metrics" else d.message.encode()
                    ctype = "application/json" if path != "/metrics" else "text/plain; charset=utf-8"
                    if d.status == 401:
                        self._unauthorized(ctype, body)
                    else:
                        self._send(d.status, ctype, body)
                elif d.status == 403:
                    self._send_page(web.forbidden_page(d.message))
                elif app.access.mode() == "accounts":
                    self._send_page(web.sign_in_page(auth_mod.safe_next(self.path), sso=app.sso_buttons,
                                                     passkey=app.passkeys is not None))
                else:
                    self._send_page(web.login_page(auth_mod.safe_next(self.path), sso=app.sso_buttons))
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
                self._json(200, app.api(u.path[len("/api/"):], q, d.who, route=api_r))
            except ApiError as e:
                self._json(e.status, {"error": e.message})
            except Exception:
                log.exception("API request failed")
                self._json(500, {"error": "internal error"})

        def _sign_in_out(self, path: str, mode: str, q: dict):
            """GET /login (the form) and /logout (ends the session), when the server has a token or an admin."""
            secure = self._scheme() == "https"
            if path == "/logout":
                if mode == "accounts":
                    sid = sessions_mod.cookie_value(self.headers.get("Cookie"), secure)
                    if sid and app.sessions is not None:
                        try:
                            app.sessions.end(sid)
                        except Exception as e:  # noqa: BLE001 - the cookie is cleared anyway
                            log.warning("sign-out: the session could not be ended in the store: %s", e)
                    cookie = sessions_mod.clear_cookie(secure)
                else:
                    cookie = auth_mod.clear_cookie()
                # signed in through a proxy: its own session ends at its sign-out URL, if one is set
                where = (app.proxy_auth.sign_out_url if mode == "accounts" and app.proxy_auth is not None
                         and app.proxy_auth.sign_out_url else "/login")
                self._send(303, "text/plain", b"", {"Location": where, "Set-Cookie": cookie})
            elif mode == "accounts" and app.access.proxy_person(self.headers, self._peer()) is not None:
                # the proxy signed them in already: on to the page they asked for
                self._send(303, "text/plain", b"", {"Location": auth_mod.safe_next(q.get("next"))})
            elif mode == "accounts":
                self._send_page(web.sign_in_page(auth_mod.safe_next(q.get("next")), status=200, sso=app.sso_buttons,
                                                 passkey=app.passkeys is not None))
            else:
                self._send_page(web.login_page(auth_mod.safe_next(q.get("next")), status=200, sso=app.sso_buttons))

        def _sso(self, mod, flow, what: str, path: str, q: dict):
            """GET /login/oidc (to the identity provider) and /login/oidc/callback (back from it): oidc.py; the same
            for /login/github (github.py). ``mod``: the module (paths, cookies); ``flow``: its begin and finish."""
            if flow is None:
                self._send(404, "text/plain", b"not found")
                return
            secure = self._scheme() == "https"
            client = self._client()
            if path == mod.START_PATH:
                wait = app.sso_starts.retry_after(client)
                if wait is not None:
                    self._send_page(_with_retry(web.sso_error_page(
                        "Too many sign-ins started from here. Try again in a minute.", 429), wait))
                    return
                app.sso_starts.failed(client)  # counts every start, not only failed ones
                try:
                    url, browser = flow.begin(auth_mod.safe_next(q.get("next")))
                except oidc_mod.OidcError as e:
                    log.warning("%s from %s: %s", what, client, e.detail)
                    self._send_page(web.sso_error_page(e.message, e.status))
                    return
                except Exception as e:  # noqa: BLE001 - the store
                    log.warning("%s from %s: the store is unreachable: %s", what, client, e)
                    self._send(503, "text/plain", b"the store is unreachable; try again", {"Retry-After": "1"})
                    return
                self._send(302, "text/plain", b"", {"Location": url, "Cache-Control": "no-store",
                                                    "Referrer-Policy": "no-referrer",
                                                    "Set-Cookie": mod.set_browser_cookie(browser, secure)})
                return
            clear = mod.clear_browser_cookie(secure)
            try:
                done = flow.finish(q, mod.browser_cookie(self.headers.get("Cookie"), secure))
            except oidc_mod.OidcError as e:
                log.warning("%s refused for %s: %s", what, client, e.detail)
                st, headers, body = web.sso_error_page(e.message, e.status)
                self._send_page((st, {**headers, "Set-Cookie": clear}, body))
                return
            except Exception:  # noqa: BLE001 - the store
                log.exception("%s failed", what)
                self._send(503, "text/plain", b"the store is unreachable; try again",
                           {"Retry-After": "1", "Set-Cookie": clear})
                return
            if done.how != "known" or done.user["role"] == "admin":
                app.access.state.changed()  # maybe the first admin: every page switches to accounts now
            log.info("signed in with %s: user %s (%s, %s) from %s", what, done.user["id"], done.how,
                     done.user["role"], client)
            if done.kept_admin:
                log.warning("%s: user %s is no longer in an admin group or team, but stays an admin: they are "
                            "the last admin who can sign in. Make someone else an admin first", what, done.user["id"])
            max_age = max(0, (done.expires_at_ms - time.time_ns() // 1_000_000) // 1000)
            st, headers, body = oidc_mod.continue_page(done.next_path)
            self._send_page((st, {**headers, "Set-Cookie": [sessions_mod.set_cookie(done.session_id, secure, max_age),
                                                            clear]}, body))

        def _login(self):
            """POST /login: the token from the form (no admin yet), or an email and password (once an admin exists);
            right, a session cookie and back to the page. 404 on a server with neither."""
            try:
                mode = app.access.mode()
            except access_mod.AccessUnavailable as e:
                log.warning("sign-in: %s", e)
                self.close_connection = True
                self._send(503, "text/plain", b"the store is unreachable; try again", {"Retry-After": "1"})
                return
            if mode == "off":
                self.close_connection = True
                self._send(404, "text/plain", b"not found")
                return
            n = self.headers.get("Content-Length") or ""
            if not n.isdigit() or int(n) > 8192:
                self.close_connection = True
                self._send(413, "text/plain", b"login form too large")
                return
            form = {k: v[-1] for k, v in parse_qs(self.rfile.read(int(n)).decode("utf-8", "replace")).items()}
            nxt = auth_mod.safe_next(form.get("next"))
            secure = self._scheme() == "https"  # X-Forwarded-Proto counts only from a --trusted-proxy
            if mode == "token":
                if not app.auth.token_ok(form.get("token", "")):
                    log.warning("login with a wrong token from %s", self._client())
                    self._send_page(web.login_page(nxt, error=True))
                    return
                self._send(303, "text/plain", b"", {"Location": nxt, "Set-Cookie": app.auth.set_cookie(secure)})
                return
            email = form.get("email", "")[:300]
            try:
                r = app.signin.sign_in(email, form.get("password", ""), self._client())
            except Exception:  # noqa: BLE001
                log.exception("sign-in failed")
                self._send(500, "text/plain", b"internal error")
                return
            if r.outcome == "ok":
                log.info("signed in: user %s from %s", r.user["id"], self._client())
                max_age = max(0, (r.expires_at_ms - time.time_ns() // 1_000_000) // 1000)
                self._send(303, "text/plain", b"", {"Location": nxt,
                                                    "Set-Cookie": sessions_mod.set_cookie(r.session_id, secure, max_age)})
                return
            log.warning("sign-in refused (%s) from %s", r.outcome, self._client())
            status = {"locked": 429, "slow_down": 429, "busy": 503}.get(r.outcome, 401)
            st, headers, body = web.sign_in_page(nxt, email if r.outcome != "slow_down" else "", r.outcome,
                                                 r.retry_after_s, status=status, sso=app.sso_buttons,
                                                 passkey=app.passkeys is not None)
            headers = dict(headers)
            if r.retry_after_s:
                headers["Retry-After"] = str(r.retry_after_s)
            self._send_page((st, headers, body))

        def _passkey_post(self, path: str):
            """POST /login/passkey/options (a challenge to sign) and /login/passkey (the signed answer; right, a
            session cookie): passkeys.py. JSON both ways. 404 while passkeys are off or nobody can sign in yet."""
            try:
                mode = app.access.mode()
            except access_mod.AccessUnavailable as e:
                log.warning("passkey sign-in: %s", e)
                self.close_connection = True
                self._send(503, "application/json", b'{"error": "the store is unreachable; try again"}',
                           {"Retry-After": "1"})
                return
            if app.passkeys is None or mode != "accounts":
                self.close_connection = True
                self._json(404, {"error": "passkeys are off on this server"})
                return
            n = self.headers.get("Content-Length") or ""
            if not n.isdigit() or int(n) > passkeys_mod.MAX_ANSWER:
                self.close_connection = True
                self._json(413, {"error": f"send a request body of at most {passkeys_mod.MAX_ANSWER} bytes"})
                return
            try:
                body = people_api.parse_body(self.headers.get("Content-Type"), self.rfile.read(int(n)))
            except people_api.ApiProblem as e:
                self._json(e.status, e.body())
                return
            client = self._client()
            no_store = {"Cache-Control": "no-store"}
            if path == passkeys_mod.OPTIONS_PATH:
                wait = app.passkey_starts.retry_after(client)
                if wait is not None:
                    self._send(429, "application/json", json.dumps(
                        {"error": "Too many sign-ins started from here. Try again in a minute."}).encode(),
                        {"Retry-After": str(wait)})
                    return
                app.passkey_starts.failed(client)  # counts every start, as single sign-on's
                try:
                    opts = app.passkeys.sign_in_options()
                except Exception as e:  # noqa: BLE001 - the store
                    log.warning("passkey sign-in from %s: the store is unreachable: %s", client, e)
                    self._send(503, "application/json", b'{"error": "the store is unreachable; try again"}',
                               {"Retry-After": "1"})
                    return
                self._send(200, "application/json", json.dumps({"options": opts}).encode(), no_store)
                return
            credential = body.get("credential")
            nxt = auth_mod.safe_next(body.get("next") if isinstance(body.get("next"), str) else None)
            limit = app.signin.address_limit
            wait = limit.begin(client)  # a wrong answer counts against the address, as a wrong password does
            if wait is not None:
                self._send(429, "application/json", json.dumps(
                    {"error": f"Too many failed sign-ins from here. Try again in {web.minutes(wait)}."}).encode(),
                    {"Retry-After": str(wait)})
                return
            failed = True
            try:
                if not isinstance(credential, dict):
                    self._json(400, {"error": passkeys_mod.REFUSED})
                    return
                try:
                    uid, sid, expires = app.passkeys.sign_in(credential)
                except passkeys_mod.PasskeyError as e:
                    log.warning("passkey sign-in refused from %s: %s", client, e.detail)
                    self._json(e.status, {"error": e.message})
                    return
                except Exception:  # noqa: BLE001 - the store
                    log.exception("passkey sign-in failed")
                    failed = False
                    self._send(503, "application/json", b'{"error": "the store is unreachable; try again"}',
                               {"Retry-After": "1"})
                    return
                failed = False
            finally:
                limit.end(client, failed)
            log.info("signed in with a passkey: user %s from %s", uid, client)
            secure = self._scheme() == "https"
            max_age = max(0, (expires - time.time_ns() // 1_000_000) // 1000)
            self._send(200, "application/json", json.dumps({"next": nxt}).encode(),
                       {**no_store, "Set-Cookie": sessions_mod.set_cookie(sid, secure, max_age)})

        def _setup_get(self):
            """GET /setup: the first admin's form while the store has none; the sign-in page after."""
            if app.accounts is None:
                self._send(404, "text/plain", b"not found")
                return
            try:
                has_admin = app.access.state.admin_exists()
            except access_mod.AccessUnavailable as e:
                log.warning("GET /setup: %s", e)
                self._send(503, "text/plain", b"the store is unreachable; try again", {"Retry-After": "1"})
                return
            if has_admin:
                self._send(303, "text/plain", b"", {"Location": "/login", "Referrer-Policy": "no-referrer"})
                return
            self._send_page(web.setup_page())

        def _setup_post(self):
            """POST /setup: the setup code, an email, a name and a password; right, the first admin is created and
            signed in. The code is checked before the password is hashed, so a wrong one costs no hash."""
            if app.accounts is None:
                self.close_connection = True
                self._send(404, "text/plain", b"not found")
                return
            n = self.headers.get("Content-Length") or ""
            if not n.isdigit() or int(n) > 8192:
                self.close_connection = True
                self._send(413, "text/plain", b"setup form too large")
                return
            form = {k: v[-1] for k, v in parse_qs(self.rfile.read(int(n)).decode("utf-8", "replace")).items()}
            code, email, name = form.get("code", "").strip(), form.get("email", "")[:300], form.get("name", "")[:300]
            pw, pw2 = form.get("password", ""), form.get("password2", "")
            client = self._client()
            limit = app.signin.address_limit
            wait = limit.begin(client)  # reserved now, so parallel wrong codes from one address count at once
            if wait is not None:
                self._send_page(_with_retry(web.setup_page(code, email, name, "slow_down", retry_after_s=wait,
                                                           status=429), wait))
                return
            wrong_code = False
            try:
                wrong_code = self._setup_checked(code, email, name, pw, pw2, client)
            finally:
                limit.end(client, failed=bool(wrong_code))

        def _setup_checked(self, code, email, name, pw, pw2, client) -> bool:
            """The rest of POST /setup, once the address limit has reserved an attempt. True if the code was
            wrong (that counts against the address)."""
            now = time.time_ns() // 1_000_000
            try:
                if app.access.state.admin_exists():
                    self._send(303, "text/plain", b"", {"Location": "/login"})
                    return
                code_ok = first_admin.well_formed(code) and app.accounts.setup_code_ok(first_admin.code_hash(code), now)
            except Exception as e:  # noqa: BLE001
                log.warning("setup: the store is unreachable: %s", e)
                self._send(503, "text/plain", b"the store is unreachable; try again", {"Retry-After": "1"})
                return
            if not code_ok:
                log.warning("setup: a wrong or used setup code from %s", client)
                self._send_page(web.setup_page("", email, name, "bad_code", status=403))
                return True
            try:
                email, name = check_email(email), check_name(name)
                passwords.check(pw)
                if pw != pw2:
                    self._send_page(web.setup_page(code, email, name, "mismatch", status=400))
                    return
                h = app.signin.hasher.hash(pw)
                uid = app.accounts.create_first_admin(email, name, h, now,
                                                      setup_code_sha256=first_admin.code_hash(code))
            except passwords.HashBusy:
                self._send_page(_with_retry(web.setup_page(code, email, name, "busy", status=503), 1))
                return
            except AdminExists:
                self._send(303, "text/plain", b"", {"Location": "/login"})
                return
            except BadSetupCode:
                self._send_page(web.setup_page("", email, name, "bad_code", status=403))
                return True
            except (AccountError, passwords.PasswordError) as e:
                self._send_page(web.setup_page(code, email, name, message=str(e)[:1].upper() + str(e)[1:] + ".",
                                               status=400))
                return
            except Exception:  # noqa: BLE001
                log.exception("setup failed")
                self._send(500, "text/plain", b"internal error")
                return
            app.access.state.changed()
            log.info("created the first admin from the setup link: user %d from %s", uid, client)
            secure = self._scheme() == "https"
            try:
                sid, expires = app.sessions.start(uid)
                cookie = {"Set-Cookie": sessions_mod.set_cookie(sid, secure, (expires - now) // 1000)}
            except Exception as e:  # noqa: BLE001 - the admin exists; they can sign in
                log.warning("setup: the admin was created, but their session could not be started: %s", e)
                cookie = {}
            self._send(303, "text/plain", b"", {"Location": "/", "Referrer-Policy": "no-referrer", **cookie})
            return False

        def _set_password_post(self):
            """POST /set-password: an invite or reset code and a new password; right, the password is set, the
            code used up and the person's other sessions ended in one transaction (accounts.use_user_code), and
            they are signed in. The code is checked before the password is hashed, so a wrong one costs no hash."""
            if app.accounts is None:
                self.close_connection = True
                self._send(404, "text/plain", b"not found")
                return
            n = self.headers.get("Content-Length") or ""
            if not n.isdigit() or int(n) > 8192:
                self.close_connection = True
                self._send(413, "text/plain", b"form too large")
                return
            form = {k: v[-1] for k, v in parse_qs(self.rfile.read(int(n)).decode("utf-8", "replace")).items()}
            code, pw, pw2 = form.get("code", "").strip(), form.get("password", ""), form.get("password2", "")
            client = self._client()
            limit = app.signin.address_limit
            wait = limit.retry_after(client)
            if wait is not None:
                self._send_page(_with_retry(web.password_page(code, "slow_down", retry_after_s=wait, status=429), wait))
                return
            now = time.time_ns() // 1_000_000
            try:
                found = app.accounts.user_code(first_admin.code_hash(code), now) if first_admin.well_formed(code) \
                    else None
            except Exception as e:  # noqa: BLE001
                log.warning("set-password: the store is unreachable: %s", e)
                self._send(503, "text/plain", b"the store is unreachable; try again", {"Retry-After": "1"})
                return
            if found is None:
                limit.failed(client)
                log.warning("set-password: a wrong, used or replaced link code from %s", client)
                self._send_page(web.password_page("", "bad_code", status=403))
                return
            email = found["email"]
            try:
                passwords.check(pw)
                if pw != pw2:
                    self._send_page(web.password_page(code, "mismatch", email=email, status=400))
                    return
                uid = app.accounts.use_user_code(first_admin.code_hash(code), app.signin.hasher.hash(pw), now)
            except passwords.HashBusy:
                self._send_page(_with_retry(web.password_page(code, "busy", email=email, status=503), 1))
                return
            except BadSetupCode:
                limit.failed(client)
                self._send_page(web.password_page("", "bad_code", status=403))
                return
            except passwords.PasswordError as e:
                self._send_page(web.password_page(code, message=str(e)[:1].upper() + str(e)[1:] + ".", email=email,
                                                  status=400))
                return
            except Exception:  # noqa: BLE001
                log.exception("set-password failed")
                self._send(500, "text/plain", b"internal error")
                return
            app.sessions.forget_user(uid)
            log.info("user %d set a password from a%s link, from %s", uid,
                     "n invite" if found["kind"] == "invite" else " reset", client)
            secure = self._scheme() == "https"
            try:
                sid, expires = app.sessions.start(uid)
                cookie = {"Set-Cookie": sessions_mod.set_cookie(sid, secure, (expires - now) // 1000)}
            except Exception as e:  # noqa: BLE001 - the password is set; they can sign in
                log.warning("set-password: the password was set, but the session could not be started: %s", e)
                cookie = {}
            self._send(303, "text/plain", b"", {"Location": "/", "Referrer-Policy": "no-referrer", **cookie})

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


def _with_retry(page, seconds: int):
    status, headers, body = page
    return status, {**headers, "Retry-After": str(seconds)}, body


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


def has_account_tables(db) -> bool:
    """A SQLite or Postgres store (schema 15 / 8 or later): it has users and tokens (account_tables.py)."""
    return callable(getattr(db, "reader", None)) or (hasattr(db, "reads") and hasattr(db, "ingest"))


def make_server(db: Store, host: str = "127.0.0.1", port: int = 4318, **app_options) -> ThreadingHTTPServer:
    app = App(db, **app_options)
    httpd = ThreadingHTTPServer((host, port), make_handler(app))
    httpd.daemon_threads = True
    httpd.app = app
    return httpd
