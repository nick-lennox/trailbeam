"""``trailbeam doctor``: why no data is arriving, in plain lines.

Five checks, in order, each one line, and for a failed one the likely cause and the fix:

1. the server is reachable: ``GET /api/health`` answers, and it is Trailbeam;
2. the ingest port accepts connections: a TCP connect to the ingest address, then an empty export to it;
3. the token is accepted: by the API and by ingest (or the server has none);
4. a test span sent now is stored and read back: a probe (contract section 17) to ingest, then
   ``GET /api/probes/{id}``. A probe is not a span: it never shows as a message;
5. the last real span: when the newest span was stored, and what the ingest counters say if none was.

A check that needs an earlier one that failed is skipped. The exit code is 1 when any check failed, else 0.
Only the standard library: it runs where the server's extras are not installed.
"""
from __future__ import annotations

import argparse
import datetime
import http.client
import json
import os
import socket
import ssl
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlsplit

from .start import ADDRESS_FILE, TOKEN_FILE, default_data_dir

DEFAULT_PORT = 4318
INGEST_PATH = "/v1/traces"
QUIET_AFTER_S = 15 * 60  # the newest span older than this: a warning, not a failure
READ_BACK_S = 3.0  # how long a probe accepted at ingest is looked for (a Collector forwards it later)

OK, FAIL, SKIP, WARN = "ok", "FAIL", "skip", "warn"


@dataclass
class Result:
    name: str
    status: str
    detail: str
    cause: Optional[str] = None
    fix: Optional[str] = None
    probe_rejected: bool = False  # the test span check only: the server counted doctor's own span as rejected


# The fixed starts of the reasons a server rejects the test span with (span contract section 17; otlp.py has the
# same, and a test checks they match). Copied, not imported: doctor runs without the server extra.
PROBE_UNKNOWN = "unknown trailbeam.kind 'trailbeam.probe'"  # Trailbeam 0.3 and before
PROBE_NOT_STORED = "probe span not stored:"                 # the store keeps no probes
PROBE_BAD_ID = "probe span without a trailbeam.probe.id"    # the ID was not accepted


@dataclass
class Target:
    url: str  # the server: API and web screen
    ingest_url: str  # where services send spans
    token: Optional[str] = None
    token_source: str = "none"  # said in the header line: where the token came from
    url_source: str = ""  # said in the header line when the server's address was not given: where it came from
    token_error: Optional[str] = None  # the token file could not be read or the token is unusable
    timeout_s: float = 5.0


class Unreachable(Exception):
    """No HTTP answer: ``cause`` and ``fix`` say why, in words."""

    def __init__(self, summary: str, cause: str, fix: str):
        super().__init__(summary)
        self.summary, self.cause, self.fix = summary, cause, fix


@dataclass
class Answer:
    status: int
    body: bytes
    headers: dict = field(default_factory=dict)

    def json(self):
        try:
            return json.loads(self.body)
        except ValueError:
            return None


def _hostport(url: str) -> tuple[str, int, bool]:
    u = urlsplit(url)
    https = u.scheme == "https"
    return u.hostname or "127.0.0.1", u.port or (443 if https else 80), https


def _unreachable(e: BaseException, url: str, timeout_s: float, what: str) -> Unreachable:
    host, port, _ = _hostport(url)
    if isinstance(e, ConnectionRefusedError):
        return Unreachable(
            f"nothing is listening on {host}:{port}",
            f"{what} is not running there, or it listens on another port or address.",
            "start it (trailbeam start, or the container with -p 4318:4318), or give its address with --url. A server "
            "in a container must listen on 0.0.0.0 (TRAILBEAM_HOST) to be reached from outside it.")
    if isinstance(e, socket.gaierror):
        return Unreachable(
            f"the host name {host!r} does not resolve",
            "the host in the address is misspelt, or this machine's DNS does not know it.",
            "check the host in --url (or TRAILBEAM_OTLP_TRACES_ENDPOINT for ingest).")
    if isinstance(e, (socket.timeout, TimeoutError)):
        return Unreachable(
            f"no answer from {host}:{port} within {timeout_s:g} s",
            "a firewall or security group drops the connection, or the server is too busy to answer.",
            f"open port {port} between this machine and the server, or check the server's log and load.")
    if isinstance(e, ssl.SSLError):
        return Unreachable(
            f"TLS failed with {host}:{port}: {e}",
            "the address says https but the server speaks plain HTTP, or its certificate is not trusted here.",
            "use http:// for a server without a TLS proxy in front, or fix the proxy's certificate.")
    return Unreachable(f"cannot connect to {host}:{port}: {e}", "the connection failed.",
                       "check the address and that the server is running.")


def _request(t: Target, method: str, url: str, body: Optional[bytes] = None,
             headers: Optional[dict] = None, token: bool = True, what: str = "Trailbeam") -> Answer:
    """One request to ``url`` (a full URL). Unreachable when no HTTP answer came."""
    host, port, https = _hostport(url)
    u = urlsplit(url)
    cls = http.client.HTTPSConnection if https else http.client.HTTPConnection
    conn = cls(host, port, timeout=t.timeout_s)
    h = dict(headers or {})
    if token and t.token:
        h["Authorization"] = f"Bearer {t.token}"
    try:
        conn.request(method, (u.path or "/") + (f"?{u.query}" if u.query else ""), body=body, headers=h)
        r = conn.getresponse()
        return Answer(r.status, r.read(), {k.lower(): v for k, v in r.getheaders()})
    except (OSError, http.client.HTTPException) as e:
        if isinstance(e, http.client.HTTPException):
            raise Unreachable(f"{host}:{port} answered something that is not HTTP ({type(e).__name__})",
                              "another kind of service listens on that port.",
                              f"check the port: {what} answers HTTP, by default on {DEFAULT_PORT}.") from None
        raise _unreachable(e, url, t.timeout_s, what) from None
    finally:
        conn.close()


def probe_export(probe_id: str, now_ns: Optional[int] = None) -> bytes:
    """An OTLP/JSON export of one probe span (contract section 17)."""
    now_ns = time.time_ns() if now_ns is None else now_ns

    def attr(k, v):
        return {"key": k, "value": {"intValue": str(v)} if isinstance(v, int) else {"stringValue": v}}
    span = {"traceId": uuid.uuid4().hex, "spanId": uuid.uuid4().hex[:16], "name": "trailbeam probe", "kind": 1,
            "startTimeUnixNano": str(now_ns), "endTimeUnixNano": str(now_ns),
            "attributes": [attr("trailbeam.schema_version", 3), attr("trailbeam.kind", "trailbeam.probe"),
                           attr("trailbeam.probe.id", probe_id)]}
    return json.dumps({"resourceSpans": [{
        "resource": {"attributes": [attr("service.name", "trailbeam-doctor")]},
        "scopeSpans": [{"scope": {"name": "trailbeam"}, "spans": [span]}]}]}).encode()


def _default_token_path():
    """Where trailbeam start keeps the token: TOKEN_FILE in its data directory (TRAILBEAM_DATA_DIR)."""
    from pathlib import Path
    return Path(default_data_dir()) / TOKEN_FILE


def _token_cause(t: Target, where: str) -> tuple[str, str]:
    if t.token_error:
        return (f"the token could not be used: {t.token_error}.",
                "fix or recreate the token file, or set TRAILBEAM_TOKEN to the token the server was started with.")
    if not t.token:
        return (f"the server has an access token and {where} needs it, but none was given.",
                f"pass --token-file PATH, or set TRAILBEAM_TOKEN, to the token the server was started with "
                f"(trailbeam start prints it, and keeps it in {_default_token_path()}). Services need it too: "
                "OTEL_EXPORTER_OTLP_HEADERS=Authorization=Bearer%20<token>.")
    at, flag = (t.ingest_url, "--ingest-url") if where == "ingest" else (t.url, "--url")
    return (f"{where} at {at} does not accept the token from {t.token_source}: that server was started with "
            "another one, or it is not the server you mean.",
            f"check that {at} is your server (else give {flag}), and use the token it was started with (its "
            "--token-file or TRAILBEAM_TOKEN). After the token "
            "changes, give services the new one in OTEL_EXPORTER_OTLP_HEADERS and restart them.")


def _ago(s: float) -> str:
    s = abs(s)
    if s < 90:
        return f"{s:.0f} s"
    if s < 90 * 60:
        return f"{s / 60:.0f} min"
    if s < 36 * 3600:
        return f"{s / 3600:.1f} h"
    return f"{s / 86400:.1f} days"


def run(t: Target, now_ns: Optional[int] = None) -> list[Result]:
    now_ns = time.time_ns() if now_ns is None else now_ns
    results: list[Result] = []
    health: Optional[dict] = None

    # 1. the server
    name = "Server reachable"
    try:
        a = _request(t, "GET", t.url + "/api/health")
        doc = a.json()
        if a.status != 200 or not isinstance(doc, dict) or doc.get("status") != "ok" or "version" not in doc:
            results.append(Result(name, FAIL, f"{t.url} answers HTTP {a.status}, but not as Trailbeam does",
                                  "another service listens at that address, or a proxy in front does not pass "
                                  "/api/ to Trailbeam.",
                                  f"check the address and port (Trailbeam's default is {DEFAULT_PORT}), and the "
                                  "proxy's routes."))
        else:
            health = doc
            results.append(Result(name, OK, f"Trailbeam {doc['version']} answers at {t.url}"))
    except Unreachable as e:
        results.append(Result(name, FAIL, e.summary, e.cause, e.fix))
    if results[-1].status == FAIL and t.url_source:
        results[-1].fix = (results[-1].fix or "") + (f" doctor took {t.url} from {t.url_source}; if your server "
                                                     "is elsewhere, give its address with --url.")

    # 2. the ingest port
    name = "Ingest port accepts connections"
    host, port, _ = _hostport(t.ingest_url)
    empty: Optional[Answer] = None
    try:
        socket.create_connection((host, port), timeout=t.timeout_s).close()
        empty = _request(t, "POST", t.ingest_url, b"{}", {"Content-Type": "application/json"},
                         what="the ingest endpoint")
        if empty.status in (404, 405):
            results.append(Result(name, FAIL, f"{host}:{port} is open, but POST {urlsplit(t.ingest_url).path or '/'} answers "
                                              f"{empty.status}",
                                  "the path is not Trailbeam's ingest endpoint, or a proxy does not pass it on.",
                                  f"services export to http://{host}:{port}{INGEST_PATH} "
                                  "(TRAILBEAM_OTLP_TRACES_ENDPOINT); check that path, and the proxy's routes."))
        else:
            results.append(Result(name, OK, f"{host}:{port} accepts connections ({t.ingest_url})"))
    except OSError as e:
        u = _unreachable(e, t.ingest_url, t.timeout_s, "the ingest endpoint")
        results.append(Result(name, FAIL, u.summary, u.cause,
                              u.fix + f" Services send to this address: TRAILBEAM_OTLP_TRACES_ENDPOINT={t.ingest_url}."))
    except Unreachable as e:
        results.append(Result(name, FAIL, e.summary, e.cause, e.fix))

    # 3. the token
    name = "Token accepted"
    if health is None or empty is None or empty.status in (404, 405):
        results.append(Result(name, SKIP, "needs the server and the ingest port"))
    else:
        api_ok = "config" in health  # without the token, a server that has one says only status and version
        auth = (health.get("config") or {}).get("auth")
        if not api_ok:
            cause, fix = _token_cause(t, "the API")
            results.append(Result(name, FAIL, "the API answers without the server's details: the token is "
                                              + ("missing" if not t.token else "not accepted"), cause, fix))
        elif empty.status == 401:
            if auth == "off" and (health.get("config") or {}).get("ingest_auth") == "token":
                results.append(Result(name, FAIL, "ingest answers 401: the server has ingest tokens",
                                      "the server has no TRAILBEAM_TOKEN, but it holds at least one ingest token "
                                      "(tbi_...), so ingest needs one.",
                                      "give doctor and your services an ingest token (--token-file, or "
                                      "x-trailbeam-key in OTEL_EXPORTER_OTLP_HEADERS)."))
            elif auth == "off":
                results.append(Result(name, FAIL, "the server at --url has no token, but ingest answers 401",
                                      "the ingest address is another server, with a token of its own.",
                                      "point --ingest-url (and your services) at the same server as --url, or "
                                      "give the token that server was started with."))
            else:
                cause, fix = _token_cause(t, "ingest")
                if t.token:
                    cause = ("the API accepts the token but ingest does not: a proxy in front of ingest drops the "
                             "Authorization header, or the ingest address is another server.")
                    fix = "let the proxy pass Authorization, or point --ingest-url at the same server as --url."
                results.append(Result(name, FAIL, "ingest answers 401", cause, fix))
        elif auth == "off":
            results.append(Result(name, OK, "the server has no access token: every request is accepted"
                                  + (" (the token given is not needed)" if t.token else "")))
        else:
            results.append(Result(name, OK, f"the API and ingest accept the token from {t.token_source}"))
    token_ok = results[-1].status == OK

    # 4. the probe
    name = "Test span stored and read back"
    if not token_ok:
        results.append(Result(name, SKIP, "needs the checks above"))
    else:
        results.append(_probe(t))
    probe_rejected = results[-1].probe_rejected

    # 5. the last real span
    name = "Last real span"
    if not token_ok:
        results.append(Result(name, SKIP, "needs the checks above"))
    else:
        try:  # read again: the counters after the probe, and a store that went away since
            a = _request(t, "GET", t.url + "/api/health")
            health = a.json() if a.status == 200 and isinstance(a.json(), dict) else health
        except Unreachable:
            pass
        results.append(_last_span(health, now_ns, probe_ok=results[-1].status == OK, t=t,
                                  probe_rejected=probe_rejected))
    return results


def _probe(t: Target) -> Result:
    name = "Test span stored and read back"
    probe_id = "doctor-" + uuid.uuid4().hex[:16]
    t0 = time.monotonic()
    try:
        a = _request(t, "POST", t.ingest_url, probe_export(probe_id),
                     {"Content-Type": "application/json"})
    except Unreachable as e:
        return Result(name, FAIL, f"sending it failed: {e.summary}", e.cause, e.fix)
    if a.status in (429, 503):
        doc = a.json() or {}
        return Result(name, FAIL, f"ingest answered {a.status}, Retry-After {a.headers.get('retry-after', '?')} s: "
                                  f"{doc.get('message') or a.body[:200].decode('utf-8', 'replace')}",
                      "the server is overloaded (429, 503 writer behind), or cannot reach its database (503 store "
                      "unreachable). Exporters retry these; nothing is lost while they do.",
                      "check GET /api/ready and the server's log; for Postgres, that the database is up. See the README, "
                      "\"When Trailbeam is overloaded\".")
    if a.status != 200:
        return Result(name, FAIL, f"ingest answered {a.status}: {a.body[:200].decode('utf-8', 'replace')}",
                      "the server could not store it.", "see the server's log for this request.")
    rejected = ((a.json() or {}).get("partialSuccess") or {}).get("errorMessage")
    if rejected:
        if PROBE_UNKNOWN in rejected:
            return Result(name, FAIL, "the server took the export but does not know test spans",
                          "the server is older than this trailbeam doctor (Trailbeam 0.3 and before keep no test "
                          "spans). Real spans are not affected.",
                          "upgrade the server to the version of this command, or skip this check's result.",
                          probe_rejected=True)
        if PROBE_NOT_STORED in rejected:
            return Result(name, FAIL, "the server knows test spans, but its store does not keep them",
                          "the server runs with a store that has no place for test spans (a custom store, or one "
                          "not updated for span contract section 17). Real spans are not affected.",
                          "use the SQLite or Postgres store of this version, or skip this check's result.",
                          probe_rejected=True)
        if PROBE_BAD_ID in rejected:
            return Result(name, FAIL, "the server did not accept the test span's ID",
                          "this trailbeam doctor and the server disagree on test span IDs (span contract section "
                          "17): one of them is from a different version.",
                          "use the same Trailbeam version for the server and this command.", probe_rejected=True)
        return Result(name, FAIL, f"the server rejected it: {rejected}", "the server could not store it.",
                      "see the server's log for this request.", probe_rejected=True)
    found: Optional[Answer] = None
    while True:
        try:
            found = _request(t, "GET", f"{t.url}/api/probes/{probe_id}")
        except Unreachable as e:
            return Result(name, FAIL, f"reading it back failed: {e.summary}", e.cause, e.fix)
        if found.status != 404 or time.monotonic() - t0 > READ_BACK_S:
            break
        time.sleep(0.2)
    ms = (time.monotonic() - t0) * 1000
    if found.status == 200:
        return Result(name, OK, f"sent to {t.ingest_url} and read back from {t.url} in {ms:.0f} ms")
    err = str((found.json() or {}).get("error", ""))
    if found.status == 404 and err.startswith("no such endpoint"):
        return Result(name, FAIL, f"{t.url} has no /api/probes/",
                      "the server at --url is older than this trailbeam doctor, or is not the one ingest went to.",
                      "upgrade the server, and point --url and --ingest-url at the same one.")
    if found.status == 404:
        return Result(name, FAIL, f"ingest at {t.ingest_url} took it, but {t.url} does not have it "
                                  f"{READ_BACK_S:g} s later",
                      "ingest and the server you read are two different Trailbeams (or replicas, each with its own "
                      "SQLite file), or a Collector between them does not forward Trailbeam's spans.",
                      "send spans to the server you open: one address for services and the web screen. Several "
                      "replicas need a shared Postgres (README, \"Several replicas on one Postgres\"). A Collector "
                      "must pass scope trailbeam spans through (collector/).")
    return Result(name, FAIL, f"reading it back answered {found.status}: {err or found.body[:200]!r}",
                  "the server could not read it.", "see the server's log for this request.")


def _last_span(health: dict, now_ns: int, probe_ok: bool, t: Target, probe_rejected: bool = False) -> Result:
    name = "Last real span"
    data = health.get("data") or {}
    ingest = health.get("ingest") or {}
    if data.get("store") == "unreachable":
        return Result(name, FAIL, "the server cannot reach its database",
                      "the database (Postgres) is down or unreachable from the server.",
                      "check the database, and TRAILBEAM_STORE_URL; GET /api/ready answers 503 until it is back.")
    last_ns, last_row = data.get("last_span_ns"), data.get("last_row") or 0
    if last_ns is None and last_row:
        return Result(name, OK, f"{last_row} rows stored; this server does not say when the last arrived")
    if last_ns is None:
        why = []
        # the server counts doctor's own rejected test span as a rejected span (span contract section 17)
        rejected = max((ingest.get("rejected") or 0) - (1 if probe_rejected else 0), 0)
        if rejected:
            why.append(f"{rejected} Trailbeam span(s) arrived since the server started but break the span "
                       "contract; the server's log says why (a package much older than the server?)")
        if ingest.get("ignored"):
            why.append(f"{ingest['ignored']} span(s) arrived that are not Trailbeam's: your services send their own "
                       "traces here, but the Trailbeam hooks are not installed in them")
        cause = ("; ".join(why) + ".") if why else (
            ("Trailbeam works (the test span went through), so " if probe_ok else "")
            + "your services are not sending to it: the hooks are not installed, the services have not touched a "
              "queue yet, or they send somewhere else.")
        return Result(name, FAIL, "no span stored yet", cause,
                      "in each service, call trailbeam.boto.install() (Python), trailbeam.instrument(client) (Node.js) "
                      f"or wrap the Lambda handler, and set TRAILBEAM_OTLP_TRACES_ENDPOINT={t.ingest_url}"
                      + (" and OTEL_EXPORTER_OTLP_HEADERS=Authorization=Bearer%20<token>" if t.token else "")
                      + ". A service in a container reaches the host as host.docker.internal, not localhost. "
                        "Then send a message and run trailbeam doctor again.")
    when = datetime.datetime.fromtimestamp(last_ns / 1e9, datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    age_s = (now_ns - last_ns) / 1e9
    if age_s < -60:
        return Result(name, WARN, f"newest span {when}, {_ago(age_s)} in the future",
                      "the clock of a service, or of this machine, is wrong.",
                      "sync the clocks (NTP): timelines order steps by their time.")
    if age_s > QUIET_AFTER_S:
        return Result(name, WARN, f"newest span {when}, {_ago(age_s)} ago",
                      "nothing has been sent since: the services are idle, stopped, or now send elsewhere.",
                      f"if they are busy now, check they still have TRAILBEAM_OTLP_TRACES_ENDPOINT={t.ingest_url}.")
    return Result(name, OK, f"newest span {when}, {_ago(age_s)} ago")


def report(t: Target, results: list[Result], out=None) -> int:
    out = out or sys.stdout
    shown = f"{t.url} (from {t.url_source})" if t.url_source else t.url
    out.write(f"trailbeam doctor: server {shown}, ingest {t.ingest_url}, token: {t.token_source}\n\n")
    for r in results:
        out.write(f"  {r.status:<5} {r.name}: {r.detail}\n")
        if r.cause:
            out.write(f"        Likely cause: {r.cause}\n")
        if r.fix:
            out.write(f"        Fix: {r.fix}\n")
    failed = sum(r.status == FAIL for r in results)
    out.write("\n" + (f"{failed} of {len(results)} checks failed.\n" if failed else "All checks passed.\n"))
    return 1 if failed else 0


def _with_scheme(url: str) -> str:
    return url if "://" in url else "http://" + url


def _base_of(url: str) -> Optional[str]:
    """Scheme, host and port of a URL (the default port of its scheme when it has none), or None."""
    try:
        u = urlsplit(_with_scheme(url))
        host = u.hostname
        port = u.port or (443 if u.scheme == "https" else 80)
    except ValueError:
        return None
    if not host:
        return None
    return f"{u.scheme}://{f'[{host}]' if ':' in host else host}:{port}"


def _address_file():
    from pathlib import Path
    return Path(default_data_dir()) / ADDRESS_FILE


def url_candidates(ingest: str, ingest_given: str) -> list[tuple[str, str]]:
    """Where the server may be when --url, TRAILBEAM_URL and TRAILBEAM_PORT are not given, with where each came
    from (QV-220): the server of the ingest address the services were given, and the address trailbeam start
    last listened on. Port 4318 only when neither is known: after ``trailbeam start --port N`` another
    server on 4318 must not be taken for this one."""
    out: list[tuple[str, str]] = []
    if ingest_given:
        base = _base_of(ingest)
        if base:
            out.append((base, ingest_given))
    path = _address_file()
    try:
        text = path.read_text(encoding="utf-8").strip()
    except OSError:
        text = ""
    base = _base_of(text) if text else None
    if base and base not in [u for u, _ in out]:
        out.append((base, f"{path}, written by trailbeam start"))
    return out


def _is_trailbeam(url: str, timeout_s: float) -> bool:
    try:
        a = _request(Target(url=url, ingest_url=url + INGEST_PATH, timeout_s=timeout_s), "GET", url + "/api/health")
        doc = a.json()
        return a.status == 200 and isinstance(doc, dict) and doc.get("status") == "ok" and "version" in doc
    except (Unreachable, OSError, ValueError):
        return False


def choose_url(t: Target, candidates: list[tuple[str, str]]) -> Target:
    """Of two candidates, the first that answers as Trailbeam, else the first: when services send through a
    Collector, the ingest address is the Collector and the server is the one trailbeam start wrote down."""
    if len(candidates) > 1:
        for url, source in candidates:
            if _is_trailbeam(url, min(t.timeout_s, 2.0)):
                t.url, t.url_source = url, source
                return t
    if candidates:
        t.url, t.url_source = candidates[0]
    return t


def target_from_args(a, discover: bool = True) -> Target:
    """The target from the flags and the environment. With ``discover`` and no server address given, the
    candidates of url_candidates() are looked at (one GET /api/health each when there are two)."""
    explicit = (a.url or os.environ.get("TRAILBEAM_URL", "").strip()
                or (f"http://127.0.0.1:{os.environ['TRAILBEAM_PORT'].strip()}"
                    if os.environ.get("TRAILBEAM_PORT", "").strip() else ""))
    ingest_given = ("--ingest-url" if a.ingest_url else "TRAILBEAM_OTLP_TRACES_ENDPOINT"
                    if os.environ.get("TRAILBEAM_OTLP_TRACES_ENDPOINT", "").strip() else "")
    ingest_env = a.ingest_url or os.environ.get("TRAILBEAM_OTLP_TRACES_ENDPOINT", "").strip()
    candidates = [] if explicit else url_candidates(ingest_env, ingest_given)
    url = _with_scheme(explicit or f"http://127.0.0.1:{DEFAULT_PORT}").rstrip("/")
    ingest = _with_scheme(ingest_env) if ingest_env else ""
    t = Target(url=url, ingest_url=ingest, timeout_s=a.timeout)
    if candidates:
        if discover:
            choose_url(t, candidates)
        else:
            t.url, t.url_source = candidates[0]
    t.ingest_url = t.ingest_url or t.url + INGEST_PATH
    path = a.token_file or os.environ.get("TRAILBEAM_TOKEN_FILE", "").strip()
    if path:
        t.token_source = path
    elif os.environ.get("TRAILBEAM_TOKEN", "").strip():
        t.token, t.token_source = os.environ["TRAILBEAM_TOKEN"].strip(), "TRAILBEAM_TOKEN"
        return t
    elif _default_token_path().is_file():
        path = str(_default_token_path())
        t.token_source = path
    else:
        return t
    try:
        with open(path, encoding="utf-8") as f:
            t.token = f.read().strip()
        if not t.token:
            t.token_error = f"{path} is empty"
    except OSError as e:
        t.token_error = f"cannot read {path}: {e.strerror or e}"
    return t


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="trailbeam doctor",
        description="Check why no data is arriving: the server, the ingest port, the token, a test span sent and "
                    "read back, and the last real span. Exit code 1 when a check fails.")
    p.add_argument("--url", help="the server's address (env TRAILBEAM_URL, or the port in TRAILBEAM_PORT; default "
                                 "the server of TRAILBEAM_OTLP_TRACES_ENDPOINT, or the address trailbeam start last "
                                 "listened on, else http://127.0.0.1:4318)")
    p.add_argument("--ingest-url", help="where services send spans (env TRAILBEAM_OTLP_TRACES_ENDPOINT, as the "
                                        "services read it; default the server's /v1/traces)")
    p.add_argument("--token-file", help="a file holding the access token (env TRAILBEAM_TOKEN_FILE, or the token "
                                        f"itself in TRAILBEAM_TOKEN; default {_default_token_path()}, which trailbeam "
                                        "start creates, if it exists)")
    p.add_argument("--timeout", type=float, default=5.0, help="seconds to wait for each answer (default 5)")
    return p


def main(argv=None) -> int:
    t = target_from_args(parser().parse_args(argv))
    return report(t, run(t))


if __name__ == "__main__":
    sys.exit(main())
