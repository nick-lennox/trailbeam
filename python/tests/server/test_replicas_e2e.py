"""Two Trailbeam server processes on one Postgres, behind a load balancer (QV-81), end to end over HTTP.

- Started at the same moment, they create the schema once (the migration lock).
- Each span of contract/fixtures is sent on its own to a random replica, in a random order, many at once: every
  timeline either replica serves is exactly the one a single SQLite Trailbeam serves for the same spans.
- One shared token works on both, and a browser session made on one works on the other (no sticky sessions).
- /metrics is per replica: each counts the exports and spans it took itself, behind the same token.
- The first admin, created with ``trailbeam users`` on the shared store while both run, switches both to accounts
  within 5 seconds: the shared token then opens only ingest and /metrics, a session made on one replica works on
  the other, and a personal API token and an ingest token made from the command line work on both.
- A replica killed (SIGKILL) during ingest loses no acknowledged span: the exporter retries what was in flight
  and the load balancer sends it to the other replica.

The load balancer is tests/server/tcp_proxy.py, round robin, skipping a replica it can't connect to. The
exporter here retries as the OpenTelemetry OTLP/HTTP exporter does (connection errors, and 429/502/503/504
honouring Retry-After). Runs when TRAILBEAM_TEST_POSTGRES_URL is set.
"""
import base64
import concurrent.futures
import json
import os
import random
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from urllib.parse import quote

import pytest
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.trace.v1.trace_pb2 import ResourceSpans, ScopeSpans

from spans import SEC, T0, Consumer, to_proto
from tcp_proxy import Proxy

PG = os.environ.get("TRAILBEAM_TEST_POSTGRES_URL")
pytestmark = pytest.mark.skipif(not PG, reason="TRAILBEAM_TEST_POSTGRES_URL is not set")
ROOT = Path(__file__).resolve().parents[3]
FIXTURE = ROOT / "contract" / "fixtures" / "contract-3.2-spans.json"
TOKEN = "replica-test-token-0123456789abcdef"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
PROTO = {"Content-Type": "application/x-protobuf"}


# ---- the replicas ----------------------------------------------------------------------------------------

class Replica:
    def __init__(self, store_url: str, *args: str):
        env = {k: v for k, v in os.environ.items() if not k.startswith("TRAILBEAM_")}
        env.update(TRAILBEAM_TOKEN=TOKEN, TRAILBEAM_ACCOUNTS="1", PYTHONUNBUFFERED="1")
        self.p = subprocess.Popen(
            [sys.executable, "-m", "trailbeam.server", "--store-url", store_url, "--host", "127.0.0.1", "--port", "0",
             "--retention", "forever", *args], env=env, stderr=subprocess.PIPE, text=True)
        self.log: list[str] = []
        self.port = None
        self._seen = threading.Event()
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self):
        for line in self.p.stderr:
            self.log.append(line)
            if self.port is None and "listening on http://127.0.0.1:" in line:
                self.port = int(line.split("http://127.0.0.1:")[1].split()[0])
                self._seen.set()
        self._seen.set()

    def wait_ready(self, timeout=60):
        assert self._seen.wait(timeout) and self.port, "".join(self.log)
        deadline = time.monotonic() + timeout
        while call(self.url, "/api/ready")[0] != 200:
            assert time.monotonic() < deadline, "".join(self.log)
            time.sleep(0.1)
        return self

    @property
    def url(self):
        return f"http://127.0.0.1:{self.port}"

    def stop(self):
        if self.p.poll() is None:
            self.p.terminate()
            try:
                self.p.wait(15)
            except subprocess.TimeoutExpired:
                self.p.kill()
                self.p.wait()


def call(url, path, body=None, headers=None, timeout=30):
    req = urllib.request.Request(url + path, data=body, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


def api(url, path):
    status, _, body = call(url, path, headers=AUTH)
    assert status == 200, (path, status, body)
    return json.loads(body)


@pytest.fixture
def cluster():
    """``start(n)``: n replicas on one fresh schema, started at once and ready."""
    sys.path.insert(0, str(ROOT / "python" / "tests" / "conformance"))
    from pg_helpers import drop_schema, new_schema

    conninfo = new_schema()
    schema = conninfo.split("search_path=")[1].split()[0].strip("'")
    store_url = PG + ("&" if "?" in PG else "?") + "options=" + quote(f"-c search_path={schema}")
    made = []

    def start(n=2):
        rs = [Replica(store_url) for _ in range(n)]
        made.extend(rs)
        return [r.wait_ready() for r in rs]
    start.conninfo = conninfo
    start.store_url = store_url
    yield start
    for r in made:
        r.stop()
    drop_schema(conninfo)


def export_with_retries(url, body, deadline_s=60, cut=None):
    """POST one export the way the OTLP/HTTP exporter does: retried on a connection error and on 429, 502, 503
    and 504 (after Retry-After when given). True once it is acknowledged (200). ``cut``: a list a connection
    error is appended to."""
    end, backoff = time.monotonic() + deadline_s, 0.05
    while time.monotonic() < end:
        try:
            status, headers, _ = call(url, "/v1/traces", body, {**PROTO, **AUTH}, timeout=10)
        except (OSError, urllib.error.URLError) as e:
            status, headers = None, {}
            if cut is not None:
                cut.append(e)
        if status == 200:
            return True
        if status not in (None, 429, 502, 503, 504):
            raise AssertionError(f"export answered {status}")
        time.sleep(float(headers.get("Retry-After", backoff)))
        backoff = min(backoff * 2, 1.0)
    return False


def one_span_requests(raw: bytes) -> list[bytes]:
    """An OTLP request split into one request per span, each with its resource and scope."""
    req = ExportTraceServiceRequest.FromString(raw)
    out = []
    for rs in req.resource_spans:
        for ss in rs.scope_spans:
            for sp in ss.spans:
                one = ExportTraceServiceRequest(resource_spans=[ResourceSpans(
                    resource=rs.resource, schema_url=rs.schema_url,
                    scope_spans=[ScopeSpans(scope=ss.scope, schema_url=ss.schema_url, spans=[sp])])])
                out.append(one.SerializeToString())
    return out


def reference_timelines(requests: list[bytes]) -> dict:
    """What one SQLite Trailbeam serves for these requests, sent in order: {path: JSON}."""
    from trailbeam.server.db import SqliteStore
    from trailbeam.server.server import App

    store = SqliteStore(":memory:")
    app = App(store)
    for body in requests:
        assert app.ingest(body, "application/x-protobuf")[0] == 200
    out = {"/api/messages?limit=500": app.api("messages", {"limit": "500"})}
    for m in out["/api/messages?limit=500"]["messages"]:
        out[f"/api/messages/{quote(m['message_id'], safe='')}"] = app.api(f"messages/{quote(m['message_id'], safe='')}", {})
    out["/api/stats"] = app.api("stats", {})
    store.close()
    return out


def scrape(url) -> dict:
    """/metrics as {(name, frozenset of label pairs): value}."""
    status, _, body = call(url, "/metrics", headers=AUTH)
    assert status == 200, (status, body)
    out = {}
    for line in body.decode().splitlines():
        if not line or line.startswith("#"):
            continue
        key, value = line.rsplit(" ", 1)
        name, _, labels = key.partition("{")
        pairs = (kv.split("=", 1) for kv in labels.rstrip("}").split(",") if kv)
        out[(name, frozenset((k, v.strip('"')) for k, v in pairs))] = float(value)
    return out


def metric(scraped: dict, name: str, **labels) -> float:
    return scraped.get((name, frozenset(labels.items())), 0.0)


def _stable(obj):
    """Without what depends on the moment of the request (``now``)."""
    if isinstance(obj, dict):
        return {k: _stable(v) for k, v in obj.items() if k not in ("now_ms", "generated_ms", "age_ms", "now")}
    if isinstance(obj, list):
        return [_stable(v) for v in obj]
    return obj


# ---- the tests -------------------------------------------------------------------------------------------

def test_replicas_started_together_migrate_once_and_share_the_token(cluster):
    import psycopg

    a, b = cluster(2)
    with psycopg.connect(cluster.conninfo) as c:
        assert c.execute("SELECT COUNT(*) FROM schema_version").fetchone()[0] == 1
    for r in (a, b):
        assert call(r.url, "/api/messages")[0] == 401
        assert call(r.url, "/api/messages", headers=AUTH)[0] == 200
        assert not any("Traceback" in line for line in r.log), "".join(r.log)
    # a browser session made on one replica works on the other
    import http.client

    c = http.client.HTTPConnection("127.0.0.1", a.port, timeout=10)
    c.request("POST", "/login", f"token={TOKEN}&next=/".encode(), {"Content-Type": "application/x-www-form-urlencoded"})
    r = c.getresponse()
    assert r.status == 303
    cookie = r.getheader("Set-Cookie").split(";")[0]
    c.close()
    assert call(b.url, "/api/messages", headers={"Cookie": cookie})[0] == 200


def test_spans_to_random_replicas_in_random_order_give_the_fixture_timelines(cluster):
    replicas = cluster(2)
    requests = []
    for e in json.loads(FIXTURE.read_text())["exports"]:
        assert e["contentType"] == "application/x-protobuf"
        requests += one_span_requests(base64.b64decode(e["base64"]))
    want = reference_timelines(requests)
    assert len(want) > 10  # the listing, the stats and a timeline per message
    rng = random.Random(81)
    work = [(rng.choice(replicas).url, body) for body in requests]
    rng.shuffle(work)
    with concurrent.futures.ThreadPoolExecutor(16) as pool:
        assert all(pool.map(lambda w: export_with_retries(*w), work))
    for r in replicas:
        for path, expected in want.items():
            assert _stable(api(r.url, path)) == _stable(expected), (r.url, path)
    # /metrics is per replica: each counts the exports sent to it (retries go to the same replica here), and
    # together they count every span once. The token guards it on both.
    scraped = {r.url: scrape(r.url) for r in replicas}
    for r in replicas:
        assert call(r.url, "/metrics")[0] == 401
        sent = sum(1 for url, _ in work if url == r.url)
        assert 0 < sent < len(work)
        assert metric(scraped[r.url], "trailbeam_ingest_requests_total", code="200") == sent, r.url
    assert sum(metric(m, "trailbeam_ingest_spans_total", result="inserted") for m in scraped.values()) == len(requests)


def test_killing_a_replica_during_ingest_loses_no_acknowledged_span(cluster):
    a, b = cluster(2)
    lb = Proxy([("127.0.0.1", a.port), ("127.0.0.1", b.port)])
    rng = random.Random(7)
    w, dlq = Consumer(), Consumer(client_id="dlq-1", queue="orders-dlq")
    spans = []
    for i in range(400):
        t, m, h = T0 + i * SEC, f"m-{i}", f"h-{i}"
        spans += [w.send(t, m), w.receive(t + SEC // 4, m, h), w.delete(t + SEC // 2, h)]
        if i % 10 == 0:  # a redelivery, then the dead-letter queue
            spans += [w.receive(t + SEC // 3, m, h + "b", count=2), dlq.receive(t + SEC, m, h + "c", dlq_source="orders")]
    rng.shuffle(spans)  # settles before their receives, and on another replica, as often as not
    exports = [to_proto(spans[i:i + 8]).SerializeToString() for i in range(0, len(spans), 8)]
    acked, cut, killed = [], [], threading.Event()
    lock = threading.Lock()

    def send(i):
        ok = export_with_retries(f"http://127.0.0.1:{lb.port}", exports[i], cut=cut)
        with lock:
            if ok:
                acked.append(i)
            if len(acked) == len(exports) // 3 and not killed.is_set():
                killed.set()
                threading.Thread(target=kill_mid_ingest, daemon=True).start()
        return ok

    def kill_mid_ingest():
        # Freeze replica a, wait until the load balancer has sent it an export (which it can't have answered),
        # then SIGKILL it: an export is cut mid-flight on every run, however busy the machine is.
        a.p.send_signal(signal.SIGSTOP)
        with lb._lock:
            before = lb.opened[0]
        end = time.monotonic() + 10
        while time.monotonic() < end:
            with lb._lock:
                if lb.opened[0] > before:
                    break
            time.sleep(0.005)
        a.p.send_signal(signal.SIGKILL)

    try:
        with concurrent.futures.ThreadPoolExecutor(8) as pool:
            results = list(pool.map(send, range(len(exports))))
        assert killed.is_set() and a.p.wait(10) == -signal.SIGKILL
        assert all(results), f"{results.count(False)} exports never acknowledged"
        assert cut, "no export was in flight on the killed replica"
    finally:
        lb.close()
    # every acknowledged span is stored, once, and the timelines are a single Trailbeam's for the same spans
    stats = api(b.url, "/api/stats")["data"]
    assert sum(stats["spans"].values()) == len(spans)
    assert stats["unjoined_settles"] == 0
    want = reference_timelines(exports)
    assert len(want["/api/messages?limit=500"]["messages"]) == 400
    for path, expected in want.items():
        assert _stable(api(b.url, path)) == _stable(expected), path


def cli(store_url, *argv, password=None, tmp=None):
    env = {k: v for k, v in os.environ.items() if not k.startswith("TRAILBEAM_")}
    env["TRAILBEAM_ACCOUNTS"] = "1"
    args = list(argv)
    if password is not None:
        f = Path(tmp) / "pw"
        f.write_text(password + "\n")
        args += ["--password-file", str(f)]
    r = subprocess.run([sys.executable, "-m", "trailbeam.server", *args, "--store-url", store_url], env=env,
                       capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stderr
    return r.stdout.strip(), r.stderr


def eventually(check, within_s, what):
    end = time.monotonic() + within_s
    while not check():
        assert time.monotonic() < end, what
        time.sleep(0.1)


def test_the_first_admin_switches_every_replica_and_the_shared_token_keeps_ingest_and_metrics(cluster, tmp_path):
    a, b = cluster(2)
    for r in (a, b):
        assert api(r.url, "/api/me")["auth"] == "token"
    _, err = cli(cluster.store_url, "users", "create", "--email", "root@example.com", "--role", "admin",
                 password="correct horse battery", tmp=tmp_path)
    assert "Upgrade every replica to this version before creating the first admin" in err
    t0 = time.monotonic()
    for r in (a, b):  # within 5 s, both refuse the shared token on the API ...
        eventually(lambda: call(r.url, "/api/messages", headers=AUTH)[0] == 401, 6, "the switch on " + r.url)
    assert time.monotonic() - t0 < 6
    body = ExportTraceServiceRequest(resource_spans=[]).SerializeToString()
    for r in (a, b):  # ... and keep it for ingest and /metrics
        assert call(r.url, "/v1/traces", body, {**PROTO, **AUTH})[0] == 200
        assert call(r.url, "/metrics", headers=AUTH)[0] == 200
        assert json.loads(call(r.url, "/api/health")[2]) .keys() == {"status", "version"}  # says no more to nobody
    # a session made on one replica works on the other
    form = "email=root%40example.com&password=correct+horse+battery&next=%2F".encode()
    req = urllib.request.Request(a.url + "/login", data=form, headers={"Content-Type": "application/x-www-form-urlencoded"})

    class Keep(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, *args, **kw):
            return None
    try:
        urllib.request.build_opener(Keep).open(req, timeout=30)
        raise AssertionError("expected a 303")
    except urllib.error.HTTPError as e:
        assert e.code == 303
        cookie = e.headers["Set-Cookie"].split(";")[0]
    me = json.loads(call(b.url, "/api/me", headers={"Cookie": cookie})[2])
    assert me["via"] == "session" and me["user"]["role"] == "admin"
    # tokens made from the command line, used on both
    personal, _ = cli(cluster.store_url, "tokens", "create", "--name", "scripts", "--personal", "--user",
                      "root@example.com")
    ingest, _ = cli(cluster.store_url, "tokens", "create", "--name", "orders-api", "--yes")
    for r in (a, b):
        assert call(r.url, "/api/messages", headers={"Authorization": f"Bearer {personal}"})[0] == 200
        assert call(r.url, "/metrics", headers={"Authorization": f"Bearer {personal}"})[0] == 200
        assert call(r.url, "/v1/traces", body, {**PROTO, "x-trailbeam-key": ingest})[0] == 200
        assert call(r.url, "/api/messages", headers={"x-trailbeam-key": ingest})[0] == 401  # it can't read
        assert call(r.url, "/v1/traces", body, {**PROTO, "Authorization": f"Bearer {personal}"})[0] == 401
