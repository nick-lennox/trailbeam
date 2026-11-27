"""contract/openapi.yaml and the server agree (QV-95).

The server is a plain http.server handler with no route or parameter table, so the file is written by hand.
These tests fail when the two drift apart:

1. Routes: every route the server serves is in the file, and every route in the file is served. The server's
   routes are found by asking it (a probe of each candidate path), not from a list kept here.
2. Parameters: the query parameters each endpoint reads (a dict that records the keys it is asked for) are
   the ones the file lists, and each integer's minimum, maximum and default are the server's (``_int``).
3. Responses: every status the file lists for an operation is answered by the server in these tests, and
   every answer validates against the file's schema for it. The check itself is in openapi_contract.py and
   runs on every response of every server test (conftest.py ``record_api_responses``).
4. The file: it is valid OpenAPI 3.1, and every example in it validates against its schema.
"""
from __future__ import annotations

import base64
import contextlib
import http.client
import json
import re
import threading
import sys
from pathlib import Path
from urllib.parse import quote, urlencode, urlsplit

import pytest
from openapi_spec_validator import validate as validate_openapi

from openapi_contract import Contract, contract, pointer
from spans import SEC, T0, Consumer, to_json, to_proto
from trailbeam.server import auth as auth_mod, doctor, proxy_auth, server
from trailbeam.server.backpressure import IngestGate
from trailbeam.server.proxy import TrustedProxies, parse_networks
from trailbeam.server.store import StoreUnavailable, WriterBusy

FIXTURE = Path(__file__).resolve().parents[3] / "contract" / "fixtures" / "contract-3.2-spans.json"
EXPORTS = json.loads(FIXTURE.read_text())["exports"]
TOKEN = "openapi-test-token-0123456789abcdef"
MID = "m-bill-1"  # a message in the fixture
PROBE_ID = "doctor-3f2a9c1e0b7d4e55"  # the probe in get-probe's 200 example

# Paths the server matches by name that are not part of the API the file describes, so not in it: the
# Prometheus text on /metrics, and the token login pages. Every other GET path outside /api/ is the web
# screen. A new literal path in server.py fails test_every_literal_path_in_the_server_is_probed until it is
# added to the file or here.
NOT_IN_THE_FILE = {"/metrics", "/login", "/logout"}

# A server bug, found by this test (reported on QV-95, not fixed here: the server is not changed in this pull
# request). These accept from_ms/to_ms up to 10^15, as the file says, but convert them to nanoseconds and pass
# them to SQLite, which takes 64-bit integers only: 10^15 ms is 10^21 ns, so the answer is a 500 from about
# 9.2 * 10^12 ms (year 2262) on. Listed so the test still checks the rest; it fails once they answer 200.
OVERFLOWS_AT_MAXIMUM = {
    ("GET /api/messages", "from_ms"), ("GET /api/messages", "to_ms"),
    ("GET /api/duplicates", "from_ms"), ("GET /api/duplicates", "to_ms"),
    ("GET /api/stuck", "from_ms"),
}


# ---- a server to ask -------------------------------------------------------------------------------------

@contextlib.contextmanager
def serve(app):
    """``app`` behind the real handler, on a free port. Yields its base URL. ``server.make_handler`` is looked
    up here, so the recorder conftest.py installs sees every response."""
    httpd = server.ThreadingHTTPServer(("127.0.0.1", 0), server.make_handler(app))
    httpd.daemon_threads = True
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    try:
        yield f"http://127.0.0.1:{httpd.server_address[1]}"
    finally:
        httpd.shutdown()
        httpd.server_close()


def request(base, method, path, body=None, headers=None):
    """(status, headers, body). Never raises on an HTTP status; a connection the server closed without an
    answer is (None, {}, b"")."""
    u = urlsplit(base)
    conn = http.client.HTTPConnection(u.hostname, u.port, timeout=10)
    try:
        conn.request(method, path, body=body, headers=headers or {})
        r = conn.getresponse()
        return r.status, dict(r.getheaders()), r.read()
    except (ConnectionError, http.client.RemoteDisconnected):
        return None, {}, b""
    finally:
        conn.close()


def load_fixture(base, token=None):
    """The fixture's 32 spans, POSTed the way the packages sent them."""
    h = {"Authorization": f"Bearer {token}"} if token else {}
    for e in EXPORTS:
        status, _, _ = request(base, "POST", "/v1/traces", base64.b64decode(e["base64"]),
                               {"Content-Type": e["contentType"], **h})
        assert status == 200, e["service"]
    # trailbeam doctor's test span (contract section 17), with the fixture message's ID, so that every
    # {path parameter} route has something stored under MID
    status, _, _ = request(base, "POST", "/v1/traces", doctor.probe_export(MID), {"Content-Type": "application/json", **h})
    assert status == 200, "probe"
    # and the one get-probe's 200 example and x-example-request name
    status, _, _ = request(base, "POST", "/v1/traces", doctor.probe_export(PROBE_ID),
                           {"Content-Type": "application/json", **h})
    assert status == 200, "example probe"


@pytest.fixture
def app(db):
    return server.App(db, ui_dir=None)


@pytest.fixture
def fixture_server(app):
    with serve(app) as base:
        load_fixture(base)
        yield base


# ---- 4. the file itself ----------------------------------------------------------------------------------

def test_the_file_is_valid_openapi_3_1():
    spec = contract().spec
    assert re.fullmatch(r"3\.1\.\d+", spec["openapi"]), spec["openapi"]
    validate_openapi(spec)


def _examples(node, ptr=""):
    """(schema pointer, example, where) for every example in the file: on a media type, parameter or header
    (``example``, ``examples.*.value``) and inside a schema (``examples``, ``example``)."""
    if isinstance(node, list):
        for i, v in enumerate(node):
            yield from _examples(v, ptr + pointer(i))
        return
    if not isinstance(node, dict):
        return
    in_schema = "/schema" in ptr or ptr.startswith("/components/schemas/")
    if "schema" in node and not in_schema:  # a media type, parameter or header object
        if "example" in node:
            yield ptr + "/schema", node["example"], ptr + "/example"
        for name, ex in (node.get("examples") or {}).items():
            if "$ref" in ex:
                ex, _ = contract().resolve(ex)
            if "value" in ex:
                yield ptr + "/schema", ex["value"], ptr + pointer("examples", name, "value")
    elif in_schema:  # a schema object
        if isinstance(node.get("examples"), list):
            for i, ex in enumerate(node["examples"]):
                yield ptr, ex, ptr + pointer("examples", i)
        if "example" in node:
            yield ptr, node["example"], ptr + "/example"
    for k, v in node.items():
        if k in ("example", "examples"):
            continue
        if in_schema and k == "properties":  # property names are not schemas, their values are
            for name, sub in v.items():
                yield from _examples(sub, ptr + pointer("properties", name))
            continue
        yield from _examples(v, ptr + pointer(k))


def test_every_example_in_the_file_validates_against_its_schema():
    c = contract()
    found = list(_examples(c.spec))
    # every 200 of the API has an example, and the errors and the ingest endpoint have several
    assert len(found) >= 20, [w for _, _, w in found]
    bad = {where: c.errors(sptr, ex) for sptr, ex, where in found if c.errors(sptr, ex)}
    assert not bad, json.dumps(bad, indent=1)


def test_every_x_example_request_answers_200_on_the_fixture(fixture_server, record_api_responses):
    """The request each 200 example came from is a request the file allows, and the server answers it."""
    asked = 0
    for op in contract().ops:
        req = op.op.get("x-example-request")
        if req and not _people_op(op):  # those need an admin: test_people_api.py asks them
            status, _, body = request(fixture_server, op.method, req)
            assert status == 200, (req, body[:200])
            asked += 1
    assert asked >= 9
    assert not record_api_responses.problems


def test_every_operation_with_a_path_parameter_has_an_x_example_request():
    """docs.trailbeam.app's generator refuses one without: it cannot write the example's request (QV-287)."""
    missing = [op.key for op in contract().ops if op.n_params and not op.op.get("x-example-request")]
    assert not missing, missing


def test_get_probe_example_is_the_answer_to_its_x_example_request(fixture_server):
    """The ID in the 200 example is the one its x-example-request asks for, and the server answers with it."""
    op = next(o for o in contract().ops if o.op.get("operationId") == "get-probe")
    example = op.op["responses"]["200"]["content"]["application/json"]["example"]
    assert op.op["x-example-request"] == f"/api/probes/{example['data']['id']}"
    status, _, body = request(fixture_server, "GET", op.op["x-example-request"])
    assert status == 200
    assert json.loads(body)["data"]["id"] == example["data"]["id"]


# ---- the checker catches what it must -------------------------------------------------------------------

def test_the_check_fails_a_field_the_file_does_not_name(fixture_server, record_api_responses):
    c = contract()
    _, _, body = request(fixture_server, "GET", f"/api/messages/{MID}")
    t = json.loads(body)
    assert c.check("GET", f"/api/messages/{MID}", 200, "application/json", {}, t)[1] == []
    t["events"][0]["new_field"] = 1
    [problem] = c.check("GET", f"/api/messages/{MID}", 200, "application/json", {}, t)[1]
    assert "new_field" in problem and "events/0" in problem
    t = json.loads(body)
    t["notices"].append({"code": "a_new_notice", "message": "x"})
    assert c.check("GET", f"/api/messages/{MID}", 200, "application/json", {}, t)[1]


def test_the_check_fails_a_status_a_content_type_or_a_header_the_file_does_not_have():
    c = contract()
    assert "does not list" in c.check("GET", "/api/stats", 404, "application/json", {}, {"error": "x"})[1][0]
    assert "text/html" in c.check("GET", "/api/stats", 200, "text/html", {}, b"<p>")[1][0]
    [p] = c.check("POST", "/v1/traces", 401, "text/plain", {}, b"no")[1]
    assert "WWW-Authenticate" in p
    status = {"code": 14, "message": "x"}
    assert c.check("POST", "/v1/traces", 503, "application/json", {"Retry-After": "1"}, status)[1] == []
    assert "Retry-After" in c.check("POST", "/v1/traces", 503, "application/json", {"Retry-After": "5"}, status)[1][0]
    assert "text/plain" in c.check("POST", "/v1/traces", 503, "text/plain", {"Retry-After": "1"}, b"x")[1][0]
    assert "no such operation" in c.check("GET", "/api/new", 200, "application/json", {}, {})[1][0]


# ---- 1. routes -------------------------------------------------------------------------------------------

def _concrete(template):
    return re.sub(r"\{[^}]+\}", quote(MID, safe=""), template)


PEOPLE_TAGS = {"People", "Tokens"}  # need an admin or a person: test_people_api.py, and _people_and_tokens below


def _people_op(op) -> bool:
    return bool(PEOPLE_TAGS & set(op.op.get("tags", [])))


def test_every_route_in_the_file_is_served(fixture_server):
    """Each operation, with the fixture's message for a path parameter, answers 200."""
    for op in contract().ops:
        if _people_op(op):
            continue
        path = _concrete(op.template)
        if op.method == "GET":
            status, _, body = request(fixture_server, "GET", path)
        else:
            e = EXPORTS[0]
            status, _, body = request(fixture_server, op.method, path, base64.b64decode(e["base64"]),
                                      {"Content-Type": e["contentType"]})
        assert status == 200, (op.key, status, body[:200])


def _server_paths(app):
    """Candidate paths, from the server's own routes: ``app.routes``, every "/api/..." or "/v1/..." literal in
    server.py (/api/ready and /api/health are matched there, before App.api), and each of those with one or
    two more segments, a message ID or a word, as a handler that reads ``rest`` would take."""
    source = Path(server.__file__).read_text()
    firsts = set(app.routes) | {
        m.split("/")[2] for m in re.findall(r'"(/api/[a-z_]+)', source)}
    roots = {f"/api/{k}" for k in firsts} | set(re.findall(r'"(/v1/[a-z_/]+)"', source))
    extra = [MID, "body", "search", "series", "x", "1", "ingest", "tokens", "link"]
    out = set(roots)
    for r in roots:
        for a in extra:
            out.add(f"{r}/{a}")
            for b in extra:
                out.add(f"{r}/{a}/{b}")
    return sorted(out)


def test_every_literal_path_in_the_server_is_probed():
    """Each path server.py compares a request path with is under /api/ or /v1/ (probed below) or named in
    NOT_IN_THE_FILE."""
    source = Path(server.__file__).read_text()
    literals = set(re.findall(r'"(/[a-z][a-z0-9_/]*)"', source))
    assert literals, "found no path literal in server.py"
    other = {p for p in literals if not p.startswith(("/api/", "/v1/"))}
    assert other == NOT_IN_THE_FILE, (f"new: {sorted(other - NOT_IN_THE_FILE)}; "
                                      f"no longer matched: {sorted(NOT_IN_THE_FILE - other)}")


def test_every_route_the_server_serves_is_in_the_file(app, fixture_server):
    """A path the server answers with anything but 404, for GET, POST, PATCH or DELETE, is an operation in the
    file."""
    c = contract()
    served, missing = [], []
    for path in _server_paths(app):
        for method in ("GET", "POST", "PATCH", "DELETE"):
            body = base64.b64decode(EXPORTS[0]["base64"]) if method == "POST" else None
            status, _, out = request(fixture_server, method, quote(path, safe="/"), body,
                                     {"Content-Type": EXPORTS[0]["contentType"]} if body else {})
            if status == 404:
                continue
            served.append(f"{method} {path}")
            if c.operation(method, quote(path, safe="/")) is None:
                missing.append(f"{method} {path} -> {status} {out[:80]!r}")
    assert not missing, "served, but not in contract/openapi.yaml:\n" + "\n".join(missing)
    # the probe found the routes: one served path per operation at least
    for op in c.ops:
        assert any(op.regex.match(quote(s.split(" ", 1)[1], safe="/")) and s.startswith(op.method)
                   for s in served), op.key


# ---- 2. parameters ---------------------------------------------------------------------------------------

class Reads(dict):
    """A query dict that records every key a handler asks it for."""

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.read: set[str] = set()

    def __contains__(self, k):
        self.read.add(k)
        return super().__contains__(k)

    def __getitem__(self, k):
        self.read.add(k)
        return super().__getitem__(k)

    def get(self, k, default=None):
        self.read.add(k)
        return super().get(k, default)


class Ints:
    """Every ``_int`` call: (name, default, lo, hi, given)."""

    def __init__(self, monkeypatch):
        self.calls = []
        orig = server._int

        def _int(q, name, default, lo, hi):
            self.calls.append((name, default, lo, hi, dict.__contains__(q, name)))
            return orig(q, name, default, lo, hi)

        monkeypatch.setattr(server, "_int", _int)


def _query_params(op):
    return {p["name"]: p for p in contract().parameters(op) if p["in"] == "query"}


def _sample(p):
    """A value the file says ``p`` takes: its example, its first enum value, its default or its minimum."""
    s = p.get("schema", {})
    if p["name"] == "cursor":
        return None  # a cursor comes from a previous page; any other is a 400 before the rest is read
    if "example" in p:
        return p["example"]
    if "enum" in s:
        return s["enum"][0]
    if s.get("type") == "integer":
        return s.get("default", s.get("minimum", 1))
    return "x"


def _ask(app, op, q):
    """App.api for ``op`` with query ``q``: the keys it read."""
    if op.template == "/api/ready":
        return set()  # matched in do_GET, before App.api: it reads no parameter
    q = Reads({k: str(v) for k, v in q.items() if v is not None})
    path = _concrete(op.template)[len("/api/"):]
    try:
        app.api(path, q)
    except server.ApiError:
        pass
    return q.read


def _api_ops():
    return [o for o in contract().ops if o.method == "GET"]


def _explore(app, op, ints):
    """What ``op`` reads on any query: none given, each parameter (the file's and the ones read) alone, every
    pair of them (a handler can read one only when another is given: ``since_s`` with ``body``), and all
    of them. Returns (keys read, {key: the first query it was read on}, _int calls)."""
    ints.calls.clear()
    params = _query_params(op)
    value = lambda n: _sample(params[n]) if n in params else "1"  # noqa: E731
    contexts: dict[str, dict] = {}

    def ask(q):
        for k in _ask(app, op, q):
            contexts.setdefault(k, q)

    ask({})
    names = sorted(set(params) | set(contexts))
    for i, n in enumerate(names):
        ask({n: value(n)})
        for m in names[i + 1:]:
            ask({n: value(n), m: value(m)})
    ask({n: value(n) for n in names})
    return set(contexts), contexts, list(ints.calls)


@pytest.fixture
def loaded_app(app, fixture_server):
    return app


def test_each_endpoint_reads_the_query_parameters_the_file_lists(loaded_app, monkeypatch):
    ints = Ints(monkeypatch)
    wrong = []
    for op in _api_ops():
        read, _, _ = _explore(loaded_app, op, ints)
        listed = set(_query_params(op))
        for n in sorted(read - listed):
            wrong.append(f"{op.key} reads {n!r}, the file does not list it")
        for n in sorted(listed - read):
            wrong.append(f"{op.key}: the file lists {n!r}, the server never reads it")
    assert not wrong, "\n".join(wrong)


def _bounds(app, op, ints):
    """{name: (lo, hi)} of each integer ``op`` parses, with the parameter given."""
    _, _, calls = _explore(app, op, ints)
    out = {}
    for name, _, lo, hi, given in calls:
        if given:
            assert out.setdefault(name, (lo, hi)) == (lo, hi), (op.key, name, "two different ranges")
    return out


def _defaults(app, op, ints, contexts):
    """{name: default} of each integer ``op`` parses, asked without it on a query it is read on."""
    out = {}
    for name, ctx in contexts.items():
        ints.calls.clear()
        _ask(app, op, {k: v for k, v in ctx.items() if k != name})
        for n, default, _, _, given in ints.calls:
            if n == name and not given:
                out[name] = default
    return out


def test_integer_parameters_have_the_servers_minimum_and_maximum(loaded_app, monkeypatch):
    ints = Ints(monkeypatch)
    wrong = []
    for op in _api_ops():
        params = _query_params(op)
        bounds = _bounds(loaded_app, op, ints)
        for name, (lo, hi) in sorted(bounds.items()):
            s = params.get(name, {}).get("schema", {})
            if s.get("type") != "integer":
                wrong.append(f"{op.key} {name}: the server reads an integer, the file says {s.get('type')!r}")
            if (s.get("minimum"), s.get("maximum")) != (lo, hi):
                wrong.append(f"{op.key} {name}: the server takes {lo}..{hi}, "
                             f"the file says {s.get('minimum')}..{s.get('maximum')}")
        for name, p in sorted(params.items()):
            if p.get("schema", {}).get("type") == "integer" and name not in bounds:
                wrong.append(f"{op.key} {name}: an integer in the file, never parsed as one by the server")
    assert not wrong, "\n".join(wrong)


def test_integer_parameters_have_the_servers_default(db, monkeypatch, fixture_server):
    """A default that is the same for any clock and any configuration is the file's ``default``. One that
    depends on them (``to_ms`` is now; ``lambda_timeout_s`` and ``older_than_s`` are configured) has none in
    the file."""
    ints = Ints(monkeypatch)
    a = server.App(db, ui_dir=None, now=lambda: T0)
    b = server.App(db, ui_dir=None, now=lambda: T0 + 7 * 86_400 * SEC, stuck_after_s=301, lambda_timeout_s=901)
    wrong = []
    for op in _api_ops():
        params = _query_params(op)
        _, contexts, _ = _explore(a, op, ints)
        da, dbb = _defaults(a, op, ints, contexts), _defaults(b, op, ints, contexts)
        for name in sorted(set(da) | set(dbb)):
            fixed = da.get(name) == dbb.get(name)
            in_file = params.get(name, {}).get("schema", {}).get("default")
            if fixed and in_file != da[name]:
                wrong.append(f"{op.key} {name}: the server's default is {da[name]}, the file's {in_file}")
            if not fixed and in_file is not None:
                wrong.append(f"{op.key} {name}: the server's default depends on the clock or the configuration "
                             f"({da.get(name)}, {dbb.get(name)}), the file gives {in_file}")
    assert not wrong, "\n".join(wrong)


def test_integer_bounds_are_enforced_as_the_file_says(loaded_app, fixture_server, monkeypatch):
    """Over HTTP: minimum and maximum are accepted, one past either is a 400 that names the parameter. Each
    is asked on a query the endpoint reads the parameter on (``since_s`` of /api/messages needs ``body``)."""
    ints = Ints(monkeypatch)
    wrong = []
    for op in _api_ops():
        params = _query_params(op)
        _, contexts, _ = _explore(loaded_app, op, ints)
        for name, p in params.items():
            s = p.get("schema", {})
            if s.get("type") != "integer":
                continue
            ctx = {k: v for k, v in contexts[name].items() if k != name}
            for v, ok in ((s["minimum"], True), (s["maximum"], True), (s["minimum"] - 1, False), (s["maximum"] + 1, False)):
                q = {**ctx, name: v}
                if ok and name == "from_ms" and "to_ms" in params:
                    q["to_ms"] = v  # from_ms must not be after to_ms
                if ok and name == "to_ms" and "from_ms" in params:
                    q["from_ms"] = 0  # from_ms must not be after to_ms
                status, _, body = request(fixture_server, "GET", f"{_concrete(op.template)}?{urlencode(q)}")
                if ok and v == s["maximum"] and (op.key, name) in OVERFLOWS_AT_MAXIMUM:
                    if status != 500:
                        wrong.append(f"{op.key}?{urlencode(q)} -> {status}: no longer a 500, drop "
                                     f"{(op.key, name)} from OVERFLOWS_AT_MAXIMUM")
                elif ok and status != 200 or not ok and (status != 400 or name not in json.loads(body)["error"]):
                    wrong.append(f"{op.key}?{urlencode(q)} -> {status} {body[:100]!r}")
    assert not wrong, "\n".join(wrong)


def test_enum_parameters_take_each_value_the_file_lists_and_no_other(fixture_server):
    wrong, asked = [], 0
    for op in _api_ops():
        for name, p in _query_params(op).items():
            for v in p.get("schema", {}).get("enum", []):
                asked += 1
                status, _, _ = request(fixture_server, "GET", f"{_concrete(op.template)}?{urlencode({name: v})}")
                if status != 200:
                    wrong.append(f"{op.key} {name}={v} -> {status}")
            if "enum" in p.get("schema", {}):
                status, _, _ = request(fixture_server, "GET", f"{_concrete(op.template)}?{name}=not_a_value")
                if status != 400:
                    wrong.append(f"{op.key} {name}=not_a_value -> {status}")
    assert asked and not wrong, "\n".join(wrong)


# ---- 3. responses: every status the file lists, and every step, notice and state ------------------------

class _Broken(Exception):
    pass


def _raise(*a, **kw):
    raise _Broken("a test's broken store")


def test_every_status_the_file_lists_is_answered_and_matches_it(db, record_api_responses):
    """Each response is checked against the file by the recorder; here, that the file lists no status the
    server never gives."""
    ops = contract().ops
    with serve(server.App(db, ui_dir=None)) as base:
        load_fixture(base)
        for op in ops:
            path = _concrete(op.template)
            if op.method == "GET":
                request(base, "GET", path)
                request(base, "GET", _concrete(op.template).replace(quote(MID, safe=""), "no-such-message"))
                for name, p in _query_params(op).items():  # one past the minimum: 400
                    if "minimum" in p.get("schema", {}):
                        request(base, "GET", f"{path}?{name}={p['schema']['minimum'] - 1}")
                request(base, "GET", f"{path}?cursor=not-a-cursor")
        request(base, "GET", "/api/probes/short")  # under 8 characters: not a probe ID, 400
        # ingest: JSON, a rejected span, every error the handler has
        ok = to_json([Consumer().send(T0, "m-json")])
        request(base, "POST", "/v1/traces", json.dumps(ok).encode(), {"Content-Type": "application/json"})
        bad = to_json([Consumer().send(T0, "m-bad")])
        bad["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["attributes"][0]["value"] = {"intValue": "1"}
        request(base, "POST", "/v1/traces", json.dumps(bad).encode(), {"Content-Type": "application/json"})
        request(base, "POST", "/v1/traces", b"\xff\xff not protobuf", {"Content-Type": "application/x-protobuf"})
        request(base, "POST", "/v1/traces", b"x", {"Content-Encoding": "br"})
        request(base, "POST", "/v1/traces", b"x", {"Transfer-Encoding": "gzip"})
        request(base, "POST", "/v1/traces", None, {"Content-Length": str(server.MAX_BODY + 1)})

    # 500: every API endpoint's handler fails, and the store fails under ingest
    broken = server.App(db, ui_dir=None)
    for k in broken.routes:
        if k != "health":  # it answers while the store is down; the file gives it no 500
            broken.routes[k] = _raise
    with pytest.MonkeyPatch.context() as mp, serve(broken) as base:
        mp.setattr(db, "insert", _raise)
        for op in ops:
            if op.method == "GET":
                request(base, "GET", _concrete(op.template))
        request(base, "POST", "/v1/traces", to_proto([Consumer().send(T0, "m")]).SerializeToString(),
                {"Content-Type": "application/x-protobuf"})

    # 503: the store is unreachable
    def unavailable(*a, **kw):
        raise StoreUnavailable("a test's unreachable store")

    def busy(*a, **kw):
        raise WriterBusy()

    export_proto = to_proto([Consumer().send(T0, "m")]).SerializeToString()
    export_json = json.dumps(to_json([Consumer().send(T0, "m")])).encode()

    def both_encodings(base):
        request(base, "POST", "/v1/traces", export_proto, {"Content-Type": "application/x-protobuf"})
        request(base, "POST", "/v1/traces", export_json, {"Content-Type": "application/json"})

    with pytest.MonkeyPatch.context() as mp, serve(server.App(db, ui_dir=None)) as base:
        mp.setattr(db, "insert", unavailable)
        mp.setattr(db, "ping", unavailable)
        request(base, "GET", "/api/ready")
        request(base, "GET", "/api/health")
        both_encodings(base)

    # 501: a store without the queue events (QV-363: Postgres, until stage 3)
    def not_yet(*a, **kw):
        raise NotImplementedError("not on this store yet")

    with pytest.MonkeyPatch.context() as mp, serve(server.App(db, ui_dir=None)) as base:
        mp.setattr(db, "queue_flows", not_yet)
        mp.setattr(db, "queue_keys", not_yet)
        request(base, "GET", "/api/flows")
        request(base, "GET", "/api/queues/orders/series")

    # 503: the writer is behind (backpressure.py)
    with pytest.MonkeyPatch.context() as mp, serve(server.App(db, ui_dir=None)) as base:
        mp.setattr(db, "insert", busy)
        both_encodings(base)

    # 429: the queue is full (every place taken), with each Retry-After the file allows: the recent write
    # time, rounded up, from 1 s to the most (3 s)
    for write_s in (0, 1.5, 10):
        full = server.App(db, ui_dir=None, gate=IngestGate(max_pending=1))
        if write_s:
            full.gate.wrote(write_s)
        full.gate.pending = 1
        with serve(full) as base:
            both_encodings(base)

    # 401: a server with an access token, asked without it (health and ready still answer, as the file says)
    with serve(server.App(db, ui_dir=None, auth=auth_mod.Auth(TOKEN))) as base:
        for op in ops:
            if op.method == "GET":
                request(base, "GET", _concrete(op.template))
                request(base, "GET", _concrete(op.template), headers={"Authorization": f"Bearer {TOKEN}"})
        request(base, "POST", "/v1/traces", b"", {"Content-Type": "application/x-protobuf"})
        request(base, "POST", "/v1/traces", b"", {"Content-Type": "application/x-protobuf",
                                                  "Authorization": "Bearer wrong"})

    _people_and_tokens(db)  # it creates an admin, which changes what the token above opens; before the proxy block, which adds one too

    # 403: signed in through a proxy (proxy_auth.py) as someone who may not be: last, as it adds an admin to the store
    proxied = server.App(db, ui_dir=None, trusted_proxies=TrustedProxies(parse_networks("127.0.0.1")),
                         proxy_auth=proxy_auth.configure("X-Forwarded-Email",
                                                         proxies=TrustedProxies(parse_networks("127.0.0.1"))))
    proxied.accounts.create_user("root@example.com", "Root", "admin", None, 0)
    proxied.access.state.changed()
    with serve(proxied) as base:
        for op in ops:
            if op.method == "GET":
                request(base, "GET", _concrete(op.template), headers={"X-Forwarded-Email": "root@example.com"})

    assert not record_api_responses.problems, "\n".join(record_api_responses.problems)
    answered = {(s.op.key, s.status) for s in record_api_responses.seen}
    never = [f"{op.key} {status}" for op in ops for status in op.op["responses"]
             if (op.key, int(status)) not in answered]
    assert not never, "the file lists these statuses, the server never gave them:\n" + "\n".join(never)


def _people_and_tokens(db):
    """Every status of the people and token routes (people_api.py), on a server with an admin, a member and
    TRAILBEAM_TOKEN; the first ingest token's 409 on one without the token."""
    from trailbeam.server import access as access_mod
    from trailbeam.server.accounts import Accounts
    from trailbeam.server.tokens import Tokens

    acc = Accounts(db)
    j = {"Content-Type": "application/json"}

    def body(o):
        return json.dumps(o).encode()

    # the first ingest token closes open ingest: 409 without "close_open_ingest" (no TRAILBEAM_TOKEN here)
    admin = acc.create_first_admin("ada@example.com", "Ada", "x", 1)
    member = acc.create_user("ben@example.com", "Ben", "member", "x", 1)
    target = acc.create_user("dee@example.com", "Dee", "member", "x", 1)  # the {id} of the loops (DELETE removes her)
    t = Tokens(acc)
    a = {"Authorization": "Bearer " + t.create_personal(admin, "a")[1]}
    m = {"Authorization": "Bearer " + t.create_personal(member, "m")[1]}
    if not acc.auth_state()[1]:
        with serve(server.App(db, ui_dir=None)) as base:
            assert request(base, "POST", "/api/tokens/ingest", body({"name": "w"}), {**j, **a})[0] == 409

    app = server.App(db, ui_dir=None, auth=auth_mod.Auth(TOKEN))
    with serve(app) as base:
        def r(method, path, data=None, headers=None):
            return request(base, method, path, data, headers)

        people = [op for op in contract().ops if _people_op(op)]
        for op in people:
            path = re.sub(r"\{[^}]+\}", str(target), op.template)
            r(op.method, path, b"{}", j)  # 401
            r(op.method, path, b"{}", {**j, "Authorization": f"Bearer {TOKEN}"})  # 401: people only now
            r(op.method, path, b"{}", {**j, **m})  # 403 on the admin routes
            if op.method != "GET":
                r(op.method, path, b"{}", {**j, **a, "Sec-Fetch-Site": "cross-site"})  # 403: csrf
                r(op.method, path, None, {**a, "Content-Length": "99999"})  # 413
                r(op.method, path, b"[1]", {**a, "Content-Type": "text/plain"})  # 415 (or ignored: DELETE)
                r(op.method, path, b"[1]", {**j, **a})  # 400 (or ignored: DELETE)
            r(op.method, re.sub(r"\{[^}]+\}", "999", op.template), b"{}", {**j, **a})  # 404 with an ID
        # people
        assert r("GET", "/api/users", None, a)[0] == 200
        assert r("POST", "/api/users", body({"email": "nope"}), {**j, **a})[0] == 400
        st, _, out = r("POST", "/api/users", body({"email": "cy@example.com"}), {**j, **a})
        assert st == 201, out
        cy = json.loads(out)["user"]["id"]
        assert r("POST", "/api/users", body({"email": "CY@example.com"}), {**j, **a})[0] == 409
        assert r("PATCH", f"/api/users/{admin}", body({"role": "member"}), {**j, **a})[0] == 409
        assert r("DELETE", f"/api/users/{admin}", None, a)[0] == 409
        assert r("POST", f"/api/users/{cy}/link", b"{}", {**j, **a})[0] == 201
        assert r("PATCH", f"/api/users/{cy}", body({"disabled": True}), {**j, **a})[0] == 200
        assert r("POST", f"/api/users/{cy}/link", b"{}", {**j, **a})[0] == 409
        assert r("DELETE", f"/api/users/{cy}", None, a)[0] == 200
        # ingest tokens
        assert r("GET", "/api/tokens/ingest", None, a)[0] == 200
        st, _, out = r("POST", "/api/tokens/ingest", body({"name": "orders"}), {**j, **a})
        assert st == 201, out
        assert r("DELETE", f"/api/tokens/ingest/{json.loads(out)['token']['id']}", None, a)[0] == 200
        # your personal tokens
        assert r("GET", "/api/me/tokens", None, m)[0] == 200
        st, _, out = r("POST", "/api/me/tokens", body({"name": "ci", "expires_in_days": 30}), {**j, **m})
        assert st == 201, out
        assert r("DELETE", f"/api/me/tokens/{json.loads(out)['token']['id']}", None, m)[0] == 200
        # 500: a handler fails; 503: the store can't say who this is
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(app.people, "call", _raise)
            for op in people:
                r(op.method, re.sub(r"\{[^}]+\}", str(target), op.template), b"{}", {**j, **a})

        def unreachable(*a_, **kw):
            raise access_mod.AccessUnavailable("a test's unreachable store")

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(app.access, "check", unreachable)
            for op in people:
                if op.method != "GET":
                    r(op.method, re.sub(r"\{[^}]+\}", str(target), op.template), b"{}", {**j, **a})


def _scenario_spans():
    """Spans for each step type, notice and state no fixture has (the cases are those of test_timeline.py)."""
    p, c, fn = Consumer("producer", service="orders-api"), Consumer(), Consumer("env-1", source="lambda")
    dlq = Consumer("dlq-worker", queue="orders-dlq")
    wrong_key = Consumer("w", key="another-key-000000")
    dflt = Consumer("d", key="trailbeam-default-receipt-hash-key")
    proc = Consumer(scope="proc-1")
    ms = 1_000_000
    return [
        # deliveries_not_observed: the receive count skips 1, 2 and 4
        c.send(T0, "gap"), c.receive(T0 + 100 * SEC, "gap", "rh-g3", count=3),
        c.receive(T0 + 200 * SEC, "gap", "rh-g5", count=5), c.delete(T0 + 201 * SEC, "rh-g5"),
        # deleted_by_lambda, deleted_inferred
        p.send(T0, "lam-ok"), fn.receive(T0 + SEC, "lam-ok", "rh-l1", lambda_inv="inv-1"),
        fn.outcome(T0 + 2 * SEC, "lam-ok", "success", "inv-1"),
        # processing (within the Lambda timeout), lambda_invocation_ended_without_outcome (after it)
        p.send(T0, "lam-open"), fn.receive(T0 + SEC, "lam-open", "rh-l2", lambda_inv="inv-2"),
        # order_conflict: the host clock jumped back between two calls of one process
        proc.receive(T0 + 5 * SEC, "conflict", "rh-c1", start=T0 + 5 * SEC - ms, order=(1, 2)),
        proc.delete(T0 + 4 * SEC, "rh-c1", start=T0 + 4 * SEC - ms, order=(3, 4)),
        # key_mismatch, and a delete whose status is unmatched_handle
        c.send(T0, "mismatch"), c.receive(T0, "mismatch", "rh-k"), wrong_key.delete(T0 + SEC, "rh-k", mid="mismatch"),
        # unmatched_handle notice: same key, a handle no receive has
        c.send(T0, "unmatched"), c.receive(T0, "unmatched", "rh-u"), c.delete(T0 + SEC, "rh-elsewhere", mid="unmatched"),
        # default_key, send_not_observed
        dflt.receive(T0, "dflt", "rh-d"), dflt.delete(T0 + SEC, "rh-d"),
        # dlq_move_not_seen: receive count 3 = maxReceiveCount of orders, no DLQ receive
        p.send(T0, "poison"), *[c.receive(T0 + (1 + 31 * i) * SEC, "poison", f"rh-p{i}", count=i + 1) for i in range(3)],
        # moved_to_dead_letter_queue, visibility_timeout_expired, stale_handle, failed
        p.send(T0, "moved"), *[c.receive(T0 + (1 + 31 * i) * SEC, "moved", f"rh-m{i}", count=i + 1) for i in range(3)],
        c.delete(T0 + 95 * SEC, "rh-m0"), c.delete(T0 + 96 * SEC, "rh-m1", ok=False),
        dlq.receive(T0 + 200 * SEC, "moved", "rh-dlq", count=4, dlq_source="orders"),
        dlq.delete(T0 + 201 * SEC, "rh-dlq"),
        # visibility_changed
        p.send(T0, "vis"), c.receive(T0 + SEC, "vis", "rh-v"), c.change_visibility(T0 + 2 * SEC, "rh-v", 30),
        # purged and queue_purged (removal per_sqs_documentation, then possible): on a queue of their own
        p.send(T0, "purged", queue="scratch"), p.purge(T0 + 10 * SEC, queue="scratch", start=T0 + 9 * SEC),
        p.send(T0 + 30 * SEC, "maybe-purged", queue="scratch"),
        # published (joined by envelope and by publish ID), arrived_via_sns (no publish recorded): contract 21.5
        p.publish(T0 + SEC, "sns-1", publish_id="pid-1", start=T0),
        c.sns_receive(T0 + 3 * SEC, "via-sns", "rh-s1", sns_id="sns-1", sns_ts_ms=T0 // ms + 1000,
                      sent_ms=T0 // ms + 1040, start=T0 + 2 * SEC),
        c.sns_receive(T0 + 3 * SEC, "via-raw", "rh-s2", publish_id="pid-1", start=T0 + 2 * SEC),
        c.sns_receive(T0 + 3 * SEC, "via-unknown", "rh-s3", sns_id="sns-unrecorded", start=T0 + 2 * SEC),
        # arrived_via_eventbridge and s3_event (inferred from the body's shape): contract 23.3
        c.receive_with(T0 + 3 * SEC, "via-eventbridge", "rh-e1", {
            "trailbeam.eventbridge.event_id": "6a7e8feb-b491-4cf7-a9f1-bf3703467718",
            "trailbeam.eventbridge.source": "aws.ec2", "trailbeam.eventbridge.detail_type": "EC2 Instance State-change Notification",
            "trailbeam.eventbridge.account": "111122223333", "trailbeam.eventbridge.region": "us-west-1",
            "trailbeam.eventbridge.time_ms": T0 // ms}, sent_ms=T0 // ms + 300, start=T0 + 2 * SEC),
        c.receive_with(T0 + 3 * SEC, "via-s3", "rh-e2", {
            "trailbeam.s3.event_name": "ObjectCreated:Put", "trailbeam.s3.bucket": "amzn-s3-demo-bucket",
            "trailbeam.s3.region": "us-west-2", "trailbeam.s3.event_time_ms": T0 // ms,
            "trailbeam.s3.configuration_id": "testConfigRule", "trailbeam.s3.sequencer": "0055AED6DCD90281E5",
            "trailbeam.s3.object_key": "red+flower.jpg", "trailbeam.s3.record_count": 2},
            sent_ms=T0 // ms + 900, start=T0 + 2 * SEC),
        # put_event: a recorded PutEvents entry joined by the event ID (contract 25.3), and a failed one
        p.put_event(T0 + SEC, "11710aed-b79e-4468-a20b-bb3c0c3b4860", bus="orders-bus", time_given=True, start=T0),
        p.put_event(T0 + SEC, error="ThrottlingException", index=1, start=T0),
        c.eb_receive(T0 + 3 * SEC, "via-put-events", "rh-e3", "11710aed-b79e-4468-a20b-bb3c0c3b4860",
                     time_ms=T0 // ms, sent_ms=T0 // ms + 400, start=T0 + 2 * SEC),
        # s3_upload: a recorded PutObject joined by bucket, key and event time (contract 27.3), and a failed one. The
        # keys are stored only with the server's opt-in for the bucket and the queue (QV-564; the test opts them in)
        p.s3_upload(T0 + SEC, bucket="photos", key="red flower.jpg", version_id="v1", start=T0),
        p.s3_upload(T0 + SEC, bucket="photos", error="AccessDenied", start=T0),
        Consumer("thumbs", queue="photo-events").s3_receive(T0 + 3 * SEC, "via-s3-upload", "rh-s3", bucket="photos",
                                                              key="red+flower.jpg",
                     etag="9dd4e461268c8034f5c8564e155c67a6", version_id="v1",
                     event_time_ms=T0 // ms + 800, sent_ms=T0 // ms + 1000, start=T0 + 2 * SEC),
    ]


def _enum(c: Contract, name, *path):
    node = c.spec["components"]["schemas"][name]
    for k in path:
        node = node[k]
    return set(node)


def test_every_step_type_notice_and_state_in_the_file_is_answered(db, record_api_responses):
    """The file's lists of step types, notice codes, states and delete statuses are exactly the ones the
    server gives, each in a response the file validates."""
    c = contract()
    now = T0 + 120 * SEC
    app = server.App(db, ui_dir=None, now=lambda: now, max_receive_counts={"orders": 3})
    db.store_bodies = frozenset({"photos", "photo-events"})  # the S3 keys the s3_upload join needs (QV-564)
    with serve(app) as base:
        load_fixture(base)
        status, _, _ = request(base, "POST", "/v1/traces", to_proto(_scenario_spans()).SerializeToString(),
                               {"Content-Type": "application/x-protobuf"})
        assert status == 200
        sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "conformance"))
        from test_amqp_conformance import FIXTURE, otlp_json  # RabbitMQ messages: their steps and notices
        # x_death_not_a_move (QV-527): m-stuck delivered again on another queue with an x-death naming orders,
        # whose delivery of it is still held
        held = next(sp for sp in FIXTURE["spans"] if sp["scenario"] == "stuck"
                    and sp["attributes"]["trailbeam.kind"] == "amqp.deliver")
        forged = dict(held, start_time_unix_nano=held["start_time_unix_nano"] + SEC,
                      end_time_unix_nano=held["end_time_unix_nano"] + SEC,
                      attributes=dict(held["attributes"], **{
                          "messaging.destination.name": "attacker-q", "rabbitmq.message.delivery_tag": 9001,
                          "trailbeam.order.returned": held["attributes"]["trailbeam.order.returned"] + 1000,
                          "trailbeam.dead_letter.source_queue": "orders", "trailbeam.dead_letter.reason": "rejected",
                          "trailbeam.dead_letter.count": 1}))
        status, _, _ = request(base, "POST", "/v1/traces", otlp_json(FIXTURE["spans"] + [forged]),
                               {"Content-Type": "application/json"})
        assert status == 200
        _, _, body = request(base, "GET", "/api/messages?limit=1000")
        mids = [m["message_id"] for m in json.loads(body)["messages"]]
        for mid in mids:
            for timeout in (900, 60):  # lam-open: processing at 900 s, ended without outcome at 60 s
                request(base, "GET", f"/api/messages/{quote(mid, safe='')}?lambda_timeout_s={timeout}")
        for path in ("/api/duplicates", "/api/stuck?older_than_s=0", "/api/tenants", "/api/stats", "/api/health"):
            request(base, "GET", path)

    assert not record_api_responses.problems, "\n".join(record_api_responses.problems)
    timelines = [s.value for s in record_api_responses.seen if s.op.key == "GET /api/messages/{message_id}"
                 and s.status == 200]
    events = [e for t in timelines for e in t["events"]]
    seen = {
        "step types": {e["type"] for e in events},
        "notice codes": {n["code"] for t in timelines for n in t["notices"]},
        "states": {t["state"] for t in timelines},
        "delete statuses": {e["status"] for e in events if e["type"] == "deleted"},
    }
    in_file = {
        "step types": set(c.spec["components"]["schemas"]["Step"]["discriminator"]["mapping"]),
        "notice codes": _enum(c, "Notice", "properties", "code", "enum"),
        "states": _enum(c, "State", "enum"),
        "delete statuses": _enum(c, "DeletedStep", "properties", "status", "enum"),
    }
    for what in seen:
        assert seen[what] == in_file[what], (what, "never answered:", in_file[what] - seen[what],
                                             "not in the file:", seen[what] - in_file[what])
