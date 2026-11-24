"""contract/openapi.yaml, as a checker of the server's responses.

``Contract`` loads the file and validates one HTTP response against what the file says the operation answers
for that status: the status is listed, the content type is listed, a JSON body validates against the schema
(JSON Schema 2020-12, as OpenAPI 3.1 has it), and the response headers the file names are sent.

``Recorder`` collects the responses a test makes the server give and checks each. conftest.py installs one for
every server test (``record_api_responses``), so every case in tests/server/ that reaches the API is also a
check of the file; test_openapi.py holds the checks that are about the file itself.
"""
from __future__ import annotations

import contextlib
import json
import re
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator, Optional
from urllib.parse import quote, urlsplit

import yaml
from jsonschema import Draft202012Validator
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT202012

SPEC_PATH = Path(__file__).resolve().parents[3] / "contract" / "openapi.yaml"
BASE = "urn:trailbeam:openapi"  # the file's URI in the schema registry; pointers are into the whole file
METHODS = ("get", "put", "post", "delete", "options", "head", "patch", "trace")


def pointer(*parts) -> str:
    """A JSON pointer from raw keys (RFC 6901 escaping)."""
    return "".join("/" + str(p).replace("~", "~0").replace("/", "~1") for p in parts)


@dataclass
class Operation:
    method: str  # upper case
    template: str  # as in the file, e.g. /api/messages/{message_id}
    op: dict
    ptr: str  # JSON pointer of the operation object
    regex: re.Pattern = field(init=False)

    def __post_init__(self):
        segs = []
        for s in self.template.split("/"):
            segs.append("[^/]+" if s.startswith("{") and s.endswith("}") else re.escape(s))
        self.regex = re.compile("^" + "/".join(segs) + "$")

    @property
    def key(self) -> str:
        return f"{self.method} {self.template}"

    @property
    def n_params(self) -> int:
        return self.template.count("{")


class Contract:
    def __init__(self, path: Path = SPEC_PATH):
        self.path = path
        self.spec = yaml.safe_load(path.read_text())
        self.registry = Registry().with_resource(BASE, Resource.from_contents(self.spec, default_specification=DRAFT202012))
        self.ops: list[Operation] = []
        for template, item in self.spec["paths"].items():
            for m in METHODS:
                if m in item:
                    self.ops.append(Operation(m.upper(), template, item[m], pointer("paths", template, m)))
        # literal segments win over a parameter (/api/tenants/search is not /api/tenants/{x})
        self.ops.sort(key=lambda o: o.n_params)
        self._validators: dict[str, Draft202012Validator] = {}
        self._lock = threading.Lock()

    # ---- the file --------------------------------------------------------------------------------

    def resolve(self, obj: dict) -> tuple[dict, Optional[str]]:
        """A response, parameter or header object's ``$ref`` within the file, followed: (the object, its
        pointer). (obj, None) for an inline one. Not for schemas: the validator follows those."""
        ptr = None
        while isinstance(obj, dict) and "$ref" in obj:
            ref = obj["$ref"]
            assert ref.startswith("#/"), ref
            ptr = ref[1:]
            obj = self.at(ptr)
        return obj, ptr

    def at(self, ptr: str):
        node = self.spec
        for raw in ptr.split("/")[1:]:
            key = raw.replace("~1", "/").replace("~0", "~")
            node = node[int(key)] if isinstance(node, list) else node[key]
        return node

    def validator(self, ptr: str) -> Draft202012Validator:
        with self._lock:
            v = self._validators.get(ptr)
            if v is None:
                v = Draft202012Validator({"$ref": f"{BASE}#{ptr}"}, registry=self.registry)
                self._validators[ptr] = v
            return v

    def errors(self, ptr: str, value) -> list[str]:
        """Where ``value`` does not validate against the schema at ``ptr``, most specific first."""
        errs = sorted(self.validator(ptr).iter_errors(value), key=lambda e: -len(e.absolute_path))
        out = []
        for e in errs[:5]:
            where = "/".join(str(p) for p in e.absolute_path) or "(top)"
            out.append(f"at {where}: {_leaf(e)}")
        return out

    def operation(self, method: str, raw_path: str) -> Optional[Operation]:
        for o in self.ops:
            if o.method == method.upper() and o.regex.match(raw_path):
                return o
        return None

    def response(self, op: Operation, status: int) -> tuple[Optional[dict], Optional[str]]:
        """The response object the file gives ``op`` for ``status``, and its pointer."""
        responses = op.op.get("responses", {})
        for key in (str(status), f"{str(status)[0]}XX", "default"):
            if key in responses:
                r = responses[key]
                obj, ptr = self.resolve(r)
                return obj, ptr or op.ptr + pointer("responses", key)
        return None, None

    def parameters(self, op: Operation) -> list[dict]:
        """The operation's parameters, path-level ones included, with ``$ref`` followed."""
        item = self.spec["paths"][op.template]
        out = {}
        for p in list(item.get("parameters", [])) + list(op.op.get("parameters", [])):
            p, _ = self.resolve(p)
            out[(p["name"], p["in"])] = p
        return list(out.values())

    # ---- a response ----------------------------------------------------------------------------

    def check(self, method: str, raw_path: str, status: int, content_type: str, headers: dict,
              body) -> tuple[Optional[Operation], list[str]]:
        """(the operation, what in this response the file does not allow). ``body`` is bytes, or for an
        answer taken from App directly, the object it returned. (None, []) for a path that is not a route
        the file has and the server did not serve either (a 404)."""
        op = self.operation(method, raw_path)
        if op is None:
            if status == 404:
                return None, []
            return None, [f"{method} {raw_path} answered {status}, but contract/openapi.yaml has no such operation"]
        r, rptr = self.response(op, status)
        if r is None:
            return op, [f"{op.key}: the server answered {status}, which the file does not list "
                        f"(it lists {sorted(op.op.get('responses', {}))})"]
        problems = []
        lower = {k.lower(): v for k, v in (headers or {}).items()}
        for name, h in (r.get("headers") or {}).items():
            h, hptr = self.resolve(h)
            if name.lower() not in lower:
                problems.append(f"{op.key} {status}: the file names the header {name}, the server did not send it")
            elif "schema" in h:
                hptr = (hptr or rptr + pointer("headers", name)) + "/schema"
                problems += [f"{op.key} {status}: header {name} {e}" for e in self.errors(hptr, lower[name.lower()])]
        content = r.get("content")
        ctype = (content_type or "").split(";")[0].strip().lower()
        if not content:
            if body not in (b"", None):
                problems.append(f"{op.key} {status}: the file gives no content, the server sent {ctype}")
            return op, problems
        if ctype not in content:
            return op, problems + [f"{op.key} {status}: the server answered {ctype!r}, the file lists {sorted(content)}"]
        media = content[ctype]
        if ctype == "application/json":
            if isinstance(body, (bytes, bytearray)):
                try:
                    value = json.loads(body)
                except ValueError:
                    return op, problems + [f"{op.key} {status}: the body is not JSON"]
            else:
                value = json.loads(json.dumps(body))  # as it goes over the wire: tuples are lists
            if "schema" in media:
                sptr = rptr + pointer("content", ctype, "schema")
                problems += [f"{op.key} {status}: {e}" for e in self.errors(sptr, value)]
        return op, problems


def _leaf(e) -> str:
    """For a oneOf/anyOf that failed, the reason from the branch the value was meant for, when that is clearer
    than "is not valid under any": the branch with no failed ``const``/``enum`` (a step's ``type``, say), then
    the one with the fewest errors."""
    if e.validator in ("oneOf", "anyOf") and e.context:
        branches: dict = {}
        for c in e.context:
            branches.setdefault(c.schema_path[0], []).append(c)

        def score(errs):
            return (any(c.validator in ("const", "enum") for c in errs), len(errs))

        errs = min(branches.values(), key=score)
        best = max(errs, key=lambda c: len(c.absolute_path))
        return _leaf(best)
    return e.message[:400]


_contract: Optional[Contract] = None


def contract() -> Contract:
    global _contract
    if _contract is None:
        _contract = Contract()
    return _contract


@dataclass
class Seen:
    op: Operation
    status: int
    path: str
    query: str
    value: object  # the JSON body, parsed; None for one that is not JSON


class Recorder:
    """Every API and ingest response a test made the server give: over HTTP (``make_server``), from
    ``App.api`` called directly, and timelines from ``queries.message`` called directly."""

    def __init__(self, c: Optional[Contract] = None):
        self.contract = c or contract()
        self.problems: list[str] = []
        self.seen: list[Seen] = []
        self._lock = threading.Lock()
        self._inside = threading.local()  # one response is recorded at the outermost layer only

    @contextlib.contextmanager
    def outermost(self) -> Iterator[bool]:
        was = getattr(self._inside, "on", False)
        self._inside.on = True
        try:
            yield not was
        finally:
            self._inside.on = was

    def record(self, method: str, raw_url: str, status: int, content_type: str, headers: dict, body):
        u = urlsplit(raw_url)
        if not (u.path.startswith("/api/") or u.path == "/v1/traces"):
            return  # the web UI's pages and /login: not in the file
        op, problems = self.contract.check(method, u.path, status, content_type, headers, body)
        value = None
        if (content_type or "").startswith("application/json"):
            try:
                value = json.loads(body) if isinstance(body, (bytes, bytearray)) else json.loads(json.dumps(body))
            except ValueError:
                pass
        with self._lock:
            self.problems += [f"{p}\n    (request: {method} {raw_url})" for p in problems]
            if op is not None:
                self.seen.append(Seen(op, status, u.path, u.query, value))

    def install(self, monkeypatch):
        from trailbeam.server import queries, server

        rec = self
        orig_make_handler = server.make_handler

        def make_handler(app):
            base = orig_make_handler(app)

            class Recording(base):
                def _send(self, status, ctype, body, headers=None):
                    rec.record(self.command, self.path, status, ctype, headers or {}, body)
                    return super()._send(status, ctype, body, headers)

                def do_GET(self):
                    with rec.outermost():
                        super().do_GET()

                def do_POST(self):
                    with rec.outermost():
                        super().do_POST()

                def do_PATCH(self):
                    with rec.outermost():
                        super().do_PATCH()

                def do_DELETE(self):
                    with rec.outermost():
                        super().do_DELETE()

            Recording.__name__ = base.__name__
            return Recording

        orig_api = server.App.api

        def api(app, path, query, *who):
            with rec.outermost() as outer:
                if not outer:
                    return orig_api(app, path, query, *who)
                try:
                    out = orig_api(app, path, query, *who)
                except server.ApiError as e:
                    rec.record("GET", "/api/" + path, e.status, "application/json", {}, {"error": e.message})
                    raise
                rec.record("GET", "/api/" + path, 200, "application/json", {}, out)
                return out

        orig_message = queries.message

        def message(db, message_id, *a, **kw):
            with rec.outermost() as outer:
                out = orig_message(db, message_id, *a, **kw)
                if outer and out is not None:
                    rec.record("GET", "/api/messages/" + quote(message_id, safe=""), 200, "application/json", {}, out)
                return out

        monkeypatch.setattr(server, "make_handler", make_handler)
        monkeypatch.setattr(server.App, "api", api)
        monkeypatch.setattr(queries, "message", message)
