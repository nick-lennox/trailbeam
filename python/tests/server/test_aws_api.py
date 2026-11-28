"""/api/settings/aws (aws_api.py, QV-589) through the real handler: admins only, a change saved and answered, the
settings kept by the store across a restart, /api/health's config.aws and trailbeam doctor's line from it, and the
promise that a server with AWS reads off makes no AWS call: it never imports boto3 at all."""
from __future__ import annotations

import contextlib
import http.client
import json
import subprocess
import sys
import textwrap
import threading
from urllib.parse import urlsplit

import pytest
from trailbeam.server import aws_api, aws_reader, doctor, server
from trailbeam.server.accounts import Accounts
from trailbeam.server.db import Database
from trailbeam.server.tokens import Tokens

PATH = "/api/settings/aws"
J = {"Content-Type": "application/json"}


@contextlib.contextmanager
def serve(app):
    httpd = server.ThreadingHTTPServer(("127.0.0.1", 0), server.make_handler(app))
    httpd.daemon_threads = True
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    try:
        yield f"http://127.0.0.1:{httpd.server_address[1]}"
    finally:
        httpd.shutdown()
        httpd.server_close()


def call(base, method, path=PATH, body=None, headers=None):
    u = urlsplit(base)
    c = http.client.HTTPConnection(u.hostname, u.port, timeout=10)
    try:
        data = json.dumps(body).encode() if body is not None else None
        c.request(method, path, body=data, headers={**(J if data is not None else {}), **(headers or {})})
        r = c.getresponse()
        raw = r.read()
        return r.status, json.loads(raw) if raw else None
    finally:
        c.close()


def no_lookup(region):
    raise AssertionError("credentials looked up")


def test_before_accounts_anyone_may_turn_reads_on_and_they_outlive_a_restart(tmp_path):
    path = str(tmp_path / "trailbeam.sqlite")
    db = Database(path)
    app = server.App(db, ui_dir=None)
    app.aws.status.source = no_lookup  # reads off: never asked
    with serve(app) as base:
        status, got = call(base, "GET")
        assert status == 200 and got["settings"]["enabled"] is False and got["status"]["on"] is False
        assert got["status"]["credentials"] is None and got["status"]["problem"] is None
        h = call(base, "GET", "/api/health")[1]
        assert h["config"]["aws"]["on"] is False
        assert doctor.aws_reads(h).status == doctor.OK and "makes no AWS calls" in doctor.aws_reads(h).detail

        app.aws.status.source = lambda region: ("env", region or "eu-west-1")
        status, got = call(base, "PATCH", body={"enabled": True, "queues": "orders\nbilling-*\n", "region": "eu-west-1"})
        assert status == 200
        assert got["settings"] | {"updatedAt": None} == {"enabled": True, "queues": ["orders", "billing-*"],
                                                          "region": "eu-west-1", "accountId": None,
                                                          "updatedAt": None, "updatedBy": "none"}
        assert got["status"] == {"on": True, "queues": 2, "boto3": True, "credentials": "env", "region": "eu-west-1",
                                 "problem": None}
        h = call(base, "GET", "/api/health")[1]
        line = doctor.aws_reads(h)
        assert line.status == doctor.OK and line.detail == "on for 2 queue names in eu-west-1, with credentials from env"

        status, got = call(base, "PATCH", body={"queues": ["not a queue"]})
        assert status == 400 and got["error"].startswith("not a queue is not a queue name.")
        assert call(base, "POST", body={})[0] == 404
        assert call(base, "DELETE")[0] == 404
    db.close()
    db = Database(path)
    try:
        again = server.App(db, ui_dir=None)
        assert again.aws.settings.current()["queues"] == ["orders", "billing-*"] and again.aws.on()
    finally:
        db.close()


def test_with_accounts_only_an_admin_reads_or_changes_them():
    db = Database(":memory:")
    try:
        acc = Accounts(db)
        admin = acc.create_first_admin("ada@example.com", "Ada", "x", 1)
        member = acc.create_user("ben@example.com", "Ben", "member", "x", 1)
        t = Tokens(acc)
        a = {"Authorization": "Bearer " + t.create_personal(admin, "a")[1]}
        m = {"Authorization": "Bearer " + t.create_personal(member, "m")[1]}
        app = server.App(db, ui_dir=None)
        app.aws.status.source = no_lookup
        with serve(app) as base:
            assert call(base, "GET")[0] == 401
            assert call(base, "GET", headers=m) == (403, {"error": "this needs an admin"})
            assert call(base, "PATCH", body={"enabled": True}, headers=m)[0] == 403
            assert app.aws.settings.current()["enabled"] is False
            status, got = call(base, "PATCH", body={"enabled": True}, headers=a)
            assert status == 200 and got["settings"]["updatedBy"] == "Ada"
            assert got["status"]["on"] is False  # on, but no queue named: still off
    finally:
        db.close()


def test_a_save_drops_the_kept_reads():
    store = aws_api.ServerAws(aws_api.aws_settings.MemorySettings(), lambda who: True, now_ms=lambda: 5,
                              clients_for=lambda region: {}, installed=lambda: True, source=no_lookup)
    store.reader.cache.get("attrs|eu-west-1|u", 60_000, 0, lambda: {"visible": 1})
    assert len(store.reader.cache) == 1
    store.call("PATCH", PATH, {"enabled": False}, who=None)
    assert len(store.reader.cache) == 0


@pytest.mark.parametrize("aws, status, detail", [
    (None, doctor.SKIP, "this server is an older Trailbeam, without AWS reads"),
    ({"on": True, "queues": 1, "boto3": False, "credentials": None, "region": None,
      "problem": aws_reader.NO_BOTO3}, doctor.FAIL, "on for 1 queue name, but they can't work"),
    ({"on": True, "queues": 3, "boto3": True, "credentials": None, "region": "eu-west-1",
      "problem": aws_reader.NO_CREDENTIALS}, doctor.FAIL, "on for 3 queue names, but they can't work"),
    ({"on": True, "queues": 1, "boto3": True, "credentials": "iam-role", "region": None, "problem": None},
     doctor.OK, "on for 1 queue name, with credentials from iam-role"),
])
def test_the_doctor_line(aws, status, detail):
    r = doctor.aws_reads({"config": {"aws": aws} if aws else {}})
    assert (r.status, r.detail) == (status, detail)
    if status == doctor.FAIL:
        assert r.cause == aws["problem"]
        assert ("pip install" in r.fix) == (aws["boto3"] is False)
        assert "cloudwatch:GetMetricData" in r.fix or "pip install" in r.fix
    assert doctor.aws_reads(None).status == doctor.SKIP


def test_a_server_with_reads_off_never_imports_boto3(tmp_path):
    """A fresh process: start the app, answer the settings, health and a page read, and boto3 is not loaded."""
    code = textwrap.dedent(f"""
        import sys
        from trailbeam.server import server
        from trailbeam.server.db import Database
        db = Database({str(tmp_path / "t.sqlite")!r})
        app = server.App(db, ui_dir=None)
        app.api("health", {{}})
        app.aws.call("GET", "/api/settings/aws", None, None)
        app.aws.call("PATCH", "/api/settings/aws", {{"queues": ["orders"]}}, None)  # named, but not switched on
        out = app.aws.reader.read([{{"key": "orders", "name": "orders", "account": None, "region": None,
                                    "url": None}}], [], 0)
        assert out["orders"]["status"] == "not_configured", out
        assert "boto3" not in sys.modules and "botocore" not in sys.modules, "boto3 was imported"
        print("ok")
    """)
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=60)
    assert r.returncode == 0 and r.stdout.strip() == "ok", r.stderr
