"""Users and roles are a preview in 0.5.0, off unless the server runs with --accounts or TRAILBEAM_ACCOUNTS=1
(QV-349). Off, a server built from this version behaves as 0.4.0 does: the shared token alone, no setup link, no
/setup, and people and ingest tokens already in the store are ignored, with a warning. The admin's environment
variables and the users and tokens commands refuse to run, so the preview can't be switched on half-way."""
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request

import pytest

from trailbeam.server import access, first_admin
from trailbeam.server.accounts import Accounts
from trailbeam.server.db import Database
from trailbeam.server.tokens import Tokens

TOKEN = "shared-token-0123456789abcdef"


def _env(**extra):
    env = {k: v for k, v in os.environ.items() if not k.startswith(("TRAILBEAM_", "OTEL_"))}
    env.update(extra)
    return env


@pytest.mark.parametrize("value,on", [(None, False), ("", False), ("0", False), ("off", False), ("false", False),
                                      ("1", True), ("true", True), ("YES", True), (" on ", True)])
def test_the_switch_reads_on_and_off(value, on):
    assert access.switch_on({} if value is None else {access.SWITCH_ENV: value}) is on


def test_a_value_that_is_neither_stops_the_server(tmp_path):
    with pytest.raises(ValueError):
        access.switch_on({access.SWITCH_ENV: "maybe"})
    r = subprocess.run([sys.executable, "-m", "trailbeam.server", "--db", str(tmp_path / "t.sqlite"), "--port", "0"],
                       env=_env(TRAILBEAM_ACCOUNTS="maybe"), capture_output=True, text=True, timeout=60)
    assert r.returncode == 2 and "TRAILBEAM_ACCOUNTS='maybe': expected 1 (on) or 0 (off)" in r.stderr


def _store_with_an_admin_and_an_ingest_token(path):
    s = Database(str(path))
    try:
        Accounts(s).create_user("root@example.com", "Root", "admin", None, 1_800_000_000_000)
        return Tokens(Accounts(s)).create_ingest("ci")
    finally:
        s.close()


class Server:
    def __init__(self, db, *args, **env):
        self.p = subprocess.Popen([sys.executable, "-m", "trailbeam.server", "--db", str(db), "--port", "0", *args],
                                  env=_env(PYTHONUNBUFFERED="1", **env), stderr=subprocess.PIPE, text=True)
        self.log = []
        for line in self.p.stderr:
            self.log.append(line)
            if " retention " in line:  # after "listening on" and what it says about access
                break
        text = "".join(self.log)
        self.url = "http://" + re.search(r"listening on http://([0-9.]+:[0-9]+)", text).group(1)

    def get(self, path, headers=None, method="GET", body=None):
        req = urllib.request.Request(self.url + path, headers=headers or {}, method=method, data=body)
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                return r.status, r.read().decode()
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode()

    def stop(self):
        self.p.terminate()
        self.p.wait(10)


def test_off_the_shared_token_guards_everything_and_people_in_the_store_are_ignored(tmp_path):
    db = tmp_path / "t.sqlite"
    _store_with_an_admin_and_an_ingest_token(db)
    s = Server(db, TRAILBEAM_TOKEN=TOKEN)
    try:
        log = "".join(s.log)
        assert "the store holds an admin account and ingest tokens, ignored: users and roles are off" in log
        assert "access token set: ingest, the API and the web screen need it" in log
        assert "works only for ingest and /metrics" not in log
        bearer = {"Authorization": f"Bearer {TOKEN}"}
        status, body = s.get("/api/health", bearer)
        assert status == 200 and '"auth": "token"' in body.replace('":"', '": "')
        assert s.get("/api/messages", bearer)[0] == 200  # the shared token still opens the API, as in 0.4.0
        assert s.get("/api/messages")[0] == 401
        assert s.get("/setup")[0] == 404
        assert s.get("/api/me", bearer)[0] in (200, 404)
    finally:
        s.stop()


def test_off_an_ingest_token_in_the_store_does_not_close_ingest(tmp_path):
    db = tmp_path / "t.sqlite"
    _store_with_an_admin_and_an_ingest_token(db)
    s = Server(db)  # no TRAILBEAM_TOKEN: open, as in 0.4.0
    try:
        status, _ = s.get("/v1/traces", {"Content-Type": "application/x-protobuf"}, "POST", b"")
        assert status == 200
        assert s.get("/api/messages")[0] == 200
    finally:
        s.stop()


def test_on_the_same_store_switches_to_accounts(tmp_path):
    db = tmp_path / "t.sqlite"
    _store_with_an_admin_and_an_ingest_token(db)
    for args, env in ((["--accounts"], {}), ([], {"TRAILBEAM_ACCOUNTS": "1"})):
        s = Server(db, *args, TRAILBEAM_TOKEN=TOKEN, **env)
        try:
            assert "ignored" not in "".join(s.log)
            assert s.get("/api/messages", {"Authorization": f"Bearer {TOKEN}"})[0] == 401
        finally:
            s.stop()


def test_off_the_admin_from_the_environment_stops_the_server(tmp_path):
    pw = tmp_path / "pw"
    pw.write_text("correct horse battery\n")
    r = subprocess.run([sys.executable, "-m", "trailbeam.server", "--db", str(tmp_path / "t.sqlite"), "--port", "0"],
                       env=_env(**{first_admin.EMAIL_ENV: "root@example.com", first_admin.PASSWORD_FILE_ENV: str(pw)}),
                       capture_output=True, text=True, timeout=60)
    assert r.returncode == 2
    assert "TRAILBEAM_ADMIN_EMAIL is set, but users and roles are off on this server" in r.stderr


@pytest.mark.parametrize("argv", [["users", "list"], ["tokens", "list"],
                                  ["tokens", "create", "--name", "x", "--yes"]])
def test_off_the_users_and_tokens_commands_refuse(tmp_path, argv):
    db = tmp_path / "t.sqlite"
    Database(str(db)).close()
    r = subprocess.run([sys.executable, "-m", "trailbeam.server", *argv, "--db", str(db)], env=_env(),
                       capture_output=True, text=True, timeout=60)
    assert r.returncode == 2 and "users and roles are off on this server" in r.stderr and not r.stdout
    s = Database(str(db))
    try:
        assert Accounts(s).auth_state() == (False, False)  # nothing was made
    finally:
        s.close()
    r = subprocess.run([sys.executable, "-m", "trailbeam.server", *argv, "--db", str(db), "--accounts"], env=_env(),
                       capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stderr


def test_off_trailbeam_start_prints_no_setup_link(tmp_path):
    p = subprocess.Popen([sys.executable, "-m", "trailbeam.server", "start", "--data-dir", str(tmp_path / "data"),
                          "--port", "0"], env=_env(TRAILBEAM_START_TERMINAL="1"), stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, text=True)
    out = []
    for line in p.stdout:
        out.append(line)
        if "Stop with Ctrl-C" in line:
            break
    p.terminate()
    _, err = p.communicate(timeout=20)
    text = "".join(out) + err
    assert "/setup#code=" not in text and "admin account" not in text and "users create" not in text
    s = Database(str(tmp_path / "data" / "trailbeam.sqlite"))
    try:
        assert Accounts(s).auth_state() == (False, False)
    finally:
        s.close()


def test_off_the_people_and_tokens_routes_manage_nobody(tmp_path):
    """QV-226: the People and Tokens API behaves as on a store without accounts, even with people in the store."""
    db = tmp_path / "t.sqlite"
    _store_with_an_admin_and_an_ingest_token(db)
    s = Server(db, TRAILBEAM_TOKEN=TOKEN)
    try:
        bearer = {"Authorization": f"Bearer {TOKEN}"}
        got = {p: s.get(p, bearer) for p in ("/api/users", "/api/tokens/ingest", "/api/me/tokens", "/set-password")}
        assert {p: g[0] for p, g in got.items()} == {
            "/api/users": 403, "/api/tokens/ingest": 403, "/api/me/tokens": 404, "/set-password": 404}
        for p, (status, body) in got.items():
            assert "root@example.com" not in body, p
    finally:
        s.stop()
