"""``trailbeam users`` and ``trailbeam tokens`` (cli.py) on SQLite and Postgres: what each command does to the store,
what it prints where, and what it refuses (a store it would have to create or upgrade, the last admin, a first
ingest token nobody confirmed).

The Postgres cases run when TRAILBEAM_TEST_POSTGRES_URL names a database, as in test_postgres.py."""
import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path
from urllib.parse import quote

import pytest

from trailbeam.server import cli, migrations, passwords
from trailbeam.server.accounts import Accounts
from trailbeam.server.db import Database
from trailbeam.server.sessions import Sessions
from trailbeam.server.tokens import Tokens, well_formed

ROOT = Path(__file__).resolve().parents[2]
PG = os.environ.get("TRAILBEAM_TEST_POSTGRES_URL")
PW = "correct horse battery"


@pytest.fixture(autouse=True)
def quick_hashes(monkeypatch):
    real = passwords.Hasher
    monkeypatch.setattr(passwords, "Hasher", lambda *a, **k: real(log2_n=10, r=8, p=1))
    for name in ("TRAILBEAM_TOKEN", "TRAILBEAM_TOKEN_FILE", "TRAILBEAM_DB", "TRAILBEAM_STORE_URL"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(cli, "_interactive", lambda: False)


class Target:
    """A store the CLI is pointed at: ``args`` (its --db or --store-url), and ``open()`` a server's view of it."""

    def __init__(self, args, open_store, close=lambda: None):
        self.args = args
        self.open = open_store
        self.close = close


@pytest.fixture(params=["sqlite", pytest.param("postgres", marks=pytest.mark.skipif(
    not PG, reason="TRAILBEAM_TEST_POSTGRES_URL is not set"))])
def target(request, tmp_path):
    if request.param == "sqlite":
        path = str(tmp_path / "trailbeam.sqlite")
        Database(path).close()  # the server made it
        t = Target(["--db", path], lambda: Database(path))
    else:
        sys.path.insert(0, str(ROOT / "tests" / "conformance"))
        from pg_helpers import drop_schema, new_schema
        from trailbeam.server.postgres import PostgresStore

        conninfo = new_schema()
        schema = conninfo.split("search_path=")[1].split()[0].strip("'")
        PostgresStore(conninfo).close()  # the server made it
        url = PG + ("&" if "?" in PG else "?") + "options=" + quote(f"-c search_path={schema}")
        t = Target(["--store-url", url], lambda: PostgresStore(conninfo), lambda: drop_schema(conninfo))
    yield t
    t.close()


def run(capsys, *argv):
    code = cli.main(list(argv))
    out, err = capsys.readouterr()
    return code, out, err


def password_file(tmp_path, pw=PW):
    f = tmp_path / "pw"
    f.write_text(pw + "\n")
    return str(f)


def people(t):
    s = t.open()
    try:
        return {u["email"]: u for u in Accounts(s).users()}
    finally:
        s.close()


# ---- users ----------------------------------------------------------------------------------------------

def test_create_the_first_admin_and_list_people(target, tmp_path, capsys):
    code, out, err = run(capsys, "users", "create", *target.args, "--email", "Ada@Example.com", "--role", "admin",
                         "--password-file", password_file(tmp_path))
    assert code == 0 and out == ""
    assert "Created admin Ada@Example.com" in err and "This is the first admin" in err
    if "--store-url" in target.args:
        assert "Upgrade every replica to this version before creating the first admin" in err
    u = people(target)["Ada@Example.com"]
    assert u["role"] == "admin" and u["name"] == "Ada"
    assert passwords.Hasher().verify(PW, u["password_hash"])
    code, out, err = run(capsys, "users", "create", *target.args, "--email", "bob@example.com", "--name", "Bob B",
                         "--password-file", password_file(tmp_path))
    assert code == 0 and "Created member bob@example.com" in err and "first admin" not in err
    code, out, _ = run(capsys, "users", "list", *target.args, "--json")
    rows = json.loads(out)
    assert [(r["email"], r["role"], r["status"]) for r in rows] == [("Ada@Example.com", "admin", "active"),
                                                                    ("bob@example.com", "member", "active")]
    assert all("password_hash" not in r for r in rows)
    code, out, _ = run(capsys, "users", "list", *target.args)
    assert out.splitlines()[0].split() == ["ID", "EMAIL", "NAME", "ROLE", "STATUS", "PROXY", "LAST", "SIGN-IN"]


def test_create_refuses_what_it_can_t_store(target, tmp_path, capsys):
    pw = password_file(tmp_path)
    assert run(capsys, "users", "create", *target.args, "--email", "a@example.com", "--password-file", pw)[0] == 0
    code, _, err = run(capsys, "users", "create", *target.args, "--email", "A@EXAMPLE.com", "--password-file", pw)
    assert code == 1 and "already has an account" in err
    code, _, err = run(capsys, "users", "create", *target.args, "--email", "nope", "--password-file", pw)
    assert code == 1 and "name@example.com" in err
    code, _, err = run(capsys, "users", "create", *target.args, "--email", "b@example.com",
                       "--password-file", password_file(tmp_path, "short"))
    assert code == 2 and "at least 10 characters" in err
    code, _, err = run(capsys, "users", "create", *target.args, "--email", "b@example.com")
    assert code == 2 and "use --password-file" in err  # no terminal to ask in
    assert list(people(target)) == ["a@example.com"]


def test_the_password_is_asked_for_twice(target, capsys, monkeypatch):
    monkeypatch.setattr(cli, "_interactive", lambda: True)
    answers = iter([PW, PW + "!"])
    monkeypatch.setattr(cli.getpass, "getpass", lambda prompt="": next(answers))
    code, _, err = run(capsys, "users", "create", *target.args, "--email", "a@example.com")
    assert code == 2 and "not the same" in err and people(target) == {}
    answers = iter([PW, PW])
    assert run(capsys, "users", "create", *target.args, "--email", "a@example.com")[0] == 0


def test_role_password_disable_and_remove(target, tmp_path, capsys):
    pw = password_file(tmp_path)
    run(capsys, "users", "create", *target.args, "--email", "root@example.com", "--role", "admin", "--password-file", pw)
    run(capsys, "users", "create", *target.args, "--email", "bob@example.com", "--password-file", pw)
    s = target.open()
    try:
        sessions = Sessions(Accounts(s))
        sid, _ = sessions.start(people(target)["bob@example.com"]["id"])
        code, _, err = run(capsys, "users", "set-role", *target.args, "bob@example.com", "admin")
        assert code == 0 and "now admin" in err and "Running servers see it within 20 seconds" in err
        sessions.forget_user(people(target)["bob@example.com"]["id"])
        assert sessions.lookup(sid) is None  # a new role ends their sessions
        sid, _ = sessions.start(people(target)["bob@example.com"]["id"])
        new = password_file(tmp_path, "another password!")
        code, _, err = run(capsys, "users", "reset-password", *target.args, "bob@example.com", "--password-file", new)
        assert code == 0 and "New password set" in err
        sessions.forget_user(people(target)["bob@example.com"]["id"])
        assert sessions.lookup(sid) is None
        assert passwords.Hasher().verify("another password!", people(target)["bob@example.com"]["password_hash"])
    finally:
        s.close()
    bob = str(people(target)["bob@example.com"]["id"])
    assert run(capsys, "users", "disable", *target.args, bob)[0] == 0  # by ID
    assert run(capsys, "users", "list", *target.args, "--json")[1].count('"disabled"') == 1
    assert run(capsys, "users", "enable", *target.args, "bob@example.com")[0] == 0
    code, _, err = run(capsys, "users", "remove", *target.args, "bob@example.com")
    assert code == 2 and "pass --yes" in err and "bob@example.com" in people(target)
    assert run(capsys, "users", "remove", *target.args, "bob@example.com", "--yes")[0] == 0
    assert list(people(target)) == ["root@example.com"]
    code, _, err = run(capsys, "users", "set-role", *target.args, "nobody@example.com", "member")
    assert code == 2 and "no person 'nobody@example.com'" in err


@pytest.mark.parametrize("command", [["set-role", "root@example.com", "member"], ["disable", "root@example.com"],
                                     ["remove", "root@example.com", "--yes"]])
def test_the_last_admin_can_t_be_taken_away(target, tmp_path, capsys, command):
    run(capsys, "users", "create", *target.args, "--email", "root@example.com", "--role", "admin",
        "--password-file", password_file(tmp_path))
    code, _, err = run(capsys, "users", command[0], *target.args, *command[1:])
    assert code == 1 and "at least one admin" in err
    u = people(target)["root@example.com"]
    assert u["role"] == "admin" and u["disabled_at_ms"] is None


# ---- tokens ---------------------------------------------------------------------------------------------

def test_the_first_ingest_token_says_it_closes_ingest_first(target, capsys, monkeypatch):
    code, out, err = run(capsys, "tokens", "create", *target.args, "--name", "orders-api prod")
    assert code == 2 and out == "" and "first ingest token" in err and "pass --yes" in err
    s = target.open()
    try:
        assert Accounts(s).ingest_tokens() == []  # said before it happened, and nothing happened
    finally:
        s.close()
    monkeypatch.setattr(cli, "_interactive", lambda: True)
    monkeypatch.setattr("builtins.input", lambda q="": "n")
    assert run(capsys, "tokens", "create", *target.args, "--name", "orders-api prod")[0] == 2
    monkeypatch.setattr("builtins.input", lambda q="": "y")
    code, out, err = run(capsys, "tokens", "create", *target.args, "--name", "orders-api prod")
    assert code == 0 and well_formed(out.strip(), "tbi_") and out.count("\n") == 1  # alone on stdout
    monkeypatch.setattr(cli, "_interactive", lambda: False)
    code, out2, err = run(capsys, "tokens", "create", *target.args, "--name", "payments")  # not the first: no question
    assert code == 0 and "first ingest token" not in err
    s = target.open()
    try:
        t = Tokens(Accounts(s))
        assert t.ingest(out.strip()) is not None and t.ingest(out2.strip()) is not None
    finally:
        s.close()


def test_with_a_shared_token_the_first_ingest_token_closes_nothing(target, capsys, monkeypatch):
    monkeypatch.setenv("TRAILBEAM_TOKEN", "shared-token-0123456789abcdef")
    code, out, err = run(capsys, "tokens", "create", *target.args, "--name", "orders")
    assert code == 0 and "first ingest token" not in err and well_formed(out.strip(), "tbi_")
    code, out, _ = run(capsys, "tokens", "list", *target.args)
    assert out.splitlines()[0].startswith("legacy shared token (from TRAILBEAM_TOKEN): ingest and /metrics")


def test_list_and_revoke_ingest_tokens(target, capsys):
    run(capsys, "tokens", "create", *target.args, "--name", "orders", "--yes")
    _, tok, _ = run(capsys, "tokens", "create", *target.args, "--name", "payments")
    code, out, _ = run(capsys, "tokens", "list", *target.args)
    lines = out.splitlines()
    assert lines[0].split() == ["ID", "NAME", "TOKEN", "CREATED", "LAST", "USED", "STATUS"]
    assert tok.strip()[:8] + "..." in out and tok.strip() not in out  # the prefix only
    rows = json.loads(run(capsys, "tokens", "list", *target.args, "--json")[1])
    pid = next(r["id"] for r in rows if r["name"] == "payments")
    code, _, err = run(capsys, "tokens", "revoke", *target.args, str(pid))
    assert code == 0 and "the other ingest tokens keep working" in err
    assert run(capsys, "tokens", "revoke", *target.args, str(pid))[0] == 2  # already
    rows = json.loads(run(capsys, "tokens", "list", *target.args, "--json")[1])
    assert {r["name"]: r["status"] for r in rows} == {"orders": "active", "payments": "revoked"}


def test_personal_tokens(target, tmp_path, capsys):
    run(capsys, "users", "create", *target.args, "--email", "ci@example.com", "--password-file",
        password_file(tmp_path))
    code, _, err = run(capsys, "tokens", "create", *target.args, "--name", "ci", "--personal")
    assert code == 2 and "--personal needs --user" in err
    code, out, err = run(capsys, "tokens", "create", *target.args, "--name", "ci", "--personal", "--user",
                         "ci@example.com", "--expires", "90d")
    tok = out.strip()
    assert code == 0 and well_formed(tok, "tbp_") and "(member)" in err and "working until" in err
    s = target.open()
    try:
        owner = Tokens(Accounts(s)).personal(tok)
        assert owner.email == "ci@example.com" and owner.role == "member"
        row = Accounts(s).personal_tokens()[0]
        assert 89 * 86_400_000 < row["expires_at_ms"] - row["created_at_ms"] <= 90 * 86_400_000 + 1000
    finally:
        s.close()
    rows = json.loads(run(capsys, "tokens", "list", *target.args, "--personal", "--json")[1])
    assert [(r["owner"], r["name"], r["status"]) for r in rows] == [("ci@example.com", "ci", "active")]
    assert run(capsys, "tokens", "list", *target.args)[2].startswith("No ingest tokens yet")
    assert run(capsys, "tokens", "revoke", *target.args, str(rows[0]["id"]))[0] == 2  # not an ingest token
    assert run(capsys, "tokens", "revoke", *target.args, str(rows[0]["id"]), "--personal")[0] == 0
    s = target.open()
    try:
        assert Tokens(Accounts(s)).personal(tok) is None
    finally:
        s.close()
    code, _, err = run(capsys, "tokens", "create", *target.args, "--name", "x", "--expires", "9d")
    assert code == 2 and "for --personal tokens" in err


# ---- the store it works on --------------------------------------------------------------------------------

def test_it_never_creates_a_store(tmp_path, capsys, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TRAILBEAM_DATA_DIR", str(tmp_path / "data"))
    code, _, err = run(capsys, "users", "list", "--db", str(tmp_path / "typo.sqlite"))
    assert code == 2 and "no such file" in err and not (tmp_path / "typo.sqlite").exists()
    code, _, err = run(capsys, "users", "list")
    assert code == 2 and "no trailbeam.sqlite here or in" in err
    code, _, err = run(capsys, "users", "list", "--db", ":memory:")
    assert code == 2 and "the server's own memory" in err
    (tmp_path / "empty.sqlite").touch()
    code, _, err = run(capsys, "users", "list", "--db", str(tmp_path / "empty.sqlite"))
    assert code == 2 and "not a Trailbeam store yet" in err


def test_it_finds_the_trailbeam_start_file(tmp_path, capsys, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TRAILBEAM_DATA_DIR", str(tmp_path / "data"))
    (tmp_path / "data").mkdir()
    Database(str(tmp_path / "data" / "trailbeam.sqlite")).close()
    code, _, err = run(capsys, "users", "list")
    assert code == 0 and str(tmp_path / "data" / "trailbeam.sqlite") in err


@pytest.mark.parametrize("version", ["older", "newer"])
def test_it_refuses_a_store_at_another_schema_version(tmp_path, capsys, version):
    path = tmp_path / "t.sqlite"
    Database(str(path)).close()
    c = sqlite3.connect(path)
    if version == "older":  # as if this file had not yet been upgraded to the latest version
        c.execute("UPDATE schema_version SET version = ? WHERE version = ?", (migrations.LATEST - 1, migrations.LATEST))
    else:
        c.execute("INSERT INTO schema_version (version, applied_at_ms, name, trailbeam_version) "
                  "VALUES (?, 0, 'later', '9.9.9')", (migrations.LATEST + 1,))
    c.commit()
    c.close()
    code, _, err = run(capsys, "users", "list", "--db", str(path))
    assert code == 2
    if version == "older":
        assert f"schema version {migrations.LATEST - 1}" in err and "start it once" in err
    else:
        assert "newer Trailbeam" in err
    c = sqlite3.connect(path)
    try:  # and left it as it was
        assert c.execute("SELECT MAX(version) FROM schema_version").fetchone()[0] == (
            migrations.LATEST - 1 if version == "older" else migrations.LATEST + 1)
    finally:
        c.close()


def test_the_trailbeam_command_runs_it(tmp_path):
    path = tmp_path / "t.sqlite"
    Database(str(path)).close()
    env = {k: v for k, v in os.environ.items() if not k.startswith(("TRAILBEAM_", "OTEL_"))}
    r = subprocess.run([sys.executable, "-m", "trailbeam.server", "tokens", "create", "--db", str(path), "--name", "x",
                        "--yes"], env=env, capture_output=True, text=True, timeout=60)
    assert r.returncode == 0 and well_formed(r.stdout.strip(), "tbi_")
    r = subprocess.run([sys.executable, "-m", "trailbeam.server", "users", "--help"], env=env, capture_output=True,
                       text=True, timeout=60)
    assert r.returncode == 0 and "reset-password" in r.stdout
