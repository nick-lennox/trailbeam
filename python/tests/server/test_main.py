"""The trailbeam command: starts, serves, and exits cleanly on SIGTERM (docker stop, systemd)."""
import os
import signal
import subprocess
import sys
import time
import urllib.request

import pytest


@pytest.mark.skipif(os.name != "posix", reason="POSIX signals")
def test_sigterm_stops_cleanly(tmp_path):
    env = {**os.environ, "TRAILBEAM_DB": str(tmp_path / "q.sqlite"), "TRAILBEAM_PORT": "0"}
    p = subprocess.Popen([sys.executable, "-m", "trailbeam.server"], env=env, stderr=subprocess.PIPE, text=True)
    try:
        line = p.stderr.readline()  # "... listening on http://127.0.0.1:<port> ..."
        port = int(line.split("http://127.0.0.1:")[1].split()[0])
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=5) as r:
            assert r.status == 200 and b"Look up a message" in r.read()
        p.send_signal(signal.SIGTERM)
        assert p.wait(timeout=5) == 0
    finally:
        if p.poll() is None:
            p.kill()
    assert (tmp_path / "q.sqlite").exists()


def test_max_receive_count_option():
    import argparse

    import pytest

    from trailbeam.server.__main__ import _max_receive_counts

    assert _max_receive_counts("") == {}
    assert _max_receive_counts("orders=3, payments=5") == {"orders": 3, "payments": 5}
    for bad in ("orders", "orders=", "=3", "orders=0", "orders=x"):
        with pytest.raises(argparse.ArgumentTypeError):
            _max_receive_counts(bad)


NUMERIC_ENV = ("TRAILBEAM_PORT", "TRAILBEAM_STUCK_AFTER_S", "TRAILBEAM_LAMBDA_TIMEOUT_S", "TRAILBEAM_PURGE_INTERVAL_S",
               "TRAILBEAM_PURGE_BATCH", "TRAILBEAM_PURGE_DUTY", "TRAILBEAM_INGEST_MAX_PENDING",
               "TRAILBEAM_INGEST_MAX_WAIT_S", "TRAILBEAM_PG_INGEST_CONNECTIONS", "TRAILBEAM_PG_READ_CONNECTIONS")


def _parse(monkeypatch, env: dict, argv=()):
    """The parsed arguments, stopping main() just after argparse."""
    import argparse

    from trailbeam.server import __main__ as m

    for k, v in env.items():
        monkeypatch.setenv(k, v)
    seen = {}

    def stop(self, args=None, namespace=None):
        seen["a"] = orig(self, args, namespace)
        raise SystemExit(0)
    orig = argparse.ArgumentParser.parse_args
    monkeypatch.setattr(argparse.ArgumentParser, "parse_args", stop)
    with pytest.raises(SystemExit):
        m.main(list(argv))
    return seen["a"]


def test_empty_numeric_env_is_unset(monkeypatch):
    """Compose files pass ${NAME:-}: an empty variable means the default, as for the list variables (QV-100)."""
    a = _parse(monkeypatch, {k: "" for k in NUMERIC_ENV})
    assert (a.port, a.stuck_after, a.lambda_timeout) == (4318, 300, 900)
    assert (a.purge_interval, a.purge_batch, a.purge_duty) == (600, 50, 0.25)
    assert a.ingest_max_pending >= 1 and a.ingest_max_wait > 0
    assert (a.pg_ingest_connections, a.pg_read_connections) == (None, None)  # the store's defaults


def test_numeric_env_is_parsed(monkeypatch):
    a = _parse(monkeypatch, {"TRAILBEAM_PORT": "0", "TRAILBEAM_PURGE_DUTY": "1", "TRAILBEAM_PURGE_BATCH": "7"})
    assert (a.port, a.purge_duty, a.purge_batch) == (0, 1.0, 7)


@pytest.mark.parametrize("name,value", [("TRAILBEAM_PORT", "x"), ("TRAILBEAM_PORT", "70000"),
                                        ("TRAILBEAM_STUCK_AFTER_S", "0"), ("TRAILBEAM_PURGE_DUTY", "1.5"),
                                        ("TRAILBEAM_PURGE_DUTY", "0"), ("TRAILBEAM_PURGE_INTERVAL_S", "nan"),
                                        ("TRAILBEAM_PURGE_BATCH", "0"), ("TRAILBEAM_PG_READ_CONNECTIONS", "0"),
                                        ("TRAILBEAM_PG_INGEST_CONNECTIONS", "two")])
def test_bad_numeric_env_is_one_line(monkeypatch, name, value):
    from trailbeam.server import __main__ as m

    monkeypatch.setenv(name, value)
    with pytest.raises(SystemExit) as e:
        m.main([])
    assert str(e.value).startswith(f"trailbeam: {name}: ")


def test_service_link_port_says_what_it_is(monkeypatch):
    """QV-291: a Kubernetes Service named trailbeam sets TRAILBEAM_PORT=tcp://<ip>:4318 in every pod of the namespace."""
    from trailbeam.server import __main__ as m

    monkeypatch.setenv("TRAILBEAM_PORT", "tcp://10.96.24.77:4318")
    with pytest.raises(SystemExit) as e:
        m.main([])
    msg = str(e.value)
    assert msg.startswith("trailbeam: TRAILBEAM_PORT: ") and "\n" not in msg
    assert "service link" in msg and "enableServiceLinks: false" in msg


@pytest.mark.parametrize("flag,value", [("--purge-duty", "2"), ("--purge-duty", "0"), ("--port", "-1"),
                                        ("--purge-batch", "0"), ("--purge-interval", "0")])
def test_bad_numeric_flag_is_argparse_error(capsys, flag, value):
    from trailbeam.server import __main__ as m

    with pytest.raises(SystemExit) as e:
        m.main([flag, value])
    assert e.value.code == 2
    assert f"argument {flag}" in capsys.readouterr().err


def test_empty_port_env_is_no_traceback(tmp_path):
    """The reproduction from QV-100: TRAILBEAM_PORT= used to be a ValueError traceback."""
    env = {**os.environ, "TRAILBEAM_PORT": "", "TRAILBEAM_PURGE_DUTY": "5"}
    r = subprocess.run([sys.executable, "-m", "trailbeam.server", "--db", str(tmp_path / "x.sqlite")], env=env,
                       capture_output=True, text=True, timeout=30)
    assert r.returncode != 0 and "Traceback" not in r.stderr
    assert "TRAILBEAM_PURGE_DUTY" in r.stderr


@pytest.mark.parametrize("env,argv,want", [({}, (), "full"), ({"TRAILBEAM_DURABILITY": ""}, (), "full"),
                                            ({"TRAILBEAM_DURABILITY": "Relaxed"}, (), "relaxed"),
                                            ({}, ("--durability", "relaxed"), "relaxed")])
def test_durability_option(monkeypatch, env, argv, want):
    """QV-78: --durability relaxed is opt-in. By default every acknowledged export is fsynced."""
    monkeypatch.delenv("TRAILBEAM_DURABILITY", raising=False)
    assert _parse(monkeypatch, env, argv).durability == want


def test_durability_sets_synchronous(tmp_path):
    from trailbeam.server.db import SqliteStore

    for durability, synchronous in (("full", 2), ("relaxed", 1)):  # PRAGMA synchronous: FULL 2, NORMAL 1
        db = SqliteStore(str(tmp_path / f"{durability}.sqlite"), durability=durability)
        try:
            assert db.conn.execute("PRAGMA synchronous").fetchone()[0] == synchronous
        finally:
            db.close()
    assert SqliteStore(str(tmp_path / "default.sqlite")).conn.execute("PRAGMA synchronous").fetchone()[0] == 2
    with pytest.raises(ValueError):
        SqliteStore(str(tmp_path / "x.sqlite"), durability="off")


def test_bad_durability_is_one_line(monkeypatch, capsys):
    from trailbeam.server import __main__ as m

    monkeypatch.setenv("TRAILBEAM_DURABILITY", "off")
    with pytest.raises(SystemExit) as e:
        m.main([])
    assert str(e.value).startswith("trailbeam: TRAILBEAM_DURABILITY: ")
    monkeypatch.delenv("TRAILBEAM_DURABILITY")
    with pytest.raises(SystemExit) as e:
        m.main(["--durability", "off"])
    assert e.value.code == 2 and "argument --durability" in capsys.readouterr().err


def test_relaxed_durability_is_sqlite_only(tmp_path):
    env = {**os.environ, "TRAILBEAM_STORE_URL": "postgres://u@127.0.0.1:1/x", "TRAILBEAM_DURABILITY": "relaxed"}
    r = subprocess.run([sys.executable, "-m", "trailbeam.server", "--db", str(tmp_path / "x.sqlite")], env=env,
                       capture_output=True, text=True, timeout=30)
    assert r.returncode == 2 and "Traceback" not in r.stderr and "synchronous_commit" in r.stderr


@pytest.mark.parametrize("env,argv,want", [({}, (), (None, None)),
                                            ({"TRAILBEAM_PG_INGEST_CONNECTIONS": "6", "TRAILBEAM_PG_READ_CONNECTIONS": "20"},
                                             (), (6, 20)),
                                            ({"TRAILBEAM_PG_READ_CONNECTIONS": "20"},
                                             ("--pg-read-connections", "3", "--pg-ingest-connections", "2"), (2, 3))])
def test_pool_size_options(monkeypatch, env, argv, want):
    """QV-127 (3c): the Postgres pools are settings; a flag overrides its environment variable."""
    for k in ("TRAILBEAM_PG_INGEST_CONNECTIONS", "TRAILBEAM_PG_READ_CONNECTIONS"):
        monkeypatch.delenv(k, raising=False)
    a = _parse(monkeypatch, env, argv)
    assert (a.pg_ingest_connections, a.pg_read_connections) == want


def test_pool_sizes_are_postgres_only(tmp_path):
    env = {**os.environ, "TRAILBEAM_PG_READ_CONNECTIONS": "12"}
    env.pop("TRAILBEAM_STORE_URL", None)
    r = subprocess.run([sys.executable, "-m", "trailbeam.server", "--db", str(tmp_path / "x.sqlite")], env=env,
                       capture_output=True, text=True, timeout=30)
    assert r.returncode == 2 and "Traceback" not in r.stderr and "TRAILBEAM_STORE_URL" in r.stderr
    assert not (tmp_path / "x.sqlite").exists()


def _taken_port():
    """A socket listening on a free 127.0.0.1 port, as another OTLP receiver would."""
    import socket
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    s.listen()
    return s


@pytest.mark.parametrize("command", [[], ["start"]])
def test_port_in_use_is_one_line(tmp_path, command):
    taken = _taken_port()
    port = taken.getsockname()[1]
    env = {**os.environ, "TRAILBEAM_DB": str(tmp_path / "q.sqlite"), "TRAILBEAM_PORT": str(port),
           "TRAILBEAM_HOST": "127.0.0.1"}
    env.pop("TRAILBEAM_TOKEN", None)
    argv = command + (["--data-dir", str(tmp_path / "data")] if command else [])
    try:
        p = subprocess.run([sys.executable, "-m", "trailbeam.server", *argv], env=env, capture_output=True,
                           text=True, timeout=30)
    finally:
        taken.close()
    assert p.returncode == 2
    assert "Traceback" not in p.stderr
    last = p.stderr.strip().splitlines()[-1]
    prog = "trailbeam start" if command else "trailbeam"
    assert last == (f"trailbeam: port {port} on 127.0.0.1 is already in use (another OTLP receiver?). Stop it or "
                    f"run {prog} --port {port + 1} and set TRAILBEAM_OTLP_TRACES_ENDPOINT to match.")
    assert p.stdout == ""  # no banner for a server that isn't running


def test_bind_error_messages():
    import errno
    import socket

    from trailbeam.server.__main__ import bind_error

    assert bind_error(OSError(errno.EADDRINUSE, "Address already in use"), "127.0.0.1", 4318) == (
        "trailbeam: port 4318 on 127.0.0.1 is already in use (another OTLP receiver?). Stop it or run trailbeam "
        "--port 4319 and set TRAILBEAM_OTLP_TRACES_ENDPOINT to match.")
    assert "--port 65534" in bind_error(OSError(errno.EADDRINUSE, "in use"), "0.0.0.0", 65535)
    assert "ports below 1024" in bind_error(OSError(errno.EACCES, "Permission denied"), "127.0.0.1", 80)
    assert "not an address of this machine" in bind_error(OSError(errno.EADDRNOTAVAIL, "x"), "10.9.9.9", 4318)
    assert bind_error(socket.gaierror(-2, "Name or service not known"), "nohost", 4318).startswith(
        "trailbeam: --host nohost: Name or service not known.")
    assert bind_error(OSError(errno.ENOBUFS, "No buffer space"), "127.0.0.1", 4318) == (
        "trailbeam: can't listen on port 4318 on 127.0.0.1: No buffer space")
