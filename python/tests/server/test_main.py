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
               "TRAILBEAM_INGEST_MAX_WAIT_S")


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


def test_numeric_env_is_parsed(monkeypatch):
    a = _parse(monkeypatch, {"TRAILBEAM_PORT": "0", "TRAILBEAM_PURGE_DUTY": "1", "TRAILBEAM_PURGE_BATCH": "7"})
    assert (a.port, a.purge_duty, a.purge_batch) == (0, 1.0, 7)


@pytest.mark.parametrize("name,value", [("TRAILBEAM_PORT", "x"), ("TRAILBEAM_PORT", "70000"),
                                        ("TRAILBEAM_STUCK_AFTER_S", "0"), ("TRAILBEAM_PURGE_DUTY", "1.5"),
                                        ("TRAILBEAM_PURGE_DUTY", "0"), ("TRAILBEAM_PURGE_INTERVAL_S", "nan"),
                                        ("TRAILBEAM_PURGE_BATCH", "0")])
def test_bad_numeric_env_is_one_line(monkeypatch, name, value):
    from trailbeam.server import __main__ as m

    monkeypatch.setenv(name, value)
    with pytest.raises(SystemExit) as e:
        m.main([])
    assert str(e.value).startswith(f"trailbeam: {name}: ")


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
