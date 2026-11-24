"""The configuration file (config.py): parsing, the checks at start, and the flags that override it."""
import json
import os
import subprocess
import sys
import urllib.request

import pytest

from trailbeam.server import config

GOOD = """
[defaults]
retention = "3d"

[queues.orders]
max_receive_count = 5
store_bodies = true
retention = "30d"

[queues."orders.fifo"]
store_bodies = true

[queues.audit]
retention = "forever"
"""


def write(tmp_path, text, name="trailbeam.toml"):
    p = tmp_path / name
    p.write_text(text)
    return str(p)


def test_parses_queues_and_defaults(tmp_path):
    c = config.load(write(tmp_path, GOOD))
    assert c.retention == 3 * 86_400
    assert c.retention_queues == {"orders": 30 * 86_400, "audit": None}
    assert c.store_bodies == {"orders", "orders.fifo"}
    assert c.max_receive_counts == {"orders": 5}


def test_empty_file_sets_nothing(tmp_path):
    c = config.load(write(tmp_path, ""))
    assert c.retention is config.UNSET and not c.retention_queues and not c.store_bodies and not c.max_receive_counts


def test_store_every_queues_bodies():
    assert config.parse({"defaults": {"store_bodies": True}, "queues": {"a": {"store_bodies": True}}}).store_bodies == {"*"}


def test_hundreds_of_queues():
    data = {"queues": {f"q-{i}": {"max_receive_count": 1 + i % 10, "store_bodies": i % 2 == 0} for i in range(500)}}
    c = config.parse(data)
    assert len(c.max_receive_counts) == 500 and len(c.store_bodies) == 250


@pytest.mark.parametrize("text, problem", [
    ("[queues.orders]\nmax_recieve_count = 5\n", "queues.orders.max_recieve_count: unknown setting"),
    ("[queues.orders]\nmax_receive_count = 0\n", "queues.orders.max_receive_count: expected a whole number from 1 to 1000"),
    ("[queues.orders]\nmax_receive_count = \"5\"\n", "expected a whole number"),
    ("[queues.orders]\nmax_receive_count = true\n", "expected a whole number"),
    ("[queues.orders]\nstore_bodies = \"yes\"\n", "queues.orders.store_bodies: expected true or false"),
    ("[queues.orders]\nretention = \"2 weeks\"\n", "queues.orders.retention: expected a duration"),
    ("[queues.orders]\nretention = 7\n", "queues.orders.retention: expected a string"),
    ("[defaults]\nretention = \"0d\"\n", "defaults.retention: a retention must be more than 0"),
    ("[defaults]\nmax_receive_count = 3\n", "defaults.max_receive_count: unknown setting"),
    ("[queue.orders]\nretention = \"1d\"\n", "queue: unknown table"),
    ("queues = 3\n", "queues: must be tables"),
    ("[queues]\norders = 3\n", "queues.orders: must be a table"),
    ("[queues.\"a,b\"]\nretention = \"1d\"\n", "queues.\"a,b\": not a queue name"),
    ("[defaults]\nstore_bodies = true\n[queues.a]\nstore_bodies = false\n", "false has no effect"),
    ("[queues.orders\n", "not valid TOML"),
])
def test_problems_name_their_place(tmp_path, text, problem):
    with pytest.raises(config.ConfigError) as e:
        config.load(write(tmp_path, text))
    assert problem in str(e.value)
    assert str(tmp_path / "trailbeam.toml") in str(e.value)


def test_every_problem_is_listed(tmp_path):
    with pytest.raises(config.ConfigError) as e:
        config.load(write(tmp_path, "[queues.a]\nmax_receive_count = 0\n[queues.b]\nretention = \"x\"\nfoo = 1\n"))
    assert len(e.value.problems) == 3 and "3 problems" in str(e.value)


def test_missing_file(tmp_path):
    with pytest.raises(config.ConfigError, match="cannot read the file"):
        config.load(str(tmp_path / "nope.toml"))


def test_flags_override_the_file(tmp_path):
    c = config.load(write(tmp_path, GOOD))
    m = config.merge(c, default_retention=7 * 86_400)
    assert m == {"retention": 3 * 86_400, "retention_queues": {"orders": 30 * 86_400, "audit": None},
                 "store_bodies": {"orders", "orders.fifo"}, "max_receive_counts": {"orders": 5}}
    m = config.merge(c, retention=(None,), retention_queues={"orders": 86_400, "x": 60},
                     store_bodies=frozenset({"payments"}), max_receive_counts={"orders": 2, "y": 9},
                     default_retention=7 * 86_400)
    assert m == {"retention": None, "retention_queues": {"orders": 86_400, "audit": None, "x": 60},
                 "store_bodies": {"payments"}, "max_receive_counts": {"orders": 2, "y": 9}}
    assert config.merge(config.Config(), default_retention=7)["retention"] == 7


def _env(tmp_path, **extra):
    env = {k: v for k, v in os.environ.items() if not k.startswith("TRAILBEAM_")}
    return {**env, "TRAILBEAM_DB": str(tmp_path / "q.sqlite"), "TRAILBEAM_PORT": "0", **extra}


def test_bad_file_stops_the_server(tmp_path):
    path = write(tmp_path, "[queues.orders]\nmax_receive_count = 5000\n")
    p = subprocess.run([sys.executable, "-m", "trailbeam.server", "--config", path], env=_env(tmp_path),
                       capture_output=True, text=True, timeout=30)
    assert p.returncode == 2
    assert "queues.orders.max_receive_count: expected a whole number from 1 to 1000, got 5000" in p.stderr
    assert not (tmp_path / "q.sqlite").exists()


def test_server_runs_with_the_file_and_overrides(tmp_path):
    path = write(tmp_path, GOOD)
    # an empty environment variable (compose's ${X:-}) leaves the file's value
    env = _env(tmp_path, TRAILBEAM_CONFIG=path, TRAILBEAM_STORE_BODIES="", TRAILBEAM_MAX_RECEIVE_COUNT="audit=4")
    p = subprocess.Popen([sys.executable, "-m", "trailbeam.server", "--retention-queues", "orders=2d"],
                         env=env, stderr=subprocess.PIPE, text=True)
    try:
        port = None
        while port is None:
            line = p.stderr.readline()
            assert line, "the server exited"
            if "listening on http://127.0.0.1:" in line:
                port = int(line.split("http://127.0.0.1:")[1].split()[0])
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/health", timeout=5) as r:
            cfg = json.loads(r.read())["config"]
        assert cfg["max_receive_counts"] == {"orders": 5, "audit": 4}
        assert cfg["store_bodies"] == ["orders", "orders.fifo"]
        assert cfg["retention"]["default"] == "3d"
        assert cfg["retention"]["queues"] == {"orders": "2d", "audit": "forever"}
    finally:
        p.terminate()
        p.wait(timeout=10)


def test_a_rabbitmq_delivery_limit_is_the_queues_limit(tmp_path):
    c = config.load(write(tmp_path, "[queues.payments]\ndelivery_limit = 20\n"))
    assert c.max_receive_counts == {"payments": 20}


@pytest.mark.parametrize("text,problem", [
    ("[queues.p]\ndelivery_limit = 0\n", "queues.p.delivery_limit: expected a whole number from 1"),
    ("[queues.p]\ndelivery_limit = true\n", "queues.p.delivery_limit: expected a whole number"),
    ("[queues.p]\nmax_receive_count = 3\ndelivery_limit = 3\n", "not both"),
])
def test_a_bad_delivery_limit_is_a_problem(tmp_path, text, problem):
    with pytest.raises(config.ConfigError) as e:
        config.load(write(tmp_path, text))
    assert problem in str(e.value)
