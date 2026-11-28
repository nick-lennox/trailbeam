"""The AWS settings (aws_settings.py, QV-589; SQLite schema 31, Postgres schema 25): the checks on a change, the
settings table on every store (in memory, SQLite as a file and :memory:, Postgres), and AwsSettings: off until saved
on with a queue, kept a few seconds, a save seen at once. Upgrades from every older version are in test_migrations.py
and test_postgres.py.

The Postgres cases run when TRAILBEAM_TEST_POSTGRES_URL names a database, as in test_postgres.py."""
import os
import sys
from pathlib import Path

import pytest
from trailbeam.server import aws_settings
from trailbeam.server.aws_settings import AwsSettings, MemorySettings, SettingsProblem, SettingsTables, check_change
from trailbeam.server.db import Database

ROOT = Path(__file__).resolve().parents[2]
PG = os.environ.get("TRAILBEAM_TEST_POSTGRES_URL")
needs_pg = pytest.mark.skipif(not PG, reason="TRAILBEAM_TEST_POSTGRES_URL is not set")


def pg_store():
    sys.path.insert(0, str(ROOT / "tests" / "conformance"))
    from pg_helpers import postgres_store
    return postgres_store()


@pytest.fixture(params=["memory", "sqlite", "sqlite-memory", pytest.param("postgres", marks=needs_pg)])
def table(request, tmp_path):
    backing = None
    if request.param == "memory":
        t = MemorySettings()
    else:
        backing = {"sqlite": lambda: Database(str(tmp_path / "trailbeam.sqlite")),
                   "sqlite-memory": lambda: Database(":memory:"), "postgres": pg_store}[request.param]()
        t = SettingsTables(backing)
    yield t
    if backing is not None:
        backing.close()


# ── the checks ───────────────────────────────────────────────────────────────────────────────────────────────────

def test_a_change_is_tidied():
    assert check_change({"enabled": True, "queues": [" orders ", "", "orders", "billing-*", "jobs.fifo"],
                         "region": " eu-west-1 ", "accountId": "123456789012"}) == {
        "enabled": True, "queues": ["orders", "billing-*", "jobs.fifo"], "region": "eu-west-1",
        "accountId": "123456789012"}
    # one name per line, as a text box gives them; empty region and account clear them
    assert check_change({"queues": "orders\r\n\n  billing\n", "region": "", "accountId": None}) == {
        "queues": ["orders", "billing"], "region": None, "accountId": None}
    assert check_change({}) == {}
    # the record as read back may be sent back whole
    assert check_change({**aws_settings.DEFAULT, "updatedAt": 5, "updatedBy": "Ada"}) == {
        "enabled": False, "queues": [], "redrive": [], "region": None, "regions": [], "accountId": None}
    # the other regions, as a list or typed one per line or with commas
    assert check_change({"regions": [" us-east-1", "us-east-1", "", "ap-south-1"]}) == {
        "regions": ["us-east-1", "ap-south-1"]}
    assert check_change({"regions": "us-east-1, eu-west-2\n"}) == {"regions": ["us-east-1", "eu-west-2"]}
    assert check_change({"regions": None}) == {"regions": []}
    for region in ("us-east-1", "us-gov-west-1", "cn-north-1", "ap-southeast-5", "eu-central-2"):
        assert check_change({"region": region}) == {"region": region}
    assert check_change({"queues": ["*"]}) == {"queues": ["*"]}


@pytest.mark.parametrize("body, text", [
    ([], "Send the settings as a JSON object."),
    ({"secretAccessKey": "x"}, "These are not AWS settings: secretAccessKey."),
    ({"enabled": "yes"}, "enabled is true or false."),
    ({"queues": [1]}, "queues is a list of queue names."),
    ({"queues": {"orders": True}}, "queues is a list of queue names."),
    ({"queues": ["orders queue"]}, "orders queue is not a queue name."),
    ({"queues": ["a" * 81]}, "is not a queue name."),
    ({"queues": ["https://sqs.eu-west-1.amazonaws.com/123456789012/orders"]}, "is not a queue name."),
    ({"queues": [f"q{i}" for i in range(201)]}, "Name at most 200 queues."),
    ({"region": "Ireland"}, "Ireland is not an AWS region, such as eu-west-1."),
    ({"region": 7}, "7 is not an AWS region"),
    ({"regions": ["Ireland"]}, "Ireland is not an AWS region, such as eu-west-1."),
    ({"regions": [7]}, "regions is a list of AWS regions."),
    ({"regions": [f"eu-west-{i}" for i in range(41)]}, "Name at most 40 regions."),
    ({"accountId": "1234"}, "1234 is not a 12-digit AWS account ID, such as 123456789012."),
])
def test_a_change_the_settings_do_not_take_says_why(body, text):
    with pytest.raises(SettingsProblem) as e:
        check_change(body)
    assert text in str(e.value)


def test_a_queue_name_is_one_sqs_takes():
    for name in ("orders", "a", "a" * 80, "jobs.fifo", "a" * 75 + ".fifo", "Orders_EU-1"):
        assert aws_settings.is_queue_name(name), name
    for name in ("", "a" * 81, "a" * 76 + ".fifo", "a/b", "a?b", "a b", "orders.fifo.fifo", ".fifo", "a.b", "*",
                 "https://sqs.eu-west-1.amazonaws.com/123456789012/orders", None, 7):
        assert not aws_settings.is_queue_name(name), name


def test_reads_are_on_only_switched_on_with_a_queue_named():
    assert not aws_settings.is_on(aws_settings.DEFAULT)
    assert not aws_settings.is_on({**aws_settings.DEFAULT, "enabled": True})
    assert not aws_settings.is_on({**aws_settings.DEFAULT, "queues": ["orders"]})
    assert aws_settings.is_on({**aws_settings.DEFAULT, "enabled": True, "queues": ["orders"]})


# ── the table and AwsSettings ────────────────────────────────────────────────────────────────────────────────────

def test_the_table_keeps_one_row_per_key(table):
    assert table.get("aws") is None
    table.put("aws", {"enabled": True, "queues": ["orders", "ünïcode-ok"]}, 10, "Ada")
    table.put("aws", {"enabled": False, "queues": ["billing"]}, 20, "Ben")
    table.put("other", {"x": 1}, 30, "Cy")
    assert table.get("aws") == {"value": {"enabled": False, "queues": ["billing"]}, "updatedAt": 20,
                                "updatedBy": "Ben"}
    assert table.get("other")["value"] == {"x": 1}


def test_off_until_saved_on_and_a_save_is_seen_at_once(table):
    now = [0.0]
    s = AwsSettings(table, clock=lambda: now[0])
    assert s.current() == aws_settings.DEFAULT and not s.on()
    seen = []
    s.on_change.append(seen.append)
    after = s.save({"enabled": True, "queues": ["orders"]}, "Ada", 1000)
    assert after == {"enabled": True, "queues": ["orders"], "redrive": [], "region": None, "regions": [],
                     "accountId": None, "updatedAt": 1000, "updatedBy": "Ada"}
    assert s.on() and s.current() == after and seen == [after]
    # a change keeps the fields it doesn't name
    s.save({"enabled": False}, "Ben", 2000)
    assert s.current()["queues"] == ["orders"] and not s.on()
    # a row saved before redrive was a setting (QV-590) reads with it off
    old = AwsSettings(MemorySettings({"aws": {"value": {"enabled": True, "queues": ["orders"]}, "updatedAt": 1,
                                              "updatedBy": "Ada"}}))
    assert old.current()["redrive"] == [] and old.on()
    # another replica of the store sees it after its few seconds
    other = AwsSettings(table, clock=lambda: now[0])
    assert other.current()["updatedBy"] == "Ben"
    s.save({"enabled": True}, "Cy", 3000)
    assert other.current()["updatedBy"] == "Ben"
    now[0] += AwsSettings.KEEP_S
    assert other.current()["updatedBy"] == "Cy" and other.on()


def test_a_store_that_cant_be_read_has_reads_off():
    class Down:
        def get(self, key):
            raise ConnectionError("down")

    s = AwsSettings(Down())
    assert s.on() is False
    with pytest.raises(ConnectionError):
        s.current()


def test_a_store_without_the_table_keeps_them_in_memory():
    assert isinstance(aws_settings.tables_for(object()), MemorySettings)
    db = Database(":memory:")
    try:
        assert isinstance(aws_settings.tables_for(db), SettingsTables)
    finally:
        db.close()
