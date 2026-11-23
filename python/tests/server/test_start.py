"""``trailbeam start`` (QV-143): first run creates the secrets, a second run reuses them, the printed variables work
in a service, and the plain ``trailbeam`` command with flags is unchanged."""
import json
import os
import signal
import stat
import subprocess
import sys
import textwrap
import time
import urllib.error
import urllib.request

import pytest

from trailbeam.server import start as st

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX signals and file modes")


def _env(**extra):
    env = {k: v for k, v in os.environ.items() if not k.startswith(("TRAILBEAM_", "OTEL_"))}
    return {**env, **extra}


class Started:
    def __init__(self, args, env=None, cwd=None):
        self.p = subprocess.Popen([sys.executable, "-m", "trailbeam.server", *args], env=env or _env(), cwd=cwd,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.out = []
        for line in self.p.stdout:
            self.out.append(line)
            if "Stop with Ctrl-C" in line:
                break
        else:
            self.stop()
            raise AssertionError("trailbeam start ended early: " + "".join(self.out) + self.p.stderr.read())
        text = "".join(self.out)
        self.text = text
        self.url = text.split("open ")[1].split()[0].rstrip("/")
        self.token = text.split("Access token (the web screen asks for it once): ")[1].split()[0]
        # The first block of export lines: the Python service's.
        block = text.split("In each Python service")[1].split("In each Node.js service")[0]
        self.python_env = dict(line.strip()[len("export "):].split("=", 1)
                               for line in block.splitlines() if line.strip().startswith("export "))
        block = text.split("In each Node.js service")[1]
        self.node_env = dict(line.strip()[len("export "):].split("=", 1)
                             for line in block.splitlines() if line.strip().startswith("export "))

    def get(self, path, token=None):
        req = urllib.request.Request(self.url + path, headers={"Authorization": f"Bearer {token}"} if token else {})
        try:
            with urllib.request.urlopen(req, timeout=5) as r:
                return r.status, r.read()
        except urllib.error.HTTPError as e:
            return e.code, e.read()

    def stop(self):
        if self.p.poll() is None:
            self.p.send_signal(signal.SIGTERM)
            try:
                self.p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.p.kill()
        return self.p.returncode


def _mode(path):
    return stat.S_IMODE(os.stat(path).st_mode)


def test_first_run_creates_secrets_and_prints_what_services_need(tmp_path):
    data = tmp_path / "tb"
    s = Started(["start", "--data-dir", str(data), "--port", "0"])
    try:
        assert _mode(data) == 0o700
        assert _mode(data / "token") == 0o600 and _mode(data / "receipt-hash-key") == 0o600
        token = (data / "token").read_text().strip()
        key = (data / "receipt-hash-key").read_text().strip()
        assert len(token) == 64 and len(key) == 64 and token != key
        assert (data / "trailbeam.sqlite").exists()
        assert s.token == token
        assert s.url.startswith("http://127.0.0.1:")
        want = {"TRAILBEAM_OTLP_TRACES_ENDPOINT": s.url + "/v1/traces",
                "OTEL_EXPORTER_OTLP_HEADERS": f"Authorization=Bearer%20{token}",
                "TRAILBEAM_RECEIPT_HASH_KEY": key}
        assert s.python_env == {**want, "OTEL_SERVICE_NAME": "my-python-service"}
        assert s.node_env == {**want, "OTEL_SERVICE_NAME": "my-node-service"}
        # The token is on: the API needs it, and works with it.
        assert s.get("/api/messages")[0] == 401
        assert s.get("/api/messages", token)[0] == 200
    finally:
        assert s.stop() == 0


def test_second_run_keeps_the_token_and_key(tmp_path):
    data = tmp_path / "tb"
    first = Started(["start", "--data-dir", str(data), "--port", "0"])
    first.stop()
    files = {n: ((data / n).read_bytes(), os.stat(data / n).st_mtime_ns) for n in ("token", "receipt-hash-key")}
    second = Started(["start", "--data-dir", str(data), "--port", "0"])
    try:
        assert second.token == first.token
        assert second.python_env["TRAILBEAM_RECEIPT_HASH_KEY"] == first.python_env["TRAILBEAM_RECEIPT_HASH_KEY"]
        assert {n: ((data / n).read_bytes(), os.stat(data / n).st_mtime_ns) for n in files} == files
        assert second.get("/api/messages", first.token)[0] == 200
    finally:
        assert second.stop() == 0
    assert "created the" not in second.p.stderr.read()


SERVICE = textwrap.dedent("""
    import sys, boto3, trailbeam.boto
    provider = trailbeam.boto.install()
    sqs = boto3.client("sqs", endpoint_url=sys.argv[1], region_name="us-east-1")
    url = sqs.create_queue(QueueName="trailbeam-start")["QueueUrl"]
    mid = sqs.send_message(QueueUrl=url, MessageBody="hello")["MessageId"]
    m = sqs.receive_message(QueueUrl=url, WaitTimeSeconds=1)["Messages"][0]
    sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])
    provider.force_flush()
    print(mid)
""")


def test_printed_variables_get_a_message_on_screen(tmp_path, sqs_endpoint):
    """The done condition: paste the printed variables into a service, and its message shows up."""
    s = Started(["start", "--data-dir", str(tmp_path / "tb"), "--port", "0"])
    try:
        env = _env(**s.python_env)
        r = subprocess.run([sys.executable, "-c", SERVICE, sqs_endpoint], env=env, cwd=tmp_path,
                           capture_output=True, text=True, timeout=60)
        assert r.returncode == 0, r.stderr
        mid = r.stdout.split()[-1]
        assert "published default key" not in r.stderr  # the printed key was used
        deadline = time.time() + 10
        while True:
            status, body = s.get(f"/api/messages/{mid}", s.token)
            if status == 200 and {"sent", "received", "deleted"} <= {e["type"] for e in json.loads(body)["events"]}:
                break
            assert time.time() < deadline, (status, body)
            time.sleep(0.1)
        status, page = s.get("/", s.token)  # the first page lists it
        assert status == 200 and mid.encode() in page
    finally:
        s.stop()


def test_start_flags_replace_the_data_directory_defaults(tmp_path):
    data, db, tok = tmp_path / "tb", tmp_path / "elsewhere.sqlite", tmp_path / "my.token"
    tok.write_text("my-own-token-0123456789\n")
    s = Started(["start", "--data-dir", str(data), "--port", "0", "--db", str(db), "--token-file", str(tok),
                 "--stuck-after", "60"])
    try:
        assert s.token == "my-own-token-0123456789"
        assert db.exists() and not (data / "trailbeam.sqlite").exists() and not (data / "token").exists()
        assert (data / "receipt-hash-key").exists()  # the services still need one
        assert s.get("/api/messages", "my-own-token-0123456789")[0] == 200
    finally:
        s.stop()


def test_data_dir_from_the_environment(tmp_path):
    data = tmp_path / "from-env"
    s = Started(["start", "--port", "0"], env=_env(TRAILBEAM_DATA_DIR=str(data)))
    try:
        assert (data / "token").read_text().strip() == s.token
    finally:
        s.stop()


def test_shown_host_and_data_dir_for_npx(tmp_path):
    """npx trailbeam start runs this in the container: the banner names the developer's address and Docker volume."""
    data = tmp_path / "data"
    s = Started(["start", "--data-dir", str(data), "--host", "0.0.0.0", "--port", "0"],
                env=_env(TRAILBEAM_START_SHOWN_HOST="localhost",
                         TRAILBEAM_START_SHOWN_DATA_DIR="the Docker volume trailbeam-data"))
    try:
        assert s.url.startswith("http://localhost:")
        assert "Data directory: the Docker volume trailbeam-data (keep it private" in s.text
        assert str(data) not in s.text and (data / "token").exists()
        assert s.python_env["TRAILBEAM_OTLP_TRACES_ENDPOINT"] == s.url + "/v1/traces"
    finally:
        s.stop()


def test_bad_existing_secret_stops_with_one_line(tmp_path):
    data = tmp_path / "tb"
    data.mkdir()
    (data / "token").write_text("short\n")
    r = subprocess.run([sys.executable, "-m", "trailbeam.server", "start", "--data-dir", str(data), "--port", "0"],
                       env=_env(), capture_output=True, text=True, timeout=30)
    assert r.returncode == 2 and "at least 16 characters" in r.stderr and "Traceback" not in r.stderr
    assert (data / "token").read_text() == "short\n"  # never rewritten


def test_ensure_secret_warns_about_a_readable_file(tmp_path, caplog):
    p = tmp_path / "receipt-hash-key"
    p.write_text("k" * 32)
    p.chmod(0o644)
    assert st.ensure_secret(p) == ("k" * 32, False)
    assert "can be read by other users" in caplog.text and _mode(p) == 0o644


def test_legacy_invocation_is_unchanged(tmp_path):
    """trailbeam with flags (the Docker image, existing users): no data directory, no token, no banner."""
    home = tmp_path / "home"
    home.mkdir()
    env = _env(HOME=str(home), TRAILBEAM_PORT="0")
    p = subprocess.Popen([sys.executable, "-m", "trailbeam.server", "--db", str(tmp_path / "q.sqlite"),
                          "--stuck-after", "60"], env=env, cwd=tmp_path, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, text=True)
    try:
        line = p.stderr.readline()
        port = int(line.split("http://127.0.0.1:")[1].split()[0])
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/messages", timeout=5) as r:
            assert r.status == 200  # no token
        p.send_signal(signal.SIGTERM)
        assert p.wait(timeout=10) == 0
    finally:
        if p.poll() is None:
            p.kill()
    assert p.stdout.read() == ""
    assert not (home / ".trailbeam").exists()
    assert sorted(f.name for f in tmp_path.iterdir() if f.name != "home")[:1] == ["q.sqlite"]


def test_url_host():
    assert st._url_host("0.0.0.0") == "127.0.0.1"
    assert st._url_host("::") == "[::1]"
    assert st._url_host("::1") == "[::1]"
    assert st._url_host("127.0.0.1") == "127.0.0.1"


def test_start_on_an_existing_database_warns_what_the_new_secrets_change(tmp_path):
    """A 0.3.0 data directory has the SQLite file only (0.3.0 had no start): the token and key start makes are
    new to the services sending to it, so start says so (QV-170). A second run, with the files, says nothing."""
    data = tmp_path / "tb"
    data.mkdir()
    import sqlite3

    from trailbeam.server.migrations import open_schema
    c = sqlite3.connect(data / "trailbeam.sqlite", isolation_level=None)
    open_schema(c)
    c.close()
    s = Started(["start", "--data-dir", str(data), "--port", "0"])
    s.stop()
    err = s.p.stderr.read()
    assert "is an existing database, and the access token is now on" in err
    assert f"run trailbeam --db {data / 'trailbeam.sqlite'}" in err and "--token-file" in err
    assert "is an existing database, and a new receipt-hash key was made" in err
    again = Started(["start", "--data-dir", str(data), "--port", "0"])
    again.stop()
    assert "existing database" not in again.p.stderr.read()


def test_start_on_an_existing_database_with_its_secrets_does_not_warn(tmp_path):
    data = tmp_path / "tb"
    data.mkdir()
    (data / "trailbeam.sqlite").write_bytes(b"")  # empty: a new database, not an existing one
    (data / "token").write_text("t" * 32)
    s = Started(["start", "--data-dir", str(data), "--port", "0"])
    s.stop()
    err = s.p.stderr.read()
    assert "existing database" not in err and "created the receipt-hash key" in err
