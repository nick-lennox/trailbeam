"""``trailbeam start`` (QV-143): first run creates the secrets, a second run reuses them, the printed variables work
in a service, and the plain ``trailbeam`` command with flags is unchanged. Only the run that created the token and
key prints them; later runs print the commands that show them (QV-203)."""
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
        # The token, or on a later run "show it with: cat ...".
        self.token = text.split("Access token (the web screen asks for it once): ")[1].splitlines()[0]
        # The first block of export lines: the Python service's.
        block = text.split("In each Python service")[1].split("In each Node.js service")[0]
        self.python_env = dict(line.strip()[len("export "):].split("=", 1)
                               for line in block.splitlines() if line.strip().startswith("export "))
        block = text.split("In each Node.js service")[1]
        self.node_env = dict(line.strip()[len("export "):].split("=", 1)
                             for line in block.splitlines() if line.strip().startswith("export "))

    def exported(self, env=None):
        """The Python service's variables as a shell sets them from the printed export lines."""
        block = self.text.split("In each Python service")[1].split("In each Node.js service")[0]
        exports = "\n".join(line.strip() for line in block.splitlines() if line.strip().startswith("export "))
        script = exports + "\nenv -0"
        out = subprocess.run(["sh", "-c", script], env=env or _env(), capture_output=True, text=True, check=True)
        got = dict(item.split("=", 1) for item in out.stdout.split("\0") if "=" in item)
        return {k: got[k] for k in self.python_env}

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
        assert "show it with" not in s.text and "Receipt-hash key" not in s.text
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


def test_second_run_keeps_the_token_and_key_and_does_not_print_them(tmp_path):
    """A later run's stdout (journald, docker logs, CI) holds neither secret: it says how to show them, and the
    printed export lines read the files, so pasting them still sets the same values."""
    home = tmp_path / "home"
    data = home / ".trailbeam"
    env = _env(HOME=str(home))
    first = Started(["start", "--port", "0"], env=env)
    first.stop()
    key = (data / "receipt-hash-key").read_text().strip()
    assert first.token == (data / "token").read_text().strip()
    files = {n: ((data / n).read_bytes(), os.stat(data / n).st_mtime_ns) for n in ("token", "receipt-hash-key")}
    second = Started(["start", "--port", "0"], env=env)
    try:
        assert first.token not in second.text and key not in second.text
        assert second.token == "show it with: cat ~/.trailbeam/token"
        assert "  Receipt-hash key: show it with: cat ~/.trailbeam/receipt-hash-key\n" in second.text
        assert second.python_env["OTEL_EXPORTER_OTLP_HEADERS"] == "Authorization=Bearer%20$(cat ~/.trailbeam/token)"
        assert second.python_env["TRAILBEAM_RECEIPT_HASH_KEY"] == "$(cat ~/.trailbeam/receipt-hash-key)"
        # The same values, once a shell runs the lines (the port is new: --port 0).
        assert second.exported(env) == {**first.python_env, "TRAILBEAM_OTLP_TRACES_ENDPOINT": second.url + "/v1/traces"}
        assert {n: ((data / n).read_bytes(), os.stat(data / n).st_mtime_ns) for n in files} == files
        assert second.get("/api/messages", first.token)[0] == 200
    finally:
        assert second.stop() == 0
    assert "created the" not in second.p.stderr.read()


def test_only_the_secret_just_created_is_printed(tmp_path):
    data = tmp_path / "tb"
    Started(["start", "--data-dir", str(data), "--port", "0"]).stop()
    key = (data / "receipt-hash-key").read_text().strip()
    (data / "token").unlink()
    s = Started(["start", "--data-dir", str(data), "--port", "0"])
    try:
        token = (data / "token").read_text().strip()
        assert s.token == token  # new, so printed
        assert key not in s.text
        assert s.python_env["TRAILBEAM_RECEIPT_HASH_KEY"] == f"$(cat {data / 'receipt-hash-key'})"
        assert s.exported()["TRAILBEAM_RECEIPT_HASH_KEY"] == key
    finally:
        s.stop()


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
        assert "my-own-token" not in s.text  # not created here: never printed
        assert s.token == f"show it with: cat {tok}"
        assert s.exported()["OTEL_EXPORTER_OTLP_HEADERS"] == "Authorization=Bearer%20my-own-token-0123456789"
        assert db.exists() and not (data / "trailbeam.sqlite").exists() and not (data / "token").exists()
        assert (data / "receipt-hash-key").exists()  # the services still need one
        assert s.get("/api/messages", "my-own-token-0123456789")[0] == 200
    finally:
        s.stop()


def test_token_from_the_environment_is_not_printed(tmp_path):
    s = Started(["start", "--data-dir", str(tmp_path / "tb"), "--port", "0"],
                env=_env(TRAILBEAM_TOKEN="env-token-0123456789"))
    try:
        assert "env-token" not in s.text and not (tmp_path / "tb" / "token").exists()
        assert s.token == "the one in TRAILBEAM_TOKEN"
        assert s.python_env["OTEL_EXPORTER_OTLP_HEADERS"] == "Authorization=Bearer%20$TRAILBEAM_TOKEN"
        assert s.get("/api/messages", "env-token-0123456789")[0] == 200
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


def test_later_npx_run_shows_the_docker_command(tmp_path):
    """npx trailbeam start with the Docker volume: the files are only in the volume, so the banner gives the
    command the launcher passed, and --token-file names the developer's file, not the container's variable."""
    data = tmp_path / "data"
    env = _env(TRAILBEAM_START_SHOWN_HOST="localhost", TRAILBEAM_START_SHOWN_DATA_DIR="the Docker volume tb",
               TRAILBEAM_START_SHOWN_CAT="docker run --rm -v tb:/data --entrypoint cat img /data/{}")
    Started(["start", "--data-dir", str(data), "--port", "0"], env=env).stop()
    token, key = ((data / n).read_text().strip() for n in ("token", "receipt-hash-key"))
    s = Started(["start", "--data-dir", str(data), "--port", "0"], env=env)
    try:
        assert token not in s.text and key not in s.text and str(data) not in s.text
        assert s.token == "show it with: docker run --rm -v tb:/data --entrypoint cat img /data/token"
        assert s.python_env["TRAILBEAM_RECEIPT_HASH_KEY"] == \
            "$(docker run --rm -v tb:/data --entrypoint cat img /data/receipt-hash-key)"
    finally:
        s.stop()
    s = Started(["start", "--data-dir", str(data), "--port", "0"],
                env={**env, "TRAILBEAM_TOKEN": "mine-0123456789abcdef",
                     "TRAILBEAM_START_SHOWN_TOKEN_FILE": "/home/dev/my token"})
    try:
        assert "mine-0123" not in s.text
        assert s.token == "show it with: cat '/home/dev/my token'"
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


ENV_KEY = "team-key-0123456789abcdef"


def test_receipt_hash_key_from_the_environment_wins_and_no_file_is_made(tmp_path):
    """QV-206: a team that already has TRAILBEAM_RECEIPT_HASH_KEY is told to keep using it. This run did not
    make it, so (as with TRAILBEAM_TOKEN) it is not printed: the export lines name the variable."""
    data = tmp_path / "tb"
    env = _env(TRAILBEAM_RECEIPT_HASH_KEY=ENV_KEY)
    s = Started(["start", "--data-dir", str(data), "--port", "0"], env=env)
    try:
        assert s.python_env["TRAILBEAM_RECEIPT_HASH_KEY"] == "$TRAILBEAM_RECEIPT_HASH_KEY"
        assert s.node_env["TRAILBEAM_RECEIPT_HASH_KEY"] == "$TRAILBEAM_RECEIPT_HASH_KEY"
        assert "Receipt-hash key: the one in TRAILBEAM_RECEIPT_HASH_KEY" in s.text
        assert ENV_KEY not in s.text and "receipt-hash-key)" not in s.text
        assert s.exported(env)["TRAILBEAM_RECEIPT_HASH_KEY"] == ENV_KEY
        assert not (data / "receipt-hash-key").exists() and (data / "token").exists()
    finally:
        assert s.stop() == 0
    assert "differs" not in s.p.stderr.read()


def test_receipt_hash_key_from_the_environment_over_a_different_file_warns(tmp_path):
    data = tmp_path / "tb"
    data.mkdir(mode=0o700)
    (data / "receipt-hash-key").write_text("file-key-0123456789abcdef\n")
    (data / "receipt-hash-key").chmod(0o600)
    env = _env(TRAILBEAM_RECEIPT_HASH_KEY=ENV_KEY)
    s = Started(["start", "--data-dir", str(data), "--port", "0"], env=env)
    try:
        assert s.python_env["TRAILBEAM_RECEIPT_HASH_KEY"] == "$TRAILBEAM_RECEIPT_HASH_KEY"
        assert s.exported(env)["TRAILBEAM_RECEIPT_HASH_KEY"] == ENV_KEY  # not the file's
        assert (data / "receipt-hash-key").read_text() == "file-key-0123456789abcdef\n"  # left alone
    finally:
        s.stop()
    err = s.p.stderr.read()
    assert "TRAILBEAM_RECEIPT_HASH_KEY differs from the key in" in err and "using TRAILBEAM_RECEIPT_HASH_KEY" in err


def test_short_receipt_hash_key_in_the_environment_stops_with_one_line(tmp_path):
    data = tmp_path / "tb"
    r = subprocess.run([sys.executable, "-m", "trailbeam.server", "start", "--data-dir", str(data), "--port", "0"],
                       env=_env(TRAILBEAM_RECEIPT_HASH_KEY="short"), capture_output=True, text=True, timeout=30)
    assert r.returncode == 2 and "TRAILBEAM_RECEIPT_HASH_KEY: the receipt-hash key must be at least 16" in r.stderr
    assert "Traceback" not in r.stderr and not (data / "receipt-hash-key").exists()


def test_ensure_secret_tightens_a_readable_file_of_ours(tmp_path, caplog):
    p = tmp_path / "receipt-hash-key"
    p.write_text("k" * 32)
    p.chmod(0o644)
    assert st.ensure_secret(p) == ("k" * 32, False)
    assert "could be read by other users" in caplog.text and _mode(p) == 0o600


def _run_start(data, *extra):
    return subprocess.run([sys.executable, "-m", "trailbeam.server", "start", "--data-dir", str(data), "--port", "0",
                           *extra], env=_env(), capture_output=True, text=True, timeout=30)


def test_existing_readable_data_dir_is_made_700_and_the_db_600(tmp_path):
    """QV-201: mkdir -m 755 d; trailbeam start --data-dir d left d 755 and the SQLite file 644 (umask 022)."""
    data = tmp_path / "tb"
    data.mkdir(mode=0o755)
    data.chmod(0o755)
    old = os.umask(0o022)
    try:
        s = Started(["start", "--data-dir", str(data), "--port", "0"])
    finally:
        os.umask(old)
    try:
        assert _mode(data) == 0o700
        assert s.get("/api/messages", s.token)[0] == 200
        for name in ("trailbeam.sqlite", "trailbeam.sqlite-wal", "trailbeam.sqlite-shm"):
            if (data / name).exists() or name == "trailbeam.sqlite":
                assert _mode(data / name) == 0o600, name
    finally:
        assert s.stop() == 0
    assert "made it 700" in s.p.stderr.read()


def test_db_elsewhere_is_600_and_an_existing_readable_one_is_tightened(tmp_path):
    db = tmp_path / "shared" / "x.sqlite"
    db.parent.mkdir()
    s = Started(["start", "--data-dir", str(tmp_path / "tb"), "--port", "0", "--db", str(db)])
    s.stop()
    assert _mode(db) == 0o600
    db.chmod(0o644)
    s = Started(["start", "--data-dir", str(tmp_path / "tb"), "--port", "0", "--db", str(db)])
    s.stop()
    assert _mode(db) == 0o600


def test_group_writable_data_dir_of_ours_is_made_700(tmp_path):
    """A 002 umask gives 775 directories: tightened, not refused."""
    data = tmp_path / "tb"
    data.mkdir()
    data.chmod(0o775)
    s = Started(["start", "--data-dir", str(data), "--port", "0"])
    s.stop()
    assert _mode(data) == 0o700


def test_db_of_another_user_is_refused(tmp_path, monkeypatch):
    db = tmp_path / "x.sqlite"
    db.write_bytes(b"")
    uid = os.geteuid()
    monkeypatch.setattr(os, "geteuid", lambda: uid + 1)
    with pytest.raises(st.SetupError, match="belongs to another user"):
        st._tighten_db(str(db))


def test_data_dir_others_can_write_is_refused(tmp_path):
    data = tmp_path / "tb"
    data.mkdir()
    data.chmod(0o777)
    r = _run_start(data)
    assert r.returncode == 2 and "can be written by any user" in r.stderr and "Traceback" not in r.stderr
    assert list(data.iterdir()) == [] and _mode(data) == 0o777  # nothing created, nothing changed


def test_planted_symlink_token_is_refused(tmp_path):
    """A token planted as a symlink to a file the planter knows is not followed."""
    data = tmp_path / "tb"
    data.mkdir(mode=0o700)
    planted = tmp_path / "planted"
    planted.write_text("planted-token-0123456789\n")
    (data / "token").symlink_to(planted)
    r = _run_start(data)
    assert r.returncode == 2 and "is a symlink" in r.stderr and "Traceback" not in r.stderr
    assert "planted-token" not in r.stdout


def test_secret_owned_by_another_user_is_refused(tmp_path, monkeypatch):
    """Without root no test can chown a file to someone else, so this user's files look like another's."""
    p = tmp_path / "token"
    p.write_text("planted-token-0123456789\n")
    p.chmod(0o644)
    uid = os.geteuid()
    monkeypatch.setattr(os, "geteuid", lambda: uid + 1)
    with pytest.raises(st.SetupError, match="belongs to another user"):
        st.ensure_secret(p)
    assert _mode(p) == 0o644


def test_data_dir_owned_by_another_user_is_refused(tmp_path, monkeypatch):
    data = tmp_path / "tb"
    data.mkdir(mode=0o700)
    uid = os.geteuid()
    monkeypatch.setattr(os, "geteuid", lambda: uid + 1)
    with pytest.raises(st.SetupError, match="belongs to another user"):
        st._make_dir(data)


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
