"""``trailbeam start``: first-run setup in one command.

In a data directory (``--data-dir``, env ``TRAILBEAM_DATA_DIR``, default ``~/.trailbeam``) it keeps:

- ``token``: the access token. Ingest, the API and the web screen need it;
- ``receipt-hash-key``: the key every service hashes receipt handles with (``TRAILBEAM_RECEIPT_HASH_KEY``).
  The server never reads it; it is kept here so that every service is given the same one;
- ``trailbeam.sqlite``: the spans.

Each secret is created on the first run, 32 random bytes as hex, in a file only its owner can read, and read
back on every later run, so the token and key never change unless the file is deleted. Then the server starts
as ``trailbeam`` would, and once it listens, the address, the token and the environment variables for a Python
and a Node.js service are printed on stdout (the logs stay on stderr).

The token and the key are printed in full only by the run that created them. A later run (under systemd,
``docker run -d``, nohup or CI, whose stdout often ends up in a log) prints the command that shows each one
instead (``cat ~/.trailbeam/token``), and the environment variable lines read the files with ``$(cat ...)``, so
the secrets reach neither the log nor the shell history. A token given with ``--token-file`` or
``TRAILBEAM_TOKEN`` was not created here, so it is never printed.

Every flag of ``trailbeam`` works here too. ``--db`` or ``--store-url`` replaces the SQLite file in the data
directory, and ``--token-file`` or ``TRAILBEAM_TOKEN`` replaces the token file.

``npx trailbeam start`` (the npm package) runs this in the container image. Inside it the server listens on
0.0.0.0 and the data directory is ``/data``, so it sets ``TRAILBEAM_START_SHOWN_HOST`` and
``TRAILBEAM_START_SHOWN_DATA_DIR`` to what the banner should say instead: the address on the developer's
machine and the Docker volume (or directory) the data is in. With the data in a Docker volume it also sets
``TRAILBEAM_START_SHOWN_CAT``, the command that prints a file of the data directory on the developer's machine
(``{}`` where the file name goes), and with ``--token-file`` it sets ``TRAILBEAM_START_SHOWN_TOKEN_FILE`` to that
file's path on the developer's machine (the token itself reaches the container as ``TRAILBEAM_TOKEN``).
"""
from __future__ import annotations

import logging
import os
import secrets
import shlex
import stat
import sys
from pathlib import Path

from . import auth

TOKEN_FILE = "token"
KEY_FILE = "receipt-hash-key"
DB_FILE = "trailbeam.sqlite"
DATA_DIR_ENV = "TRAILBEAM_DATA_DIR"
SHOWN_HOST_ENV = "TRAILBEAM_START_SHOWN_HOST"  # set by npx trailbeam start: see the module docstring
SHOWN_DATA_DIR_ENV = "TRAILBEAM_START_SHOWN_DATA_DIR"
SHOWN_CAT_ENV = "TRAILBEAM_START_SHOWN_CAT"
SHOWN_TOKEN_FILE_ENV = "TRAILBEAM_START_SHOWN_TOKEN_FILE"
MIN_KEY_LENGTH = 16  # the packages warn below 16 bytes

log = logging.getLogger("trailbeam.server")


class SetupError(Exception):
    pass


def default_data_dir() -> str:
    return os.environ.get(DATA_DIR_ENV, "").strip() or str(Path("~/.trailbeam").expanduser())


def start(argv) -> None:
    from .__main__ import build_parser, serve

    p = build_parser(
        "trailbeam start",
        "First-run setup and start: creates an access token and a receipt-hash key in the data directory if "
        "they are not there (and reuses them after), starts Trailbeam with its SQLite file there, and prints "
        "the web screen's address, the token and the environment variables for your services.")
    p.epilog = None
    p.add_argument("--data-dir", default=default_data_dir(),
                   help=f"where the token, the receipt-hash key and the SQLite file are kept (env {DATA_DIR_ENV}, "
                        "default ~/.trailbeam). Created with owner-only permissions")
    # None: not given, so the SQLite file goes in the data directory. TRAILBEAM_DB and --db still win.
    p.set_defaults(db=os.environ.get("TRAILBEAM_DB", "").strip() or None)
    a = p.parse_args(argv)
    a.command = "trailbeam start"  # in the one-line errors serve() prints
    logging.basicConfig(level=a.log_level.upper(), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    data = Path(a.data_dir).expanduser()
    try:
        _make_dir(data)
        key, made_key = ensure_secret(data / KEY_FILE)
        if len(key) < MIN_KEY_LENGTH:
            raise SetupError(f"{data / KEY_FILE}: the receipt-hash key must be at least {MIN_KEY_LENGTH} characters")
        made_token = False
        token_file = a.token_file  # given with --token-file; None with TRAILBEAM_TOKEN or the data directory's
        if not a.token_file and not os.environ.get("TRAILBEAM_TOKEN", "").strip():
            a.token_file = str(data / TOKEN_FILE)
            _, made_token = ensure_secret(a.token_file)
    except (OSError, SetupError, auth.TokenError) as e:
        log.error("trailbeam start: %s", e)
        sys.exit(2)
    for what, made, path in (("access token", made_token, a.token_file), ("receipt-hash key", made_key, data / KEY_FILE)):
        if made:
            log.info("created the %s in %s", what, path)
    if a.db is None:
        a.db = str(data / DB_FILE)

    shown_host = os.environ.get(SHOWN_HOST_ENV, "").strip()
    shown_data_env = os.environ.get(SHOWN_DATA_DIR_ENV, "").strip()
    shown_data = shown_data_env or data
    cat = os.environ.get(SHOWN_CAT_ENV, "").strip()

    def show(name):
        """The shell command that prints the file ``name`` of the data directory, on the developer's machine."""
        if cat:
            return cat.replace("{}", name)
        return "cat " + _shell_path(Path(shown_data_env) / name if shown_data_env else data / name)

    key_cmd = None if made_key else show(KEY_FILE)
    token_cmd = token_var = None
    if made_token:
        pass
    elif token_file:
        token_cmd = "cat " + _shell_path(token_file)
    elif a.token_file:  # the data directory's
        token_cmd = show(TOKEN_FILE)
    else:
        shown_file = os.environ.get(SHOWN_TOKEN_FILE_ENV, "").strip()
        if shown_file:
            token_cmd = "cat " + _shell_path(shown_file)
        else:
            token_var = "TRAILBEAM_TOKEN"

    def announce(host, port, token):
        sys.stdout.write(banner(_url_host(shown_host or host), port, token, key, shown_data,
                                token_cmd=token_cmd, key_cmd=key_cmd, token_var=token_var))
        sys.stdout.flush()

    serve(a, on_listening=announce)


def ensure_secret(path) -> tuple:
    """The secret in ``path``, and whether it was just created. A missing file is created, readable and writable
    by its owner only, holding 32 random bytes as hex; an existing one is read and never rewritten."""
    path = Path(path)
    value = secrets.token_hex(32)
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        text = path.read_text(encoding="utf-8").strip()
        if not text:
            raise SetupError(f"{path} is empty: delete it to have a new secret made, or put one in it") from None
        mode = path.stat().st_mode
        if os.name == "posix" and mode & (stat.S_IRWXG | stat.S_IRWXO):
            log.warning("%s can be read by other users (mode %o): chmod 600 %s", path, stat.S_IMODE(mode), path)
        return auth.check_token(text) if path.name == TOKEN_FILE else text, False
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(value + "\n")
        f.flush()
        os.fsync(f.fileno())
    return value, True


def _make_dir(path: Path) -> None:
    if path.exists() and not path.is_dir():
        raise SetupError(f"the data directory {path} is a file")
    path.mkdir(mode=0o700, parents=True, exist_ok=True)


def _shell_path(path) -> str:
    """``path`` for a shell command: ``~/...`` under the home directory when that needs no quoting."""
    text = str(path)
    home = str(Path.home())
    if home not in ("", "/") and text.startswith(home + "/"):
        rest = text[len(home) + 1:]
        if rest and shlex.quote(rest) == rest:
            return "~/" + rest
    return shlex.quote(text)


def _url_host(host: str) -> str:
    """The host to put in a URL a developer opens on this machine."""
    if host in ("", "0.0.0.0"):
        return "127.0.0.1"
    if host == "::":
        return "[::1]"
    return f"[{host}]" if ":" in host else host


def service_env(url: str, token, key: str, service: str) -> list:
    """``token`` and ``key`` are what goes after ``=`` in a shell: the secret itself, ``$(cat ...)`` or ``$VAR``."""
    env = [("TRAILBEAM_OTLP_TRACES_ENDPOINT", f"{url}/v1/traces")]
    if token:
        env.append(("OTEL_EXPORTER_OTLP_HEADERS", f"Authorization=Bearer%20{token}"))
    env += [("TRAILBEAM_RECEIPT_HASH_KEY", key), ("OTEL_SERVICE_NAME", service)]
    return env


def banner(host: str, port: int, token, key: str, data, token_cmd=None, key_cmd=None, token_var=None) -> str:
    """The text printed once the server listens. When the token (key) was not created by this run, it is not in
    the text: ``token_cmd`` (``key_cmd``) is the shell command that prints it, or ``token_var`` the environment
    variable that holds it, and the environment variable lines use that."""
    url = f"http://{host}:{port}"
    lines = ["", f"Trailbeam is running: open {url}/"]
    if token:
        if token_cmd:
            token, shown = f"$({token_cmd})", f"show it with: {token_cmd}"
        elif token_var:
            token, shown = f"${token_var}", f"the one in {token_var}"
        else:
            shown = token
        lines.append(f"  Access token (the web screen asks for it once): {shown}")
    if key_cmd:
        key = f"$({key_cmd})"
        lines.append(f"  Receipt-hash key: show it with: {key_cmd}")
    lines += [f"  Data directory: {data} (keep it private: it holds the token and the receipt-hash key)", ""]
    for lang, install, service, code in (
            ("Python", "pip install trailbeam", "my-python-service",
             "import trailbeam.boto; trailbeam.boto.install()  # once per process, at startup"),
            ("Node.js", "npm install trailbeam", "my-node-service",
             "const sqs = require('trailbeam').instrument(new SQSClient({}));")):
        lines.append(f"In each {lang} service ({install}), set these environment variables:")
        lines.append("")
        lines += [f"  export {k}={v}" for k, v in service_env(url, token, key, service)]
        lines += ["", f"  and in its code: {code}", ""]
    lines += ["Give every service its own OTEL_SERVICE_NAME, and the same receipt-hash key. Running trailbeam start "
              "again keeps this token and key, and prints them in full only on the run that created them. "
              "Stop with Ctrl-C.", ""]
    return "\n".join(lines) + "\n"
