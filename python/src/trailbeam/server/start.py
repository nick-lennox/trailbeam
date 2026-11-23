"""``trailbeam start``: first-run setup in one command.

In a data directory (``--data-dir``, env ``TRAILBEAM_DATA_DIR``, default ``~/.trailbeam``) it keeps:

- ``token``: the access token. Ingest, the API and the web screen need it;
- ``receipt-hash-key``: the key every service hashes receipt handles with (``TRAILBEAM_RECEIPT_HASH_KEY``).
  The server never reads it; it is kept here so that every service is given the same one;
- ``trailbeam.sqlite``: the spans.

Each secret is created on the first run, 32 random bytes as hex, in a file only its owner can read, and read
back on every later run, so the token and key never change unless the file is deleted.

Others on the machine must not be able to read the spans or plant a token (QV-201). On POSIX the data directory
has to be the user's own and not writable by every user, else ``trailbeam start`` stops; one the group or
others can use is made 0700. A secret file has to be a regular file (not a symlink) of the user's; one others
can read is made 0600. Files are created with a 077 umask, so the SQLite file (and its -wal and -shm) is 0600
wherever it is; an existing one must be the user's too, and is made 0600. Then the server starts
as ``trailbeam`` would, and once it listens, the address, the token and the environment variables for a Python
and a Node.js service are printed on stdout (the logs stay on stderr).

Every flag of ``trailbeam`` works here too. ``--db`` or ``--store-url`` replaces the SQLite file in the data
directory, and ``--token-file`` or ``TRAILBEAM_TOKEN`` replaces the token file.

``npx trailbeam start`` (the npm package) runs this in the container image. Inside it the server listens on
0.0.0.0 and the data directory is ``/data``, so it sets ``TRAILBEAM_START_SHOWN_HOST`` and
``TRAILBEAM_START_SHOWN_DATA_DIR`` to what the banner should say instead: the address on the developer's
machine and the Docker volume (or directory) the data is in.
"""
from __future__ import annotations

import logging
import os
import secrets
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
    if os.name == "posix":
        os.umask(0o077)  # everything this process creates (the SQLite file, -wal, -shm) is the user's only
    db_file = None if a.store_url else Path(a.db).expanduser() if a.db else data / DB_FILE
    had_data = db_file is not None and db_file.is_file() and db_file.stat().st_size > 0
    try:
        _make_dir(data)
        key, made_key = ensure_secret(data / KEY_FILE)
        if len(key) < MIN_KEY_LENGTH:
            raise SetupError(f"{data / KEY_FILE}: the receipt-hash key must be at least {MIN_KEY_LENGTH} characters")
        made_token = False
        if not a.token_file and not os.environ.get("TRAILBEAM_TOKEN", "").strip():
            a.token_file = str(data / TOKEN_FILE)
            _, made_token = ensure_secret(a.token_file)
    except (OSError, SetupError, auth.TokenError) as e:
        log.error("trailbeam start: %s", e)
        sys.exit(2)
    for what, made, path in (("access token", made_token, a.token_file), ("receipt-hash key", made_key, data / KEY_FILE)):
        if made:
            log.info("created the %s in %s", what, path)
    if had_data:
        warn_existing_data(db_file, made_token and a.token_file, made_key and data / KEY_FILE)
    if a.db is None:
        a.db = str(data / DB_FILE)
    if not a.store_url:
        try:
            _tighten_db(a.db)
        except (OSError, SetupError) as e:
            log.error("trailbeam start: %s", e)
            sys.exit(2)

    shown_host = os.environ.get(SHOWN_HOST_ENV, "").strip()
    shown_data = os.environ.get(SHOWN_DATA_DIR_ENV, "").strip() or data

    def announce(host, port, token):
        sys.stdout.write(banner(_url_host(shown_host or host), port, token, key, shown_data))
        sys.stdout.flush()

    serve(a, on_listening=announce)


def warn_existing_data(db_file, new_token_file, new_key_file) -> None:
    """``trailbeam start`` made a secret for a database that already existed: most likely one an earlier
    ``trailbeam`` (0.3.0 had no ``start``) ran on without that file. Say what changes for the services sending
    to it, which keep sending as before until they are given the new values."""
    npx = bool(os.environ.get(SHOWN_DATA_DIR_ENV, "").strip())  # in the image: the user runs docker, not trailbeam
    as_before = ("docker run the image as before, without start" if npx else f"run trailbeam --db {db_file}")
    if new_token_file:
        log.warning("trailbeam start: %s is an existing database, and the access token is now on (new token in %s): "
                    "services that send without it are refused (401) and their spans are lost. Give each one "
                    "OTEL_EXPORTER_OTLP_HEADERS as printed below. If this server had a token before, start with "
                    "--token-file and a file holding it instead. To run without a token as before, %s.",
                    db_file, new_token_file, as_before)
    if new_key_file:
        log.warning("trailbeam start: %s is an existing database, and a new receipt-hash key was made (%s). Your "
                    "services keep hashing with the key they have (TRAILBEAM_RECEIPT_HASH_KEY, or the published "
                    "default key if none is set) until you change it; messages in flight while services change keys "
                    "are not joined. To keep the key they use, put it in %s and start again.",
                    db_file, new_key_file, new_key_file)


def ensure_secret(path) -> tuple:
    """The secret in ``path``, and whether it was just created. A missing file is created, readable and writable
    by its owner only, holding 32 random bytes as hex; an existing one is read and never rewritten. On POSIX an
    existing one must be a regular file (a symlink is not followed) owned by this user, else SetupError; one
    others can read is made 0600."""
    path = Path(path)
    value = secrets.token_hex(32)
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | _NOFOLLOW, 0o600)
    except FileExistsError:
        text = _read_own_secret(path)
        if not text:
            raise SetupError(f"{path} is empty: delete it to have a new secret made, or put one in it") from None
        return auth.check_token(text) if path.name == TOKEN_FILE else text, False
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(value + "\n")
        f.flush()
        os.fsync(f.fileno())
    return value, True


_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)


def _read_own_secret(path: Path) -> str:
    try:
        fd = os.open(path, os.O_RDONLY | _NOFOLLOW)
    except OSError as e:
        if path.is_symlink():
            raise SetupError(f"{path} is a symlink: a secret file must be a regular file of yours. Delete it to "
                             "have a new one made") from None
        raise
    with os.fdopen(fd, "r", encoding="utf-8") as f:
        st = os.fstat(f.fileno())
        if not stat.S_ISREG(st.st_mode):
            raise SetupError(f"{path} is not a regular file")
        if os.name == "posix":
            if st.st_uid != os.geteuid():
                raise SetupError(f"{path} belongs to another user (uid {st.st_uid}): it can't be trusted as a "
                                 "secret. Delete it to have a new one made")
            mode = stat.S_IMODE(st.st_mode)
            if mode & 0o077:
                os.fchmod(f.fileno(), 0o600)
                log.warning("%s could be read by other users (mode %o): made it 600. If they could, make a new "
                            "secret: delete it and run trailbeam start again", path, mode)
        return f.read().strip()


def _make_dir(path: Path) -> None:
    """Creates the data directory 0700, or checks an existing one: on POSIX it must be this user's and not
    writable by every user (else SetupError); one the group or others can use is made 0700."""
    if path.exists() and not path.is_dir():
        raise SetupError(f"the data directory {path} is a file")
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    if os.name != "posix":
        return
    st = os.stat(path)
    if st.st_uid != os.geteuid():
        raise SetupError(f"the data directory {path} belongs to another user (uid {st.st_uid}): use a directory "
                         "of your own (--data-dir)")
    mode = stat.S_IMODE(st.st_mode)
    if mode & stat.S_IWOTH:
        raise SetupError(f"the data directory {path} can be written by any user (mode {mode:o}): they could plant "
                         f"a token. Check what is in it, then chmod 700 {path}, or use another --data-dir")
    if mode & 0o077:
        # Group-writable is the usual with a 002 umask (a group of one's own). What a group member may have put
        # here is caught file by file: a secret or SQLite file that isn't this user's is refused.
        os.chmod(path, 0o700)
        log.info("the data directory %s could be read by other users (mode %o): made it 700", path, mode)


def _tighten_db(db: str) -> None:
    """An existing SQLite file (or its -wal, -shm) must be a regular file of this user's (else SetupError: whoever
    owns it can read every span); one others can read is made 0600. New ones are 0600 already (the umask)."""
    if os.name != "posix":
        return
    for p in (db, db + "-wal", db + "-shm"):
        try:
            st = os.lstat(p)
        except FileNotFoundError:
            continue
        if not stat.S_ISREG(st.st_mode):
            raise SetupError(f"{p} is not a regular file (a symlink?): the SQLite file must be a file of yours")
        if st.st_uid != os.geteuid():
            raise SetupError(f"{p} belongs to another user (uid {st.st_uid}), who can read every span in it: use "
                             "a file of your own (--db)")
        mode = stat.S_IMODE(st.st_mode)
        if mode & 0o077:
            os.chmod(p, 0o600)
            log.info("%s could be read by other users (mode %o): made it 600", p, mode)


def _url_host(host: str) -> str:
    """The host to put in a URL a developer opens on this machine."""
    if host in ("", "0.0.0.0"):
        return "127.0.0.1"
    if host == "::":
        return "[::1]"
    return f"[{host}]" if ":" in host else host


def service_env(url: str, token, key: str, service: str) -> list:
    env = [("TRAILBEAM_OTLP_TRACES_ENDPOINT", f"{url}/v1/traces")]
    if token:
        env.append(("OTEL_EXPORTER_OTLP_HEADERS", f"Authorization=Bearer%20{token}"))
    env += [("TRAILBEAM_RECEIPT_HASH_KEY", key), ("OTEL_SERVICE_NAME", service)]
    return env


def banner(host: str, port: int, token, key: str, data) -> str:
    url = f"http://{host}:{port}"
    lines = ["", f"Trailbeam is running: open {url}/"]
    if token:
        lines.append(f"  Access token (the web screen asks for it once): {token}")
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
              "again keeps this token and key. Stop with Ctrl-C.", ""]
    return "\n".join(lines) + "\n"
