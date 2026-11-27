"""The configuration file (``trailbeam --config trailbeam.toml``, env ``TRAILBEAM_CONFIG``): per-queue settings
for many queues, where command-line flags don't scale.

TOML, two tables, every key optional::

    [defaults]
    retention = "7d"              # a duration (90m, 36h, 7d, 3600s) or "forever"; default 7d
    store_bodies = false          # true: store the bodies of every queue (TRAILBEAM_STORE_BODIES=*)

    [queues.orders]
    max_receive_count = 5         # the redrive policy's maxReceiveCount, 1 to 1000
    store_bodies = true
    retention = "30d"

    [queues."orders-dlq"]         # quote names with characters TOML keys can't have
    retention = "forever"

    [queues.payments]             # a RabbitMQ quorum queue
    delivery_limit = 20           # its x-delivery-limit (RabbitMQ's default since 4.0 is 20)

``delivery_limit`` is a RabbitMQ queue's ``x-delivery-limit``. It goes in the same per-queue limit as
``max_receive_count`` (a queue has one or the other), and a RabbitMQ message's timeline checks it against the
x-delivery-count its copy reached (rabbitmq.py); an SQS message's, against its receive count.

The file is read once, at start, and checked in full: an unknown table or key, a wrong type or a bad value
stops the server with every problem listed, each with its place in the file (``ConfigError``). A typo never
silently means "not set".

The flags and environment variables stay, and override the file (``merge``): ``--max-receive-count`` and
``--retention-queues`` replace the file's value for the queues they name and keep the others;
``--store-bodies`` and ``--retention`` replace the file's list and default.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass, field
from typing import Optional

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover - Python 3.10
    import tomli as tomllib

from .retention import parse_duration

UNSET = object()  # [defaults] retention not given
MAX_RECEIVE_COUNT = (1, 1000)  # what SQS accepts in a redrive policy
DEFAULT_KEYS = ("retention", "store_bodies")
QUEUE_KEYS = ("max_receive_count", "delivery_limit", "store_bodies", "retention")
DELIVERY_LIMIT = (1, 2 ** 31 - 1)  # a RabbitMQ x-delivery-limit; 0 (or none) means no limit, so not set here


class ConfigError(ValueError):
    def __init__(self, path: str, problems: list[str]):
        self.problems = problems
        super().__init__(f"{path}: " + ("; ".join(problems) if len(problems) == 1 else
                                        f"{len(problems)} problems:\n  " + "\n  ".join(problems)))


@dataclass
class Config:
    retention: object = UNSET               # seconds, None = forever, UNSET = not in the file
    retention_queues: dict = field(default_factory=dict)  # queue -> seconds or None
    store_bodies: frozenset = frozenset()   # queue names, or {"*"}
    max_receive_counts: dict = field(default_factory=dict)  # queue -> N


def load(path: str) -> Config:
    """Read and check the file. Raises ConfigError (also for a missing or unreadable file)."""
    try:
        with open(path, "rb") as f:
            data = tomllib.load(f)
    except OSError as e:
        raise ConfigError(path, [f"cannot read the file: {e.strerror or e}"]) from None
    except tomllib.TOMLDecodeError as e:
        raise ConfigError(path, [f"not valid TOML: {e}"]) from None
    return parse(data, path)


def parse(data: dict, path: str = "config") -> Config:
    problems: list[str] = []
    cfg = Config()
    for k in data:
        if k not in ("defaults", "queues"):
            problems.append(f"{k}: unknown table (expected [defaults] or [queues.<name>])")
    defaults = data.get("defaults", {})
    all_bodies = False
    if not isinstance(defaults, dict):
        problems.append("defaults: must be a table ([defaults])")
        defaults = {}
    for k, v in defaults.items():
        where = f"defaults.{k}"
        if k == "retention":
            cfg.retention = _duration(where, v, problems)
        elif k == "store_bodies":
            all_bodies = _bool(where, v, problems) is True
        else:
            problems.append(f"{where}: unknown setting (expected one of {', '.join(DEFAULT_KEYS)})")
    queues = data.get("queues", {})
    if not isinstance(queues, dict):
        problems.append("queues: must be tables, one per queue ([queues.<name>])")
        queues = {}
    bodies = set()
    for name, settings in queues.items():
        where = f"queues.{_key(name)}"
        if not name.strip() or name != name.strip() or name == "*" or "," in name:
            problems.append(f"{where}: not a queue name")
            continue
        if not isinstance(settings, dict):
            problems.append(f"{where}: must be a table of settings ([queues.{_key(name)}])")
            continue
        for k, v in settings.items():
            at = f"{where}.{k}"
            if k == "max_receive_count":
                lo, hi = MAX_RECEIVE_COUNT
                if isinstance(v, bool) or not isinstance(v, int) or not lo <= v <= hi:
                    problems.append(f"{at}: expected a whole number from {lo} to {hi}, got {v!r}")
                else:
                    cfg.max_receive_counts[name] = v
            elif k == "delivery_limit":
                lo, hi = DELIVERY_LIMIT
                if isinstance(v, bool) or not isinstance(v, int) or not lo <= v <= hi:
                    problems.append(f"{at}: expected a whole number from {lo} to {hi}, got {v!r}")
                elif "max_receive_count" in settings:
                    problems.append(f"{at}: a queue has max_receive_count (SQS) or delivery_limit (RabbitMQ), "
                                    "not both")
                else:
                    cfg.max_receive_counts[name] = v
            elif k == "store_bodies":
                b = _bool(at, v, problems)
                if b is True:
                    bodies.add(name)
                elif b is False and all_bodies:
                    problems.append(f"{at}: false has no effect while defaults.store_bodies is true: every "
                                    "queue's bodies are stored. List the queues to store instead")
            elif k == "retention":
                d = _duration(at, v, problems)
                if d is not UNSET:
                    cfg.retention_queues[name] = d
            else:
                problems.append(f"{at}: unknown setting (expected one of {', '.join(QUEUE_KEYS)})")
    if problems:
        raise ConfigError(path, problems)
    cfg.store_bodies = frozenset({"*"}) if all_bodies else frozenset(bodies)
    return cfg


def merge(cfg: Config, *, retention=None, retention_queues=None, store_bodies=None, max_receive_counts=None,
          default_retention: Optional[int] = None) -> dict:
    """The settings the server runs with: the flag or environment value where one was given (not None), else the
    file's, else the built-in default."""
    if retention is not None:
        ret = retention[0]  # (seconds or None,): None alone would mean "not given"
    elif cfg.retention is not UNSET:
        ret = cfg.retention
    else:
        ret = default_retention
    return {"retention": ret,
            "retention_queues": {**cfg.retention_queues, **(retention_queues or {})},
            "store_bodies": cfg.store_bodies if store_bodies is None else frozenset(store_bodies),
            "max_receive_counts": {**cfg.max_receive_counts, **(max_receive_counts or {})}}


def _duration(where: str, v, problems: list):
    if not isinstance(v, str):
        problems.append(f"{where}: expected a string such as \"7d\" or \"forever\", got {v!r}")
        return UNSET
    try:
        return parse_duration(v)
    except ValueError as e:
        problems.append(f"{where}: {e}")
        return UNSET


def _bool(where: str, v, problems: list) -> Optional[bool]:
    if not isinstance(v, bool):
        problems.append(f"{where}: expected true or false, got {v!r}")
        return None
    return v


def _key(name: str) -> str:
    bare = name and all(c.isalnum() and c.isascii() or c in "-_" for c in name)
    return name if bare else '"' + name.replace("\\", "\\\\").replace('"', '\\"') + '"'
