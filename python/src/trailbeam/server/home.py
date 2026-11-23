"""Trailbeam's data directory: where ``trailbeam start`` keeps what it creates (the access token, the receipt hash
key, the SQLite file), and where ``trailbeam doctor`` looks for the token when it is not given one.

``TRAILBEAM_HOME``, or ``~/.trailbeam``.
"""
from __future__ import annotations

import os
from pathlib import Path

TOKEN_FILE = "token"  # the access token, one line, owner-only permissions


def data_dir() -> Path:
    return Path(os.environ.get("TRAILBEAM_HOME", "").strip() or Path.home() / ".trailbeam")


def token_path(directory=None) -> Path:
    return Path(directory) / TOKEN_FILE if directory else data_dir() / TOKEN_FILE
