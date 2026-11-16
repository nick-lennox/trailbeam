"""Trailbeam itself: OTLP/HTTP ingest of Trailbeam spans into SQLite, per-message SQS timelines, a JSON API and a web screen.

Installed with the server extra (pip install "trailbeam[server]") and run with the trailbeam command.
"""
from .. import __version__  # noqa: F401  one version for the whole package
