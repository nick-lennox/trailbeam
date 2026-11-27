"""Trailbeam itself: OTLP/HTTP ingest of Trailbeam spans into SQLite, per-message SQS timelines, a JSON API and a web screen.

The trailbeam-server distribution (pip install "trailbeam[server]", which pulls it in at the same version, or
pip install trailbeam-server), run with the trailbeam command. It installs into the trailbeam package next to the
client library, so ``import trailbeam.server`` and ``python -m trailbeam.server`` work as they did before 0.5.0.
"""

__version__ = "0.4.0"
