"""Writes expected.json: cases.snapshot of every dataset, from the SQLite store.

Only rerun it when the server is meant to answer differently (a contract change), and say why in the commit:
the file is what every Store, SQLite included, is checked against."""
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import cases  # noqa: E402
from trailbeam.server.db import Database  # noqa: E402

out = {}
for name in cases.DATASETS:
    store = Database(":memory:", store_bodies=cases.store_bodies(name))
    out[name] = {"ingest": [list(store.insert(records)) for records in cases.exports(name)]}
    out[name].update(cases.snapshot(store, name))
    store.close()
(HERE / "expected.json").write_text(cases.canonical(out) + "\n")
