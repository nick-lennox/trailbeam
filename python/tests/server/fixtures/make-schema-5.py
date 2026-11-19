"""Writes schema-5.sqlite: a database made by Trailbeam 0.1 (schema 5, commit fc2dee2), for test_migrations.py.

Run it from a checkout of that commit, never from a newer one: the point of the file is that the code that
wrote it is the code teams run today. ``git worktree add /tmp/tb-0.1 fc2dee2`` then
``python /tmp/tb-0.1/python/tests/server/fixtures/make-schema-5.py OUT`` (the script is not in that commit:
copy it there first).

What is in it: the 32 recorded spans of contract/fixtures/contract-3.2-spans.json, stored with bodies kept
for ``orders`` only (so other queues' rows have body_dropped set), plus spans built here that exercise the
other columns: a settle span exported before its receive (joined on the receipt hash later), a delete that
never joined (receive_row NULL), call-order attributes and a DLQ receive.
"""
import base64
import json
import sqlite3
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
sys.path[:0] = [str(ROOT / "python" / "src"), str(HERE.parent)]

from spans import SEC, T0, Consumer, to_proto  # noqa: E402
from trailbeam.server.db import Database  # noqa: E402
from trailbeam.server.otlp import decode  # noqa: E402


def extra_spans():
    c = Consumer(client_id="c-upgrade", scope="scope-upgrade")
    tid = lambda n: n.to_bytes(16, "big")  # noqa: E731  fixed IDs, so the file is the same every run
    early_delete = c.delete(T0 + 3 * SEC, "rh-late", trace_id=tid(1), order=(3, 3))
    return [
        [c.send(T0, "m-up-1", trace_id=tid(2), start=T0 - SEC // 10, order=(1, 1))],
        [early_delete],  # its receive arrives in the next export: joined by the receive-side join
        [c.receive(T0 + 2 * SEC, "m-up-1", "rh-late", trace_id=tid(2), order=(2, 2))],
        [c.delete(T0 + 4 * SEC, "rh-never-received", trace_id=tid(3))],  # stays unjoined
        [c.receive(T0 + 5 * SEC, "m-up-dlq", "rh-dlq", count=4, queue="orders-dlq", trace_id=tid(4))],
    ]


def main(out: str) -> None:
    path = Path(out)
    for p in (path, Path(out + "-wal"), Path(out + "-shm")):
        p.unlink(missing_ok=True)
    db = Database(str(path), store_bodies=frozenset({"orders"}))
    for e in json.loads((ROOT / "contract" / "fixtures" / "contract-3.2-spans.json").read_text())["exports"]:
        db.insert(decode(base64.b64decode(e["base64"]), e["contentType"]).records)
    for spans in extra_spans():
        for s in spans:
            s.span_id = (int.from_bytes(s.trace_id, "big") * 100 + s.t // SEC % 100).to_bytes(8, "big")
        db.insert(decode(to_proto(spans).SerializeToString(), "application/x-protobuf").records)
    db.close()
    # one self-contained file, no -wal beside it. After close: the store's checkpoint thread has a connection
    # open while it runs, and SQLite only leaves WAL mode with no other connection.
    c = sqlite3.connect(str(path))
    c.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    c.execute("PRAGMA journal_mode=DELETE")
    c.close()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else str(HERE / "schema-5.sqlite"))
