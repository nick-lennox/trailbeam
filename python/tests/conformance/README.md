# Store conformance suite

What any Trailbeam storage backend must do, checked through the `Store` interface
(`python/src/trailbeam/server/store.py`) only. CI runs it against SQLite, in memory and on a WAL file.

```sh
pytest tests/conformance                                              # SQLite
TRAILBEAM_CONFORMANCE_STORE=mypackage.store:make pytest tests/conformance   # plus another backend
```

`make(tmp_path, store_bodies)` returns an empty `Store`.

- `cases.py`: the datasets (the recorded spans in `contract/fixtures`, and spans built here for the joins)
  and `snapshot`, which runs every read the server makes on a store holding one.
- `expected.json`: those snapshots, from SQLite. A backend must give the same answers. Regenerate it with
  `python tests/conformance/make-expected.py` only when the server is meant to answer differently, and say
  why in the commit.
- `test_store_conformance.py`: the snapshots, the shared expected outputs in `contract/fixtures`
  (`tenants-expected.json`, `body-views.json`), arrival order, retries, atomic inserts, concurrent
  ingest and reads, and retention.
