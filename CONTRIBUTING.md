# Contributing

## Pull requests

- Branch from `main` and name the branch after the task: `qv-NNN-short-name`. Put `QV-NNN` in the title too. The
  merge watchdog merges a pull request that names its task once it is up to date with `main`, merges cleanly and
  `ci-ok` is green. Open it as a draft, or add the `hold` label, while it isn't ready to ship.
- **Changelog:** add your entry as `changelog.d/QV-NNN.md`, not in `CHANGELOG.md`. The format is in
  [changelog.d/README.md](changelog.d/README.md). CI fails a pull request that adds a line under Unreleased in
  `CHANGELOG.md`.
- CI runs in tiers (see the top of `.github/workflows/ci.yml`): every pull request gets the fast core, and the
  other jobs run when their paths change. A change to `ci.yml` or `docs.yml` runs everything.

## Releases

See [RELEASING.md](RELEASING.md).
