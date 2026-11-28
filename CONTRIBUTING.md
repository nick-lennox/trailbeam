# Contributing

## Pull requests

- Branch from `main` and name the branch after the task: `qv-NNN-short-name`. Put `QV-NNN` in the title too. The
  merge watchdog merges a pull request that names its task once it merges cleanly with `main` and `ci-ok` is green
  on its head commit (it need not contain the latest `main`). It only does this for branches pushed by the owner's
  account; pull requests from anyone else are merged by a person. Open it as a draft, or add the `hold` label, while it isn't ready to ship.
- **Changelog:** add your entry as `changelog.d/QV-NNN.md`, not in `CHANGELOG.md`. The format is in
  [changelog.d/README.md](changelog.d/README.md). CI fails a pull request that adds a line under Unreleased in
  `CHANGELOG.md`.
- CI runs in tiers (see the top of `.github/workflows/ci.yml`): every pull request gets the fast core, and the
  other jobs run when their paths change. A change to `ci.yml` or `docs.yml` runs everything.

## Contributor License Agreement

Before we can merge your first pull request, you need to sign the Trailbeam Contributor License Agreement
([CLA.md](CLA.md)). You sign once, and it covers all your future contributions.

- When you open a pull request, the CLA Assistant bot comments with a link to the agreement. Follow the link,
  sign in with GitHub and click "I agree". The bot then marks the `license/cla` check as passed.
- The check stays red, and the pull request cannot be merged, until every author of its commits has signed. If a
  commit is attributed to an email address that is not linked to your GitHub account, the bot cannot match it:
  add that address to your GitHub account, or amend the commit.
- Why we ask: the agreement lets the project owner distribute your contribution under the project's open-source
  licences and also under commercial licences. You keep the copyright in your work and can use it however you like.
- The maintainer's own account is exempt, and so are pull requests the project's agents open under it.

> The agreement is a **draft awaiting legal review** and may change. If it does, the bot will ask you to sign the
> new version on your next pull request.

## Releases

See [RELEASING.md](RELEASING.md).
