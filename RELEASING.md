# Releasing Trailbeam

The steps the 0.4.0 (QV-179) and 0.5.0 (QV-349) releases followed. An agent prepares the release in one pull
request with the `hold` label; the board merges it, tags and publishes. The Java package is not published yet;
its own steps come with QV-319 (`java/RELEASING.md`).

## Prepare (one pull request, labelled `hold`)

1. **Fold the changelog fragments:** `python3 scripts/changelog.py build`. It adds every `changelog.d/QV-*.md`
   under Unreleased in `CHANGELOG.md`, grouped into Added, Changed and Fixed, and deletes the fragments
   (`--dry-run` shows the result first). See [changelog.d/README.md](changelog.d/README.md).
2. **Write the release section:** rename `## Unreleased` to `## X.Y.Z: <one-line summary>`, add a short summary,
   *What's new* and *Upgrading from <last release>*, keep every entry under *All changes*, and fix entries that
   later changes made stale. Leave an empty `## Unreleased` above it.
3. **Bump the version** in the Python and npm packages, the example lockfiles and the Helm chart, and the tests
   that check it.
4. **Update the README** release note and upgrade notes, and the CI upgrade job so it also upgrades from the last
   release.
5. **Dry-run the release workflow locally:** the tag check, the release notes, the wheel and sdist,
   `npm publish --dry-run`, both images, and `npx trailbeam start` on the local image.
6. **Run the upgrade test** from the last published release (SQLite, the Docker image on one volume, Postgres).
7. **List the held docs pull requests** (in `nick-lennox/trailbeam-app`) that should merge at release.

A pull request that merges into `main` after step 1 adds a new fragment. Run `build` again on the release branch
after merging `main` in, then move the new entries into the release section.

## Release (the board)

1. Check the release pull request's CI is green, remove `hold` and merge it.
2. Tag the merge commit: `git tag vX.Y.Z <merge-sha> && git push origin vX.Y.Z`. `release.yaml` publishes PyPI,
   npm, the ghcr.io images and the GitHub release notes.
3. Check it: `pip install "trailbeam[server]==X.Y.Z" && trailbeam start` and `npx trailbeam@X.Y.Z start`.
4. Merge the held docs pull requests and resync the docs site's copies of the span contract and OpenAPI file from
   the release commit.
