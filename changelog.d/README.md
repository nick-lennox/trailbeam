# Changelog fragments

Each pull request adds its changelog entry here, as a file of its own, instead of a line under **Unreleased** in
[`CHANGELOG.md`](../CHANGELOG.md). Every pull request used to add its line at the same place, so each merge made the
other open pull requests conflict (QV-448). Separate files never conflict.

## Writing one

Name the file after your task: `changelog.d/QV-NNN.md`. If one task needs two entries in different sections, add
`QV-NNN-word.md` for the second (lowercase letters, digits and dashes after the number).

```markdown
---
section: Fixed
---

- **What changed, in a few words** (QV-NNN). What a user sees now, and what they saw before. Wrap long lines
  with two spaces of indent, as in CHANGELOG.md.
```

- `section` is one of `Added` (something new), `Changed` (something works differently) or `Fixed` (a bug).
- The entry is one Markdown list item, or a few, in the style of CHANGELOG.md. No headings: the release adds them.
- Don't touch `CHANGELOG.md` itself. CI's *Changelog* job fails a pull request that adds a line under
  Unreleased there. Fixing the wording of an entry that is already there is fine.

Check a fragment before pushing: `python3 scripts/changelog.py check`.

## At release

`python3 scripts/changelog.py build` adds every fragment under Unreleased in `CHANGELOG.md`, grouped under
`### Added`, `### Changed` and `### Fixed` and in task order, after what Unreleased already holds, then deletes the
fragments. `--dry-run` prints the result and changes nothing. [RELEASING.md](../RELEASING.md) has the full steps.

The script is `scripts/changelog.py` (standard library only; tests in `scripts/test_changelog.py`). We chose a short
script over towncrier so CI and the release need nothing installed, and so the fragment name can be just the task.
