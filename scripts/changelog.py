#!/usr/bin/env python3
"""Changelog fragments (QV-448).

Each pull request adds its changelog entry as a file of its own, changelog.d/QV-NNN.md, instead of a line at the
top of Unreleased in CHANGELOG.md. Pull requests then stop conflicting with each other on CHANGELOG.md. At release
time `build` folds the fragments into CHANGELOG.md and deletes them. The format is in changelog.d/README.md.

    python3 scripts/changelog.py check [--base REF] [--branch NAME]   # CI: fragments are valid, Unreleased untouched
    python3 scripts/changelog.py build [--dry-run]                     # release: fold fragments into CHANGELOG.md

Standard library only, so CI and a release run it without installing anything.
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SECTIONS = ("Added", "Changed", "Fixed")
NAME = re.compile(r"^QV-(\d+)(-[a-z0-9-]+)?\.md$")
NOT_FRAGMENTS = {"README.md"}

# Branches whose pull requests were open with an entry under Unreleased when fragments came in (QV-448). They merge
# as they are. Delete a line once its branch has merged; the list should end up empty.
GRANDFATHERED = {
    "qv-226-people-page", "qv-226-people-tokens", "qv-226-tokens-page", "qv-227-proxy-signin", "qv-228-oidc",
    "qv-313-sns-fanout", "qv-314-inferred-origin", "qv-320-dotnet",
    "qv-321-go-middleware", "qv-349-release-0.5.0-prep", "qv-364-queue-page", "qv-367-causes-endpoint",
    "qv-367-needs-attention-ui", "qv-409-start-log-driver", "qv-434-start-stop",
}

HOW = ("Add the entry as changelog.d/QV-NNN.md instead (QV-NNN is your task), with its section, Added, Changed or "
       "Fixed, as changelog.d/README.md shows. Leave CHANGELOG.md alone: the release folds the fragments in.")


class FragmentError(ValueError):
    pass


def parse_fragment(path: Path) -> tuple[str, str]:
    """(section, body) of a fragment. The file is front matter with `section:`, then the Markdown entry."""
    m = NAME.match(path.name)
    if not m:
        raise FragmentError(f"{path.name}: name it QV-NNN.md after its task (QV-NNN-word.md for a second one)")
    text = path.read_text(encoding="utf-8")
    fm = re.match(r"\A---\n(.*?)\n---\n(.*)\Z", text, re.S)
    if not fm:
        raise FragmentError(f"{path.name}: start with front matter: ---, section: Added|Changed|Fixed, ---")
    section = None
    for line in fm.group(1).splitlines():
        key, _, value = line.partition(":")
        if key.strip() == "section":
            section = value.strip()
    if section not in SECTIONS:
        raise FragmentError(f"{path.name}: section is {section!r}, it must be one of {', '.join(SECTIONS)}")
    body = fm.group(2).strip("\n")
    if not body.strip():
        raise FragmentError(f"{path.name}: the entry is empty")
    if not body.startswith("- "):
        raise FragmentError(f"{path.name}: the entry must be a Markdown list item starting with '- '")
    if re.search(r"^#{1,3} ", body, re.M):
        raise FragmentError(f"{path.name}: no headings in an entry; the release adds the section heading")
    return section, body


def fragments(folder: Path) -> list[tuple[int, str, Path]]:
    """Fragment files in task order, then by name."""
    out = []
    for p in folder.glob("*.md"):
        if p.name in NOT_FRAGMENTS:
            continue
        m = NAME.match(p.name)
        out.append((int(m.group(1)) if m else 10**9, p.name, p))
    return sorted(out)


def unreleased_span(lines: list[str]) -> tuple[int, int] | None:
    """[start, end) of the lines after `## Unreleased`, up to the next `## ` heading."""
    for i, line in enumerate(lines):
        if line.strip() == "## Unreleased":
            for j in range(i + 1, len(lines)):
                if lines[j].startswith("## "):
                    return i + 1, j
            return i + 1, len(lines)
    return None


def fold(changelog: str, entries: dict[str, list[str]]) -> str:
    """CHANGELOG text with the entries added under Unreleased, one `### Section` each.

    What is already under Unreleased stays first, as it is. An entry joins its section if Unreleased already has one;
    otherwise the section is added, Added, Changed and Fixed in that order, before any other `###` heading there.
    """
    lines = changelog.split("\n")
    span = unreleased_span(lines)
    if span is None:
        raise SystemExit("CHANGELOG.md has no '## Unreleased' heading to fold the fragments into")
    start, end = span
    body = lines[start:end]
    # preamble, then (heading, lines) for each ### subsection
    first = next((i for i, l in enumerate(body) if l.startswith("### ")), len(body))
    preamble, subs = body[:first], []
    for line in body[first:]:
        if line.startswith("### "):
            subs.append([line[4:].strip(), []])
        else:
            subs[-1][1].append(line)

    def trim(block: list[str]) -> list[str]:
        while block and not block[-1].strip():
            block = block[:-1]
        while block and not block[0].strip():
            block = block[1:]
        return block

    existing = {name: i for i, (name, _) in enumerate(subs)}
    for name in SECTIONS:
        new = entries.get(name)
        if not new:
            continue
        if name in existing:
            sub = subs[existing[name]]
            sub[1] = trim(sub[1]) + new
        else:
            subs.append([name, list(new)])
    order = [s for s in subs if s[0] in SECTIONS]
    order.sort(key=lambda s: SECTIONS.index(s[0]))
    order += [s for s in subs if s[0] not in SECTIONS]

    out = [""] if trim(preamble) or order else []
    if trim(preamble):
        out += trim(preamble) + [""]
    for name, block in order:
        out += [f"### {name}", ""] + trim(block) + [""]
    return "\n".join(lines[:start] + out + lines[end:])


def build(root: Path, dry_run: bool = False) -> str:
    folder, changelog = root / "changelog.d", root / "CHANGELOG.md"
    found = fragments(folder)
    if not found:
        print("No fragments in changelog.d/: nothing to fold.")
        return changelog.read_text(encoding="utf-8")
    entries: dict[str, list[str]] = {}
    errors = []
    for _, _, path in found:
        try:
            section, body = parse_fragment(path)
        except FragmentError as e:
            errors.append(str(e))
            continue
        entries.setdefault(section, []).extend(body.split("\n"))
    if errors:
        raise SystemExit("Fix these fragments first:\n  " + "\n  ".join(errors))
    text = fold(changelog.read_text(encoding="utf-8"), entries)
    if dry_run:
        print(text)
        return text
    changelog.write_text(text, encoding="utf-8")
    for _, _, path in found:
        path.unlink()
    print(f"Folded {len(found)} fragments into CHANGELOG.md under Unreleased and deleted them.")
    return text


def git(root: Path, *args: str) -> str:
    return subprocess.run(["git", *args], cwd=root, check=True, capture_output=True, text=True).stdout


def direct_entries(root: Path, base: str) -> list[str]:
    """Lines a change adds under Unreleased in CHANGELOG.md, compared with `base`.

    Only hunks that add lines and remove none count: that is a new entry. Rewording or moving an entry that is
    already there (a hunk that also removes lines) is allowed.
    """
    head = (root / "CHANGELOG.md").read_text(encoding="utf-8").split("\n")
    span = unreleased_span(head)
    if span is None:
        return []
    diff = git(root, "diff", "-U0", f"{base}...HEAD", "--", "CHANGELOG.md")
    found = []
    for hunk in re.finditer(r"^@@ -\d+(?:,(\d+))? \+(\d+)(?:,(\d+))? @@.*\n((?:[-+\\].*\n?)*)", diff, re.M):
        removed = int(hunk.group(1) if hunk.group(1) is not None else 1)
        new_start, added = int(hunk.group(2)), int(hunk.group(3) if hunk.group(3) is not None else 1)
        if removed or not added:
            continue
        for n in range(new_start, new_start + added):  # 1-based line numbers in HEAD
            if span[0] <= n - 1 < span[1] and head[n - 1].strip():
                found.append(f"CHANGELOG.md:{n}: {head[n - 1].strip()[:100]}")
    return found


def check(root: Path, base: str | None, branch: str | None) -> int:
    problems = []
    folder = root / "changelog.d"
    for _, _, path in fragments(folder) if folder.is_dir() else []:
        try:
            parse_fragment(path)
        except FragmentError as e:
            problems.append(f"changelog.d/{e}")
    if base:
        if branch in GRANDFATHERED:
            print(f"{branch} was open before changelog fragments (QV-448): its CHANGELOG.md entries merge as they are.")
        elif branch and branch.startswith("release/"):
            print(f"{branch} is a release branch: CHANGELOG.md changes are expected.")
        else:
            deleted = git(root, "diff", "--name-only", "--diff-filter=D", f"{base}...HEAD", "--", "changelog.d/")
            if deleted.strip():
                print("This change deletes fragments, so it is a release fold: CHANGELOG.md changes are expected.")
            else:
                added = direct_entries(root, base)
                if added:
                    problems.append("This pull request adds lines under Unreleased in CHANGELOG.md:\n    "
                                    + "\n    ".join(added) + "\n  " + HOW)
    for p in problems:
        first, *rest = p.split("\n")
        print("\n".join([f"::error::{first}", *rest]))
    if not problems:
        print("Changelog: OK.")
    return 1 if problems else 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("check", help="fragments are valid; with --base, no new lines under Unreleased")
    c.add_argument("--base", help="the commit to compare with (a pull request's base)")
    c.add_argument("--branch", help="the pull request's branch")
    b = sub.add_parser("build", help="fold changelog.d/ into CHANGELOG.md under Unreleased, then delete the fragments")
    b.add_argument("--dry-run", action="store_true", help="print the new CHANGELOG.md and change nothing")
    args = ap.parse_args(argv)
    if args.cmd == "check":
        return check(ROOT, args.base or None, args.branch or None)
    build(ROOT, args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
