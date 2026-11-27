#!/usr/bin/env python3
"""Which licence each file is under, and the checks that keep the two apart (QV-568, LICENSING.md).

The repository holds two licences. The client libraries, the span contract, the conformance kit, the examples, the
demo and the deployment files are Apache-2.0. The server and the UI are AGPL-3.0-only. A file's licence is the
LICENSE file in the nearest folder above it, unless one of its first lines says otherwise with an
``SPDX-License-Identifier:`` line.

    python3 scripts/licences.py check     # CI: the checks below; exit 1 on any problem
    python3 scripts/licences.py which PATH...   # the licence of each file, and why

`check` fails when:
- a LICENSE file isn't exactly the Apache-2.0 or the AGPL-3.0 text (whitespace aside), or is in a folder EXPECTED
  doesn't list (a stray copy would relabel everything under it);
- a folder in EXPECTED has no LICENSE of its own, or the wrong one;
- a manifest's licence metadata (pyproject.toml, package.json, pom.xml, Directory.Build.props, Chart.yaml, the
  server's Dockerfile label) disagrees with EXPECTED_METADATA;
- an SPDX line isn't exactly one of these two licences, or a file of EXPECTED_FILES lacks its line;
- an Apache-2.0 SPDX line is on a file in an AGPL-3.0 folder that isn't in EXPECTED_FILES (an SPDX line may make a
  file stricter than its folder, but only the listed files may make it looser);
- python/MANIFEST.in doesn't keep the tests, benchmarks and smoke checks out of the Apache-2.0 sdist (some import
  the server and are AGPL-3.0-only);
- an Apache-2.0 file imports an AGPL-3.0 one. Python (parsed with ast) and JavaScript or TypeScript import, require
  and export-from: ``trailbeam.server`` and ``@trailbeam/ui`` are AGPL, and so is a relative import that lands in
  an AGPL file or, under a ``src/`` folder, names ``trailbeam.server`` (``from .server import db`` in the library:
  trailbeam-server installs into the library's own ``trailbeam/`` folder). Java, Go and .NET can't import the server's Python or the UI's TypeScript, so they aren't scanned.
  Starting the server as a program (``python -m trailbeam.server``, the container image) or calling its HTTP API
  is not an import and is allowed: that is how the libraries' tests and the examples use it.
- LICENSING.md doesn't list a folder of EXPECTED.

Standard library only. Only files git tracks are read.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parent.parent
APACHE, AGPL = "Apache-2.0", "AGPL-3.0-only"

# sha256 of each licence text with its whitespace collapsed to single spaces: the Apache-2.0 text as it has been in
# LICENSE since the start, and the AGPL-3.0 text as https://www.gnu.org/licenses/agpl-3.0.txt has it.
TEXTS = {
    "0ffddef9e48f8a09aed5caf2d44f7ba1c1be2d9b8e0a6f693b1635b2d5566645": APACHE,
    "2cd0fb7883a3b3553dedbb0bad171646d46f51e2642951aae7c59cb5cec86c46": AGPL,
}

# Folders that must hold a LICENSE of their own, and which. "" is the repository root: the default for everything
# not under another LICENSE (the root Dockerfile, which builds the server's image, scripts/, .github/). The board
# decided the split on 2026-11-27 (QV-568).
EXPECTED = {
    "": AGPL,
    "server": AGPL,
    "ui": AGPL,
    # Tests, benchmarks and smoke checks of the server, inside python/ since QV-567 moved the server out. They import
    # the server, so they are under its licence. They are not in the trailbeam package.
    "python/tests/server": AGPL,
    "python/tests/conformance": AGPL,
    "python/bench": AGPL,
    "python/smoke": AGPL,
    "python": APACHE,
    "node": APACHE,
    "java": APACHE,
    "go": APACHE,
    "dotnet": APACHE,
    "contract": APACHE,
    "conformance": APACHE,
    "examples": APACHE,
    "demo": APACHE,
    "deploy": APACHE,
    "deploy/helm/trailbeam": APACHE,   # helm package puts this folder in the chart
    "collector": APACHE,
    "compat": APACHE,
}

# path -> what its licence metadata must say
EXPECTED_METADATA = {
    "server/pyproject.toml": AGPL,
    "ui/package.json": AGPL,
    "Dockerfile": AGPL,   # org.opencontainers.image.licenses of the server's image
    "python/pyproject.toml": APACHE,
    "node/package.json": APACHE,
    "java/pom.xml": APACHE,
    "dotnet/Directory.Build.props": APACHE,
    "deploy/helm/trailbeam/Chart.yaml": APACHE,
    "contract/openapi.yaml": APACHE,
    "examples/node-lambda/package.json": APACHE,
    "examples/node-sdk-v3/package.json": APACHE,
    "demo/node/package.json": APACHE,
}

# Files whose licence differs from their folder's, by their SPDX line: the Compose file and its settings are
# Apache-2.0 like the Helm chart, though the root (where Compose wants them) is AGPL-3.0-only.
EXPECTED_FILES = {
    "docker-compose.yaml": APACHE,
    ".env.example": APACHE,
}

# Folders under neither licence: the logo, like the name Trailbeam, is a trademark (LICENSING.md).
NOT_LICENSED_FOLDERS = ("assets",)
NOT_LICENSED = "neither licence"

# Imports of a module or package name that is AGPL, wherever it is installed from.
AGPL_PYTHON = ("trailbeam.server",)
AGPL_JS = ("@trailbeam/ui",)

# The whole rest of the line is the licence expression (a comment's closing */ or --> aside), so that
# "Apache-2.0 OR MIT" or a remark after the identifier is not read as plain Apache-2.0. The check accepts only the two
# plain identifiers: an explanation goes on the next line.
SPDX = re.compile(r"SPDX-License-Identifier:[ \t]*(.*?)[ \t]*(?:\*/|-->)?[ \t]*$")
PY_EXT = {".py"}
JS_EXT = {".js", ".mjs", ".cjs", ".ts", ".tsx", ".jsx", ".mts", ".cts"}
JS_IMPORT = re.compile(
    r"""(?:^|[^\w.$])(?:import\s*(?:[\w*${}\s,]+\s*from\s*)?|export\s*[\w*${}\s,]*\s*from\s*|require\s*\(\s*|import\s*\(\s*)"""
    r"""(["'])([^"'\n]+)\1""", re.M)


def normalised_hash(text: str) -> str:
    return hashlib.sha256(" ".join(text.split()).encode()).hexdigest()


class Tree:
    """The files git tracks, their LICENSE files and each file's licence."""

    def __init__(self, root: Path = ROOT, files: list[str] | None = None):
        self.root = root
        if files is None:
            out = subprocess.run(["git", "ls-files", "-z"], cwd=root, capture_output=True, check=True).stdout
            files = [f for f in out.decode().split("\0") if f]
        self.files = sorted(f for f in files if (root / f).is_file())
        self.fileset = set(self.files)
        self.licence_files: dict[str, str | None] = {}   # folder -> licence of its LICENSE (None: unknown text)
        for f in self.files:
            p = PurePosixPath(f)
            if p.name == "LICENSE":
                folder = "" if str(p.parent) == "." else str(p.parent)
                self.licence_files[folder] = TEXTS.get(normalised_hash((root / f).read_text(encoding="utf-8")))
        self._spdx: dict[str, str | None] = {}

    def spdx(self, f: str) -> str | None:
        if f not in self._spdx:
            try:
                head = (self.root / f).read_text(encoding="utf-8", errors="replace").splitlines()[:5]
            except OSError:
                head = []
            m = next((m for line in head if (m := SPDX.search(line))), None)
            self._spdx[f] = (m.group(1) or "(empty)") if m else None
        return self._spdx[f]

    def folder_licence(self, f: str) -> tuple[str | None, str]:
        """The licence of the nearest LICENSE above f, and that folder."""
        parts = PurePosixPath(f).parts[:-1]
        for i in range(len(parts), -1, -1):
            folder = "/".join(parts[:i])
            if folder in self.licence_files:
                return self.licence_files[folder], folder
        return None, ""

    def not_licensed(self, f: str) -> bool:
        return PurePosixPath(f).parts[0] in NOT_LICENSED_FOLDERS

    def licence(self, f: str) -> str | None:
        if self.not_licensed(f):
            return NOT_LICENSED
        return self.spdx(f) or self.folder_licence(f)[0]

    def why(self, f: str) -> str:
        if self.not_licensed(f):
            return f"{NOT_LICENSED} (the logo is a trademark; LICENSING.md)"
        if self.spdx(f):
            return f"{self.spdx(f)} (its SPDX-License-Identifier line)"
        lic, folder = self.folder_licence(f)
        return f"{lic} ({(folder + '/') if folder else ''}LICENSE)"


# ---- imports ----------------------------------------------------------------------------------------------------

def python_imports(source: str) -> list[tuple[int, str, int]]:
    """(line, module, relative level) of each import in a Python file, importlib.import_module("...") included."""
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []
    found = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found += [(node.lineno, a.name, 0) for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            found.append((node.lineno, mod, node.level))
            # from trailbeam import server
            found += [(node.lineno, f"{mod}.{a.name}" if mod else a.name, node.level) for a in node.names]
        elif (isinstance(node, ast.Call) and node.args and isinstance(node.args[0], ast.Constant)
              and isinstance(node.args[0].value, str)
              and ((isinstance(node.func, ast.Attribute) and node.func.attr == "import_module")
                   or (isinstance(node.func, ast.Name) and node.func.id in ("import_module", "__import__")))):
            found.append((node.lineno, node.args[0].value, 0))
    return found


def strip_js_comments(source: str) -> str:
    """source with its // and /* */ comments blanked (newlines kept, so line numbers hold). Strings and template
    literals are skipped over, so a URL in a string stays; a regular expression literal holding a quote can throw
    this off, which at worst makes the check miss or flag an import there."""
    out, i, n = [], 0, len(source)
    while i < n:
        c = source[i]
        if c in "'\"`":
            j = i + 1
            while j < n and source[j] != c and not (c != "`" and source[j] == "\n"):
                j += 2 if source[j] == "\\" else 1
            out.append(source[i:j + 1])
            i = j + 1
        elif source.startswith("//", i):
            j = source.find("\n", i)
            i = n if j < 0 else j
        elif source.startswith("/*", i):
            j = source.find("*/", i + 2)
            j = n if j < 0 else j + 2
            out.append("\n" * source.count("\n", i, j))
            i = j
        else:
            out.append(c)
            i += 1
    return "".join(out)


def js_imports(source: str) -> list[tuple[int, str]]:
    found = []
    source = strip_js_comments(source)
    for m in JS_IMPORT.finditer(source):
        found.append((source.count("\n", 0, m.start(2)) + 1, m.group(2)))
    return found


def is_agpl_name(name: str, roots: tuple[str, ...]) -> bool:
    return any(name == r or name.startswith(r + ".") or name.startswith(r + "/") for r in roots)


def absolute_module(f: str, module: str, level: int) -> str | None:
    """The dotted name a relative import in f means, for a file under a src/ folder (where the package's import names
    start); None elsewhere or when the import climbs above src/."""
    parts = PurePosixPath(f).parts
    if "src" not in parts[:-1]:
        return None
    i = len(parts) - 2 - parts[:-1][::-1].index("src")   # the nearest src/ above f
    package = list(parts[i + 1:-1])
    if level - 1 > len(package) - 1:
        return None
    package = package[:len(package) - (level - 1)]
    return ".".join(package + [p for p in module.split(".") if p])


def python_targets(tree: Tree, f: str, module: str, level: int) -> list[str]:
    """The repository files a Python import can mean. A bare module name is looked up next to the importing file
    first, then anywhere in the repository (tests put other folders on sys.path); a name no tracked file has is a
    third-party or standard library module, and gives nothing."""
    parts = [p for p in module.split(".") if p]
    here = PurePosixPath(f).parent
    if level:
        base = here
        for _ in range(level - 1):
            base = base.parent
        bases = [base]
    else:
        bases = [here]
    names = []
    for n in range(len(parts), 0, -1):
        rel = "/".join(parts[:n])
        names += [rel + ".py", rel + "/__init__.py"]
    for base in bases:
        hits = [str(base / n) if str(base) != "." else n for n in names]
        hits = [h for h in hits if h in tree.fileset]
        if hits:
            return hits[:1]
    if level or not parts:
        return []
    want = {"/".join(parts) + ".py", "/".join(parts) + "/__init__.py"} if len(parts) > 1 else \
        {parts[0] + ".py", parts[0] + "/__init__.py"}
    return [c for c in tree.files if any(c == w or c.endswith("/" + w) for w in want)]


def js_target(tree: Tree, f: str, spec: str) -> str | None:
    if not spec.startswith("."):
        return None
    base = PurePosixPath(f).parent / spec
    parts: list[str] = []
    for p in base.parts:
        if p == "..":
            if parts:
                parts.pop()
        elif p != ".":
            parts.append(p)
    path = "/".join(parts)
    stem = re.sub(r"\.(js|mjs|cjs|jsx)$", "", path)
    for c in [path, *(stem + e for e in sorted(JS_EXT)), *(path + "/index" + e for e in sorted(JS_EXT))]:
        if c in tree.fileset:
            return c
    return path   # not a tracked file (a build output): judged by its folder


def import_problems(tree: Tree) -> list[str]:
    bad: list[str] = []
    seen: set[tuple[str, int]] = set()

    def flag(f: str, line: int, what: str) -> None:
        if (f, line) not in seen:   # one line per import statement, not per name it imports
            seen.add((f, line))
            bad.append(f"{f}:{line}: {what}")

    for f in tree.files:
        ext = PurePosixPath(f).suffix
        if ext not in PY_EXT | JS_EXT or tree.licence(f) != APACHE:
            continue
        source = (tree.root / f).read_text(encoding="utf-8", errors="replace")
        if ext in PY_EXT:
            for line, mod, level in python_imports(source):
                name = absolute_module(f, mod, level) if level else mod
                if name and is_agpl_name(name, AGPL_PYTHON):
                    shown = mod if not level else "." * level + mod + f" ({name})"
                    flag(f, line, f"imports {shown}, which is {AGPL} (server/)")
                    continue
                targets = python_targets(tree, f, mod, level)
                if targets and all(tree.licence(t) == AGPL for t in targets):
                    flag(f, line, f"imports {mod} ({targets[0]}), which is {tree.why(targets[0])}")
        else:
            for line, spec in js_imports(source):
                if is_agpl_name(spec, AGPL_JS):
                    flag(f, line, f"imports {spec}, which is {AGPL} (ui/)")
                    continue
                t = js_target(tree, f, spec)
                if t and tree.licence(t) == AGPL:
                    flag(f, line, f"imports {spec} ({t}), which is {tree.why(t)}")
    # A dependency in an Apache-2.0 manifest on an AGPL package.
    for f in tree.files:
        if PurePosixPath(f).name == "package.json" and tree.licence(f) == APACHE:
            try:
                data = json.loads((tree.root / f).read_text(encoding="utf-8"))
            except ValueError:
                continue
            for key in ("dependencies", "peerDependencies", "optionalDependencies"):
                for name in data.get(key) or {}:
                    if is_agpl_name(name, AGPL_JS):
                        bad.append(f"{f}: {key} has {name}, which is {AGPL}")
    return bad


# ---- LICENSE files, metadata and SPDX lines -----------------------------------------------------------------------

def licence_file_problems(tree: Tree) -> list[str]:
    bad = []
    for folder, lic in sorted(tree.licence_files.items()):
        if lic is None:
            bad.append(f"{folder + '/' if folder else ''}LICENSE is neither the {APACHE} nor the {AGPL} text "
                       "(copy it from LICENSE or node/LICENSE)")
        elif folder not in EXPECTED:
            bad.append(f"{folder + '/' if folder else ''}LICENSE is in a folder scripts/licences.py EXPECTED doesn't "
                       "list: remove it, or add the folder to EXPECTED and LICENSING.md")
    for folder, want in EXPECTED.items():
        name = f"{folder + '/' if folder else ''}LICENSE"
        if folder not in tree.licence_files:
            bad.append(f"{name} is missing: {folder or 'the root'} must be {want}")
        elif tree.licence_files[folder] not in (None, want):
            bad.append(f"{name} is {tree.licence_files[folder]}; it must be {want}")
    return bad


def metadata_licence(path: Path) -> str | None:
    text = path.read_text(encoding="utf-8")
    name = path.name
    if name == "pyproject.toml":
        m = re.search(r'^license\s*=\s*"([^"]+)"', text, re.M)
    elif name == "package.json":
        return json.loads(text).get("license")
    elif name == "pom.xml":
        m = re.search(r"<licenses>\s*<license>\s*<name>([^<]+)</name>", text)
    elif name.endswith(".props") or name.endswith(".csproj"):
        m = re.search(r"<PackageLicenseExpression>([^<]+)</PackageLicenseExpression>", text)
    elif name == "Chart.yaml":
        m = re.search(r"artifacthub\.io/license:\s*(\S+)", text)
    elif name == "Dockerfile":
        m = re.search(r'org\.opencontainers\.image\.licenses="([^"]+)"', text)
    elif name == "openapi.yaml":
        m = re.search(r"^\s+license:\s*\n\s+name:[^\n]*\n\s+identifier:\s*(\S+)", text, re.M)
    else:
        return None
    return m.group(1).strip() if m else None


def metadata_problems(tree: Tree) -> list[str]:
    bad = []
    for f, want in EXPECTED_METADATA.items():
        p = tree.root / f
        if not p.is_file():
            bad.append(f"{f} is missing (scripts/licences.py EXPECTED_METADATA lists it)")
            continue
        got = metadata_licence(p)
        if got != want:
            bad.append(f"{f} says its licence is {got!r}; it must say {want!r}")
        if tree.licence(f) != want:
            bad.append(f"{f} is itself {tree.why(f)}, but describes a {want} package")
    for f, want in EXPECTED_FILES.items():
        if f in tree.fileset and tree.licence(f) != want:
            bad.append(f"{f} is {tree.why(f)}; it must be {want} (add an SPDX-License-Identifier line)")
    return bad


def spdx_problems(tree: Tree) -> list[str]:
    bad = []
    for f in tree.files:
        lic = tree.spdx(f)
        if not lic:
            continue
        if lic not in (APACHE, AGPL):
            bad.append(f"{f}: SPDX-License-Identifier {lic!r} is neither {APACHE} nor {AGPL} (put any explanation "
                       "on the next line)")
        elif lic == APACHE and tree.folder_licence(f)[0] == AGPL and f not in EXPECTED_FILES:
            bad.append(f"{f}: an {APACHE} SPDX line in an {AGPL} folder; only the files in scripts/licences.py "
                       "EXPECTED_FILES may have one")
    return bad


# Folders of python/ that the Apache-2.0 sdist must leave out: setuptools would otherwise add tests/test*.py, and
# some of those import the server (AGPL-3.0-only).
SDIST_PRUNED = ("tests", "bench", "smoke")


def sdist_problems(tree: Tree) -> list[str]:
    if "python/pyproject.toml" not in tree.fileset:
        return []
    manifest = tree.root / "python" / "MANIFEST.in"
    lines = manifest.read_text(encoding="utf-8").split("\n") if manifest.is_file() else []
    return [f"python/MANIFEST.in must have `prune {d}`: the {APACHE} sdist must not ship {AGPL} test files"
            for d in SDIST_PRUNED if f"prune {d}" not in (line.strip() for line in lines)]


def doc_problems(tree: Tree) -> list[str]:
    doc = tree.root / "LICENSING.md"
    if not doc.is_file():
        return ["LICENSING.md is missing"]
    text = doc.read_text(encoding="utf-8")
    return [f"LICENSING.md doesn't list `{folder}/`" for folder in EXPECTED if folder and f"`{folder}/`" not in text]


def check(tree: Tree) -> list[str]:
    return (licence_file_problems(tree) + metadata_problems(tree) + spdx_problems(tree) + sdist_problems(tree)
            + import_problems(tree) + doc_problems(tree))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("check")
    w = sub.add_parser("which")
    w.add_argument("paths", nargs="+")
    a = ap.parse_args(argv)
    tree = Tree()
    if a.cmd == "which":
        for p in a.paths:
            print(f"{p}: {tree.why(p)}")
        return 0
    problems = check(tree)
    for p in problems:
        print(f"::error::{p}" if "GITHUB_ACTIONS" in __import__("os").environ else p)
    if problems:
        print(f"\n{len(problems)} licence problem(s). LICENSING.md says which licence each folder is under.")
        return 1
    print(f"Licences OK: {len(tree.files)} files, {len(tree.licence_files)} LICENSE files.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
