"""The compatibility matrix (QV-335): compat/matrix.toml to GitHub Actions matrices and to the README table.

    python3 compat/matrix.py github-matrix         # name=<json> lines for $GITHUB_OUTPUT: python, node, postgres
    python3 compat/matrix.py readme [--check]      # write (or check) the table in README.md

Python 3.11 or later (tomllib). No dependencies.
"""
from __future__ import annotations

import argparse
import json
import sys
import tomllib
from pathlib import Path

HERE = Path(__file__).resolve().parent
MATRIX = HERE / "matrix.toml"
README = HERE.parent / "README.md"
START = "<!-- compat-matrix:start (generated from compat/matrix.toml by compat/matrix.py; don't edit by hand) -->"
END = "<!-- compat-matrix:end -->"


def load(path: Path = MATRIX) -> dict:
    with path.open("rb") as f:
        m = tomllib.load(f)
    for k in m.get("known_failure", []):
        if k.get("axis") not in ("python", "node", "postgres") or not k.get("issue") or not k.get("cell"):
            raise SystemExit(f"compat/matrix.toml: a [[known_failure]] needs axis, cell and issue: {k}")
    return m


def _known(m: dict, axis: str, cell: dict) -> dict | None:
    for k in m.get("known_failure", []):
        if k["axis"] == axis and all(str(cell.get(key)) == str(v) for key, v in k["cell"].items()):
            return k
    return None


def github_matrices(m: dict) -> dict[str, dict]:
    def cell(axis: str, values: dict) -> dict:
        k = _known(m, axis, values)
        return {**values, "allow_failure": bool(k), "issue": k["issue"] if k else ""}

    python = [cell("python", {"python": v, "deps": name, "label": d["label"], "pins": " ".join(d["pins"])})
              for v in m["python"]["versions"] for name, d in m["python"]["deps"].items()]
    node = [cell("node", {"node": v, "sdk": name, "label": s["label"], "version": s["version"]})
            for v in m["node"]["versions"] for name, s in m["node"]["sdk"].items()]
    postgres = [cell("postgres", {"postgres": v}) for v in m["postgres"]["versions"]]
    return {"python": {"include": python}, "node": {"include": node}, "postgres": {"include": postgres}}


def _pin(pins: list[str], package: str) -> str:
    for p in pins:
        name, _, version = p.partition("==")
        if name.strip().lower() == package:
            return version.strip()
    raise SystemExit(f"compat/matrix.toml: python.deps.oldest has no pin for {package}")


def _versions(vs: list[str]) -> str:
    return ", ".join(vs)


def table(m: dict) -> str:
    oldest = m["python"]["deps"]["oldest"]["pins"]
    sdk_oldest = m["node"]["sdk"]["oldest"]["version"]
    rows = [
        ("Python", f"{_versions(m['python']['versions'])}, each with the SQLite built into it"),
        ("boto3 / botocore", f"{_pin(oldest, 'boto3')} / {_pin(oldest, 'botocore')} to the newest"),
        ("OpenTelemetry for Python",
         f"API and SDK {_pin(oldest, 'opentelemetry-api')}, `opentelemetry-instrumentation-botocore` "
         f"{_pin(oldest, 'opentelemetry-instrumentation-botocore')}, to the newest"),
        ("Node.js", _versions(m["node"]["versions"])),
        ("AWS SDK for JavaScript v3 (`@aws-sdk/client-sqs`)", f"{sdk_oldest} to the newest"),
        ("Postgres (the server's store)", _versions(m["postgres"]["versions"])),
    ]
    lines = [START, "", "| | Supported and tested every night |", "|---|---|"]
    lines += [f"| {a} | {b} |" for a, b in rows]
    lines.append("")
    lines.append(f"Each Python version runs with the oldest and the newest Python dependencies, and each Node.js "
                 f"version with the oldest and the newest AWS SDK, so {len(m['python']['versions']) * 2} Python "
                 f"runs, {len(m['node']['versions']) * 2} Node.js runs and {len(m['postgres']['versions'])} "
                 f"Postgres runs ([compat/matrix.toml](compat/matrix.toml), "
                 f"[the nightly workflow](.github/workflows/compat.yml)).")
    known = m.get("known_failure", [])
    if known:
        lines += ["", "Not working yet:", ""]
        for k in known:
            cell = ", ".join(f"{key} {v}" for key, v in k["cell"].items())
            lines.append(f"- {k['axis'].capitalize()} ({cell}): {k.get('what', 'fails')} ({k['issue']}).")
    lines += ["", "Not supported:", ""]
    lines += [f"- {text}" for text in m.get("not_supported", {}).values()]
    lines += ["", END]
    return "\n".join(lines)


def render_readme(text: str, m: dict) -> str:
    if text.count(START) != 1 or text.count(END) != 1:
        raise SystemExit(f"README.md needs exactly one pair of markers:\n{START}\n{END}")
    before, rest = text.split(START, 1)
    _, after = rest.split(END, 1)
    return before + table(m) + after


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("github-matrix")
    r = sub.add_parser("readme")
    r.add_argument("--check", action="store_true", help="exit 1 if README.md isn't what the matrix file gives")
    r.add_argument("--readme", type=Path, default=README)
    args = ap.parse_args(argv)
    m = load()
    if args.cmd == "github-matrix":
        for name, value in github_matrices(m).items():
            print(f"{name}={json.dumps(value, separators=(',', ':'))}")
        return 0
    old = args.readme.read_text(encoding="utf-8")
    new = render_readme(old, m)
    if args.check:
        if old != new:
            print("README.md's supported-versions table is out of date: run python3 compat/matrix.py readme",
                  file=sys.stderr)
            return 1
        return 0
    if old != new:
        args.readme.write_text(new, encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
