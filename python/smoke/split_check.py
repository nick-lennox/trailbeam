"""The two Python distributions (QV-567): trailbeam (the client library) and trailbeam-server (Trailbeam itself).

    python python/smoke/split_check.py                       # from the repository root
    python python/smoke/split_check.py --ui                  # after npm run build in ui/: the server wheel has it
    python python/smoke/split_check.py --upgrade-from 0.4.0  # also pip install -U over a published release
    python python/smoke/split_check.py --ui --wheels dist    # the release's own wheels (release.yaml)

What it checks, each install in a new venv of its own, from wheels built from this checkout:

1. The wheels. trailbeam's holds the library's modules only: nothing under ``trailbeam/server/``, no web screen file,
   no ``trailbeam`` command. trailbeam-server's holds ``trailbeam/server/`` (with ``ui/index.html`` when ``--ui``),
   no ``trailbeam/__init__.py`` (the library's), and the ``trailbeam`` command. Each pins the other at the same
   version: trailbeam's server, postgres, sso and passkeys extras, and trailbeam-server's dependency on trailbeam.
2. ``pip install trailbeam``: the hooks import; ``import trailbeam.server`` doesn't, and there is no ``trailbeam``
   command.
3. ``pip install "trailbeam[server]"``: trailbeam-server comes with it, at the same version, without a Postgres
   driver; ``trailbeam --version``, ``python -m trailbeam.server --version`` and ``trailbeam start --help`` answer.
4. With ``--upgrade-from V``: ``pip install "trailbeam[server]==V"`` from PyPI (one distribution with the server
   inside), then ``pip install -U "trailbeam[server]"`` to this checkout. pip must install the library before the
   server, or removing the old trailbeam would delete the new server's files: every file of the server wheel must
   be on disk afterwards, and the command must answer with the new version.

The wheels are built at a version no index has (this checkout's, plus ``.post9999``), so pip can only pick
them, and the upgrade in step 4 is a real one even while this checkout still carries the last release's number.
With ``--wheels``, the wheels given are checked at their own version, which must not be on PyPI yet.
Exit status 0 when every check passes. Needs network access for the dependencies (and for step 4, the release).
"""
from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
import tempfile
import venv
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
VERSIONED = {  # the files that carry the version, per distribution directory
    "python": ["pyproject.toml", "src/trailbeam/__init__.py"],
    "server": ["pyproject.toml", "src/trailbeam/server/__init__.py"],
}
UI_SUFFIXES = (".html", ".js", ".css", ".woff", ".woff2", ".svg", ".map")

failures: list = []


def fail(what: str) -> None:
    failures.append(what)
    print(f"FAIL: {what}", flush=True)


def check(cond: bool, what: str) -> None:
    (print(f"ok: {what}", flush=True) if cond else fail(what))


def version() -> str:
    m = re.search(r'^version = "([^"]+)"$', (ROOT / "python/pyproject.toml").read_text(), re.M)
    return m.group(1)


def library_wheel_problems(names: list) -> list:
    """What, in the trailbeam wheel's file list, belongs to the server or the web screen (empty when nothing)."""
    bad = []
    for n in names:
        if n.split("/", 1)[0].endswith(".dist-info"):
            if n.endswith("entry_points.txt"):
                bad.append(f"{n} (the trailbeam command belongs to trailbeam-server)")
            continue
        if not n.startswith("trailbeam/"):
            bad.append(f"{n} (outside the trailbeam package)")
        elif n.startswith("trailbeam/server/") or "/ui/" in n or n.endswith(UI_SUFFIXES):
            bad.append(n)
        elif n.count("/") != 1 or not n.endswith(".py"):
            bad.append(f"{n} (the library is the modules directly in trailbeam/)")
    return bad


def build(work: Path, test_version: str) -> dict:
    """Both wheels, built from copies of python/ and server/ set to test_version."""
    real = version()
    pin = re.compile(r'(?<=["=])' + re.escape(real) + r'(?=")')
    out = work / "wheels"
    for d in VERSIONED:
        src = work / "src" / d
        shutil.copytree(ROOT / d, src, ignore=shutil.ignore_patterns("tests", "bench", "smoke", "*.egg-info",
                                                                       "build", "dist", "__pycache__"))
        for f in VERSIONED[d]:
            p = src / f
            text, n = pin.subn(test_version, p.read_text())
            if not n:
                raise SystemExit(f"{d}/{f} has no {real} to set")
            p.write_text(text)
        subprocess.run([sys.executable, "-m", "pip", "wheel", "-q", "--no-deps", "-w", str(out), str(src)], check=True)
    wheels = {}
    for w in out.glob("*.whl"):
        wheels["server" if w.name.startswith("trailbeam_server-") else "library"] = w
    return wheels


def new_venv(path: Path) -> Path:
    venv.create(path, with_pip=True)
    return path / "bin"


def run(cmd: list, **kw) -> subprocess.CompletedProcess:
    return subprocess.run([str(c) for c in cmd], capture_output=True, text=True, **kw)


def pip(bin_: Path, *args) -> None:
    r = run([bin_ / "python", "-m", "pip", "install", "-q", "--disable-pip-version-check", *args])
    if r.returncode:
        raise SystemExit(f"pip install {' '.join(map(str, args))} failed:\n{r.stdout}{r.stderr}")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--ui", action="store_true", help="the web screen is built (npm run build in ui/): the server wheel must have it")
    p.add_argument("--upgrade-from", metavar="VERSION", help="a published release to pip install -U from, e.g. 0.4.0")
    p.add_argument("--wheels", metavar="DIR", type=Path,
                   help="check the two wheels in DIR, at this checkout's version (the release's), instead of building them")
    p.add_argument("--keep", action="store_true", help="keep the work directory")
    a = p.parse_args()

    work = Path(tempfile.mkdtemp(prefix="trailbeam-split-"))
    try:
        if a.wheels:
            tv = version()
            wheels = {"library": a.wheels / f"trailbeam-{tv}-py3-none-any.whl",
                      "server": a.wheels / f"trailbeam_server-{tv}-py3-none-any.whl"}
            for w in wheels.values():
                if not w.is_file():
                    raise SystemExit(f"no {w}")
            shutil.copytree(a.wheels, work / "wheels", ignore=shutil.ignore_patterns("*.tar.gz"))
            wheels = {k: work / "wheels" / w.name for k, w in wheels.items()}
        else:
            tv = f"{version()}.post9999"
            wheels = build(work, tv)
        wh = str(wheels["library"].parent)

        # 1. The wheels
        lib = zipfile.ZipFile(wheels["library"])
        srv = zipfile.ZipFile(wheels["server"])
        lib_names, srv_names = lib.namelist(), srv.namelist()
        bad = library_wheel_problems(lib_names)
        check(not bad, "the trailbeam wheel has no file of the server or the web screen" + (f": {bad}" if bad else ""))
        check(any(n == "trailbeam/boto.py" for n in lib_names), "the trailbeam wheel has trailbeam/boto.py")
        check("trailbeam/server/__main__.py" in srv_names, "the trailbeam-server wheel has trailbeam/server/")
        check("trailbeam/__init__.py" not in srv_names, "the trailbeam-server wheel leaves trailbeam/__init__.py to the library")
        check(all(n.startswith(("trailbeam/server/", "trailbeam_server-")) for n in srv_names),
              "every file of the trailbeam-server wheel is under trailbeam/server/ (or its metadata)")
        if a.ui:
            check("trailbeam/server/ui/index.html" in srv_names, "the trailbeam-server wheel has the web screen")
        meta = lambda z: next(z.read(n).decode() for n in z.namelist() if n.endswith(".dist-info/METADATA"))
        lib_meta, srv_meta = meta(lib), meta(srv)
        for extra, req in (("server", "trailbeam-server"), ("postgres", "trailbeam-server[postgres]"),
                           ("sso", "trailbeam-server[sso]"), ("passkeys", "trailbeam-server[passkeys]")):
            check(f"Requires-Dist: {req}=={tv}; extra == \"{extra}\"" in lib_meta,
                  f"trailbeam[{extra}] pulls in {req} at the same version")
        check(f"Requires-Dist: trailbeam=={tv}\n" in srv_meta, "trailbeam-server depends on trailbeam at the same version")
        ep = next((srv.read(n).decode() for n in srv_names if n.endswith("entry_points.txt")), "")
        check("trailbeam = trailbeam.server.__main__:main" in ep, "trailbeam-server provides the trailbeam command")

        # 2. pip install trailbeam
        b = new_venv(work / "lib")
        pip(b, "--find-links", wh, f"trailbeam=={tv}")
        r = run([b / "python", "-c", "import trailbeam, trailbeam.boto, trailbeam.aws_lambda; print(trailbeam.__version__)"])
        check(r.returncode == 0 and r.stdout.strip() == tv, f"pip install trailbeam: the hooks import ({r.stdout.strip() or r.stderr[-300:]})")
        r = run([b / "python", "-c", "import trailbeam.server"])
        check(r.returncode != 0 and "No module named 'trailbeam.server'" in r.stderr,
              "pip install trailbeam: import trailbeam.server fails (no server)")
        check(not (b / "trailbeam").exists(), "pip install trailbeam: no trailbeam command")
        r = run([b / "python", "-m", "pip", "list", "--format=freeze"])
        check("trailbeam-server" not in r.stdout and "trailbeam_server" not in r.stdout, "pip install trailbeam: no trailbeam-server")

        # 3. pip install "trailbeam[server]"
        b = new_venv(work / "server")
        pip(b, "--find-links", wh, f"trailbeam[server]=={tv}")
        r = run([b / "python", "-m", "pip", "list", "--format=freeze"])
        check(f"trailbeam-server=={tv}" in r.stdout, f'pip install "trailbeam[server]": trailbeam-server {tv} with it')
        check(not any(x in r.stdout.lower() for x in ("psycopg", "pyjwt")), 'pip install "trailbeam[server]": no Postgres driver or PyJWT')
        r = run([b / "python", "-m", "pip", "check"])
        check(r.returncode == 0, f'pip install "trailbeam[server]": pip check ({r.stdout.strip()})')
        for cmd in ([b / "trailbeam", "--version"], [b / "python", "-m", "trailbeam.server", "--version"]):
            r = run(cmd)
            check(r.returncode == 0 and r.stdout.strip() == f"trailbeam {tv}",
                  f"{Path(cmd[0]).name} {' '.join(map(str, cmd[1:]))}: {r.stdout.strip() or r.stderr[-300:]}")
        r = run([b / "trailbeam", "start", "--help"])
        check(r.returncode == 0 and r.stdout.startswith("usage: trailbeam start "), "trailbeam start --help")
        r = run([b / "trailbeam", "doctor", "--help"])
        check(r.returncode == 0 and "usage: trailbeam doctor" in r.stdout, "trailbeam doctor --help")
        if a.ui:
            r = run([b / "python", "-c", "from trailbeam.server import ui_files; print(ui_files.DEFAULT_UI_DIR.joinpath('index.html').is_file())"])
            check(r.stdout.strip() == "True", 'pip install "trailbeam[server]": the web screen is installed')

        # 4. pip install -U over a release where trailbeam held the server
        if a.upgrade_from:
            b = new_venv(work / "upgrade")
            pip(b, f"trailbeam[server]=={a.upgrade_from}")
            r = run([b / "python", "-c", "import trailbeam.server, trailbeam; print(trailbeam.__version__)"])
            check(r.stdout.strip() == a.upgrade_from, f"trailbeam[server]=={a.upgrade_from} from PyPI")
            pip(b, "-U", "--find-links", wh, f"trailbeam[server]=={tv}")
            site = Path(run([b / "python", "-c", "import trailbeam, os; print(os.path.dirname(trailbeam.__path__[0]))"]).stdout.strip())
            missing = [n for n in srv_names if not n.startswith("trailbeam_server-") and not (site / n).is_file()]
            check(not missing, f"after pip install -U, every file of trailbeam-server is there" + (f"; missing: {missing[:5]}" if missing else ""))
            r = run([b / "trailbeam", "--version"])
            check(r.stdout.strip() == f"trailbeam {tv}", f"after pip install -U: trailbeam --version says {r.stdout.strip() or r.stderr[-300:]}")
            r = run([b / "python", "-m", "pip", "check"])
            check(r.returncode == 0, f"after pip install -U: pip check ({r.stdout.strip()})")
    finally:
        if a.keep:
            print(f"kept {work}")
        else:
            shutil.rmtree(work, ignore_errors=True)

    print(f"\n{len(failures)} failure(s)" if failures else "\nevery check passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
