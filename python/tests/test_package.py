# SPDX-License-Identifier: AGPL-3.0-only
# Imports the server, so it is under the server's licence (LICENSING.md).
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

import trailbeam

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "smoke"))
from split_check import library_wheel_problems  # noqa: E402


def test_version_is_set():
    assert trailbeam.__version__ == "0.4.0"


def test_the_server_has_the_same_version():
    import trailbeam.server

    assert trailbeam.server.__version__ == trailbeam.__version__


def test_the_library_wheel_has_no_file_of_the_server_or_the_web_screen(tmp_path):
    """The trailbeam wheel is the client library only; the server and the built UI are trailbeam-server's
    (../server). smoke/split_check.py checks the installs too."""
    src = tmp_path / "src"  # a copy, so the build leaves nothing in the checkout
    shutil.copytree(HERE.parent, src, ignore=shutil.ignore_patterns("tests", "bench", "smoke", "build", "*.egg-info",
                                                                     "__pycache__"))
    subprocess.run([sys.executable, "-m", "pip", "wheel", "-q", "--no-deps", "-w", str(tmp_path), str(src)], check=True)
    (wheel,) = tmp_path.glob("trailbeam-*.whl")
    names = zipfile.ZipFile(wheel).namelist()
    assert "trailbeam/boto.py" in names
    assert library_wheel_problems(names) == []


def test_the_wheel_check_finds_server_and_ui_files():
    assert library_wheel_problems(["trailbeam/server/store.py", "trailbeam/server/ui/index.html",
                                   "trailbeam/ui/assets/a.js", "trailbeam-0.4.0.dist-info/entry_points.txt"]) != []
    assert len(library_wheel_problems(["trailbeam/server/store.py", "trailbeam/x/ui/index.html", "trailbeam/a.css",
                                       "trailbeam-0.4.0.dist-info/entry_points.txt"])) == 4
    assert library_wheel_problems(["trailbeam/boto.py", "trailbeam-0.4.0.dist-info/RECORD"]) == []
