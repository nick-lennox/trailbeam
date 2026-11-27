"""Tests for scripts/licences.py (QV-568). Run: python3 -m unittest discover -s scripts -p 'test_*.py'"""
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import licences  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
APACHE_TEXT = (ROOT / "node" / "LICENSE").read_text(encoding="utf-8")
AGPL_TEXT = (ROOT / "LICENSE").read_text(encoding="utf-8")


class Repo:
    """A small tree in a temporary folder: root AGPL, lib/ Apache, srv/ AGPL."""

    def __init__(self, files: dict):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        base = {"LICENSE": AGPL_TEXT, "lib/LICENSE": APACHE_TEXT, "srv/LICENSE": AGPL_TEXT}
        for name, text in {**base, **files}.items():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(text, encoding="utf-8")
        self.tree = licences.Tree(self.root, [str(p.relative_to(self.root)) for p in self.root.rglob("*")
                                              if p.is_file()])

    def imports(self) -> list:
        return licences.import_problems(self.tree)


class TheRealRepository(unittest.TestCase):
    def test_passes(self):
        self.assertEqual(licences.check(licences.Tree()), [])

    def test_the_licence_texts_are_the_two_known_ones(self):
        self.assertEqual(licences.TEXTS[licences.normalised_hash(AGPL_TEXT)], licences.AGPL)
        self.assertEqual(licences.TEXTS[licences.normalised_hash(APACHE_TEXT)], licences.APACHE)
        self.assertIn("GNU AFFERO GENERAL PUBLIC LICENSE", AGPL_TEXT)

    def test_which(self):
        t = licences.Tree()
        self.assertEqual(t.licence("python/src/trailbeam/boto.py"), licences.APACHE)
        self.assertEqual(t.licence("server/src/trailbeam/server/web.py"), licences.AGPL)
        self.assertEqual(t.licence("ui/src/index.ts"), licences.AGPL)
        self.assertEqual(t.licence("docker-compose.yaml"), licences.APACHE)
        self.assertEqual(t.licence("Dockerfile"), licences.AGPL)
        self.assertEqual(t.licence("python/tests/server/conftest.py"), licences.AGPL)


class Licences(unittest.TestCase):
    def test_nearest_licence_file_and_spdx_line(self):
        r = Repo({"lib/a.py": "", "lib/deep/b.py": "", "srv/c.py": "", "top.py": "",
                  "lib/d.py": "# SPDX-License-Identifier: AGPL-3.0-only\n"})
        t = r.tree
        self.assertEqual([t.licence(f) for f in ("lib/a.py", "lib/deep/b.py", "srv/c.py", "top.py", "lib/d.py")],
                         [licences.APACHE, licences.APACHE, licences.AGPL, licences.AGPL, licences.AGPL])
        self.assertIn("SPDX", t.why("lib/d.py"))

    def test_an_unknown_licence_text_and_an_unknown_spdx_id(self):
        r = Repo({"lib/LICENSE": "MIT License\n...", "lib/a.py": "# SPDX-License-Identifier: MIT\n"})
        self.assertTrue(any("neither" in p for p in licences.licence_file_problems(r.tree)))
        self.assertTrue(any("MIT" in p for p in licences.spdx_problems(r.tree)))

    def test_a_licence_text_with_other_line_breaks_is_still_known(self):
        r = Repo({"lib/LICENSE": APACHE_TEXT.replace("\n", "\r\n  ")})
        self.assertEqual(r.tree.licence_files["lib"], licences.APACHE)

    def test_metadata(self):
        p = Path(tempfile.mkdtemp())
        (p / "pyproject.toml").write_text('[project]\nname = "x"\nlicense = "AGPL-3.0-only"\n')
        (p / "package.json").write_text('{"name": "x", "license": "Apache-2.0"}')
        (p / "Dockerfile").write_text('LABEL a="b" \\\n  org.opencontainers.image.licenses="AGPL-3.0-only" \\\n')
        (p / "pom.xml").write_text("<licenses>\n <license>\n  <name>Apache-2.0</name>")
        self.assertEqual([licences.metadata_licence(p / n) for n in ("pyproject.toml", "package.json", "Dockerfile",
                                                                       "pom.xml")],
                         ["AGPL-3.0-only", "Apache-2.0", "AGPL-3.0-only", "Apache-2.0"])


class Imports(unittest.TestCase):
    def test_a_library_importing_the_server_fails(self):
        r = Repo({"lib/a.py": "import os\nfrom trailbeam.server import db\n",
                  "lib/b.py": "import trailbeam.server.otlp as o\n",
                  "lib/c.py": "from trailbeam import server\n",
                  "lib/d.py": "import importlib\nm = importlib.import_module('trailbeam.server.web')\n"})
        found = r.imports()
        self.assertEqual(sorted(p.split(":")[0] for p in found), ["lib/a.py", "lib/b.py", "lib/c.py", "lib/d.py"])
        self.assertIn("lib/a.py:2:", found[0])

    def test_the_library_itself_and_mentions_in_text_pass(self):
        r = Repo({"lib/a.py": '"""Not this: import trailbeam.server"""\nimport trailbeam\nfrom trailbeam import boto\n'
                              "# from trailbeam.server import x\nx = 'python -m trailbeam.server'\n",
                  "lib/b.ts": "// import { x } from '@trailbeam/ui'\nimport { y } from './y';\n",
                  "lib/y.ts": ""})
        self.assertEqual(r.imports(), [])

    def test_the_server_importing_the_library_passes(self):
        r = Repo({"srv/a.py": "import trailbeam\nfrom trailbeam.boto import x\n", "srv/b.ts": "import '../lib/y';\n",
                  "lib/y.ts": ""})
        self.assertEqual(r.imports(), [])

    def test_a_relative_or_sys_path_import_of_an_agpl_file_fails(self):
        r = Repo({"srv/helper.py": "", "lib/a.py": "import helper\n", "lib/pkg/__init__.py": "",
                  "lib/pkg/b.py": "from ..x import y\n", "lib/x.py": ""})
        found = r.imports()
        self.assertEqual(len(found), 1)
        self.assertIn("srv/helper.py", found[0])

    def test_a_name_in_both_licences_is_judged_by_the_one_next_to_it(self):
        r = Repo({"srv/util.py": "", "lib/util.py": "", "lib/a.py": "import util\n"})
        self.assertEqual(r.imports(), [])

    def test_javascript_and_typescript(self):
        r = Repo({"lib/a.js": "const ui = require('@trailbeam/ui');\n",
                  "lib/b.mjs": "import { Timeline } from \"@trailbeam/ui/timeline\";\n",
                  "lib/c.ts": "export { x } from '../srv/x';\n",
                  "lib/d.tsx": "const m = await import('../srv/m.js');\n",
                  "lib/e.js": "import '../srv/side-effect.js';\n",
                  "srv/x.ts": "", "srv/m.ts": "", "srv/side-effect.js": ""})
        self.assertEqual(sorted(p.split(":")[0] for p in r.imports()),
                         ["lib/a.js", "lib/b.mjs", "lib/c.ts", "lib/d.tsx", "lib/e.js"])

    def test_a_package_json_depending_on_the_ui(self):
        r = Repo({"lib/package.json": '{"dependencies": {"@trailbeam/ui": "^1"}}'})
        self.assertEqual(len(r.imports()), 1)


if __name__ == "__main__":
    unittest.main()
