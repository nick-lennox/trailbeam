"""Tests for scripts/changelog.py (QV-448). Run: python3 -m unittest discover -s scripts -p 'test_*.py'"""
import io
import subprocess
import sys
import tempfile
import textwrap
import unittest
from contextlib import redirect_stdout
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import changelog  # noqa: E402

CHANGELOG = textwrap.dedent("""\
    # Changelog

    ## Unreleased

    - **An entry from before fragments** (QV-1). It stays first.

    ## 0.4.0: set up in one command

    - Old.
    """)


def fragment(folder: Path, name: str, section: str, body: str) -> None:
    (folder / name).write_text(f"---\nsection: {section}\n---\n\n{body}\n", encoding="utf-8")


class Build(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        (self.root / "changelog.d").mkdir()
        (self.root / "changelog.d" / "README.md").write_text("How to write a fragment.\n")
        (self.root / "CHANGELOG.md").write_text(CHANGELOG)

    def tearDown(self):
        self.tmp.cleanup()

    def run_build(self, **kw):
        with redirect_stdout(io.StringIO()):
            return changelog.build(self.root, **kw)

    def test_two_fragments_make_a_correct_section(self):
        d = self.root / "changelog.d"
        fragment(d, "QV-12.md", "Fixed", "- **A fix** (QV-12). Two lines\n  of text.")
        fragment(d, "QV-9.md", "Added", "- **A feature** (QV-9).")
        self.run_build()
        self.assertEqual((self.root / "CHANGELOG.md").read_text(), textwrap.dedent("""\
            # Changelog

            ## Unreleased

            - **An entry from before fragments** (QV-1). It stays first.

            ### Added

            - **A feature** (QV-9).

            ### Fixed

            - **A fix** (QV-12). Two lines
              of text.

            ## 0.4.0: set up in one command

            - Old.
            """))
        self.assertEqual(sorted(p.name for p in d.iterdir()), ["README.md"])

    def test_same_section_in_task_order_and_joins_an_existing_one(self):
        d = self.root / "changelog.d"
        fragment(d, "QV-30.md", "Changed", "- Thirty.")
        fragment(d, "QV-4.md", "Changed", "- Four.")
        self.run_build()
        fragment(d, "QV-50.md", "Changed", "- Fifty.")
        fragment(d, "QV-50-second.md", "Added", "- Fifty, second.")
        text = self.run_build()
        unreleased = text.split("## Unreleased")[1].split("## 0.4.0")[0]
        self.assertEqual(unreleased, "\n\n- **An entry from before fragments** (QV-1). It stays first.\n\n"
                                     "### Added\n\n- Fifty, second.\n\n### Changed\n\n- Four.\n- Thirty.\n- Fifty.\n\n")

    def test_empty_unreleased(self):
        (self.root / "CHANGELOG.md").write_text("# Changelog\n\n## Unreleased\n\n## 0.4.0\n")
        fragment(self.root / "changelog.d", "QV-7.md", "Fixed", "- Seven.")
        self.assertEqual(self.run_build(), "# Changelog\n\n## Unreleased\n\n### Fixed\n\n- Seven.\n\n## 0.4.0\n")

    def test_dry_run_changes_nothing(self):
        fragment(self.root / "changelog.d", "QV-7.md", "Fixed", "- Seven.")
        self.run_build(dry_run=True)
        self.assertEqual((self.root / "CHANGELOG.md").read_text(), CHANGELOG)
        self.assertTrue((self.root / "changelog.d" / "QV-7.md").exists())

    def test_a_bad_fragment_stops_the_build(self):
        fragment(self.root / "changelog.d", "QV-7.md", "Security", "- Seven.")
        with self.assertRaises(SystemExit):
            self.run_build()
        self.assertEqual((self.root / "CHANGELOG.md").read_text(), CHANGELOG)


class Fragments(unittest.TestCase):
    def parse(self, name, text):
        with tempfile.TemporaryDirectory() as t:
            p = Path(t) / name
            p.write_text(text)
            return changelog.parse_fragment(p)

    def test_valid(self):
        self.assertEqual(self.parse("QV-448.md", "---\nsection: Changed\n---\n\n- Entry.\n"), ("Changed", "- Entry."))

    def test_invalid(self):
        for name, text in [("notes.md", "---\nsection: Fixed\n---\n- x\n"),
                           ("QV-1.md", "- no front matter\n"),
                           ("QV-1.md", "---\nsection: fixed\n---\n- x\n"),
                           ("QV-1.md", "---\nsection: Fixed\n---\n\n"),
                           ("QV-1.md", "---\nsection: Fixed\n---\nplain text\n"),
                           ("QV-1.md", "---\nsection: Fixed\n---\n- x\n\n### Heading\n")]:
            with self.subTest(name=name, text=text), self.assertRaises(changelog.FragmentError):
                self.parse(name, text)


class Check(unittest.TestCase):
    """The CI check on a real git repository: a base commit, then a pull request's commit."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.git("init", "-q", "-b", "main")
        self.git("config", "user.email", "t@example.com")
        self.git("config", "user.name", "t")
        (self.root / "changelog.d").mkdir()
        (self.root / "changelog.d" / "README.md").write_text("x\n")
        (self.root / "CHANGELOG.md").write_text(CHANGELOG)
        self.commit()
        self.base = self.git("rev-parse", "HEAD").strip()

    def tearDown(self):
        self.tmp.cleanup()

    def git(self, *a):
        return subprocess.run(["git", *a], cwd=self.root, check=True, capture_output=True, text=True).stdout

    def commit(self):
        self.git("add", "-A")
        self.git("commit", "-q", "-m", "c", "--allow-empty")

    def check(self, branch="qv-999-new-work"):
        out = io.StringIO()
        with redirect_stdout(out):
            code = changelog.check(self.root, self.base, branch)
        return code, out.getvalue()

    def edit(self, old, new):
        p = self.root / "CHANGELOG.md"
        p.write_text(p.read_text().replace(old, new))
        self.commit()

    def test_a_fragment_passes(self):
        fragment(self.root / "changelog.d", "QV-999.md", "Added", "- New.")
        self.commit()
        self.assertEqual(self.check()[0], 0)

    def test_a_new_line_under_unreleased_fails_and_points_to_changelog_d(self):
        self.edit("## Unreleased\n\n", "## Unreleased\n\n- **New** (QV-999).\n")
        code, out = self.check()
        self.assertEqual(code, 1)
        self.assertIn("changelog.d/QV-NNN.md", out)
        self.assertIn("**New** (QV-999)", out)

    def test_new_lines_in_a_released_section_pass(self):
        self.edit("- Old.\n", "- Old.\n- A late note on 0.4.0.\n")
        self.assertEqual(self.check()[0], 0)

    def test_rewording_an_existing_entry_passes(self):
        self.edit("It stays first.", "It stays at the top.")
        self.assertEqual(self.check()[0], 0)

    def test_grandfathered_and_release_branches_pass(self):
        self.edit("## Unreleased\n\n", "## Unreleased\n\n- **New** (QV-320).\n")
        self.assertEqual(self.check("qv-320-dotnet")[0], 0)
        self.assertEqual(self.check("release/0.5")[0], 0)

    def test_the_release_fold_passes(self):
        fragment(self.root / "changelog.d", "QV-5.md", "Fixed", "- Five.")
        self.commit()
        self.base = self.git("rev-parse", "HEAD").strip()
        with redirect_stdout(io.StringIO()):
            changelog.build(self.root)
        self.commit()
        self.assertEqual(self.check()[0], 0)

    def test_a_bad_fragment_fails(self):
        fragment(self.root / "changelog.d", "QV-999.md", "Misc", "- New.")
        self.commit()
        self.assertEqual(self.check()[0], 1)

    def test_without_base_only_fragments_are_checked(self):
        self.edit("## Unreleased\n\n", "## Unreleased\n\n- **New** (QV-999).\n")
        with redirect_stdout(io.StringIO()):
            self.assertEqual(changelog.check(self.root, None, None), 0)


if __name__ == "__main__":
    unittest.main()
