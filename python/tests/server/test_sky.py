"""The sign-in pages in the pre-dawn sky (QV-666): the design system's logo and tokens, one self-contained response,
the screen rules, and everything the old pages had."""
import re
from html.parser import HTMLParser
from pathlib import Path

import pytest

from trailbeam.server import logo, sky, web

UI = Path(__file__).resolve().parents[3] / "ui" / "src"


def test_the_logo_is_the_design_systems():
    src = (UI / "design" / "Logo.tsx").read_text()
    for name in ("DOME", "DOT", "NAME"):
        assert re.search(rf'export const {name} = "([^"]+)";', src).group(1) == getattr(logo, name), name
    for name in ("W", "H", "LOCKUP_W"):
        assert float(re.search(rf"export const {name} = ([0-9.]+);", src).group(1)) == getattr(logo, name), name


def test_the_colors_are_the_sky_tokens():
    src = (UI / "design" / "tokens.css").read_text()
    for name, value in sky.TOKENS.items():
        assert re.search(rf"--color-{name}:\s*{value};", src), name


class _Page(HTMLParser):
    def __init__(self):
        super().__init__()
        self.tags: list[tuple[str, dict]] = []
        self.text: list[str] = []
        self._skip = 0

    def handle_starttag(self, tag, attrs):
        self.tags.append((tag, dict(attrs)))
        if tag in ("style", "script"):
            self._skip += 1

    def handle_endtag(self, tag):
        if tag in ("style", "script"):
            self._skip -= 1

    def handle_data(self, data):
        if not self._skip:
            self.text.append(data)


def _parse(r) -> _Page:
    p = _Page()
    p.feed(r[2].decode())
    return p


PAGES = {
    "sign in": lambda: web.sign_in_page("/stuck", "a@example.com", "locked", 90,
                                        sso=[("/login/oidc", "Okta"), ("/login/github", "GitHub")], passkey=True),
    "sign in, no extras": lambda: web.sign_in_page("/"),
    "sign in, buttons only": lambda: web.sign_in_page("/", sso=[("/login/github", "GitHub")], password=False),
    "token": lambda: web.login_page("/", error=True, sso="Okta"),
    "setup": lambda: web.setup_page("abc", "a@example.com", "Ada", "mismatch"),
    "set password": lambda: web.password_page("abc", "slow_down", retry_after_s=120, email="a@example.com"),
    "sso error": lambda: web.sso_error_page("You cancelled the sign-in."),
    "forbidden": lambda: web.forbidden_page("Only an admin can see this page"),
}


@pytest.mark.parametrize("name", PAGES)
def test_a_sign_in_page_is_one_response_in_the_sky(name):
    r = PAGES[name]()
    html = r[2].decode()
    p = _parse(r)
    tags = [t for t, _ in p.tags]
    assert "link" not in tags and "img" not in tags  # nothing fetched
    assert not any(a.get("src") for _, a in p.tags)
    assert 'class="sky"' in html and 'aria-label="Trailbeam"' in html and "<header" in html
    assert "<h1>" in html and "Server health" in html and sky.__version__ in html
    # the screen rules: no native hover notes, no "a · b" fragments, no em dashes, no tracked capitals
    assert not any("title" in a for _, a in p.tags)
    text = "".join(p.text)
    assert "·" not in text and "—" not in html
    assert "uppercase" not in sky.CSS and not re.search(r"letter-spacing:\.?[0-9]", sky.CSS)
    # every visible input has its label
    ids = {a.get("for") for t, a in p.tags if t == "label"}
    for t, a in p.tags:
        if t == "input" and a.get("type") != "hidden":
            assert a.get("id") in ids, a


def test_the_sign_in_page_keeps_what_it_had():
    html = PAGES["sign in"]()[2].decode()
    assert "Too many failed sign-ins: this account is locked for now. Try again in 2 minutes." in html
    assert 'href="/login/oidc?next=%2Fstuck"' in html and "Sign in with Okta" in html
    assert 'href="/login/github?next=%2Fstuck"' in html and "Sign in with GitHub" in html
    assert 'id="passkey" data-next="/stuck"' in html and 'id="passkey-note"' in html and "<script>" in html
    # the passkey button's wrapper is hidden until the script finds WebAuthn
    assert re.search(r'<p hidden><button type="button" class="quiet" id="passkey"', html)
    assert "Or sign in with your Trailbeam email and password." in html
    assert 'name="email" value="a@example.com"' in html and 'name="password"' in html
    assert '<input type="hidden" name="next" value="/stuck">' in html and 'action="/login"' in html
    plain = PAGES["sign in, no extras"]()[2].decode()
    assert "Sign in with your Trailbeam account." in plain and "passkey" not in plain
    only = PAGES["sign in, buttons only"]()[2].decode()
    assert "Sign in with GitHub" in only and "<form" not in only


def test_the_setup_and_password_pages_keep_what_they_had():
    html = PAGES["setup"]()[2].decode()
    for words in ("Create your admin account", "It becomes the first admin", "The two passwords are not the same.",
                  'id="code-row"', 'id="code"', 'value="abc"', 'value="Ada"', 'minlength="10"', "At least 10 characters.",
                  'name="password2"', "Create admin account", "location.hash"):
        assert words in html, words
    st, headers, _ = PAGES["setup"]()
    assert headers["Referrer-Policy"] == "no-referrer" and headers["Cache-Control"] == "no-store"
    html = PAGES["set password"]()[2].decode()
    for words in ("Choose your password", "add a passkey again on the Account page", "Try again in 2 minutes.",
                  'id="code-row"', "Link code", '<input type="hidden" name="email" value="a@example.com"',
                  "Set password and sign in"):
        assert words in html, words


def test_the_token_and_error_pages_keep_what_they_had():
    html = PAGES["token"]()[2].decode()
    assert "<code>TRAILBEAM_TOKEN</code>" in html and "That is not the token." in html and 'name="token"' in html
    assert "Sign in with Okta" in html
    assert PAGES["token"]()[0] == 401
    html = PAGES["sso error"]()[2].decode()
    assert "You cancelled the sign-in." in html and 'href="/login"' in html
    html = PAGES["forbidden"]()[2].decode()
    assert "Only an admin can see this page." in html and 'href="/logout"' in html and 'href="/"' in html
