"""Single sign-on against a real Keycloak (oidc.py): its discovery, signing keys, login form, PKCE, client secret and
group claim, through the HTTP server. Runs when TRAILBEAM_TEST_KEYCLOAK_URL names a Keycloak that imported
keycloak/trailbeam-realm.json, as the CI job does:

    docker run --rm -p 8080:8080 -e KC_BOOTSTRAP_ADMIN_USERNAME=admin -e KC_BOOTSTRAP_ADMIN_PASSWORD=admin \\
      -v "$PWD/tests/server/keycloak:/opt/keycloak/data/import:ro" quay.io/keycloak/keycloak:26.0 \\
      start-dev --import-realm
    TRAILBEAM_TEST_KEYCLOAK_URL=http://localhost:8080 pytest tests/server/test_oidc_keycloak.py

The realm has the confidential client ``trailbeam`` (secret ``trailbeam-ci-secret``, PKCE S256 required, any
http://127.0.0.1 port as redirect URI) and three people: ada (groups trailbeam-admins and engineering), bob
(engineering) and eve (no group)."""
import html
import os
import re
import urllib.error
import urllib.parse
import urllib.request

import pytest

pytest.importorskip("jwt")

from test_oidc import Browser, Trailbeam, _me, _path  # noqa: E402

from trailbeam.server.db import Database  # noqa: E402

KEYCLOAK = os.environ.get("TRAILBEAM_TEST_KEYCLOAK_URL", "").rstrip("/")
pytestmark = pytest.mark.skipif(not KEYCLOAK, reason="TRAILBEAM_TEST_KEYCLOAK_URL is not set")
SECRET = "trailbeam-ci-secret"


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *a, **kw):
        return None


class KeycloakPerson:
    """A browser at Keycloak: its own cookies (Keycloak's login session), never Trailbeam's. Kept by hand: Keycloak
    marks them Secure even on http://localhost, which browsers accept and http.cookiejar doesn't."""

    def __init__(self):
        self.cookies: dict = {}
        self.opener = urllib.request.build_opener(_NoRedirect)

    def _open(self, url, data=None):
        req = urllib.request.Request(url, data=data)
        if self.cookies:
            req.add_header("Cookie", "; ".join(f"{k}={v}" for k, v in self.cookies.items()))
        try:
            r = self.opener.open(req, timeout=20)
            status, headers, body = r.status, r.headers, r.read()
        except urllib.error.HTTPError as e:  # 302 (no redirect followed), 4xx
            status, headers, body = e.code, e.headers, e.read()
        for sc in headers.get_all("Set-Cookie") or []:
            name, _, value = sc.split(";")[0].partition("=")
            self.cookies[name.strip()] = value
        return status, headers, body.decode("utf-8", "replace")

    def sign_in(self, authorize_url: str, username: str, password: str) -> str:
        """Open the authorization URL, fill in Keycloak's login form: the callback URL Keycloak sends back to."""
        status, headers, page = self._open(authorize_url)
        assert status == 200, (status, page[:500])
        m = re.search(r'<form[^>]*id="kc-form-login"[^>]*action="([^"]+)"', page) or \
            re.search(r'action="([^"]*login-actions/authenticate[^"]*)"', page)
        assert m, page[:2000]
        form = urllib.parse.urlencode({"username": username, "password": password, "credentialId": ""}).encode()
        status, headers, page = self._open(html.unescape(m.group(1)), form)
        assert status == 302, (status, page[:1000])
        return headers["Location"]


class Issuer:
    issuer = KEYCLOAK + "/realms/trailbeam"
    client_id = "trailbeam"
    client_secret = SECRET


@pytest.fixture
def tb(tmp_path):
    t = Trailbeam(Database(str(tmp_path / "trailbeam.sqlite")), Issuer(), admin_groups="trailbeam-admins",
                  member_groups="engineering", label="Keycloak")
    yield t
    t.close()


def _sign_in(tb, username, password, next_path="/"):
    b = Browser(tb.port)
    r = b.get("/login/oidc?" + urllib.parse.urlencode({"next": next_path}))
    assert r.status == 302, r.text
    assert r.headers["Location"].startswith(Issuer.issuer + "/protocol/openid-connect/auth?")
    callback = KeycloakPerson().sign_in(r.headers["Location"], username, password)
    assert callback.startswith(f"http://127.0.0.1:{tb.port}/login/oidc/callback?"), callback
    return b, b.get(_path(callback))


def test_the_first_admin_then_a_member_then_someone_in_no_group(tb):
    # no admin yet: bob (engineering only) is refused, ada (trailbeam-admins) becomes the first admin
    b, r = _sign_in(tb, "bob", "bob-password-1")
    assert r.status == 403 and "no admin yet" in r.text
    b, r = _sign_in(tb, "ada", "ada-password-1", "/stuck")
    assert r.status == 200 and "url=/stuck" in r.text, r.text
    status, me = _me(b)
    assert status == 200 and me["auth"] == "accounts"
    assert (me["user"]["email"], me["user"]["role"], me["user"]["name"]) == ("ada@example.com", "admin", "Ada Admin")
    b, r = _sign_in(tb, "bob", "bob-password-1")
    assert r.status == 200
    assert _me(b)[1]["user"]["role"] == "member"
    b, r = _sign_in(tb, "eve", "eve-password-1")
    assert r.status == 403 and "give you no access" in r.text
    ids = tb.app.accounts.oidc_identities()
    assert {i["email"] for i in ids} == {"ada@example.com", "bob@example.com"}
    assert all(i["issuer"] == Issuer.issuer for i in ids)


def test_a_wrong_client_secret_is_refused_by_keycloak(tmp_path):
    class Wrong(Issuer):
        client_secret = "not-the-secret"

    t = Trailbeam(Database(str(tmp_path / "w.sqlite")), Wrong(), admin_groups="trailbeam-admins")
    try:
        b, r = _sign_in(t, "ada", "ada-password-1")
        assert r.status == 502 and "trailbeam_sid" not in b.cookies
    finally:
        t.close()


def test_a_callback_replayed_in_another_browser(tb):
    b = Browser(tb.port)
    r = b.get("/login/oidc")
    callback = KeycloakPerson().sign_in(r.headers["Location"], "ada", "ada-password-1")
    other = Browser(tb.port)
    assert other.get(_path(callback)).status == 400
    assert "trailbeam_sid" not in other.cookies
