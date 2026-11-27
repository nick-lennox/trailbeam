"""Sign in with GitHub (QV-587 part 2, QV-664): GitHub's OAuth web flow, beside single sign-on with OpenID Connect
(oidc.py). GitHub isn't an OpenID Connect provider for people (no ID token), so this is OAuth: the code is exchanged
for an access token, and the person is read from GitHub's REST API. Off unless ``--github-client-id`` is set. It needs
nothing beyond the standard library, so no extra.

**The flow** is oidc.py's, with its protections:

1. ``GET /login/github?next=/path`` (``GitHub.begin``) makes a ``state``, a PKCE ``code_verifier`` and a browser value,
   keeps them in the store's ``oidc_logins`` table (the row's ``nonce`` column says ``github``) for 10 minutes, sets
   the browser value in the ``trailbeam_github`` cookie (``SameSite=Lax``, ``Path=/login/github``) and sends the
   browser to ``<GitHub>/login/oauth/authorize``.
2. GitHub sends it back to ``<public URL>/login/github/callback?code=…&state=…`` (``GitHub.finish``). The row is
   deleted as it is read, and the cookie must match it. The code is exchanged at ``/login/oauth/access_token`` with
   the client secret and the verifier. Then ``GET /user`` (the account's numeric ``id``, which never changes, unlike
   its login), ``GET /user/emails`` (the primary address, which must be verified), and, when organisations or teams
   are set, ``GET /user/memberships/orgs`` and ``GET /user/teams``. The access token is used for this sign-in only
   and is not kept.
3. The account rules (accounts.oidc_sign_in, as for OpenID Connect), a session, and the page that moves on by itself
   (oidc.continue_page).

**The account** is ``oidc_identities`` with ``issuer`` the GitHub address (``https://github.com``, or a GitHub
Enterprise Server's ``--github-url``) and ``subject`` the GitHub user ID. So no tables are added, and the People page
and ``trailbeam users`` see GitHub accounts as single sign-on.

**Who gets in, and with which role** (``Rules``):

- ``--github-allowed-orgs``: if set, only active members of one of these organisations. GitHub accounts are
  everyone's, not one organisation's like an identity provider's: without allowed organisations or member teams, the
  server refuses to start unless ``--github-default-role member`` says to let any GitHub account in.
- ``--github-admin-teams`` / ``--github-member-teams`` (``org/team-slug``): someone in an admin team is an admin, else
  someone in a member team a member, else ``--github-default-role`` (``member``, or ``none``: refused; ``none`` by
  default when member teams are set, as with OpenID Connect groups). When teams are mapped, GitHub decides: the role
  is set again at every sign-in.
- Orgs and teams need the ``read:org`` scope, which is asked for only when they are set. An organisation with OAuth app
  access restrictions hides its memberships until an owner approves the app: its members are then refused, with a
  message saying so.
- The primary email must be verified at GitHub. An email that already has an account here is refused, unless
  ``--github-link-email`` is on and no other provider account signs in to it yet.
- Sessions last ``--github-session-hours`` (12 by default), as single sign-on's do.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import secrets
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Callable, NamedTuple, Optional

from . import __version__
from .accounts import AccountError, SsoRefused
from .cookies import cookie
from .oidc import (LOGIN_TTL_MS, MAX_RESPONSE, HTTP_TIMEOUT_S, Done, OidcError, SettingsError, _names,
                   _session_hours, check_url, pkce_challenge, read_secret_file)

log = logging.getLogger("trailbeam.server")

START_PATH = "/login/github"
CALLBACK_PATH = "/login/github/callback"
COOKIE = "trailbeam_github"
SECURE_COOKIE = "__Secure-" + COOKIE
GITHUB_URL = "https://github.com"
LABEL = "GitHub"
MARKER = "github"  # the oidc_logins row's nonce column: a GitHub sign-in, not an OpenID Connect one
MAX_PAGES = 10     # of 100 organisations or teams each
API_VERSION = "2022-11-28"


@dataclass(frozen=True)
class Settings:
    client_id: str
    client_secret: str
    public_url: str
    github_url: str = GITHUB_URL
    allowed_orgs: tuple = ()
    admin_teams: tuple = ()     # ("org/slug", ...), lowercased
    member_teams: tuple = ()
    default_role: Optional[str] = "member"  # None: refused
    link_email: bool = False
    session_hours: float = 12.0
    label: str = LABEL

    @property
    def session_ms(self) -> int:
        return int(self.session_hours * 3_600_000)

    @property
    def redirect_uri(self) -> str:
        return self.public_url.rstrip("/") + CALLBACK_PATH

    @property
    def maps_teams(self) -> bool:
        return bool(self.admin_teams or self.member_teams)

    @property
    def needs_orgs(self) -> bool:
        return bool(self.allowed_orgs or self.maps_teams)

    @property
    def scopes(self) -> tuple:
        return ("read:user", "user:email") + (("read:org",) if self.needs_orgs else ())

    @property
    def api_url(self) -> str:
        """github.com's API is on its own host; GitHub Enterprise Server's under ``/api/v3``."""
        if self.github_url == GITHUB_URL:
            return "https://api.github.com"
        return self.github_url + "/api/v3"


def _teams(text, flag: str) -> tuple:
    out = []
    for t in _names(text):
        org, _, slug = t.partition("/")
        if not org.strip() or not slug.strip() or "/" in slug:
            raise SettingsError(f"{flag} takes teams as org/team-slug, such as acme/platform, got {t!r}")
        out.append(f"{org.strip().lower()}/{slug.strip().lower()}")
    return tuple(out)


def make_settings(*, client_id: Optional[str], client_secret: Optional[str] = None, public_url: Optional[str] = None,
                  github_url: Optional[str] = None, allowed_orgs=None, admin_teams=None, member_teams=None,
                  default_role: Optional[str] = None, link_email: bool = False,
                  session_hours=None) -> Optional[Settings]:
    """The settings, checked; None when GitHub sign-in is off (no client ID). Raises SettingsError."""
    if not (client_id or "").strip():
        others = [n for n, v in (("--github-client-secret-file", client_secret), ("--github-url", github_url),
                                 ("--github-allowed-orgs", allowed_orgs), ("--github-admin-teams", admin_teams),
                                 ("--github-member-teams", member_teams)) if v]
        if others:
            raise SettingsError(f"{', '.join(others)} given without --github-client-id (TRAILBEAM_GITHUB_CLIENT_ID)")
        return None
    if not (client_secret or "").strip():
        raise SettingsError("--github-client-id needs the OAuth app's client secret: --github-client-secret-file "
                            "(TRAILBEAM_GITHUB_CLIENT_SECRET_FILE), or the secret in TRAILBEAM_GITHUB_CLIENT_SECRET")
    if not (public_url or "").strip():
        raise SettingsError("--github-client-id needs --public-url (TRAILBEAM_PUBLIC_URL), the address people open "
                            "Trailbeam at, such as https://trailbeam.example.com: GitHub sends them back to it")
    public_url = check_url(public_url, "--public-url")
    if urllib.parse.urlsplit(public_url).path not in ("", "/"):
        raise SettingsError(f"--public-url must be the server's address without a path, got {public_url!r}")
    base = check_url(github_url, "--github-url").rstrip("/") if (github_url or "").strip() else GITHUB_URL
    if urllib.parse.urlsplit(base).path not in ("", "/"):
        raise SettingsError(f"--github-url must be the GitHub server's address without a path, got {github_url!r}")
    admins, members = _teams(admin_teams, "--github-admin-teams"), _teams(member_teams, "--github-member-teams")
    if default_role is None or default_role == "":
        role: Optional[str] = None if members else "member"
    elif default_role == "none":
        role = None
    elif default_role == "member":
        role = "member"
    else:
        raise SettingsError(f"--github-default-role must be member or none, got {default_role!r} (admins come from "
                            "--github-admin-teams)")
    orgs = tuple(o.lower() for o in _names(allowed_orgs))
    if role is not None and not orgs and not members and not (default_role or "").strip():
        # an identity provider is your organisation's; GitHub is everyone's
        raise SettingsError("this would let any GitHub account in, as a member: set --github-allowed-orgs (or "
                            "--github-member-teams), or --github-default-role member if that is what you want")
    try:
        hours = _session_hours(session_hours)
    except SettingsError as e:
        raise SettingsError(str(e).replace("--oidc-session-hours", "--github-session-hours")) from None
    return Settings(client_id=client_id.strip(), client_secret=client_secret.strip(),
                    public_url=public_url.rstrip("/"), github_url=base,
                    allowed_orgs=orgs, admin_teams=admins,
                    member_teams=members, default_role=role, link_email=bool(link_email), session_hours=hours)


# ---- who gets in -------------------------------------------------------------------------------------------


class Person(NamedTuple):
    subject: str   # the GitHub user ID
    login: str
    email: str
    name: str
    role: Optional[str]


def primary_email(emails) -> str:
    """The primary address from ``GET /user/emails``, which must be verified. Raises OidcError."""
    rows = [e for e in emails if isinstance(e, dict)] if isinstance(emails, list) else []
    primary = next((e for e in rows if e.get("primary") is True), None)
    if primary is None or not isinstance(primary.get("email"), str) or "@" not in primary["email"]:
        raise OidcError("GitHub didn't send your email address, which this server signs people in by.",
                        f"/user/emails has no primary address: {rows[:5]!r}", 403)
    if primary.get("verified") is not True:
        raise OidcError("Your primary email address isn't verified at GitHub, so you can't sign in here with it yet. "
                        "Verify it at GitHub, then try again.", f"{primary['email']}: not verified at GitHub", 403)
    return primary["email"].strip()


class Rules:
    def __init__(self, settings: Settings):
        self.s = settings

    def role(self, teams: set) -> Optional[str]:
        if teams & set(self.s.admin_teams):
            return "admin"
        if teams & set(self.s.member_teams):
            return "member"
        return self.s.default_role

    def person(self, user: dict, email: str, orgs: set, teams: set) -> Person:
        """``user``: ``GET /user``. ``orgs``: lowercased logins of active memberships. ``teams``: ``org/slug``,
        lowercased. Raises OidcError when the rules refuse them."""
        uid = user.get("id")
        if not isinstance(uid, int) or isinstance(uid, bool) or uid <= 0:
            raise OidcError("GitHub sent an answer this server can't read.", f"/user without an id: {user!r}"[:300],
                            502)
        login = user.get("login") if isinstance(user.get("login"), str) else ""
        if self.s.allowed_orgs and not orgs & set(self.s.allowed_orgs):
            raise OidcError("Your GitHub account isn't in an organisation this server lets in. If you are, an owner "
                            "of the organisation may need to approve this server's GitHub app first.",
                            f"{login} ({uid}) is in {sorted(orgs)!r}, none of --github-allowed-orgs", 403)
        name = user.get("name")
        name = (name.strip() if isinstance(name, str) and name.strip() and name.isprintable() else
                login or email.split("@", 1)[0])
        return Person(str(uid), login, email, name[:200], self.role(teams))


# ---- GitHub ------------------------------------------------------------------------------------------------


def _next_link(header: Optional[str]) -> Optional[str]:
    for part in (header or "").split(","):
        url, _, rel = part.partition(";")
        if 'rel="next"' in rel:
            return url.strip().strip("<>")
    return None


class Provider:
    """GitHub's OAuth endpoints and REST API, over urllib with timeouts."""

    def __init__(self, settings: Settings, *, timeout_s: float = HTTP_TIMEOUT_S):
        self.s = settings
        self.timeout_s = timeout_s

    def _call(self, url: str, headers: Optional[dict] = None, data: Optional[bytes] = None):
        """``(the JSON answer, the Link header)``. Raises OidcError."""
        h = {"Accept": "application/json", "User-Agent": f"trailbeam/{__version__}", **(headers or {})}
        req = urllib.request.Request(url, data=data, headers=h)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_s) as r:  # noqa: S310 - URLs checked by check_url
                body, link = r.read(MAX_RESPONSE + 1), r.headers.get("Link")
        except urllib.error.HTTPError as e:
            text = e.read(2000).decode("utf-8", "replace")
            raise OidcError("GitHub refused the sign-in. Try again, or ask your administrator.",
                            f"{url}: HTTP {e.code}: {text}", 502) from None
        except (urllib.error.URLError, OSError, ValueError) as e:
            raise OidcError("GitHub can't be reached. Try again in a moment.", f"{url}: {e}", 502) from None
        if len(body) > MAX_RESPONSE:
            raise OidcError("GitHub sent an answer this server can't read.", f"{url}: over 1 MiB", 502)
        try:
            return json.loads(body), link
        except ValueError:
            raise OidcError("GitHub sent an answer this server can't read.", f"{url}: not JSON: {body[:200]!r}",
                            502) from None

    def authorize_url(self, state: str, verifier: str) -> str:
        q = {"client_id": self.s.client_id, "redirect_uri": self.s.redirect_uri, "scope": " ".join(self.s.scopes),
             "state": state, "code_challenge": pkce_challenge(verifier), "code_challenge_method": "S256",
             "allow_signup": "false"}
        return self.s.github_url + "/login/oauth/authorize?" + urllib.parse.urlencode(q)

    def exchange(self, code: str, verifier: str) -> str:
        """The access token for this code. Raises OidcError."""
        form = {"client_id": self.s.client_id, "client_secret": self.s.client_secret, "code": code,
                "redirect_uri": self.s.redirect_uri, "code_verifier": verifier}
        doc, _ = self._call(self.s.github_url + "/login/oauth/access_token",
                            {"Content-Type": "application/x-www-form-urlencoded"},
                            urllib.parse.urlencode(form).encode())
        if not isinstance(doc, dict):
            raise OidcError("GitHub sent an answer this server can't read.", "token endpoint: not an object", 502)
        token = doc.get("access_token")
        if doc.get("error") or not isinstance(token, str) or not token:
            # GitHub answers 200 with an error: bad_verification_code (used or old), incorrect_client_credentials...
            raise OidcError("GitHub didn't accept this sign-in. Start again from the sign-in page.",
                            f"token endpoint: error={doc.get('error')!r} {doc.get('error_description')!r}", 400)
        return token

    def _api(self, path: str, token: str, *, pages: bool = False):
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
                   "X-GitHub-Api-Version": API_VERSION}
        url: Optional[str] = self.s.api_url + path
        if not pages:
            return self._call(url, headers)[0]
        out: list = []
        api = urllib.parse.urlsplit(self.s.api_url)
        for _ in range(MAX_PAGES):
            doc, link = self._call(url, headers)
            if not isinstance(doc, list):
                raise OidcError("GitHub sent an answer this server can't read.", f"{path}: not a list", 502)
            out.extend(doc)
            url = _next_link(link)
            if not url:
                break
            n = urllib.parse.urlsplit(url)
            if (n.scheme, n.netloc) != (api.scheme, api.netloc):  # the token goes nowhere else
                break
        return out

    def user(self, token: str) -> dict:
        doc = self._api("/user", token)
        if not isinstance(doc, dict):
            raise OidcError("GitHub sent an answer this server can't read.", "/user: not an object", 502)
        return doc

    def emails(self, token: str) -> list:
        return self._api("/user/emails?per_page=100", token, pages=True)

    def orgs(self, token: str) -> set:
        rows = self._api("/user/memberships/orgs?state=active&per_page=100", token, pages=True)
        return {r["organization"]["login"].lower() for r in rows
                if isinstance(r, dict) and isinstance(r.get("organization"), dict)
                and isinstance(r["organization"].get("login"), str) and r.get("state", "active") == "active"}

    def teams(self, token: str) -> set:
        rows = self._api("/user/teams?per_page=100", token, pages=True)
        return {f"{r['organization']['login'].lower()}/{r['slug'].lower()}" for r in rows
                if isinstance(r, dict) and isinstance(r.get("slug"), str) and isinstance(r.get("organization"), dict)
                and isinstance(r["organization"].get("login"), str)}


# ---- the sign-in ------------------------------------------------------------------------------------------


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", "surrogatepass")).hexdigest()


def browser_cookie(cookie_header: Optional[str], secure: bool) -> Optional[str]:
    """As oidc.browser_cookie: over HTTPS only ``__Secure-trailbeam_github`` counts."""
    for name in ((SECURE_COOKIE,) if secure else (SECURE_COOKIE, COOKIE)):
        value = cookie(cookie_header, name)
        if value is not None and 20 <= len(value) <= 100:
            return value
    return None


def set_browser_cookie(value: str, secure: bool, max_age_s: int = LOGIN_TTL_MS // 1000) -> str:
    name = SECURE_COOKIE if secure else COOKIE
    return (f"{name}={value}; Path={START_PATH}; HttpOnly; SameSite=Lax; Max-Age={max(0, int(max_age_s))}"
            + ("; Secure" if secure else ""))


def clear_browser_cookie(secure: bool) -> str:
    return set_browser_cookie("", secure, 0)


class GitHub:
    """The two steps of a GitHub sign-in, with oidc.Oidc's interface. ``provider``: tests pass a fake GitHub's."""

    def __init__(self, settings: Settings, accounts, sessions, provider: Optional[Provider] = None, *,
                 now_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000):
        self.settings = settings
        self.accounts = accounts
        self.sessions = sessions
        self.provider = provider or Provider(settings)
        self.rules = Rules(settings)
        self.now_ms = now_ms

    def begin(self, next_path: str) -> tuple[str, str]:
        state, verifier, browser = secrets.token_urlsafe(32), secrets.token_urlsafe(48), secrets.token_urlsafe(32)
        url = self.provider.authorize_url(state, verifier)
        now = self.now_ms()
        self.accounts.create_oidc_login(_sha256(state), _sha256(browser), MARKER, verifier, next_path, now,
                                        now + LOGIN_TTL_MS)
        return url, browser

    def finish(self, query: dict, browser: Optional[str]) -> Done:
        state = query.get("state") or ""
        if query.get("error"):
            err = str(query.get("error"))[:100]
            msg = ("You cancelled the sign-in at GitHub." if err == "access_denied"
                   else "GitHub couldn't sign you in.")
            raise OidcError(msg, f"GitHub answered error={err!r} {str(query.get('error_description') or '')[:300]!r}",
                            403)
        code = query.get("code") or ""
        if not state or not code or len(state) > 200 or len(code) > 4096:
            raise OidcError("This sign-in link is incomplete. Start again from the sign-in page.",
                            "callback without code or state", 400)
        row = self.accounts.take_oidc_login(_sha256(state), self.now_ms())
        if row is None or row["nonce"] != MARKER:
            raise OidcError("This sign-in has been used or took too long (sign-ins last 10 minutes). Start again "
                            "from the sign-in page.", "unknown, used, expired or not a GitHub state", 400)
        if not browser or not hmac.compare_digest(_sha256(browser), row["browser_sha256"]):
            raise OidcError("This sign-in was started in another browser. Start again from the sign-in page.",
                            "the trailbeam_github cookie doesn't match the state (or is missing)", 400)
        token = self.provider.exchange(code, row["code_verifier"])
        user = self.provider.user(token)
        email = primary_email(self.provider.emails(token))
        orgs = self.provider.orgs(token) if self.settings.allowed_orgs else set()
        teams = self.provider.teams(token) if self.settings.maps_teams else set()
        person = self.rules.person(user, email, orgs, teams)
        issuer = self.settings.github_url
        now = self.now_ms()
        try:
            account, how = self.accounts.oidc_sign_in(
                issuer, person.subject, person.email, person.name, person.role, now,
                role_from_provider=self.settings.maps_teams, link_email=self.settings.link_email,
                email_verified=True)
        except SsoRefused as e:
            msg = {"email_taken": "This email already has an account on this server. Sign in with its password, "
                                  "or ask an admin.",
                   "no_role": "Your GitHub teams give you no access to this server. Ask your administrator.",
                   "no_admin_yet": "This server has no admin yet. Ask whoever runs it to create one first.",
                   "disabled": "Your account on this server is disabled. Ask an admin."}.get(e.reason, str(e))
            raise OidcError(msg, f"{person.email} (GitHub {person.login} {person.subject}): {e}", 403) from None
        except AccountError as e:
            raise OidcError(f"Your account can't be created here: {e}.", f"{person.email}: {e}", 403) from None
        kept = self.settings.maps_teams and person.role != "admin" and account["role"] == "admin"
        self.sessions.forget_user(int(account["id"]))
        sid, expires = self.sessions.start(int(account["id"]), self.settings.session_ms)
        return Done(account, how, sid, expires, row["next_path"], kept)


__all__ = ["Settings", "GitHub", "Provider", "Rules", "Person", "make_settings", "read_secret_file", "primary_email",
           "START_PATH", "CALLBACK_PATH", "browser_cookie", "set_browser_cookie", "clear_browser_cookie"]
