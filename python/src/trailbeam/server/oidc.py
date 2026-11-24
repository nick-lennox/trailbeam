"""Single sign-on with OpenID Connect (users and roles, phase 3b; proposal on QV-149, section 1).

"Sign in with Okta / Microsoft Entra / Google Workspace / Keycloak": the authorization code flow with PKCE, for a
confidential client (a client secret) or a public one (none). Off unless ``--oidc-issuer`` is set. Checking the
identity provider's signatures needs PyJWT and ``cryptography``, which the optional extra brings:
``pip install "trailbeam[server,sso]"``. Nothing else is imported unless single sign-on is on.

**The flow.**

1. ``GET /login/oidc?next=/path`` (``Oidc.begin``) makes a ``state``, a ``nonce`` and a PKCE ``code_verifier``,
   keeps them in the store's ``oidc_logins`` table for ``LOGIN_TTL_MS`` (10 minutes), and sends the browser to the
   provider's authorization endpoint. The row is found by the SHA-256 of ``state``, so any replica can take the
   callback: replicas share no secret, so the state can't live in a signed cookie. The browser also gets a random
   value in the ``trailbeam_oidc`` cookie (``SameSite=Lax``, so it comes back on the provider's redirect), whose
   SHA-256 is in the row: a callback URL made in another browser (someone else's code, to sign a victim in as
   them) is refused.
2. The provider sends the browser to ``<public URL>/login/oidc/callback?code=…&state=…`` (``Oidc.finish``). The row
   is deleted as it is read, so a state works once. The code is exchanged at the token endpoint, with the
   verifier and the client secret, for an ID token, whose signature (from the provider's JWKS), ``iss``, ``aud``
   (and ``azp``), ``exp``, ``iat`` and ``nonce`` are checked. Then the account rules below, then a session
   (sessions.py), the same as signing in with a password.
3. The callback answers with a small page that moves to ``next`` itself, not with a redirect: browsers hold back a
   ``SameSite=Strict`` cookie, such as the new session's, for the rest of a redirect chain that another site
   started, so a 303 would land on the sign-in page again.

**The redirect URI** is ``--public-url`` plus ``/login/oidc/callback``, never built from the request's ``Host``,
which a client or proxy can set to anything. Register that exact URI at the provider.

**Who gets in, and with which role** (``Rules``):

- ``--oidc-allowed-domains``: if set, only emails in those domains, and the provider must say the email is
  verified (``email_verified``). Google Workspace also sends ``hd``, its domain, and for Google it is required:
  a personal Google account can carry a verified address of any domain.
- Groups (``--oidc-groups-claim``, default ``groups``): someone in a group of ``--oidc-admin-groups`` is an admin,
  else someone in ``--oidc-member-groups`` a member, else they get ``--oidc-default-role``: ``member``, or
  ``none`` (refused). The default is ``member`` with no member groups given, else ``none``. When groups are mapped,
  the provider decides: a person's role is set again at every sign-in (the last admin who can sign in stays one).
  Without group settings, a new person gets the default role, and admins change it here afterwards.
- **Known limits.** Google doesn't put groups in its ID token (they need its Admin SDK): use allowed domains and a
  default role. Microsoft Entra leaves the groups out for someone in more than 200 of them and sends a pointer to
  Microsoft Graph instead (a "groups overage"); they are refused with a message saying so. Configure Entra to send
  only the groups assigned to the application to avoid it.
- An account is the provider's ``iss`` and ``sub`` (``oidc_identities``), not its email, which can change. The
  first sign-in of an account makes a person with no password, so only single sign-on lets them in, and only
  from an email the provider verified (``email_verified``), unless ``--oidc-allow-unverified-email`` is on: else
  someone who sets another person's address at the provider would hold an account under it, which an admin might
  promote. An email that already has an account here is refused, unless ``--oidc-link-email`` is on, the email is
  verified and no provider account signs in to that account yet: then the two are linked. That is off by default
  because a provider that lets people set their own email would let them take over an account, admins included.
- While the server has no admin, only someone the groups make an admin can sign in, and they become an admin.

Signing out (``/logout``) ends the Trailbeam session only, not the one at the provider.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import ipaddress
import json
import logging
import secrets
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from http.cookies import CookieError, SimpleCookie
from typing import Callable, NamedTuple, Optional

from .accounts import ROLES, AccountError, SsoRefused

log = logging.getLogger("trailbeam.server")

START_PATH = "/login/oidc"
CALLBACK_PATH = "/login/oidc/callback"
COOKIE = "trailbeam_oidc"
SECURE_COOKIE = "__Secure-" + COOKIE  # Path isn't /, so not __Host-
LOGIN_TTL_MS = 10 * 60_000
STARTS_PER_ADDRESS = 30  # sign-ins one client address may start ...
STARTS_WINDOW_S = 60.0   # ... in this many seconds, per replica (server.py)
DISCOVERY_TTL_S = 3600.0
HTTP_TIMEOUT_S = 10.0
MAX_RESPONSE = 1 << 20
CLOCK_LEEWAY_S = 60
# asymmetric signatures only: an ID token signed with the client secret (HS256) or not at all is never accepted
ALGORITHMS = ("RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256", "ES384", "ES512", "EdDSA")
GOOGLE_ISSUERS = ("https://accounts.google.com", "accounts.google.com")
EXTRA = 'pip install "trailbeam[server,sso]"'


class SettingsError(ValueError):
    pass


class OidcError(Exception):
    """A sign-in that didn't work. ``message``: for the person, on the page. ``detail``: for the server's log only."""

    def __init__(self, message: str, detail: str = "", status: int = 400):
        super().__init__(detail or message)
        self.message = message
        self.detail = detail or message
        self.status = status


def _is_loopback(host: Optional[str]) -> bool:
    if not host:
        return False
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def check_url(url: str, what: str) -> str:
    """An absolute https URL (http only to this machine, for tests and local providers); no query or fragment."""
    u = urllib.parse.urlsplit((url or "").strip())
    if u.scheme not in ("https", "http") or not u.hostname:
        raise SettingsError(f"{what} must be an absolute URL such as https://id.example.com/realms/main, got {url!r}")
    if u.scheme == "http" and not _is_loopback(u.hostname):
        raise SettingsError(f"{what} must be https (http only for localhost), got {url!r}")
    if u.query or u.fragment:
        raise SettingsError(f"{what} must not have a query or fragment, got {url!r}")
    return url.strip()


def _names(text) -> tuple:
    if text is None:
        return ()
    if isinstance(text, (list, tuple)):
        text = ",".join(text)
    return tuple(dict.fromkeys(p.strip() for p in str(text).split(",") if p.strip()))


@dataclass(frozen=True)
class Settings:
    issuer: str
    client_id: str
    public_url: str
    client_secret: Optional[str] = None
    scopes: tuple = ("openid", "email", "profile")
    groups_claim: str = "groups"
    admin_groups: tuple = ()
    member_groups: tuple = ()
    default_role: Optional[str] = "member"  # None: refused
    allowed_domains: tuple = ()
    link_email: bool = False
    allow_unverified_email: bool = False
    label: str = "single sign-on"

    @property
    def redirect_uri(self) -> str:
        return self.public_url.rstrip("/") + CALLBACK_PATH

    @property
    def maps_groups(self) -> bool:
        return bool(self.admin_groups or self.member_groups)


def make_settings(*, issuer: Optional[str], client_id: Optional[str], public_url: Optional[str],
                  client_secret: Optional[str] = None, scopes=None, groups_claim: Optional[str] = None,
                  admin_groups=None, member_groups=None, default_role: Optional[str] = None,
                  allowed_domains=None, link_email: bool = False, allow_unverified_email: bool = False,
                  label: Optional[str] = None) -> Optional[Settings]:
    """The settings, checked; None when single sign-on is off (no issuer). Raises SettingsError."""
    if not (issuer or "").strip():
        others = [n for n, v in (("--oidc-client-id", client_id), ("--oidc-admin-groups", admin_groups),
                                 ("--oidc-member-groups", member_groups), ("--oidc-allowed-domains", allowed_domains))
                  if v]
        if others:
            raise SettingsError(f"{', '.join(others)} given without --oidc-issuer (TRAILBEAM_OIDC_ISSUER)")
        return None
    issuer = check_url(issuer, "--oidc-issuer")  # kept exactly as given: it must equal the tokens' iss
    if not (client_id or "").strip():
        raise SettingsError("--oidc-issuer needs --oidc-client-id (TRAILBEAM_OIDC_CLIENT_ID)")
    if not (public_url or "").strip():
        raise SettingsError("--oidc-issuer needs --public-url (TRAILBEAM_PUBLIC_URL), the address people open "
                            "Trailbeam at, such as https://trailbeam.example.com: the identity provider sends them "
                            "back to it")
    public_url = check_url(public_url, "--public-url")
    if urllib.parse.urlsplit(public_url).path not in ("", "/"):
        raise SettingsError(f"--public-url must be the server's address without a path, got {public_url!r}")
    sc = _names((scopes or "openid email profile").replace(" ", ","))
    if "openid" not in sc:
        sc = ("openid",) + sc
    admins, members = _names(admin_groups), _names(member_groups)
    if default_role is None or default_role == "":
        role: Optional[str] = None if members else "member"
    elif default_role == "none":
        role = None
    elif default_role in ROLES and default_role != "admin":
        role = default_role
    else:
        raise SettingsError(f"--oidc-default-role must be member or none, got {default_role!r} (admins come from "
                            "--oidc-admin-groups)")
    domains = tuple(d.lower().lstrip("@") for d in _names(allowed_domains))
    claim = (groups_claim or "groups").strip()
    if not claim:
        raise SettingsError("--oidc-groups-claim must not be empty")
    secret = (client_secret or "").strip() or None
    return Settings(issuer=issuer, client_id=client_id.strip(), public_url=public_url.rstrip("/"),
                    client_secret=secret, scopes=sc, groups_claim=claim, admin_groups=admins,
                    member_groups=members, default_role=role, allowed_domains=domains, link_email=bool(link_email),
                    allow_unverified_email=bool(allow_unverified_email),
                    label=(label or "").strip() or "single sign-on")


def read_secret_file(path: str) -> str:
    try:
        with open(path, encoding="utf-8") as f:
            s = f.read().strip()
    except OSError as e:
        raise SettingsError(f"cannot read the client secret file {path}: {e.strerror or e}") from None
    if not s:
        raise SettingsError(f"the client secret file {path} is empty")
    return s


def require_pyjwt():
    """The jwt module, or SettingsError saying which extra to install."""
    try:
        import jwt  # noqa: F401
        from jwt import PyJWKClient  # noqa: F401
        import cryptography  # noqa: F401
    except ImportError:
        raise SettingsError(f"single sign-on (--oidc-issuer) needs PyJWT and cryptography: {EXTRA}") from None
    return jwt


# ---- who gets in -------------------------------------------------------------------------------------------


class Person(NamedTuple):
    """What the ID token (and userinfo) say, once the rules let the person in."""
    issuer: str
    subject: str
    email: str
    name: str
    email_verified: bool
    role: Optional[str]


class Rules:
    def __init__(self, settings: Settings):
        self.s = settings

    def groups(self, claims: dict) -> tuple:
        """The person's groups, from the groups claim (a list, or one string). Raises OidcError on an Entra
        groups overage, when groups are mapped."""
        v = claims.get(self.s.groups_claim)
        if v is None:
            names = claims.get("_claim_names")
            if self.s.maps_groups and isinstance(names, dict) and self.s.groups_claim in names:
                raise OidcError(
                    "Your identity provider didn't send your groups, because you are in too many of them (Microsoft "
                    "Entra sends at most 200). Ask your administrator to send only the groups assigned to this "
                    "application.", f"groups overage: the ID token points to {names[self.s.groups_claim]!r} "
                    "instead of listing the groups", 403)
            return ()
        if isinstance(v, str):
            return (v,)
        if isinstance(v, list):
            return tuple(g for g in v if isinstance(g, str))
        return ()

    def role(self, claims: dict) -> Optional[str]:
        g = set(self.groups(claims))
        if g & set(self.s.admin_groups):
            return "admin"
        if g & set(self.s.member_groups):
            return "member"
        return self.s.default_role

    def person(self, claims: dict) -> Person:
        """Raises OidcError when the rules refuse this account."""
        email = claims.get("email")
        if not isinstance(email, str) or "@" not in email:
            raise OidcError("Your identity provider didn't send an email address, which this server signs people in "
                            "by. Ask your administrator to add the email scope or claim.",
                            "the ID token and userinfo have no email claim", 403)
        email = email.strip()
        verified = claims.get("email_verified") in (True, "true")
        if self.s.allowed_domains:
            domain = email.rsplit("@", 1)[1].lower()
            hd = claims.get("hd")
            if self.s.issuer in GOOGLE_ISSUERS and not isinstance(hd, str):
                raise OidcError("Only accounts of your organisation's Google Workspace can sign in here.",
                                f"{email}: no hd claim (a personal Google account)", 403)
            if not verified or domain not in self.s.allowed_domains or (
                    isinstance(hd, str) and hd.lower() not in self.s.allowed_domains):
                raise OidcError("Your account isn't one this server lets in.",
                                f"{email} (verified: {verified}, hd: {hd!r}) is not in --oidc-allowed-domains", 403)
        name = next((claims[k].strip() for k in ("name", "preferred_username")
                     if isinstance(claims.get(k), str) and claims[k].strip() and claims[k].isprintable()),
                    email.split("@", 1)[0])
        return Person(claims["iss"], claims["sub"], email, name[:200], verified, self.role(claims))


# ---- the identity provider -------------------------------------------------------------------------------


def _b64url(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode("ascii")


def pkce_challenge(verifier: str) -> str:
    return _b64url(hashlib.sha256(verifier.encode("ascii")).digest())


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", "surrogatepass")).hexdigest()


class Provider:
    """Discovery, signing keys and the token endpoint of one issuer, over urllib with timeouts. Discovery is read on
    the first sign-in (a provider that is down doesn't stop the server starting) and kept for DISCOVERY_TTL_S."""

    def __init__(self, settings: Settings, *, timeout_s: float = HTTP_TIMEOUT_S,
                 clock: Callable[[], float] = time.monotonic):
        self.s = settings
        self.timeout_s = timeout_s
        self.clock = clock
        self.jwt = require_pyjwt()
        self._doc: Optional[dict] = None
        self._doc_at = float("-inf")
        self._keys = None
        self._lock = threading.Lock()

    def _get_json(self, url: str, headers: Optional[dict] = None, data: Optional[bytes] = None) -> dict:
        req = urllib.request.Request(url, data=data, headers={"Accept": "application/json", **(headers or {})})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_s) as r:  # noqa: S310 - URLs checked by check_url
                body = r.read(MAX_RESPONSE + 1)
        except urllib.error.HTTPError as e:
            text = e.read(2000).decode("utf-8", "replace")
            raise OidcError("The identity provider refused the sign-in. Try again, or ask your administrator.",
                            f"{url}: HTTP {e.code}: {text}", 502) from None
        except (urllib.error.URLError, OSError, ValueError) as e:
            raise OidcError("The identity provider can't be reached. Try again in a moment.", f"{url}: {e}", 502) \
                from None
        if len(body) > MAX_RESPONSE:
            raise OidcError("The identity provider sent an answer this server can't read.", f"{url}: over 1 MiB", 502)
        try:
            doc = json.loads(body)
        except ValueError:
            raise OidcError("The identity provider sent an answer this server can't read.",
                            f"{url}: not JSON: {body[:200]!r}", 502) from None
        if not isinstance(doc, dict):
            raise OidcError("The identity provider sent an answer this server can't read.", f"{url}: not an object",
                            502)
        return doc

    def discovery(self) -> dict:
        with self._lock:
            if self._doc is not None and self.clock() - self._doc_at < DISCOVERY_TTL_S:
                return self._doc
        url = self.s.issuer.rstrip("/") + "/.well-known/openid-configuration"
        doc = self._get_json(url)
        if doc.get("issuer") != self.s.issuer:
            raise OidcError("Single sign-on is set up wrongly on this server.",
                            f"{url} names the issuer {doc.get('issuer')!r}, not --oidc-issuer {self.s.issuer!r} "
                            "(they must be the same, character for character)", 502)
        for k in ("authorization_endpoint", "token_endpoint", "jwks_uri"):
            try:
                check_url(doc.get(k) or "", k)
            except SettingsError as e:
                raise OidcError("Single sign-on is set up wrongly on this server.", f"{url}: {e}", 502) from None
        if doc.get("userinfo_endpoint"):
            try:
                check_url(doc["userinfo_endpoint"], "userinfo_endpoint")
            except SettingsError:
                doc = {k: v for k, v in doc.items() if k != "userinfo_endpoint"}
        with self._lock:
            if self._doc is None or self._doc.get("jwks_uri") != doc["jwks_uri"]:
                self._keys = self.jwt.PyJWKClient(doc["jwks_uri"], cache_keys=True, lifespan=300,
                                                  timeout=self.timeout_s)
            self._doc, self._doc_at = doc, self.clock()
        return doc

    def authorize_url(self, state: str, nonce: str, verifier: str) -> str:
        doc = self.discovery()
        q = {"response_type": "code", "client_id": self.s.client_id, "redirect_uri": self.s.redirect_uri,
             "scope": " ".join(self.s.scopes), "state": state, "nonce": nonce,
             "code_challenge": pkce_challenge(verifier), "code_challenge_method": "S256"}
        ep = doc["authorization_endpoint"]
        return ep + ("&" if "?" in ep else "?") + urllib.parse.urlencode(q)

    def exchange(self, code: str, verifier: str) -> dict:
        """The token endpoint's answer for this code (``id_token``, ``access_token``, ...)."""
        doc = self.discovery()
        form = {"grant_type": "authorization_code", "code": code, "redirect_uri": self.s.redirect_uri,
                "code_verifier": verifier}
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        methods = doc.get("token_endpoint_auth_methods_supported") or ["client_secret_basic"]
        if self.s.client_secret is None:
            form["client_id"] = self.s.client_id
        elif "client_secret_basic" in methods or "client_secret_post" not in methods:
            q = urllib.parse.quote
            pair = f"{q(self.s.client_id, safe='')}:{q(self.s.client_secret, safe='')}"
            headers["Authorization"] = "Basic " + base64.b64encode(pair.encode()).decode()
        else:
            form.update(client_id=self.s.client_id, client_secret=self.s.client_secret)
        return self._get_json(doc["token_endpoint"], headers, urllib.parse.urlencode(form).encode())

    def userinfo(self, access_token: str) -> dict:
        ep = self.discovery().get("userinfo_endpoint")
        if not ep or not access_token:
            return {}
        return self._get_json(ep, {"Authorization": f"Bearer {access_token}"})

    def id_claims(self, id_token: str, nonce: str) -> dict:
        """The ID token's claims, once its signature and claims check out. Raises OidcError."""
        jwt = self.jwt
        doc = self.discovery()
        algs = [a for a in (doc.get("id_token_signing_alg_values_supported") or ["RS256"]) if a in ALGORITHMS] or \
            ["RS256"]
        try:
            key = self._keys.get_signing_key_from_jwt(id_token)
            claims = jwt.decode(id_token, key.key, algorithms=algs, audience=self.s.client_id, issuer=self.s.issuer,
                                leeway=CLOCK_LEEWAY_S,
                                options={"require": ["iss", "sub", "aud", "exp", "iat"], "verify_signature": True})
        except jwt.PyJWKClientError as e:
            raise OidcError("The identity provider's signing keys can't be read. Try again in a moment.",
                            f"JWKS: {e}", 502) from None
        except jwt.InvalidTokenError as e:
            raise OidcError("The identity provider's answer didn't check out, so you are not signed in.",
                            f"ID token refused: {type(e).__name__}: {e}", 400) from None
        aud = claims.get("aud")
        if isinstance(aud, list) and len(aud) > 1 and claims.get("azp") != self.s.client_id:
            raise OidcError("The identity provider's answer didn't check out, so you are not signed in.",
                            f"ID token for several audiences with azp {claims.get('azp')!r}", 400)
        if not isinstance(claims.get("nonce"), str) or not hmac.compare_digest(claims["nonce"], nonce):
            raise OidcError("The identity provider's answer didn't check out, so you are not signed in.",
                            "ID token nonce doesn't match this sign-in", 400)
        if not isinstance(claims.get("sub"), str) or not claims["sub"]:
            raise OidcError("The identity provider's answer didn't check out, so you are not signed in.",
                            "ID token without sub", 400)
        return claims


# ---- the sign-in ------------------------------------------------------------------------------------------


def browser_cookie(cookie_header: Optional[str], secure: bool) -> Optional[str]:
    """The sign-in's browser value in a Cookie header. Over HTTPS (``secure``) only ``__Secure-trailbeam_oidc``
    counts: a plain ``trailbeam_oidc`` can be planted by a sibling subdomain or whoever answers plain ``http://``,
    with the value of a sign-in the planter started, which would sign the victim in as the planter. Over HTTP
    ``__Secure-trailbeam_oidc`` if there, else ``trailbeam_oidc`` (as sessions.cookie_value)."""
    if not cookie_header:
        return None
    try:
        c = SimpleCookie(cookie_header)
    except CookieError:
        return None
    for name in ((SECURE_COOKIE,) if secure else (SECURE_COOKIE, COOKIE)):
        m = c.get(name)
        if m is not None and 20 <= len(m.value) <= 100:
            return m.value
    return None


def set_browser_cookie(value: str, secure: bool, max_age_s: int = LOGIN_TTL_MS // 1000) -> str:
    name = SECURE_COOKIE if secure else COOKIE
    # Lax: the provider's redirect back is a top-level GET from another site, which Strict would hold it back from
    return (f"{name}={value}; Path={START_PATH}; HttpOnly; SameSite=Lax; Max-Age={max(0, int(max_age_s))}"
            + ("; Secure" if secure else ""))


def clear_browser_cookie(secure: bool) -> str:
    return set_browser_cookie("", secure, 0)


class Done(NamedTuple):
    user: dict
    how: str             # known, linked or created
    session_id: str
    expires_at_ms: int
    next_path: str
    kept_admin: bool     # the groups would have made the last admin a member; they stayed an admin


class Oidc:
    """The two steps of a sign-in, for one process. ``provider``: a Provider (tests pass one with a fake issuer)."""

    def __init__(self, settings: Settings, accounts, sessions, provider: Optional[Provider] = None, *,
                 now_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000):
        self.settings = settings
        self.accounts = accounts
        self.sessions = sessions
        self.provider = provider or Provider(settings)
        self.rules = Rules(settings)
        self.now_ms = now_ms

    def begin(self, next_path: str) -> tuple[str, str]:
        """``(the provider's authorization URL, the value for the browser cookie)``. Raises OidcError when the
        provider can't be reached."""
        state, nonce = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
        verifier, browser = secrets.token_urlsafe(48), secrets.token_urlsafe(32)
        url = self.provider.authorize_url(state, nonce, verifier)  # first, so a provider that is down adds no row
        now = self.now_ms()
        self.accounts.create_oidc_login(_sha256(state), _sha256(browser), nonce, verifier, next_path, now,
                                        now + LOGIN_TTL_MS)
        return url, browser

    def finish(self, query: dict, browser: Optional[str]) -> Done:
        """The callback's query (``code`` and ``state``, or ``error``) and the browser cookie's value. Raises
        OidcError."""
        state = query.get("state") or ""
        if query.get("error"):
            # the row stays until it runs out: whoever can send the browser here can't end someone's sign-in
            err = str(query.get("error"))[:100]
            desc = str(query.get("error_description") or "")[:300]
            msg = ("You cancelled the sign-in, or the identity provider didn't let you in."
                   if err in ("access_denied", "login_required", "interaction_required", "consent_required")
                   else "The identity provider couldn't sign you in.")
            raise OidcError(msg, f"the provider answered error={err!r} {desc!r}", 403)
        code = query.get("code") or ""
        if not state or not code or len(state) > 200 or len(code) > 4096:
            raise OidcError("This sign-in link is incomplete. Start again from the sign-in page.",
                            "callback without code or state", 400)
        row = self.accounts.take_oidc_login(_sha256(state), self.now_ms())
        if row is None:
            raise OidcError("This sign-in has been used or took too long (sign-ins last 10 minutes). Start again "
                            "from the sign-in page.", "unknown, used or expired state", 400)
        if not browser or not hmac.compare_digest(_sha256(browser), row["browser_sha256"]):
            raise OidcError("This sign-in was started in another browser. Start again from the sign-in page.",
                            "the trailbeam_oidc cookie doesn't match the state (or is missing)", 400)
        tokens = self.provider.exchange(code, row["code_verifier"])
        id_token = tokens.get("id_token")
        if not isinstance(id_token, str) or not id_token:
            raise OidcError("The identity provider's answer didn't check out, so you are not signed in.",
                            "the token endpoint sent no id_token", 502)
        claims = self.provider.id_claims(id_token, row["nonce"])
        needs_more = "email" not in claims or (self.settings.maps_groups and self.settings.groups_claim not in claims
                                               and "_claim_names" not in claims)
        if needs_more and isinstance(tokens.get("access_token"), str):
            info = self.provider.userinfo(tokens["access_token"])
            if info.get("sub") == claims["sub"]:  # OIDC Core 5.3.2: userinfo is for this sub, or ignored
                claims = {**{k: v for k, v in info.items() if k not in ("iss", "aud", "sub", "nonce")}, **claims}
        person = self.rules.person(claims)
        now = self.now_ms()
        try:
            user, how = self.accounts.oidc_sign_in(
                person.issuer, person.subject, person.email, person.name, person.role, now,
                role_from_provider=self.settings.maps_groups, link_email=self.settings.link_email,
                email_verified=person.email_verified,
                allow_unverified_email=self.settings.allow_unverified_email)
        except SsoRefused as e:
            msg = {"email_taken": "This email already has an account on this server. Sign in with its password, "
                                  "or ask an admin.",
                   "email_unverified": "Your identity provider hasn't verified your email address, so you can't "
                                       "sign in here with it yet. Verify it at the provider, or ask your "
                                       "administrator.",
                   "no_role": "Your groups at the identity provider give you no access to this server. Ask your "
                              "administrator.",
                   "no_admin_yet": "This server has no admin yet. Ask whoever runs it to create one first.",
                   "disabled": "Your account on this server is disabled. Ask an admin."}.get(e.reason, str(e))
            raise OidcError(msg, f"{person.email} ({person.issuer} {person.subject}): {e}", 403) from None
        except AccountError as e:
            raise OidcError(f"Your account can't be created here: {e}.", f"{person.email}: {e}", 403) from None
        kept = self.settings.maps_groups and person.role != "admin" and user["role"] == "admin"
        self.sessions.forget_user(int(user["id"]))  # a new role ended their sessions in the store: here at once too
        sid, expires = self.sessions.start(int(user["id"]))
        return Done(user, how, sid, expires, row["next_path"], kept)


def continue_page(next_path: str) -> tuple[int, dict, bytes]:
    """The callback's answer: a page that moves to ``next_path`` by itself (see the module's point 3)."""
    from html import escape

    n = escape(next_path, quote=True)
    html = ("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">"
            f"<meta http-equiv=\"refresh\" content=\"0;url={n}\"><title>Signed in - Trailbeam</title></head>"
            f"<body><p>Signed in. <a href=\"{n}\">Continue</a></p></body></html>")
    return 200, {"Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store",
                 "Referrer-Policy": "no-referrer"}, html.encode()


__all__ = ["Settings", "SettingsError", "OidcError", "Oidc", "Provider", "Rules", "Person", "Done",
           "make_settings", "read_secret_file", "require_pyjwt", "continue_page", "START_PATH", "CALLBACK_PATH",
           "browser_cookie", "set_browser_cookie", "clear_browser_cookie"]
