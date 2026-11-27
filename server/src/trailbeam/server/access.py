"""Who may do what: the access level of every route, and the switch from the shared token to accounts.

Every route the server answers has one level (``ROUTES``, checked by ``level_of``; a path that names no route
is ``MEMBER``, so a page or file added later is never open by mistake):

- ``OPEN``: ``/api/ready``, ``/api/health`` (which says only ``status`` and ``version`` to a request that isn't let
  in at ``MEMBER``), the sign-in page and its form, the first admin's setup form (``/setup``, first_admin.py),
  which needs its one-time code instead, single sign-on's two steps (``/login/oidc``, oidc.py), and signing in
  with a passkey (``/login/passkey``, passkeys.py).
- ``INGEST``: ``POST /v1/traces``.
- ``METRICS``: ``GET /metrics``.
- ``MEMBER``: the API and the web screen, message bodies included (an admin or a member, proposal section 2).
- ``ADMIN``: managing people and ingest tokens: ``/api/users`` and ``/api/tokens/ingest`` (``API_ROUTES``,
  people_api.py), the People page (``/people``), and ``trailbeam users`` / ``trailbeam tokens`` (cli.py). A person's
  own personal API tokens (``/api/me/tokens``) and passkeys (``/api/me/passkeys``) are ``MEMBER``, and need a
  person; so are the Tokens page (``/tokens``), which shows the ingest tokens only to an admin, and the Account
  page (``/account``).

**Two modes, switched by the store, not by a setting.** ``State`` asks the store whether an admin exists at most
every ``STATE_TTL_S`` (5 s), on every replica, so when the first admin is created every replica switches within
seconds and none needs a restart.

- **No admin yet** (as in 0.3.0): with ``TRAILBEAM_TOKEN`` set, every level but ``OPEN`` needs it (as
  ``Authorization: Bearer`` or ``x-trailbeam-key``, or the login page's ``trailbeam_session`` cookie); without it,
  everything is served. ``ADMIN`` is refused either way: there is nobody to be one.
- **An admin exists:** ``MEMBER`` needs a person: a personal API token (``tbp_…``), the email a proxy in front
  of Trailbeam signed in (``--proxy-auth-header``, proxy_auth.py), or a session (``trailbeam_sid``, sessions.py),
  looked for in that order, of an admin or a member; ``ADMIN`` needs an admin. ``TRAILBEAM_TOKEN`` keeps working for
  ``INGEST`` and ``METRICS`` only, and the ``trailbeam_session`` cookie no longer works.

**Ingest** needs a token when ``TRAILBEAM_TOKEN`` is set or at least one ingest token exists (revoked ones count),
in either mode: the shared token or a live ingest token (``tbi_…``). Otherwise it is open. A person's session or
personal token never sends spans, and an ingest token never reads.

A token is read from ``x-trailbeam-key`` when that header is there, else from ``Authorization: Bearer``. Only the
headers are read: a refused export is answered before its body is.
"""
from __future__ import annotations

import logging
import re
import threading
import time
from typing import Callable, NamedTuple, Optional

from . import auth as auth_mod, proxy_auth, sessions as sessions_mod
from .tokens import INGEST_PREFIX, PERSONAL_PREFIX

log = logging.getLogger("trailbeam.server")

OPEN, INGEST, METRICS, MEMBER, ADMIN = "open", "ingest", "metrics", "member", "admin"
STATE_TTL_S = 5.0
KEY_HEADER = "x-trailbeam-key"

# (method, path) -> level. A path not here is MEMBER (pages, the shared UI's files, the API).
ROUTES: dict[tuple[str, str], str] = {
    ("GET", "/api/ready"): OPEN,
    ("GET", "/api/health"): OPEN,
    ("GET", "/login"): OPEN,
    ("POST", "/login"): OPEN,
    ("GET", "/logout"): OPEN,
    ("GET", "/setup"): OPEN,   # the first admin's form; the setup code in it is the credential (first_admin.py)
    ("POST", "/setup"): OPEN,
    ("GET", "/login/oidc"): OPEN,           # single sign-on (oidc.py): to the identity provider
    ("GET", "/login/oidc/callback"): OPEN,  # and back; its state and the browser's cookie are the credential
    ("POST", "/login/passkey/options"): OPEN,  # passkeys (passkeys.py): a challenge to sign
    ("POST", "/login/passkey"): OPEN,          # and the signed answer, which is the credential
    ("POST", "/v1/traces"): INGEST,
    ("GET", "/metrics"): METRICS,
    ("GET", "/set-password"): OPEN,  # invite and reset links (people_api.py); the code in it is the credential
    ("POST", "/set-password"): OPEN,
    ("GET", "/people"): ADMIN,  # the People page; its data is /api/users, ADMIN too
    ("GET", "/account"): MEMBER,  # the Account page: your passkeys (/api/me/passkeys)
    ("GET", "/tokens"): MEMBER,  # the Tokens page: /api/me/tokens for everyone, /api/tokens/ingest (ADMIN) for admins
}

# The routes that manage people and tokens (people_api.py): (method, path template, level, handler name).
# ``{id}`` is a positive integer. Matched by the whole path, so /api/users/x is no route of these (and a 404).
API_ROUTES: tuple[tuple[str, str, str, str], ...] = (
    ("GET", "/api/users", ADMIN, "list_users"),
    ("POST", "/api/users", ADMIN, "create_user"),
    ("PATCH", "/api/users/{id}", ADMIN, "change_user"),
    ("DELETE", "/api/users/{id}", ADMIN, "remove_user"),
    ("POST", "/api/users/{id}/link", ADMIN, "user_link"),
    ("GET", "/api/tokens/ingest", ADMIN, "list_ingest_tokens"),
    ("POST", "/api/tokens/ingest", ADMIN, "create_ingest_token"),
    ("DELETE", "/api/tokens/ingest/{id}", ADMIN, "revoke_ingest_token"),
    ("GET", "/api/me/tokens", MEMBER, "list_my_tokens"),
    ("POST", "/api/me/tokens", MEMBER, "create_my_token"),
    ("DELETE", "/api/me/tokens/{id}", MEMBER, "revoke_my_token"),
    ("GET", "/api/me/passkeys", MEMBER, "list_my_passkeys"),
    ("POST", "/api/me/passkeys/options", MEMBER, "passkey_options"),
    ("POST", "/api/me/passkeys", MEMBER, "add_my_passkey"),
    ("DELETE", "/api/me/passkeys/{id}", MEMBER, "remove_my_passkey"),
)
_API_RX = tuple((m, re.compile("^" + re.escape(t).replace(re.escape("{id}"), "([1-9][0-9]{0,17})") + "$"), lvl, h)
                for m, t, lvl, h in API_ROUTES)


def api_route(method: str, path: str) -> Optional[tuple[str, str, Optional[int]]]:
    """The people-and-tokens route of a request: (level, handler name, the ``{id}`` or None), or None."""
    for m, rx, lvl, h in _API_RX:
        if m == method:
            hit = rx.match(path)
            if hit:
                return lvl, h, (int(hit.group(1)) if rx.groups else None)
    return None


def level_of(method: str, path: str) -> str:
    r = api_route(method, path)
    return r[0] if r else ROUTES.get((method, path), MEMBER)


class AccessUnavailable(Exception):
    """The store couldn't say who this is (or whether an admin exists): answer 503 / 500, never let it in."""


class Who(NamedTuple):
    """Who a request is. ``via``: ``none``, ``shared_token``, ``shared_cookie``, ``session``, ``personal_token``,
    ``proxy`` or ``ingest_token``. ``role``: the person's, for a session or personal token."""
    via: str
    role: Optional[str] = None
    user_id: Optional[int] = None
    email: Optional[str] = None
    name: Optional[str] = None


NOBODY = Who("none")


class Decision(NamedTuple):
    ok: bool
    who: Who = NOBODY
    status: int = 200  # 401 (not signed in) or 403 (signed in, not allowed) when not ok
    message: str = ""


SWITCH_ENV = "TRAILBEAM_ACCOUNTS"
_ON, _OFF = ("1", "true", "yes", "on"), ("", "0", "false", "no", "off")
SWITCHED_OFF = ("users and roles are off on this server (a preview in this release, not yet security-reviewed): "
                "start it with --accounts or TRAILBEAM_ACCOUNTS=1 to use them")


def switch_on(env) -> bool:
    """``TRAILBEAM_ACCOUNTS``: users and roles are a preview, off unless this says 1 (or true, yes, on). Off, the
    server runs as on a store without account tables: the shared token only, as in 0.4.0. ValueError on a value
    that is neither on nor off, so a typo never leaves the server in the mode nobody asked for."""
    v = (env.get(SWITCH_ENV) or "").strip().lower()
    if v in _ON:
        return True
    if v in _OFF:
        return False
    raise ValueError(f"{SWITCH_ENV}={env.get(SWITCH_ENV)!r}: expected 1 (on) or 0 (off)")


class State:
    """Whether an admin exists and whether an ingest token exists, from the store, at most ``ttl_s`` old.

    When the store doesn't answer, the last answer is kept (and asked again after ``ttl_s``); with no answer yet,
    AccessUnavailable is raised. Without accounts (a store that has no account tables): never an admin or token."""

    def __init__(self, accounts, ttl_s: float = STATE_TTL_S, clock: Callable[[], float] = time.monotonic):
        self.accounts = accounts
        self.ttl_s = ttl_s
        self.clock = clock
        self._value: Optional[tuple[bool, bool]] = None
        self._at = float("-inf")
        self._seen_changes = None
        self._lock = threading.Lock()

    def get(self) -> tuple[bool, bool]:
        if self.accounts is None:
            return False, False
        with self._lock:
            if self._value is not None:
                if getattr(self.accounts, "only_this_process", False):
                    # a :memory: store: nobody else can change it, and its reads wait for the writer
                    if self.accounts.changes == self._seen_changes:
                        return self._value
                elif self.clock() - self._at < self.ttl_s:
                    return self._value
            changes = getattr(self.accounts, "changes", None)
            try:
                self._value = self.accounts.auth_state()
                self._seen_changes = changes
            except Exception as e:  # noqa: BLE001
                if self._value is None:
                    raise AccessUnavailable(f"the store can't say whether an admin exists: {e}") from e
                log.warning("the store can't say whether an admin exists, keeping the last answer: %s", e)
            self._at = self.clock()
            return self._value

    def admin_exists(self) -> bool:
        return self.get()[0]

    def changed(self) -> None:
        """This process just created an admin or an ingest token: ask the store on the next request."""
        with self._lock:
            self._at = float("-inf")
            self._seen_changes = None


def credential(headers) -> Optional[str]:
    """The token a request carries: ``x-trailbeam-key`` if present, else the ``Authorization: Bearer`` value."""
    key = (headers.get(KEY_HEADER) or "").strip()
    if key:
        return key
    scheme, _, value = (headers.get("Authorization") or "").strip().partition(" ")
    if scheme.lower() != "bearer":
        return None
    return value.strip() or None


SHARED_UI_GONE = ("this server has user accounts now: TRAILBEAM_TOKEN works only for ingest and /metrics. "
                  "Sign in, or send a personal API token (Authorization: Bearer tbp_...)")
NEEDS_PERSON = "sign in, or send a personal API token (Authorization: Bearer tbp_...)"
NEEDS_SHARED = "this server needs Authorization: Bearer <TRAILBEAM_TOKEN>"
NEEDS_ADMIN = "this needs an admin"
NO_ADMIN_YET = "this needs an admin, and this server has none yet: create one with trailbeam users create"
NEEDS_INGEST_KEY = ("this server needs an ingest token: x-trailbeam-key: <token> or Authorization: Bearer <token> "
                    "(an ingest token, tbi_..., or TRAILBEAM_TOKEN)")


class Access:
    """The checks, for one process. ``shared``: the TRAILBEAM_TOKEN (auth.Auth) or None. ``accounts``,
    ``sessions``, ``tokens``: None on a store without account tables (then only the shared-token rules apply)."""

    def __init__(self, shared: Optional[auth_mod.Auth] = None, accounts=None, sessions=None, tokens=None,
                 state: Optional[State] = None, proxy: Optional[proxy_auth.ProxySignIn] = None):
        self.shared = shared
        self.accounts = accounts
        self.sessions = sessions
        self.tokens = tokens
        self.proxy = proxy  # sign-in through a proxy (proxy_auth.py); None: off
        self.state = state or State(accounts)

    def mode(self) -> str:
        """``accounts`` once an admin exists, else ``token`` with TRAILBEAM_TOKEN set, else ``off``."""
        if self.state.admin_exists():
            return "accounts"
        return "token" if self.shared is not None else "off"

    def ingest_closed(self) -> bool:
        return self.shared is not None or self.state.get()[1]

    def _is_shared(self, cred: Optional[str]) -> bool:
        return cred is not None and self.shared is not None and self.shared.token_ok(cred)

    def _ingest_token(self, cred: Optional[str]) -> bool:
        if self.tokens is None or not cred or not cred.startswith(INGEST_PREFIX):
            return False
        try:
            return self.tokens.ingest(cred) is not None
        except Exception as e:  # noqa: BLE001
            raise AccessUnavailable(f"the store can't check the ingest token: {e}") from e

    def _person(self, headers, cred: Optional[str], secure: bool, peer: Optional[str] = None) -> Optional[Who]:
        """The person behind a personal token (when one is sent), else the email the proxy names (when it is on
        and the request comes from it), else a session cookie (over HTTPS only the ``__Host-`` one, sessions.py).
        Raises ProxyRefused."""
        try:
            if cred and cred.startswith(PERSONAL_PREFIX) and self.tokens is not None:
                u = self.tokens.personal(cred)
                return None if u is None else Who("personal_token", u.role, u.user_id, u.email, u.name)
            if self.proxy is not None and peer is not None:
                p = self.proxy.user(headers, peer)
                if p is not None:
                    return Who("proxy", p["role"], int(p["id"]), p["email"], p["name"])
            if self.sessions is not None:
                u = self.sessions.lookup(sessions_mod.cookie_value(headers.get("Cookie"), secure))
                return None if u is None else Who("session", u.role, u.user_id, u.email, u.name)
        except proxy_auth.ProxyRefused:
            raise
        except Exception as e:  # noqa: BLE001
            raise AccessUnavailable(f"the store can't check the sign-in: {e}") from e
        return None

    def proxy_person(self, headers, peer: str) -> Optional[Who]:
        """Who the proxy says this request is, once an admin exists; None: nobody, refused, or proxy sign-in off.
        For the sign-in page, which sends someone the proxy signed in on to the page they asked for."""
        if self.proxy is None or not self.state.admin_exists():
            return None
        try:
            p = self.proxy.user(headers, peer)
        except proxy_auth.ProxyRefused:
            return None
        except Exception as e:  # noqa: BLE001
            raise AccessUnavailable(f"the store can't check the sign-in: {e}") from e
        return None if p is None else Who("proxy", p["role"], int(p["id"]), p["email"], p["name"])

    def check(self, headers, level: str, scheme: str = "http", peer: Optional[str] = None) -> Decision:
        """May this request have a route of this level? ``scheme`` is the request's, as proxy.py says. ``peer``:
        the TCP peer's address, for sign-in through a proxy (without it, the proxy's header is not looked at).
        Raises AccessUnavailable when the store can't say."""
        if level == OPEN:
            return Decision(True)
        cred = credential(headers)
        if level == INGEST:
            if not self.ingest_closed():
                return Decision(True)
            if self._is_shared(cred):
                return Decision(True, Who("shared_token"))
            if self._ingest_token(cred):
                return Decision(True, Who("ingest_token"))
            return Decision(False, status=401, message=NEEDS_INGEST_KEY)
        if not self.state.admin_exists():
            if level == ADMIN:
                return Decision(False, status=403, message=NO_ADMIN_YET)
            if self.shared is None:
                return Decision(True)
            if self._is_shared(cred):
                return Decision(True, Who("shared_token"))
            if self.shared.session_ok(headers.get("Cookie")):
                return Decision(True, Who("shared_cookie"))
            return Decision(False, status=401, message=NEEDS_SHARED)
        # an admin exists: people, and the shared token for /metrics only
        if level == METRICS and self._is_shared(cred):
            return Decision(True, Who("shared_token"))
        try:
            who = self._person(headers, cred, scheme == "https", peer)
        except proxy_auth.ProxyRefused as e:
            return Decision(False, status=403, message=e.message)
        if who is None:
            return Decision(False, status=401, message=SHARED_UI_GONE if self._is_shared(cred) else NEEDS_PERSON)
        if level == ADMIN and who.role != "admin":
            return Decision(False, who, 403, NEEDS_ADMIN)
        if who.role not in ("admin", "member"):  # a role this version doesn't know gets nothing
            return Decision(False, who, 403, f"the role {who.role!r} has no access here")
        return Decision(True, who)
