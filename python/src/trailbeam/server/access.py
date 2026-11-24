"""Who may do what: the access level of every route, and the switch from the shared token to accounts.

Every route the server answers has one level (``ROUTES``, checked by ``level_of``; a path that names no route
is ``MEMBER``, so a page or file added later is never open by mistake):

- ``OPEN``: ``/api/ready``, ``/api/health`` (which says only ``status`` and ``version`` to a request that isn't let
  in at ``MEMBER``), the sign-in page and its form, and the first admin's setup form (``/setup``, first_admin.py),
  which needs its one-time code instead.
- ``INGEST``: ``POST /v1/traces``.
- ``METRICS``: ``GET /metrics``.
- ``MEMBER``: the API and the web screen, message bodies included (an admin or a member, proposal section 2).
- ``ADMIN``: managing people and tokens. No HTTP route has it yet: phase 1 does that from the command line
  (``trailbeam users``, ``trailbeam tokens``: cli.py).

**Two modes, switched by the store, not by a setting.** ``State`` asks the store whether an admin exists at most
every ``STATE_TTL_S`` (5 s), on every replica, so when the first admin is created every replica switches within
seconds and none needs a restart.

- **No admin yet** (as in 0.3.0): with ``TRAILBEAM_TOKEN`` set, every level but ``OPEN`` needs it (as
  ``Authorization: Bearer`` or ``x-trailbeam-key``, or the login page's ``trailbeam_session`` cookie); without it,
  everything is served. ``ADMIN`` is refused either way: there is nobody to be one.
- **An admin exists:** ``MEMBER`` needs a person: a session (``trailbeam_sid``, sessions.py) or a personal API
  token (``tbp_…``), of an admin or a member; ``ADMIN`` needs an admin. ``TRAILBEAM_TOKEN`` keeps working for
  ``INGEST`` and ``METRICS`` only, and the ``trailbeam_session`` cookie no longer works.

**Ingest** needs a token when ``TRAILBEAM_TOKEN`` is set or at least one ingest token exists (revoked ones count),
in either mode: the shared token or a live ingest token (``tbi_…``). Otherwise it is open. A person's session or
personal token never sends spans, and an ingest token never reads.

A token is read from ``x-trailbeam-key`` when that header is there, else from ``Authorization: Bearer``. Only the
headers are read: a refused export is answered before its body is.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Callable, NamedTuple, Optional

from . import auth as auth_mod, sessions as sessions_mod
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
    ("POST", "/v1/traces"): INGEST,
    ("GET", "/metrics"): METRICS,
}


def level_of(method: str, path: str) -> str:
    return ROUTES.get((method, path), MEMBER)


class AccessUnavailable(Exception):
    """The store couldn't say who this is (or whether an admin exists): answer 503 / 500, never let it in."""


class Who(NamedTuple):
    """Who a request is. ``via``: ``none``, ``shared_token``, ``shared_cookie``, ``session``, ``personal_token``
    or ``ingest_token``. ``role``: the person's, for a session or personal token."""
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
                 state: Optional[State] = None):
        self.shared = shared
        self.accounts = accounts
        self.sessions = sessions
        self.tokens = tokens
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

    def _person(self, headers, cred: Optional[str], secure: bool) -> Optional[Who]:
        """The person behind a personal token (when one is sent) or else a session cookie (over HTTPS
        only the ``__Host-`` one, sessions.py)."""
        try:
            if cred and cred.startswith(PERSONAL_PREFIX) and self.tokens is not None:
                u = self.tokens.personal(cred)
                return None if u is None else Who("personal_token", u.role, u.user_id, u.email, u.name)
            if self.sessions is not None:
                u = self.sessions.lookup(sessions_mod.cookie_value(headers.get("Cookie"), secure))
                return None if u is None else Who("session", u.role, u.user_id, u.email, u.name)
        except Exception as e:  # noqa: BLE001
            raise AccessUnavailable(f"the store can't check the sign-in: {e}") from e
        return None

    def check(self, headers, level: str, scheme: str = "http") -> Decision:
        """May this request have a route of this level? ``scheme`` is the request's, as proxy.py says.
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
        who = self._person(headers, cred, scheme == "https")
        if who is None:
            return Decision(False, status=401, message=SHARED_UI_GONE if self._is_shared(cred) else NEEDS_PERSON)
        if level == ADMIN and who.role != "admin":
            return Decision(False, who, 403, NEEDS_ADMIN)
        if who.role not in ("admin", "member"):  # a role this version doesn't know gets nothing
            return Decision(False, who, 403, f"the role {who.role!r} has no access here")
        return Decision(True, who)
