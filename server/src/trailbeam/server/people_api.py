"""The people-and-tokens API behind the web screen's People and Tokens pages (users and roles, phase 2; proposal on
QV-149, section 7). The routes and their levels are ``access.API_ROUTES``; contract/openapi.yaml documents them.

- **People** (admins): ``GET /api/users``, ``POST /api/users`` (a new person, with no password, and their one-time
  invite link), ``PATCH /api/users/{id}`` (role, or disabled), ``DELETE /api/users/{id}``, and
  ``POST /api/users/{id}/link``: a new one-time link, an invite for someone who has no password yet, else a
  password reset. Two roles only, ``admin`` and ``member``. The last admin who can sign in can't be demoted,
  disabled or removed (409): accounts.LastAdmin, checked in the same transaction as the change.
- **Ingest tokens** (admins): list, create (the token shown once), revoke. The list says whether the legacy shared
  token (``TRAILBEAM_TOKEN``) is set, never its value. The first ingest token of a server without
  ``TRAILBEAM_TOKEN`` closes ingest that was open, so creating it needs ``"close_open_ingest": true``.
- **Your personal API tokens** (``/api/me/tokens``, anyone signed in): list, create, revoke your own. They need a
  person: the shared token, or a server with no admin yet, gets 403.

**A token can't make a token** (QV-759): ``POST /api/me/tokens`` with a personal API token is 403 (``reason``
``needs_a_session``). Otherwise a short-lived CI token could create one that never expires, and that one would
keep working after the first is revoked or expires: the passkey problem of QV-695 again. A browser session can,
and so can a sign-in through the proxy, which is a browser too and has no other way to get a token; listing and
revoking your tokens still work with a token.

**Invite and reset links** are ``/set-password#code=<code>``: ``CODE_BYTES`` random bytes, only the SHA-256 kept
(``setup_codes``, kind ``invite`` or ``reset``, naming the person), in the URL fragment so request logs never see
it. Invites last ``INVITE_MS`` (7 days), resets ``RESET_MS`` (24 hours). A new link replaces the person's earlier
one. Using it sets the password, ends the person's sessions and uses the code up, in one transaction
(accounts.use_user_code), and signs them in. Until it is used, a link is as good as that person's password: the
admin who made it passes it on privately.

**Invites by email** (QV-587): with a mail server set up (mail.py, TRAILBEAM_SMTP_HOST), ``POST /api/users`` and
``POST /api/users/{id}/link`` also email the link to the person, unless the body says ``"send_email": false``.
The answer's ``email`` says whether it was sent. Once it is, ``link`` has no ``url``: only the person gets it. When
sending fails, the person or link is still made, ``email.error`` says why, and ``link.url`` is there for the
admin to pass on instead. ``"send_email": true`` without a mail server is 409. ``GET /api/users`` says whether
the server sends email (``sends_email``), for the People page.

**Your passkeys** (``/api/me/passkeys``, QV-665, passkeys.py; anyone signed in as a person): list, add (``POST
.../options`` for the browser's ``navigator.credentials.create``, then ``POST`` with its answer and a name),
remove. ``on`` in the list says whether the server has them on; with them off, the other three are 409.
Removing a passkey doesn't end sessions it started: sign out there, or reset the password, for that.

**Who can add a passkey** (QV-695): only someone signed in in a browser (a session, ``Who.via == "session"``),
since a browser makes passkeys anyway. A personal API token, or a sign-in through the proxy, gets 403
(``reason`` ``needs_a_session``): otherwise a short-lived CI token, or a request the proxy lets in, could add a
passkey a script controls (the ``none``-attestation answer is plain JSON), and that passkey would sign in long after
the token is revoked or the proxy link removed. ``can_add`` in the list says whether this request could. Each
passkey added is logged at warning level with the client's address, so it shows when an account is looked over.

**Someone else's passkeys** (admins, QV-695): ``GET /api/users/{id}/passkeys`` lists them, ``DELETE
/api/users/{id}/passkeys`` removes them all (the person's sessions stay; a reset link ends those and removes the
passkeys too). The People list says how many each person has (``passkeys``). A reset or invite link, and
``trailbeam users reset-password``, remove the person's passkeys along with ending their sessions: they are how an
account someone else got into is taken back.

**Only a browser sees a link** (QV-712): a link signs whoever opens it in, and from there they can add a passkey, so
handing one to a personal API token or the proxy would undo the rule above. An admin's token could make a reset link
for its own person, or add a new admin and take their invite, choose the password, and add a passkey that outlives
the token. So unless the request is a session, ``link`` has no ``url``.

**Only a browser sends a link either** (QV-755): emailing it is no better, since the token chooses the address when
it adds the person: it adds ``eve@attacker.example`` as an admin, and the invite goes to whoever reads that inbox.
So unless the request is a session, ``POST /api/users`` adds the person but emails nothing (``email`` is
``null``; ``"send_email": true`` is 403 ``needs_a_session``, and nobody is added), and ``POST
/api/users/{id}/link`` is always 403 ``needs_a_session``. This holds for a sign-in through the proxy too: whoever
the proxy lets in is added as a member on their first request (proxy_auth.py) and needs no invite, and an admin
who wants to invite someone with a password signs in in a browser.

**Requests that change something** go through csrf.check (server.py) and must send ``Content-Type:
application/json`` when they have a body, which a form on another site can't send without a preflight this server
never answers.
"""
from __future__ import annotations

import json
import logging
from typing import Optional

from . import first_admin, passkeys as passkeys_mod
from .mail import DeliveryError, invite_email, reset_email
from .accounts import INVITE, RESET, AccountError, EmailTaken, LastAdmin, PasskeyTaken, check_role

log = logging.getLogger("trailbeam.server")

INVITE_MS = 7 * 86_400_000
RESET_MS = 86_400_000
LINK_PATH = "/set-password"
MAX_BODY = 16 * 1024
MAX_TOKEN_DAYS = 3650
SHARED_TOKEN_LABEL = "legacy shared token (from TRAILBEAM_TOKEN)"
CLOSES_INGEST = ("this is the first ingest token, and this server has no TRAILBEAM_TOKEN: ingest is open now, and "
                 "within 5 seconds of creating it every service that sends spans without a token gets 401 and its "
                 "spans are lost. Send \"close_open_ingest\": true to create it anyway")
PASSKEYS_OFF = ("passkeys are off on this server: they need its public URL (TRAILBEAM_PUBLIC_URL) and "
                'pip install "trailbeam[server,passkeys]"')
NEEDS_A_SESSION = ("add a passkey from the Account page, signed in in your browser: a personal API token or a "
                   "sign-in through the proxy can't add one")
NEEDS_A_SESSION_FOR_LINK = ("make or email a link from the People page, signed in in your browser: a personal API "
                            "token or a sign-in through the proxy can't, since whoever opens a link is signed in")
NEEDS_A_SESSION_FOR_TOKEN = ("create a personal API token from the Tokens page, signed in in your browser: a "
                             "personal API token can't create another one")
NEEDS_A_PERSON = ("personal API tokens belong to a person: sign in, or send your personal API token "
                  "(this request was let in without one)")


_LOGS_CLIENT = frozenset({"add_my_passkey"})


class ApiProblem(Exception):
    def __init__(self, status: int, message: str, reason: Optional[str] = None):
        super().__init__(message)
        self.status = status
        self.message = message
        self.reason = reason

    def body(self) -> dict:
        return {"error": self.message, **({"reason": self.reason} if self.reason else {})}


def parse_body(content_type: Optional[str], raw: bytes) -> dict:
    """The JSON object a POST or PATCH sends. Raises ApiProblem (415, 400)."""
    if (content_type or "").split(";")[0].strip().lower() != "application/json":
        raise ApiProblem(415, "send the request body as Content-Type: application/json")
    try:
        body = json.loads(raw.decode("utf-8")) if raw.strip() else {}
    except (UnicodeDecodeError, ValueError):
        raise ApiProblem(400, "the request body is not JSON") from None
    if not isinstance(body, dict):
        raise ApiProblem(400, "the request body must be a JSON object")
    return body


def _str(body: dict, key: str, required: bool = True) -> Optional[str]:
    v = body.get(key)
    if v is None and not required:
        return None
    if not isinstance(v, str):
        raise ApiProblem(400, f"{key} must be a string" if v is not None else f"{key} is required")
    return v


def _status(u: dict, now_ms: int) -> str:
    if u["disabled_at_ms"] is not None:
        return "disabled"
    if not u["password_hash"]:
        return "invited"
    if (u["locked_until_ms"] or 0) > now_ms:
        return "locked"
    return "active"


def user_view(u: dict, now_ms: int, me: Optional[int], single_sign_on: bool, passkeys: int = 0) -> dict:
    """``single_sign_on``: the person has an identity provider account (``oidc_identities``). Removing them then
    doesn't keep them out: their next single sign-on creates them again, as a new person with the default role.
    ``passkeys``: how many they have (QV-695), so an admin sees one they didn't expect."""
    return {"id": int(u["id"]), "email": u["email"], "name": u["name"], "role": u["role"],
            "status": _status(u, now_ms), "created_at_ms": u["created_at_ms"],
            "last_sign_in_ms": u["last_sign_in_ms"], "you": me is not None and int(u["id"]) == me,
            "single_sign_on": single_sign_on, "passkeys": passkeys}


def _token_status(t: dict, now_ms: int) -> str:
    if t["revoked_at_ms"]:
        return "revoked"
    if (t.get("expires_at_ms") or now_ms + 1) <= now_ms:
        return "expired"
    return "active"


def ingest_view(t: dict, now_ms: int, emails: dict) -> dict:
    return {"id": int(t["id"]), "name": t["name"], "prefix": t["prefix"], "status": _token_status(t, now_ms),
            "created_at_ms": t["created_at_ms"], "created_by": emails.get(t["created_by"]),
            "last_used_ms": t["last_used_ms"], "revoked_at_ms": t["revoked_at_ms"]}


def personal_view(t: dict, now_ms: int) -> dict:
    return {"id": int(t["id"]), "name": t["name"], "prefix": t["prefix"], "status": _token_status(t, now_ms),
            "created_at_ms": t["created_at_ms"], "expires_at_ms": t["expires_at_ms"],
            "last_used_ms": t["last_used_ms"], "revoked_at_ms": t["revoked_at_ms"]}


class PeopleApi:
    """The handlers. ``app`` is server.App: its accounts, sessions, tokens, access and shared token."""

    def __init__(self, app):
        self.app = app

    @property
    def acc(self):
        if self.app.accounts is None:
            raise ApiProblem(404, "this store has no people or tokens (it is not SQLite or Postgres)")
        return self.app.accounts

    def call(self, handler: str, rid: Optional[int], body: Optional[dict], who, base_url: str,
             now_ms: int, client: str = "") -> tuple[int, dict]:
        """(status, JSON body). ``client``: the request's address, for the handlers that log it. Raises ApiProblem."""
        acc = self.acc
        kw = {"client": client} if handler in _LOGS_CLIENT else {}
        return getattr(self, handler)(acc, rid, body or {}, who, base_url, now_ms, **kw)

    # ---- people ---------------------------------------------------------------------------------------

    def _user(self, acc, rid: int) -> dict:
        u = acc.user(rid)
        if u is None:
            raise ApiProblem(404, f"no person with ID {rid}")
        return u

    def _forget(self, uid: int) -> None:
        """This replica stops using the person's cached sessions and tokens at once (others within 20 s)."""
        if self.app.sessions is not None:
            self.app.sessions.forget_user(uid)
        if self.app.tokens is not None:
            self.app.tokens.forget_user(uid)

    def _view(self, acc, u: dict, now_ms: int, who) -> dict:
        uid = int(u["id"])
        return user_view(u, now_ms, who.user_id, bool(acc.oidc_identities(uid)), len(acc.passkeys(uid)))

    def list_users(self, acc, rid, body, who, base_url, now_ms):
        sso = {int(i["user_id"]) for i in acc.oidc_identities()}
        keys = acc.passkey_counts()
        return 200, {"users": [user_view(u, now_ms, who.user_id, int(u["id"]) in sso, keys.get(int(u["id"]), 0))
                               for u in acc.users()],
                     "sends_email": self.mailer is not None}

    @property
    def mailer(self):
        return getattr(self.app, "mailer", None)

    def _send_email(self, body: dict, who) -> bool:
        """Whether to email the link: ``send_email`` if given, else whether a mail server is set up. Never for a
        request that isn't a browser session (QV-755): ``"send_email": true`` from one is 403."""
        v = body.get("send_email")
        if v is not None and not isinstance(v, bool):
            raise ApiProblem(400, "send_email must be true or false")
        if who.via != "session":
            if v:
                raise ApiProblem(403, NEEDS_A_SESSION_FOR_LINK, "needs_a_session")
            return False
        if v is None:
            return self.mailer is not None
        if v and self.mailer is None:
            raise ApiProblem(409, "this server has no mail server to send email with (TRAILBEAM_SMTP_HOST): "
                                  "copy the link and pass it on instead", "no_mail_server")
        return v

    @staticmethod
    def _hide_url(link: dict, who) -> dict:
        """Takes ``url`` out of ``link`` unless a browser session asked: QV-712, see the module docstring."""
        if who.via != "session":
            link.pop("url", None)
        return link

    def _email_link(self, u: dict, link: dict, who, base_url: str) -> dict:
        """Emails the link to the person. Their answer's ``email``; takes ``url`` out of ``link`` once sent."""
        site = base_url.rstrip("/")
        if link["kind"] == INVITE:
            mail = invite_email(link["url"], site, who.name, who.email, INVITE_MS // 86_400_000)
        else:
            mail = reset_email(link["url"], site, RESET_MS // 3_600_000)
        try:
            self.mailer(u["email"], mail)
        except DeliveryError as e:
            log.warning("user %s: the %s email to user %d was not sent: %s", who.user_id, link["kind"],
                        int(u["id"]), e)
            return {"sent": False, "to": u["email"], "error": str(e)}
        del link["url"]
        log.info("user %s emailed a %s link to user %d", who.user_id, link["kind"], int(u["id"]))
        return {"sent": True, "to": u["email"], "error": None}

    def create_user(self, acc, rid, body, who, base_url, now_ms):
        email = _str(body, "email")
        name = _str(body, "name", required=False) or first_admin.default_name(email.strip())
        role = _str(body, "role", required=False) or "member"
        send = self._send_email(body, who)
        try:
            check_role(role)
            uid = acc.create_user(email, name, role, None, now_ms)
        except EmailTaken as e:
            raise ApiProblem(409, str(e), "email_taken") from None
        except AccountError as e:
            raise ApiProblem(400, str(e)) from None
        self.app.access.state.changed()
        link = self._link(acc, uid, INVITE, base_url, now_ms)
        log.info("user %s added %s (user %d, %s) from the People page", who.user_id, email.strip(), uid, role)
        u = self._user(acc, uid)
        sent = self._email_link(u, link, who, base_url) if send else None
        return 201, {"user": self._view(acc, u, now_ms, who), "link": self._hide_url(link, who), "email": sent}

    def change_user(self, acc, rid, body, who, base_url, now_ms):
        u = self._user(acc, rid)
        role, disabled = body.get("role"), body.get("disabled")
        if role is None and disabled is None:
            raise ApiProblem(400, "send role, disabled, or both")
        if disabled is not None and not isinstance(disabled, bool):
            raise ApiProblem(400, "disabled must be true or false")
        try:
            if role is not None and role != u["role"]:
                if not isinstance(role, str):
                    raise ApiProblem(400, "role must be a string")
                acc.set_role(rid, role, now_ms)
            if disabled is not None and disabled != (u["disabled_at_ms"] is not None):
                acc.set_disabled(rid, disabled, now_ms)
        except LastAdmin as e:
            raise ApiProblem(409, str(e), "last_admin") from None
        except AccountError as e:
            raise ApiProblem(400, str(e)) from None
        self._forget(rid)
        self.app.access.state.changed()
        log.info("user %s changed user %d: %s", who.user_id, rid,
                 {k: v for k, v in (("role", role), ("disabled", disabled)) if v is not None})
        return 200, {"user": self._view(acc, self._user(acc, rid), now_ms, who)}

    def remove_user(self, acc, rid, body, who, base_url, now_ms):
        u = self._user(acc, rid)
        try:
            if not acc.remove_user(rid):
                raise ApiProblem(404, f"no person with ID {rid}")
        except LastAdmin as e:
            raise ApiProblem(409, str(e), "last_admin") from None
        self._forget(rid)
        self.app.access.state.changed()
        log.info("user %s removed %s (user %d)", who.user_id, u["email"], rid)
        return 200, {"removed": {"id": rid, "email": u["email"]}}

    def user_link(self, acc, rid, body, who, base_url, now_ms):
        u = self._user(acc, rid)
        if u["disabled_at_ms"] is not None:
            raise ApiProblem(409, f"{u['email']} is disabled: enable them before making a link", "disabled")
        if who.via != "session":  # QV-712, QV-755: neither shown nor emailed, so a link would only replace theirs
            raise ApiProblem(403, NEEDS_A_SESSION_FOR_LINK, "needs_a_session")
        send = self._send_email(body, who)
        kind = RESET if u["password_hash"] else INVITE
        link = self._link(acc, rid, kind, base_url, now_ms)
        log.info("user %s made a %s link for user %d", who.user_id, kind, rid)
        sent = self._email_link(u, link, who, base_url) if send else None
        return 201, {"user": self._view(acc, u, now_ms, who), "link": self._hide_url(link, who), "email": sent}

    def list_user_passkeys(self, acc, rid, body, who, base_url, now_ms):
        self._user(acc, rid)
        return 200, {"passkeys": [passkeys_mod.passkey_view(p) for p in acc.passkeys(rid)],
                     "on": self.app.passkeys is not None}

    def remove_user_passkeys(self, acc, rid, body, who, base_url, now_ms):
        u = self._user(acc, rid)
        n = acc.remove_passkeys(rid)
        log.warning("user %s removed %d passkey%s of %s (user %d)", who.user_id, n, "" if n == 1 else "s",
                    u["email"], rid)
        return 200, {"removed": {"id": rid, "passkeys": n}}

    def _link(self, acc, uid: int, kind: str, base_url: str, now_ms: int) -> dict:
        code = first_admin.new_code()
        expires = now_ms + (INVITE_MS if kind == INVITE else RESET_MS)
        if not acc.create_user_code(uid, kind, first_admin.code_hash(code), now_ms, expires):
            raise ApiProblem(404, f"no person with ID {uid}")
        return {"kind": kind, "url": f"{base_url.rstrip('/')}{LINK_PATH}#code={code}", "expires_at_ms": expires}

    # ---- ingest tokens --------------------------------------------------------------------------------

    def _shared(self) -> dict:
        return {"set": self.app.auth is not None, "label": SHARED_TOKEN_LABEL, "works_for": ["ingest", "metrics"]}

    def list_ingest_tokens(self, acc, rid, body, who, base_url, now_ms):
        emails = {u["id"]: u["email"] for u in acc.users()}
        return 200, {"tokens": [ingest_view(t, now_ms, emails) for t in acc.ingest_tokens()],
                     "shared_token": self._shared(), "ingest": "token" if self.app.access.ingest_closed() else "open"}

    def create_ingest_token(self, acc, rid, body, who, base_url, now_ms):
        name = _str(body, "name")
        if not self.app.access.ingest_closed() and body.get("close_open_ingest") is not True:
            raise ApiProblem(409, CLOSES_INGEST, "closes_open_ingest")
        try:
            tid, token = self.app.tokens.create_ingest(name, created_by=who.user_id)
        except AccountError as e:
            raise ApiProblem(400, str(e)) from None
        self.app.access.state.changed()
        log.info("user %s created ingest token %d", who.user_id, tid)
        row = next(t for t in acc.ingest_tokens() if int(t["id"]) == tid)
        return 201, {"token": ingest_view(row, now_ms, {who.user_id: who.email}), "secret": token}

    def revoke_ingest_token(self, acc, rid, body, who, base_url, now_ms):
        if not self.app.tokens.revoke_ingest(rid):
            raise ApiProblem(404, f"no ingest token {rid} that is still working")
        log.info("user %s revoked ingest token %d", who.user_id, rid)
        return 200, {"revoked": {"id": rid}}

    # ---- your personal API tokens ---------------------------------------------------------------------

    @staticmethod
    def _me(who) -> int:
        if who.user_id is None:
            raise ApiProblem(403, NEEDS_A_PERSON)
        return who.user_id

    def list_my_tokens(self, acc, rid, body, who, base_url, now_ms):
        uid = self._me(who)
        return 200, {"tokens": [personal_view(t, now_ms) for t in acc.personal_tokens(uid)]}

    def create_my_token(self, acc, rid, body, who, base_url, now_ms):
        uid = self._me(who)
        if who.via == "personal_token":  # QV-759: a short-lived token must not mint one that outlives it
            raise ApiProblem(403, NEEDS_A_SESSION_FOR_TOKEN, "needs_a_session")
        name = _str(body, "name")
        days = body.get("expires_in_days")
        if days is not None and (isinstance(days, bool) or not isinstance(days, int)
                                 or not 1 <= days <= MAX_TOKEN_DAYS):
            raise ApiProblem(400, f"expires_in_days must be a whole number of days from 1 to {MAX_TOKEN_DAYS}, "
                                  "or null for a token that works until it is revoked")
        try:
            tid, token = self.app.tokens.create_personal(uid, name, now_ms + days * 86_400_000 if days else None)
        except AccountError as e:
            raise ApiProblem(400, str(e)) from None
        log.info("user %d created personal API token %d", uid, tid)
        row = next(t for t in acc.personal_tokens(uid) if int(t["id"]) == tid)
        return 201, {"token": personal_view(row, now_ms), "secret": token}

    def revoke_my_token(self, acc, rid, body, who, base_url, now_ms):
        uid = self._me(who)
        if not self.app.tokens.revoke_personal(rid, user_id=uid):
            raise ApiProblem(404, f"no personal API token {rid} of yours that is still working")
        log.info("user %d revoked personal API token %d", uid, rid)
        return 200, {"revoked": {"id": rid}}

    # ---- your passkeys (passkeys.py) --------------------------------------------------------------------

    def _passkeys(self):
        if self.app.passkeys is None:
            raise ApiProblem(409, PASSKEYS_OFF, "passkeys_off")
        return self.app.passkeys

    def list_my_passkeys(self, acc, rid, body, who, base_url, now_ms):
        uid = self._me(who)
        return 200, {"passkeys": [passkeys_mod.passkey_view(p) for p in acc.passkeys(uid)],
                     "on": self.app.passkeys is not None, "can_add": who.via == "session"}

    @staticmethod
    def _in_a_browser(who) -> int:
        """Adding a passkey: only a browser session, never a personal token or the proxy (QV-695)."""
        uid = PeopleApi._me(who)
        if who.via != "session":
            raise ApiProblem(403, NEEDS_A_SESSION, "needs_a_session")
        return uid

    def passkey_options(self, acc, rid, body, who, base_url, now_ms):
        pk = self._passkeys()  # off is said first: it is the server's, whoever asks
        uid = self._in_a_browser(who)
        return 200, {"options": pk.register_options(self._user(acc, uid))}

    def add_my_passkey(self, acc, rid, body, who, base_url, now_ms, client=""):
        pk = self._passkeys()  # off is said first: it is the server's, whoever asks
        uid = self._in_a_browser(who)
        credential = body.get("credential")
        if not isinstance(credential, dict):
            raise ApiProblem(400, "credential is required: the browser's answer to navigator.credentials.create")
        try:
            view = pk.register(uid, _str(body, "name"), credential)
        except passkeys_mod.PasskeyError as e:
            log.warning("user %d: a passkey was not added: %s", uid, e.detail)
            raise ApiProblem(e.status, e.message) from None
        except PasskeyTaken as e:
            raise ApiProblem(409, str(e), "passkey_taken") from None
        except AccountError as e:
            raise ApiProblem(400, str(e)) from None
        # warning, not info: a passkey signs in on its own, so one that wasn't the person's must stand out (QV-695)
        log.warning("user %d added passkey %d (%r) from %s", uid, view["id"], view["name"], client or "?")
        return 201, {"passkey": view}

    def remove_my_passkey(self, acc, rid, body, who, base_url, now_ms):
        uid = self._me(who)
        if not acc.remove_passkey(rid, uid):
            raise ApiProblem(404, f"no passkey {rid} of yours")
        log.info("user %d removed passkey %d", uid, rid)
        return 200, {"removed": {"id": rid}}
