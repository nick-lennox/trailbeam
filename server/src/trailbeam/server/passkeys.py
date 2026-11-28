"""Passkeys (WebAuthn; QV-665): signing in with Face ID, Touch ID, Windows Hello, a phone or a security key instead
of a password, and the list of a person's passkeys on the Account page.

**On when.** ``--public-url`` (``TRAILBEAM_PUBLIC_URL``) is set, the store has accounts, and the ``webauthn``
library is installed (``pip install "trailbeam[server,passkeys]"``). A passkey belongs to one site, named by the
public URL's host name (the relying party ID): the browser offers it only on that site, and signs which address
it is on, which must be the public URL's origin. That is why the request's Host can't stand in for it. Set the
public URL and leave the extra out, and the server says so once at start and runs without passkeys.

**Adding one** (signed in, ``/api/me/passkeys``): ``POST /api/me/passkeys/options`` gives the browser what
``navigator.credentials.create`` needs, with a new challenge; ``POST /api/me/passkeys`` sends back what the
authenticator made, with a name. The passkey must be discoverable (the authenticator keeps it, so signing in needs
no email) and check the person (PIN, fingerprint or face: user verification), so it can stand in for a password on
its own. Attestation is not asked for: Trailbeam doesn't limit which authenticators people use.

**Signing in** (no session): ``POST /login/passkey/options`` gives a challenge, with no list of passkeys (the
browser offers whichever it has for this site); ``POST /login/passkey`` sends the signed answer. The passkey is
found by its credential ID, checked (signature, origin, relying party, user verification, counter), and a session
starts as with a password. A disabled person, an unknown passkey or a wrong answer is refused with one message.

**Challenges** are 32 random bytes, kept as their SHA-256 in ``passkey_challenges`` (account_tables.py) for
``CHALLENGE_TTL_MS`` and deleted when they come back, so each works once, on whichever replica gets the answer.
Asking for one is limited per client address (``STARTS_PER_ADDRESS`` in ``STARTS_WINDOW_S``, like single sign-on's
starts), since each adds a row; failed sign-ins count against the address like wrong passwords (signin.py).

**Counter.** An authenticator that keeps a signature counter must send a higher one each time; one that goes
back means the passkey may have been copied, and the sign-in is refused. Synced passkeys keep none (always 0).
"""
from __future__ import annotations

import base64
import hashlib
import json
import logging
import secrets
import time
import urllib.parse
from typing import Callable, NamedTuple, Optional

from .accounts import AccountError

log = logging.getLogger("trailbeam.server")

OPTIONS_PATH = "/login/passkey/options"
SIGN_IN_PATH = "/login/passkey"
EXTRA = 'install it with pip install "trailbeam[server,passkeys]"'
CHALLENGE_TTL_MS = 5 * 60_000
TIMEOUT_MS = 120_000  # how long the browser waits for the person
STARTS_PER_ADDRESS = 30
STARTS_WINDOW_S = 600.0
MAX_PER_PERSON = 20
MAX_NAME = 100
MAX_ANSWER = 16 * 1024  # bytes of JSON a browser sends back
RP_NAME = "Trailbeam"
REFUSED = "That passkey didn't work. Try another way to sign in."


class PasskeyError(Exception):
    """Refused: ``status`` and a sentence for the person (``message``); ``detail`` for the log."""

    def __init__(self, status: int, message: str, detail: str = ""):
        super().__init__(message)
        self.status = status
        self.message = message
        self.detail = detail or message


class Settings(NamedTuple):
    rp_id: str   # the public URL's host name
    origin: str  # scheme://host[:port], as the browser reports it


def settings(public_url: str) -> Settings:
    """From ``--public-url`` (already checked by mail.check_public_url): its host name and origin."""
    u = urllib.parse.urlsplit(public_url)
    host = (u.hostname or "").lower()
    default = {"https": 443, "http": 80}[u.scheme]
    netloc = f"[{host}]" if ":" in host else host
    if u.port and u.port != default:
        netloc += f":{u.port}"
    return Settings(rp_id=host, origin=f"{u.scheme}://{netloc}")


def insecure(s: Settings) -> bool:
    """Browsers offer passkeys only on https, and on http only at localhost."""
    return s.origin.startswith("http://") and s.rp_id != "localhost" and not s.rp_id.endswith(".localhost")


def available() -> bool:
    try:
        import webauthn  # noqa: F401
    except ImportError:
        return False
    return True


def _b64(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode("ascii")


def _unb64(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


def _sha(challenge: bytes) -> str:
    return hashlib.sha256(challenge).hexdigest()


def user_handle(user_id: int) -> bytes:
    """The WebAuthn user ID the authenticator keeps with the passkey: the person's ID, nothing that names them."""
    return str(int(user_id)).encode("ascii")


def passkey_view(p: dict) -> dict:
    return {"id": int(p["id"]), "name": p["name"], "created_at_ms": p["created_at_ms"],
            "last_used_ms": p["last_used_ms"], "synced": bool(p["backed_up"])}


def _challenge_of(credential: dict) -> bytes:
    """The challenge the browser signed, from the answer's clientDataJSON; PasskeyError if it has none."""
    try:
        client_data = json.loads(_unb64(credential["response"]["clientDataJSON"]))
        return _unb64(client_data["challenge"])
    except (KeyError, TypeError, ValueError, AttributeError):
        raise PasskeyError(400, REFUSED, "the answer has no readable clientDataJSON") from None


class Passkeys:
    def __init__(self, s: Settings, accounts, sessions, now_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000):
        import webauthn  # the extra; available() was checked first
        from webauthn.helpers import structs

        self.s = s
        self.accounts = accounts
        self.sessions = sessions
        self.now_ms = now_ms
        self._w = webauthn
        self._structs = structs

    def _challenge(self, kind: str, user_id: Optional[int]) -> bytes:
        c = secrets.token_bytes(32)
        now = self.now_ms()
        self.accounts.create_passkey_challenge(_sha(c), kind, user_id, now, now + CHALLENGE_TTL_MS)
        return c

    def _take(self, credential: dict, kind: str, user_id: Optional[int]) -> bytes:
        c = _challenge_of(credential)
        row = self.accounts.take_passkey_challenge(_sha(c), self.now_ms())
        if row is None or row["kind"] != kind or (user_id is not None and row["user_id"] != user_id):
            raise PasskeyError(400, "This passkey request has run out or was used already. Try again.",
                               "an unknown, used or out-of-date challenge")
        return c

    # ---- adding one -------------------------------------------------------------------------------------------

    def register_options(self, user: dict) -> dict:
        st = self._structs
        exclude = [st.PublicKeyCredentialDescriptor(id=_unb64(p["credential_id"]))
                   for p in self.accounts.passkeys(int(user["id"]))]
        opts = self._w.generate_registration_options(
            rp_id=self.s.rp_id, rp_name=RP_NAME, user_id=user_handle(user["id"]), user_name=user["email"],
            user_display_name=user["name"], challenge=self._challenge("register", int(user["id"])),
            timeout=TIMEOUT_MS, exclude_credentials=exclude,
            authenticator_selection=st.AuthenticatorSelectionCriteria(
                resident_key=st.ResidentKeyRequirement.REQUIRED,
                user_verification=st.UserVerificationRequirement.REQUIRED))
        return json.loads(self._w.options_to_json(opts))

    def register(self, user_id: int, name: str, credential: dict) -> dict:
        """Check what the authenticator made and save it. Raises PasskeyError, or AccountError (too many, saved
        already)."""
        name = check_name(name)
        challenge = self._take(credential, "register", user_id)
        try:
            v = self._w.verify_registration_response(
                credential=credential, expected_challenge=challenge, expected_rp_id=self.s.rp_id,
                expected_origin=self.s.origin, require_user_verification=True)
        except Exception as e:  # noqa: BLE001 - the library's InvalidRegistrationResponse, or a malformed answer
            raise PasskeyError(400, "The passkey wasn't saved: your browser's answer didn't check out. Try again.",
                               f"registration refused: {e}") from None
        transports = credential.get("response", {}).get("transports") or []
        if not isinstance(transports, list) or not all(isinstance(t, str) for t in transports):
            transports = []
        now = self.now_ms()
        pid = self.accounts.add_passkey(user_id, _b64(v.credential_id), _b64(v.credential_public_key), v.sign_count,
                                        name, json.dumps(transports[:10]), bool(v.credential_backed_up), now,
                                        MAX_PER_PERSON)
        return next(passkey_view(p) for p in self.accounts.passkeys(user_id) if int(p["id"]) == pid)

    # ---- signing in -------------------------------------------------------------------------------------------

    def sign_in_options(self) -> dict:
        opts = self._w.generate_authentication_options(
            rp_id=self.s.rp_id, challenge=self._challenge("sign_in", None), timeout=TIMEOUT_MS,
            user_verification=self._structs.UserVerificationRequirement.REQUIRED)
        return json.loads(self._w.options_to_json(opts))

    def sign_in(self, credential: dict) -> tuple[int, str, int]:
        """(user ID, session ID, its expiry). Raises PasskeyError."""
        challenge = self._take(credential, "sign_in", None)
        cid = credential.get("rawId") or credential.get("id")
        found = self.accounts.passkey_by_credential(cid) if isinstance(cid, str) and len(cid) <= 1400 else None
        if found is None:
            raise PasskeyError(401, REFUSED, "no saved passkey has this credential ID")
        if found["disabled_at_ms"] is not None:
            raise PasskeyError(401, REFUSED, f"user {found['user_id']} is disabled")
        handle = (credential.get("response") or {}).get("userHandle")
        if handle and _safe_unb64(handle) != user_handle(found["user_id"]):
            raise PasskeyError(401, REFUSED, "the passkey's user handle is not its owner's")
        try:
            v = self._w.verify_authentication_response(
                credential=credential, expected_challenge=challenge, expected_rp_id=self.s.rp_id,
                expected_origin=self.s.origin, credential_public_key=_unb64(found["public_key"]),
                credential_current_sign_count=int(found["sign_count"]), require_user_verification=True)
        except Exception as e:  # noqa: BLE001 - InvalidAuthenticationResponse (a bad signature, a counter that went back)
            raise PasskeyError(401, REFUSED, f"passkey {found['id']} of user {found['user_id']}: {e}") from None
        now = self.now_ms()
        self.accounts.passkey_used(int(found["id"]), int(v.new_sign_count), now)
        sid, expires = self.sessions.start(int(found["user_id"]))
        return int(found["user_id"]), sid, expires


def _safe_unb64(s) -> Optional[bytes]:
    try:
        return _unb64(s)
    except (ValueError, TypeError):
        return None


def check_name(name) -> str:
    """A passkey's name, as people see it in the list. Raises AccountError."""
    if not isinstance(name, str) or not name.strip():
        raise AccountError("name the passkey, such as the device it is on")
    name = " ".join(name.split())
    if len(name) > MAX_NAME:
        raise AccountError(f"a passkey's name is at most {MAX_NAME} characters")
    return name
