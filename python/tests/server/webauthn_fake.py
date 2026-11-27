"""A software passkey for the passkey tests (test_passkeys.py): what a browser and its authenticator send back to
``navigator.credentials.create`` and ``.get``, made from the server's options as JSON. An ES256 (P-256) key, the
``none`` attestation format, and real signatures, so the server's webauthn library checks them as it would a
phone's or a security key's. Knobs make the answers that must be refused: another origin or relying party, no user
verification, a counter that goes back.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os

import cbor2
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec

UP, UV, BE, BS, AT = 0x01, 0x04, 0x08, 0x10, 0x40


def b64(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


def unb64(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


class Authenticator:
    def __init__(self, origin: str, *, counter: bool = True, synced: bool = False):
        self.origin = origin
        self.key = ec.generate_private_key(ec.SECP256R1())
        self.cred_id = os.urandom(16)
        self.counter = counter  # False: keeps no counter (always 0), as synced passkeys do
        self.count = 0
        self.synced = synced
        self.user_handle = b""

    def _flags(self, uv: bool, extra: int = 0) -> bytes:
        f = UP | (UV if uv else 0) | ((BE | BS) if self.synced else 0) | extra
        return bytes([f])

    def _cose(self) -> bytes:
        n = self.key.public_key().public_numbers()
        return cbor2.dumps({1: 2, 3: -7, -1: 1, -2: n.x.to_bytes(32, "big"), -3: n.y.to_bytes(32, "big")})

    def create(self, options: dict, *, origin: str | None = None, rp_id: str | None = None, uv: bool = True) -> dict:
        """The answer to navigator.credentials.create({publicKey: options})."""
        self.user_handle = unb64(options["user"]["id"])
        client_data = json.dumps({"type": "webauthn.create", "challenge": options["challenge"],
                                  "origin": origin or self.origin, "crossOrigin": False}).encode()
        attested = bytes(16) + len(self.cred_id).to_bytes(2, "big") + self.cred_id + self._cose()
        auth_data = (hashlib.sha256((rp_id or options["rp"]["id"]).encode()).digest() + self._flags(uv, AT)
                     + self.count.to_bytes(4, "big") + attested)
        att = cbor2.dumps({"fmt": "none", "attStmt": {}, "authData": auth_data})
        return {"id": b64(self.cred_id), "rawId": b64(self.cred_id), "type": "public-key",
                "response": {"clientDataJSON": b64(client_data), "attestationObject": b64(att),
                             "transports": ["internal"]},
                "clientExtensionResults": {}}

    def get(self, options: dict, *, origin: str | None = None, rp_id: str | None = None, uv: bool = True,
            count: int | None = None) -> dict:
        """The answer to navigator.credentials.get({publicKey: options}), signed."""
        if count is not None:
            self.count = count
        elif self.counter:
            self.count += 1
        client_data = json.dumps({"type": "webauthn.get", "challenge": options["challenge"],
                                  "origin": origin or self.origin, "crossOrigin": False}).encode()
        auth_data = (hashlib.sha256((rp_id or options["rpId"]).encode()).digest() + self._flags(uv)
                     + self.count.to_bytes(4, "big"))
        sig = self.key.sign(auth_data + hashlib.sha256(client_data).digest(), ec.ECDSA(hashes.SHA256()))
        return {"id": b64(self.cred_id), "rawId": b64(self.cred_id), "type": "public-key",
                "response": {"clientDataJSON": b64(client_data), "authenticatorData": b64(auth_data),
                             "signature": b64(sig), "userHandle": b64(self.user_handle) if self.user_handle else None},
                "clientExtensionResults": {}}
