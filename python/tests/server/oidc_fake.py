"""A small OpenID Connect provider in the test process, for test_oidc.py: discovery, JWKS, the token endpoint (which
checks the PKCE verifier and the client's authentication) and userinfo, over HTTP on 127.0.0.1. The authorization
step is a method (``approve``) rather than a login form: it stands for the person signing in at the provider.

Tests change what it sends to try to break the server's checks: ``claims`` (merged into the next ID tokens),
``alg`` and ``key`` (how they are signed), ``discovery_issuer``, ``auth_methods`` and so on."""
from __future__ import annotations

import base64
import hashlib
import json
import secrets
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import jwt
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.algorithms import RSAAlgorithm


class FakeIdP:
    def __init__(self, client_id: str = "trailbeam", client_secret: str | None = "s3cret-value"):
        self.client_id = client_id
        self.client_secret = client_secret
        self.key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        self.kid = "k1"
        self.alg = "RS256"
        self.claims: dict = {}            # merged into every ID token (None values remove a claim)
        self.userinfo_claims: dict = {}
        self.discovery_issuer = None      # None: the real issuer
        self.auth_methods = ["client_secret_basic", "client_secret_post"]
        self.with_userinfo = True
        self.codes: dict = {}
        self.token_requests: list = []
        self.lock = threading.Lock()
        idp = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _json(self, status, obj):
                b = json.dumps(obj).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)

            def do_GET(self):
                p = urllib.parse.urlsplit(self.path).path
                if p == "/realms/test/.well-known/openid-configuration":
                    self._json(200, idp.discovery())
                elif p == "/realms/test/jwks":
                    pub = json.loads(RSAAlgorithm.to_jwk(idp.key.public_key()))
                    self._json(200, {"keys": [{**pub, "kid": idp.kid, "use": "sig", "alg": "RS256"}]})
                elif p == "/realms/test/userinfo":
                    tok = (self.headers.get("Authorization") or "").removeprefix("Bearer ")
                    sub = idp.access_tokens.get(tok)
                    if sub is None:
                        self._json(401, {"error": "invalid_token"})
                    else:
                        self._json(200, {"sub": sub, **idp.userinfo_claims})
                else:
                    self._json(404, {"error": "not found"})

            def do_POST(self):
                if urllib.parse.urlsplit(self.path).path != "/realms/test/token":
                    self._json(404, {"error": "not found"})
                    return
                n = int(self.headers.get("Content-Length") or 0)
                form = {k: v[-1] for k, v in urllib.parse.parse_qs(self.rfile.read(n).decode()).items()}
                status, body = idp.token(form, self.headers.get("Authorization"))
                self._json(status, body)

        self.access_tokens: dict = {}
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.httpd.daemon_threads = True
        self.issuer = f"http://127.0.0.1:{self.httpd.server_address[1]}/realms/test"
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()

    def discovery(self) -> dict:
        d = {"issuer": self.discovery_issuer or self.issuer,
             "authorization_endpoint": self.issuer + "/auth",
             "token_endpoint": self.issuer + "/token",
             "jwks_uri": self.issuer + "/jwks",
             "id_token_signing_alg_values_supported": ["RS256"],
             "token_endpoint_auth_methods_supported": self.auth_methods}
        if self.with_userinfo:
            d["userinfo_endpoint"] = self.issuer + "/userinfo"
        return d

    def approve(self, authorize_url: str, sub: str = "alice-sub", **claims) -> str:
        """The person signs in at the provider: the URL the provider sends the browser back to (the callback)."""
        u = urllib.parse.urlsplit(authorize_url)
        assert authorize_url.startswith(self.issuer + "/auth?"), authorize_url
        q = {k: v[-1] for k, v in urllib.parse.parse_qs(u.query).items()}
        assert q["response_type"] == "code" and q["client_id"] == self.client_id
        assert q["code_challenge_method"] == "S256" and "openid" in q["scope"].split()
        code = secrets.token_urlsafe(16)
        with self.lock:
            self.codes[code] = {"q": q, "sub": sub, "claims": claims}
        return q["redirect_uri"] + "?" + urllib.parse.urlencode({"code": code, "state": q["state"]})

    def token(self, form: dict, authorization):
        self.token_requests.append({"form": form, "authorization": authorization})
        with self.lock:
            entry = self.codes.pop(form.get("code", ""), None)
        if entry is None or form.get("grant_type") != "authorization_code":
            return 400, {"error": "invalid_grant"}
        q = entry["q"]
        if form.get("redirect_uri") != q["redirect_uri"]:
            return 400, {"error": "invalid_grant", "error_description": "redirect_uri"}
        challenge = base64.urlsafe_b64encode(hashlib.sha256(form.get("code_verifier", "").encode()).digest())
        if challenge.rstrip(b"=").decode() != q["code_challenge"]:
            return 400, {"error": "invalid_grant", "error_description": "PKCE verifier"}
        if self.client_secret is not None:
            if authorization:
                cid, _, sec = base64.b64decode(authorization.removeprefix("Basic ")).decode().partition(":")
                cid, sec = urllib.parse.unquote(cid), urllib.parse.unquote(sec)
            else:
                cid, sec = form.get("client_id"), form.get("client_secret")
            if cid != self.client_id or sec != self.client_secret:
                return 401, {"error": "invalid_client"}
        elif form.get("client_id") != self.client_id:
            return 401, {"error": "invalid_client"}
        now = int(time.time())
        claims = {"iss": self.issuer, "sub": entry["sub"], "aud": self.client_id, "iat": now, "exp": now + 300,
                  "nonce": q["nonce"], "email": "alice@example.com", "email_verified": True, "name": "Alice"}
        claims.update(entry["claims"])
        claims.update(self.claims)
        claims = {k: v for k, v in claims.items() if v is not None}
        access = secrets.token_urlsafe(16)
        self.access_tokens[access] = entry["sub"]
        return 200, {"access_token": access, "token_type": "Bearer", "id_token": self.sign(claims)}

    def sign(self, claims: dict) -> str:
        if self.alg == "HS256":
            return jwt.encode(claims, "x" * 32, algorithm="HS256")
        if self.alg == "none":
            return jwt.encode(claims, None, algorithm="none")
        return jwt.encode(claims, self.key, algorithm=self.alg, headers={"kid": self.kid})
