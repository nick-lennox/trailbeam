"""Refusing requests that change something when another site's page made the browser send them (CSRF).

A browser sends this server's cookies with a request another site's page starts (a form posted to
``/login``, later the setup and user routes). ``SameSite=Strict`` stops most of that, but a sibling subdomain
counts as the same site. So every request that changes something, except OTLP ingest, goes through ``check``:

1. ``Sec-Fetch-Site`` (every current browser sends it): only ``same-origin`` and ``none`` (typed or bookmarked)
   pass; ``same-site`` and ``cross-site`` are refused.
2. Without it, ``Origin``: it must be this server's own ``scheme://host`` (proxy.py says which, behind a
   trusted proxy). ``Origin: null`` is refused.
3. Without either, it passes. Every browser of recent years sends ``Origin`` with a POST from another site's
   page (and ``Sec-Fetch-Site`` with every request), so a request with neither isn't one: curl, scripts, the
   CLI. Those carry the cookie only if someone gives it to them.

Ingest (``POST /v1/traces``) is not checked: a cookie never authorises it, and web apps instrumented in the
browser send their spans to it from their own sites.
"""
from __future__ import annotations

from typing import Optional
from urllib.parse import urlsplit

CHANGES = ("POST", "PUT", "PATCH", "DELETE")


def check(headers, scheme: str, host: Optional[str]) -> Optional[str]:
    """None if the request may go on, else why it is refused (for the 403 and the log)."""
    site = (headers.get("Sec-Fetch-Site") or "").strip().lower()
    if site:
        if site in ("same-origin", "none"):
            return None
        return f"Sec-Fetch-Site: {site}: requests that change something must come from this server's own pages"
    origin = (headers.get("Origin") or "").strip()
    if origin:
        if origin.lower() == "null":
            return "Origin: null: requests that change something must come from this server's own pages"
        o = urlsplit(origin)
        if host and o.scheme.lower() == scheme and _norm(o.netloc, o.scheme) == _norm(host, scheme):
            return None
        return f"Origin {origin} is not this server ({scheme}://{host or '?'})"
    return None  # no browser signals: not from a browser's page


def _norm(netloc: str, scheme: str) -> str:
    n = netloc.strip().lower()
    default = ":443" if scheme.lower() == "https" else ":80"
    return n[: -len(default)] if n.endswith(default) else n
