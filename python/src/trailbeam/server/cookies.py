"""Reading one cookie out of a request's ``Cookie`` header.

Not ``http.cookies.SimpleCookie``: it gives up on the whole header when any one cookie in it does not
parse (a value with a space, say), so a single junk cookie that a sibling subdomain, the host's plain
``http://`` or a script on the site planted would hide the session cookie and sign the person out until
they cleared their cookies (QV-473). Here each ``name=value`` pair stands alone, and a part that is not
one is skipped.
"""
from __future__ import annotations

from typing import Optional


def cookie(header: Optional[str], name: str) -> Optional[str]:
    """The value of the first cookie called exactly ``name`` in a Cookie header, or None.

    Browsers send the cookie with the most specific path first (RFC 6265 5.4), so the first one wins.
    A value in double quotes loses them; nothing else is unescaped."""
    if not header:
        return None
    for part in header.split(";"):
        key, eq, value = part.partition("=")
        if not eq or key.strip() != name:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] == '"':
            value = value[1:-1]
        return value
    return None
