"""Reading one cookie out of a request's ``Cookie`` header.

Not ``http.cookies.SimpleCookie``: it gives up on the whole header when any one cookie in it does not
parse (a value with a space, say), so a single junk cookie that a sibling subdomain, the host's plain
``http://`` or a script on the site planted would hide the session cookie and sign the person out until
they cleared their cookies (QV-473). Here each ``name=value`` pair stands alone, and a part that is not
one is skipped.

Around a name and a value only a space and a tab are trimmed (RFC 6265's optional whitespace), not
everything ``str.strip()`` takes: that also strips ``\x0b``, ``\x1c``-``\x1f``, ``\x85`` and ``\xa0``, so
``\xa0__Host-trailbeam_sid``, which the browser lets anyone plant because it doesn't start with
``__Host-``, would count as the protected cookie (QV-483).
"""
from __future__ import annotations

from typing import Optional

_OWS = " \t"


def cookie(header: Optional[str], name: str) -> Optional[str]:
    """The value of the first cookie called exactly ``name`` in a Cookie header, or None.

    Browsers send the cookie with the most specific path first (RFC 6265 5.4), so the first one wins.
    Only spaces and tabs around a name or value are trimmed. A value in double quotes loses them;
    nothing else is unescaped."""
    if not header:
        return None
    for part in header.split(";"):
        key, eq, value = part.partition("=")
        if not eq or key.strip(_OWS) != name:
            continue
        value = value.strip(_OWS)
        if len(value) >= 2 and value[0] == value[-1] == '"':
            value = value[1:-1]
        return value
    return None
