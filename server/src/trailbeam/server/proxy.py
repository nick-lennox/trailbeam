"""``--trusted-proxy``: which connecting addresses are proxies whose ``X-Forwarded-*`` headers are believed.

Anyone can send ``X-Forwarded-For``, so it means something only when a proxy you run added it. With
``--trusted-proxy 10.0.0.0/8`` (repeatable, or comma-separated in ``TRAILBEAM_TRUSTED_PROXY``):

- **Client address** (``client_address``): the TCP peer, unless the peer is a trusted proxy. Then the
  ``X-Forwarded-For`` entries are read from the right (the one the nearest proxy added) and the first that is
  not a trusted proxy is the client. Entries to its left were written by the client and are ignored. The
  sign-in limit per address and the logs use it.
- **Scheme** (``scheme``): ``https`` when the request came straight over TLS (``tls``), or when a trusted proxy
  sent ``X-Forwarded-Proto: https``. It decides the session cookie's ``Secure`` flag and ``__Host-`` name.
- **Host** (``host``): ``X-Forwarded-Host`` from a trusted proxy, else ``Host``. The CSRF check compares a
  browser's ``Origin`` with it.

Not set (the default): every ``X-Forwarded-*`` header is ignored; requests come from their TCP peer.
"""
from __future__ import annotations

import ipaddress
from typing import Iterable, Optional, Union

Network = Union[ipaddress.IPv4Network, ipaddress.IPv6Network]
Address = Union[ipaddress.IPv4Address, ipaddress.IPv6Address]


def parse_networks(text: str) -> tuple:
    """``10.0.0.0/8,172.16.0.5`` (commas or spaces; a bare address is a /32 or /128). Raises ValueError."""
    nets = []
    for part in text.replace(",", " ").split():
        try:
            nets.append(ipaddress.ip_network(part, strict=False))
        except ValueError:
            raise ValueError(f"{part!r} is not an IP address or a CIDR range such as 10.0.0.0/8") from None
    if not nets:
        raise ValueError("expected an IP address or a CIDR range such as 10.0.0.0/8")
    return tuple(nets)


def _addr(text: str) -> Optional[Address]:
    t = (text or "").strip()
    if t.startswith("[") and "]" in t:  # [2001:db8::1]:443
        t = t[1:t.index("]")]
    elif t.count(":") == 1:  # 192.0.2.1:443
        t = t.split(":", 1)[0]
    try:
        a = ipaddress.ip_address(t.split("%", 1)[0])
    except ValueError:
        return None
    if isinstance(a, ipaddress.IPv6Address) and a.ipv4_mapped is not None:  # ::ffff:10.0.0.1 from a dual-stack socket
        return a.ipv4_mapped
    return a


def _last(values: Optional[str]) -> str:
    """The rightmost comma-separated value: the one the nearest proxy added."""
    return (values or "").split(",")[-1].strip()


class TrustedProxies:
    def __init__(self, networks: Iterable[Network] = ()):
        self.networks = tuple(networks)

    def __bool__(self) -> bool:
        return bool(self.networks)

    def trusts(self, peer: str) -> bool:
        a = _addr(peer)
        return a is not None and any(a.version == n.version and a in n for n in self.networks)

    def client_address(self, peer: str, forwarded_for: Iterable[str] = ()) -> str:
        """The client's address: ``peer`` (the TCP peer), or from ``X-Forwarded-For`` (every such header, in order)
        when ``peer`` is a trusted proxy."""
        if not self.trusts(peer):
            return str(_addr(peer) or peer)
        hops = [h.strip() for v in forwarded_for if v for h in v.split(",")]
        client = str(_addr(peer))
        for h in reversed(hops):
            a = _addr(h)
            if a is None:  # garbage: nothing to its left can be believed either
                break
            client = str(a)
            if not self.trusts(client):
                break
        return client

    def scheme(self, peer: str, forwarded_proto: Optional[str], tls: bool = False) -> str:
        if tls:
            return "https"
        if forwarded_proto and self.trusts(peer) and _last(forwarded_proto).lower() == "https":
            return "https"
        return "http"

    def host(self, peer: str, host: Optional[str], forwarded_host: Optional[str]) -> Optional[str]:
        if forwarded_host and self.trusts(peer) and _last(forwarded_host):
            return _last(forwarded_host)
        return host
