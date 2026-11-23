"""--trusted-proxy (proxy.py) and the CSRF check (csrf.py), including headers made up to get around them."""
import pytest

from trailbeam.server import csrf
from trailbeam.server.__main__ import build_parser, trusted_proxies
from trailbeam.server.proxy import TrustedProxies, parse_networks

NGINX = TrustedProxies(parse_networks("172.18.0.0/16, 10.0.0.1"))


def test_parse_networks():
    assert [str(n) for n in parse_networks("10.0.0.0/8 192.168.1.7,2001:db8::/32")] == [
        "10.0.0.0/8", "192.168.1.7/32", "2001:db8::/32"]
    assert str(parse_networks("10.1.2.3/8")[0]) == "10.0.0.0/8"  # host bits are allowed
    for bad in ("", "nginx", "10.0.0.0/33", "10.0.0.0/8;rm"):
        with pytest.raises(ValueError):
            parse_networks(bad)


def test_untrusted_peers_are_their_own_address_whatever_they_send():
    for p in (TrustedProxies(), NGINX):
        assert p.client_address("203.0.113.9", ["1.2.3.4"]) == "203.0.113.9"
        assert p.scheme("203.0.113.9", "https") == "http"
        assert p.host("203.0.113.9", "trailbeam.example", "evil.example") == "trailbeam.example"


def test_behind_a_trusted_proxy_the_rightmost_untrusted_address_is_the_client():
    assert NGINX.client_address("172.18.0.5", ["198.51.100.7"]) == "198.51.100.7"
    # the client wrote the first entry itself; nginx appended the real one
    assert NGINX.client_address("172.18.0.5", ["6.6.6.6, 198.51.100.7"]) == "198.51.100.7"
    # two proxies of ours: the second added the first's address, which is skipped
    assert NGINX.client_address("172.18.0.5", ["6.6.6.6, 198.51.100.7, 10.0.0.1"]) == "198.51.100.7"
    # several headers count in order
    assert NGINX.client_address("172.18.0.5", ["6.6.6.6", "198.51.100.7"]) == "198.51.100.7"
    # nothing forwarded, or only our own: the nearest address we know
    assert NGINX.client_address("172.18.0.5", []) == "172.18.0.5"
    assert NGINX.client_address("172.18.0.5", ["10.0.0.1"]) == "10.0.0.1"


def test_garbage_in_x_forwarded_for_stops_the_walk():
    assert NGINX.client_address("172.18.0.5", ["6.6.6.6, not-an-ip, 10.0.0.1"]) == "10.0.0.1"
    assert NGINX.client_address("172.18.0.5", ["198.51.100.7:51234"]) == "198.51.100.7"
    assert NGINX.client_address("172.18.0.5", ["[2001:db8::5]:443"]) == "2001:db8::5"


def test_ipv4_mapped_peers_match_ipv4_ranges():
    assert NGINX.trusts("::ffff:172.18.0.5")
    assert NGINX.client_address("::ffff:172.18.0.5", ["198.51.100.7"]) == "198.51.100.7"
    assert not NGINX.trusts("2001:db8::1") and not NGINX.trusts("") and not NGINX.trusts("garbage")


def test_scheme_and_host_from_a_trusted_proxy():
    assert NGINX.scheme("172.18.0.5", "https") == "https"
    assert NGINX.scheme("172.18.0.5", "http, https") == "https"  # the nearest proxy's value
    assert NGINX.scheme("172.18.0.5", "https, http") == "http"
    assert NGINX.scheme("203.0.113.9", None, tls=True) == "https"
    assert NGINX.host("172.18.0.5", "trailbeam:4318", "trailbeam.example") == "trailbeam.example"
    assert NGINX.host("172.18.0.5", "trailbeam:4318", None) == "trailbeam:4318"


def test_flag_and_environment(monkeypatch):
    p = build_parser()
    a = p.parse_args(["--trusted-proxy", "10.0.0.0/8", "--trusted-proxy", "172.18.0.1,192.168.0.0/24"])
    assert [str(n) for n in trusted_proxies(a)] == ["10.0.0.0/8", "172.18.0.1/32", "192.168.0.0/24"]
    monkeypatch.setenv("TRAILBEAM_TRUSTED_PROXY", "fd00::/8")
    assert [str(n) for n in trusted_proxies(p.parse_args([]))] == ["fd00::/8"]
    assert [str(n) for n in trusted_proxies(p.parse_args(["--trusted-proxy", "10.0.0.1"]))] == ["10.0.0.1/32"]
    monkeypatch.setenv("TRAILBEAM_TRUSTED_PROXY", "")
    assert trusted_proxies(p.parse_args([])) == ()
    monkeypatch.setenv("TRAILBEAM_TRUSTED_PROXY", "nope")
    with pytest.raises(SystemExit, match="TRAILBEAM_TRUSTED_PROXY"):
        trusted_proxies(p.parse_args([]))
    with pytest.raises(SystemExit):
        p.parse_args(["--trusted-proxy", "nope"])


# ---- CSRF --------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("headers", [
    {"Sec-Fetch-Site": "same-origin"},
    {"Sec-Fetch-Site": "none"},
    {"Sec-Fetch-Site": "Same-Origin", "Origin": "https://evil.example"},  # Sec-Fetch-Site decides when sent
    {"Origin": "https://trailbeam.example"},
    {"Origin": "https://trailbeam.example:443"},
    {"Origin": "HTTPS://Trailbeam.Example"},
    {},  # not a browser's page: curl, scripts, the CLI
])
def test_csrf_passes(headers):
    assert csrf.check(headers, "https", "trailbeam.example") is None


@pytest.mark.parametrize("headers", [
    {"Sec-Fetch-Site": "cross-site"},
    {"Sec-Fetch-Site": "same-site", "Origin": "https://trailbeam.example"},  # a sibling subdomain
    {"Origin": "https://evil.example"},
    {"Origin": "http://trailbeam.example"},       # the scheme counts
    {"Origin": "https://trailbeam.example:8443"},  # so does the port
    {"Origin": "https://trailbeam.example.evil.example"},
    {"Origin": "null"},                            # sandboxed frames, data: URLs
])
def test_csrf_refuses(headers):
    assert csrf.check(headers, "https", "trailbeam.example") is not None


def test_csrf_without_a_host_refuses_any_origin():
    assert csrf.check({"Origin": "https://x.example"}, "http", None) is not None
