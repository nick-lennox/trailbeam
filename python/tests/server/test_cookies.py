"""Reading a cookie from the Cookie header (cookies.py): one cookie that doesn't parse hides no other (QV-473)."""
import pytest

from trailbeam.server import auth, sessions
from trailbeam.server.cookies import cookie

JUNK = ["junk=a b", "junk=x y", 'junk="unterminated', "junk", "=nameless", "junk=\x7fé", " ", ""]


@pytest.mark.parametrize("junk", JUNK)
def test_a_junk_cookie_before_or_after_hides_nothing(junk):
    for header in (f"{junk}; sid=v1", f"sid=v1; {junk}", f"{junk}; sid=v1; {junk}; other=1", f"sid=v1;{junk}"):
        assert cookie(header, "sid") == "v1", header


def test_exact_name_first_one_wins_spaces_trimmed():
    assert cookie("sidx=a; xsid=b; SID=c", "sid") is None
    assert cookie("  sid = v1 ;sid=v2", "sid") == "v1"
    assert cookie('sid="quoted"', "sid") == "quoted"
    assert cookie("sid=a=b", "sid") == "a=b"
    assert cookie("sid=", "sid") == "" and cookie(None, "sid") is None and cookie("", "sid") is None


# what str.strip() takes but RFC 6265 doesn't trim: a name starting with one is another name (QV-483)
NOT_OWS = ["\x0b", "\x0c", "\x1c", "\x1d", "\x1e", "\x1f", "\x85", "\xa0", "\u2000", "\u3000"]


@pytest.mark.parametrize("ch", NOT_OWS)
def test_only_space_and_tab_are_trimmed(ch):
    assert cookie(f"{ch}sid=v1", "sid") is None and cookie(f"sid{ch}=v1", "sid") is None
    assert cookie(f"other=1; {ch}sid=v1", "sid") is None
    assert cookie(f"sid={ch}v1{ch}", "sid") == f"{ch}v1{ch}"
    assert cookie(f"{ch}sid=bad; sid=v1", "sid") == "v1"
    assert cookie("\t sid\t = \tv1 \t", "sid") == "v1"
    sid = sessions.new_id()
    assert sessions.cookie_value(f"{ch}__Host-trailbeam_sid={sid}", True) is None
    assert sessions.cookie_value(f"junk=1; {ch}__Host-trailbeam_sid={sid}", True) is None
    a = auth.Auth("s3cret-token-0123456789")
    value = a.set_cookie(False).split(";")[0]
    assert not a.session_ok(ch + value)


def test_session_cookie_survives_junk_and_keeps_the_https_rule():
    sid = sessions.new_id()
    for junk in ("junk=a b", "junk=x y; other=1"):
        assert sessions.cookie_value(f"{junk}; trailbeam_sid={sid}", False) == sid
        assert sessions.cookie_value(f"trailbeam_sid={sid}; {junk}", False) == sid
        assert sessions.cookie_value(f"{junk}; __Host-trailbeam_sid={sid}; {junk}", True) == sid
        # QV-408: over HTTPS only the __Host- name counts, junk or not
        assert sessions.cookie_value(f"{junk}; trailbeam_sid={sid}; {junk}", True) is None


def test_shared_token_cookie_survives_junk():
    a = auth.Auth("s3cret-token-0123456789")
    value = a.set_cookie(False).split(";")[0]
    assert a.session_ok(value)
    assert a.session_ok(f"junk=a b; {value}") and a.session_ok(f"{value}; junk=x y; other=1")
    assert not a.session_ok("junk=a b; trailbeam_session=forged") and not a.session_ok("trailbeam_session=é")
