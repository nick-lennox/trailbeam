"""The shared UI's shell: served at /, /duplicates, /stuck, /message and /tenants when it is built, and the HTML pages
otherwise."""
import threading
import urllib.error
import urllib.request

import pytest

from trailbeam.server.server import make_server


@pytest.fixture
def built(tmp_path):
    (tmp_path / "assets").mkdir()
    (tmp_path / "index.html").write_text('<!doctype html><div id="root"></div><script src="/ui/assets/app-1a2b.js"></script>')
    (tmp_path / "assets" / "app-1a2b.js").write_text("console.log('timeline')")
    (tmp_path / "assets" / "font.woff2").write_bytes(b"wOF2")
    return tmp_path


def serve(db, ui_dir):
    httpd = make_server(db, "127.0.0.1", 0, ui_dir=ui_dir)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd, f"http://127.0.0.1:{httpd.server_address[1]}"


def get(url):
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


def test_message_page_is_the_shell_when_built(db, built):
    httpd, base = serve(db, built)
    try:
        status, headers, body = get(f"{base}/message?id=abc")
        assert status == 200 and headers["Content-Type"].startswith("text/html")
        assert b'<div id="root">' in body and headers["Cache-Control"] == "no-cache"
        status, _, tenants = get(f"{base}/tenants?since_s=3600")
        assert status == 200 and tenants == body  # one shell, which draws the page the path names
        status, headers, body = get(f"{base}/ui/assets/app-1a2b.js")
        assert (status, headers["Content-Type"], body) == (200, "text/javascript", b"console.log('timeline')")
        assert "immutable" in headers["Cache-Control"]
        assert get(f"{base}/ui/assets/font.woff2")[1]["Content-Type"] == "font/woff2"
        # Messages (with the first-run screen), Duplicates and Stuck are the same shell, with or without a query
        # ... and so are the queue list and a queue's page (QV-364), and the Event explorer (QV-399)
        for path in ("/", "/?queue=orders", "/duplicates", "/stuck?older_than_s=60", "/queues", "/queue?name=orders&range=4h",
                     "/events", "/events?q=kind%3Adelete&range=7d"):
            status, _, page = get(base + path)
            assert (status, page) == (200, tenants), path
    finally:
        httpd.shutdown(); httpd.server_close()


def test_nothing_outside_the_build_is_served(db, built):
    (built.parent / "secret.txt").write_text("no")
    httpd, base = serve(db, built)
    try:
        for path in ("/ui/../secret.txt", "/ui/%2e%2e/secret.txt", "/ui/assets/missing.js", "/ui/"):
            assert get(base + path)[0] == 404, path
    finally:
        httpd.shutdown(); httpd.server_close()


def test_without_a_build_the_message_page_is_html(db, tmp_path):
    httpd, base = serve(db, tmp_path / "not-built")
    try:
        status, headers, body = get(f"{base}/message?id=abc")
        assert status == 404 and b"Message not found" in body  # the HTML page, for an unknown message
        status, _, body = get(f"{base}/tenants")
        assert status == 200 and b"<h1>Tenants</h1>" in body
        for path, h1 in (("/", b"<h1>Look up a message</h1>"), ("/duplicates", b"<h1>Duplicate deliveries</h1>"),
                         ("/stuck", b"<h1>Stuck messages</h1>")):
            status, _, body = get(base + path)
            assert status == 200 and h1 in body, path
        assert get(f"{base}/ui/assets/app.js")[0] == 404
    finally:
        httpd.shutdown(); httpd.server_close()
