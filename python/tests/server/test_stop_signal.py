"""SIGTERM stops the server wherever it lands (QV-502). The handler used to raise KeyboardInterrupt; a SIGTERM that
arrived while the main thread ran a garbage-collector callback had it printed as "Exception ignored" and dropped,
so ``trailbeam start`` went on serving after docker stop (test_start.py failed: "still running 10 s after
SIGTERM")."""
import gc
import signal
import threading
import weakref

from trailbeam.server.__main__ import stop_on_signal
from trailbeam.server.server import make_server


def _serving(db):
    httpd = make_server(db, "127.0.0.1", 0, ui_dir=None)
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    return httpd, t


def test_a_signal_inside_a_gc_callback_still_stops_serving(db):
    httpd, t = _serving(db)
    handler = stop_on_signal(httpd)
    try:
        class Thing:
            pass

        thing = Thing()
        ref = weakref.ref(thing, lambda _: handler(signal.SIGTERM, None))  # noqa: F841 (kept alive to fire)
        del thing  # the callback, so the handler, runs here: what it raises would be ignored
        gc.collect()
        t.join(10)
        assert not t.is_alive(), "serve_forever went on after the handler ran in a GC callback"
    finally:
        httpd.server_close()


def test_a_signal_before_serving_starts_makes_serve_forever_return_at_once(db):
    """SIGTERM between installing the handler and serve_forever (the retention line, the banner)."""
    httpd = make_server(db, "127.0.0.1", 0, ui_dir=None)
    try:
        stop_on_signal(httpd)(signal.SIGTERM, None)
        t = threading.Thread(target=httpd.serve_forever, daemon=True)
        t.start()
        t.join(10)
        assert not t.is_alive()
    finally:
        httpd.server_close()
