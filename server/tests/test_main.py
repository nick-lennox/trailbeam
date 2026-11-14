"""The qv2-store command: starts, serves, and exits cleanly on SIGTERM (docker stop, systemd)."""
import os
import signal
import subprocess
import sys
import time
import urllib.request

import pytest


@pytest.mark.skipif(os.name != "posix", reason="POSIX signals")
def test_sigterm_stops_cleanly(tmp_path):
    env = {**os.environ, "QV2_STORE_DB": str(tmp_path / "q.sqlite"), "QV2_STORE_PORT": "0"}
    p = subprocess.Popen([sys.executable, "-m", "qv2_store"], env=env, stderr=subprocess.PIPE, text=True)
    try:
        line = p.stderr.readline()  # "... listening on http://127.0.0.1:<port> ..."
        port = int(line.split("http://127.0.0.1:")[1].split()[0])
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=5) as r:
            assert r.status == 200 and b"Look up a message" in r.read()
        p.send_signal(signal.SIGTERM)
        assert p.wait(timeout=5) == 0
    finally:
        if p.poll() is None:
            p.kill()
    assert (tmp_path / "q.sqlite").exists()


def test_max_receive_count_option():
    import argparse

    import pytest

    from qv2_store.__main__ import _max_receive_counts

    assert _max_receive_counts("") == {}
    assert _max_receive_counts("orders=3, payments=5") == {"orders": 3, "payments": 5}
    for bad in ("orders", "orders=", "=3", "orders=0", "orders=x"):
        with pytest.raises(argparse.ArgumentTypeError):
            _max_receive_counts(bad)
