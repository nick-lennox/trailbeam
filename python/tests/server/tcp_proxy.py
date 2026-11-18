"""A small TCP proxy for tests: in front of Postgres (cut it off to make the store unreachable) or of several
Trailbeam replicas (a round-robin load balancer that skips a backend it can't connect to, as nginx does)."""
from __future__ import annotations

import itertools
import socket
import threading


class Proxy:
    def __init__(self, backends: list[tuple[str, int]]):
        self.backends = list(backends)
        self._next = itertools.count()
        self._listener = socket.create_server(("127.0.0.1", 0))
        self.port = self._listener.getsockname()[1]
        self._conns: set = set()
        self._lock = threading.Lock()
        self._closed = False
        self._thread = threading.Thread(target=self._accept, daemon=True)
        self._thread.start()

    def _accept(self) -> None:
        while True:
            try:
                client, _ = self._listener.accept()
            except OSError:
                return
            if self._closed:
                client.close()
                return
            upstream = self._connect()
            if upstream is None:
                client.close()
                continue
            with self._lock:
                self._conns |= {client, upstream}
            for a, b in ((client, upstream), (upstream, client)):
                threading.Thread(target=self._pipe, args=(a, b), daemon=True).start()

    def _connect(self):
        n = len(self.backends)
        start = next(self._next)
        for i in range(n):
            try:
                return socket.create_connection(self.backends[(start + i) % n], timeout=2)
            except OSError:
                continue
        return None

    def _pipe(self, src, dst) -> None:
        try:
            src.settimeout(None)
            while data := src.recv(65536):
                dst.sendall(data)
        except OSError:
            pass
        finally:
            for s in (src, dst):
                try:
                    s.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

    def cut(self) -> None:
        """Close every connection through the proxy and stop accepting: the backends are unreachable."""
        self._closed = True
        try:
            self._listener.shutdown(socket.SHUT_RDWR)  # wakes the accept (close alone doesn't, on Linux)
        except OSError:
            pass
        self._listener.close()
        self._thread.join(5)
        with self._lock:
            for s in self._conns:
                try:
                    s.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                s.close()
            self._conns.clear()

    close = cut
