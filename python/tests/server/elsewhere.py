"""Another host, for the redirect tests of test_github.py and test_oidc.py: it records every request it gets (method,
path and the Authorization header) and answers 200 with an empty JSON object. A sign-in that a provider redirects here
must not send it anything: the token and the client secret go to the provider only."""
from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Elsewhere:
    def __init__(self):
        self.requests: list = []
        seen = self.requests

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _record(self):
                seen.append((self.command, self.path, self.headers.get("Authorization")))
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", "2")
                self.end_headers()
                self.wfile.write(b"{}")

            do_GET = do_POST = _record

        # 127.0.0.2: another host than the providers' 127.0.0.1, as far as the URL goes
        self.httpd = ThreadingHTTPServer(("127.0.0.2", 0), H)
        self.httpd.daemon_threads = True
        self.url = f"http://127.0.0.2:{self.httpd.server_address[1]}"
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()
