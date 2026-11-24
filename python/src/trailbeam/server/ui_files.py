"""The shared UI's built shell (ui/ in the repository, built by ``npm run build`` into ./ui).

When the build is present, ``/`` (Messages, or the first-run screen while no span is stored), ``/duplicates``,
``/overview``, ``/stuck``, ``/message``, ``/tenants``, ``/queue``, ``/queues``, ``/events`` (the Event explorer, which has no HTML
page), ``/people`` (admins only, access.py) and ``/tokens`` are the shell's page, which draws them from the JSON API in the
browser, and ``/ui/...`` serves its files. When it is not (an editable install from a clone that
was never built), these return None and the server-rendered HTML pages answer instead.
"""
from __future__ import annotations

import mimetypes
from pathlib import Path
from typing import Optional

Response = tuple  # (status, headers, body), as web.handle returns

DEFAULT_UI_DIR = Path(__file__).resolve().parent / "ui"
SHELL_PAGES = ("/", "/overview", "/duplicates", "/stuck", "/message", "/tenants", "/queue", "/queues", "/events", "/people", "/tokens")
_TYPES = {".js": "text/javascript", ".css": "text/css", ".woff2": "font/woff2", ".woff": "font/woff",
          ".html": "text/html; charset=utf-8", ".svg": "image/svg+xml", ".json": "application/json"}


def handle(ui_dir: Optional[Path], path: str) -> Optional[Response]:
    """The response for ``path`` from the built shell, or None if it has no such file (or isn't built)."""
    if ui_dir is None:
        return None
    root = ui_dir.resolve()
    index = root / "index.html"
    if not index.is_file():
        return None
    if path in SHELL_PAGES:
        return 200, {"Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-cache"}, index.read_bytes()
    if not path.startswith("/ui/"):
        return None
    target = (root / path[len("/ui/"):]).resolve()
    if root not in target.parents or not target.is_file():  # never outside the build
        return None
    ctype = _TYPES.get(target.suffix) or mimetypes.guess_type(target.name)[0] or "application/octet-stream"
    # file names under assets/ carry a content hash, so they never change
    cache = "public, max-age=31536000, immutable" if "assets" in target.relative_to(root).parts else "no-cache"
    return 200, {"Content-Type": ctype, "Cache-Control": cache}, target.read_bytes()
