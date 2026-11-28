"""The shared UI's built shell (ui/ in the repository, built by ``npm run build`` into ./ui).

When the build is present, ``/`` (Messages, or the first-run screen while no span is stored), ``/duplicates``,
``/overview``, ``/issues`` and ``/issues/<cause ID>`` (Issues, QV-708; ``/stuck`` is the shell too, which sends it to
``/issues``), ``/stuck``, ``/message``, ``/tenants``, ``/queue``, ``/queues``, ``/events`` (the Event explorer, which has no HTML
page), ``/dashboards`` (which reads /api/dashboards, there only while TRAILBEAM_DASHBOARDS is on, and has no HTML
page), ``/alerts`` (alert rules and channels, QV-592; it says how to turn the alert check on while it is off),
``/people`` (admins only, access.py), ``/tokens`` and ``/account`` are the shell's page, which draws them from the JSON API in the
browser, and ``/ui/...`` serves its files. When it is not (an editable install from a clone that
was never built), these return None and the server-rendered HTML pages answer instead.
"""
from __future__ import annotations

import mimetypes
from pathlib import Path
from typing import Optional

Response = tuple  # (status, headers, body), as web.handle returns

DEFAULT_UI_DIR = Path(__file__).resolve().parent / "ui"
SHELL_PAGES = ("/", "/overview", "/duplicates", "/issues", "/stuck", "/message", "/tenants", "/queue", "/queues", "/events", "/dashboards",
               "/people", "/tokens", "/account", "/alerts", "/settings")
_TYPES = {".js": "text/javascript", ".css": "text/css", ".woff2": "font/woff2", ".woff": "font/woff",
          ".html": "text/html; charset=utf-8", ".svg": "image/svg+xml", ".json": "application/json"}


def install_problems(ui_dir: Optional[Path] = DEFAULT_UI_DIR) -> list[str]:
    """What is wrong with how this server was installed, as warnings for the log at start (QV-769); [] if nothing.

    - The client library's distribution (``trailbeam``) still holds ``trailbeam/server/``: it is 0.4.0 or older,
      from before the split (QV-567). ``pip install ./server`` from a clone gets it from PyPI, because the server
      depends on ``trailbeam`` at its own version and ``./python`` wasn't named, and its built UI is then served
      against this server's API.
    - No build of the web screen (``npm run build`` in ui/ was never run): the pages only it draws say so.
    """
    out = []
    try:
        from importlib import metadata
        files = metadata.files("trailbeam") or []
        stale = [f for f in files if str(f).replace("\\", "/").startswith("trailbeam/server/")]
        version = metadata.version("trailbeam")
    except Exception:  # noqa: BLE001 - not installed as a distribution (a source checkout on sys.path)
        stale, version = [], None
    if stale:
        out.append(f"the installed trailbeam {version} (the client library) still holds trailbeam/server/ from before "
                   "the split, with its own web screen; this server may be serving that old web screen. From a clone, "
                   "install both packages: pip install ./python ./server")
    if ui_dir is not None and not (ui_dir / "index.html").is_file():
        out.append(f"the web screen isn't built ({ui_dir} has no index.html): only the server-rendered pages are "
                   "served. Build it with npm ci && npm run build in ui/ (then pip install ./server again, unless "
                   "it is an editable install)")
    return out


def is_issue_page(path: str) -> bool:
    """``/issues/<cause ID>``: one issue's page (QV-708), as the hosted app's link to it is."""
    rest = path[len("/issues/"):] if path.startswith("/issues/") else ""
    return bool(rest.strip("/")) and "/" not in rest.rstrip("/")


def handle(ui_dir: Optional[Path], path: str) -> Optional[Response]:
    """The response for ``path`` from the built shell, or None if it has no such file (or isn't built)."""
    if ui_dir is None:
        return None
    root = ui_dir.resolve()
    index = root / "index.html"
    if not index.is_file():
        return None
    if path in SHELL_PAGES or is_issue_page(path):
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
