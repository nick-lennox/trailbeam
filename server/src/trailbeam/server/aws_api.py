"""``/api/settings/aws`` (QV-589): the AWS settings screen's API, and the server's AWS parts.

- ``GET /api/settings/aws``: ``{"settings": …, "status": …}``. ``settings`` as aws_settings.py keeps them
  (``enabled``, ``queues``, ``region``, ``accountId``, ``updatedAt``, ``updatedBy``); ``status`` is
  aws_reader.Status's (``on``, ``queues``, ``boto3``, ``credentials``, ``region``, ``problem``).
- ``PATCH /api/settings/aws``: changes the fields given (aws_settings.FIELDS), checked by
  aws_settings.check_change; answers as the GET does. A change the checks refuse is a 400 with the sentence. Every
  read cached from before the change is dropped.

Both need an admin (anyone, before accounts exist), as the settings screen is for admins only. The routes are always
mounted, so an admin can turn reads on in a running server; with reads off, neither looks for credentials.

``ServerAws`` is what server.App keeps: the settings, the reader the pages call (aws_reader.AwsReader), the status, and
this API's ``call``, which raises alerts_api.ApiProblem as the other mounted APIs do.
"""
from __future__ import annotations

import time
from typing import Callable, Optional

from . import aws_reader, aws_settings
from .alerts_api import ApiProblem, check_text

PATH = "/api/settings/aws"
#: the methods it answers; another is a 404 before the body is read (server.App.mounted)
METHODS = ("GET", "PATCH")


def is_aws_path(path: str) -> bool:
    return path == PATH


def _by(who) -> str:
    """Who changed the settings, as stored and shown: the person's name, else their email, else how the request was
    let in (as dashboards_api._by)."""
    return getattr(who, "name", None) or getattr(who, "email", None) or getattr(who, "via", None) or "none"


class ServerAws:
    def __init__(self, table, is_admin: Callable[[object], bool], now_ms: Callable[[], int] = lambda: time.time_ns()
                 // 1_000_000, clients_for=None, installed: Callable[[], bool] = aws_settings.boto3_installed,
                 source=aws_reader.credentials_source):
        self.settings = aws_settings.AwsSettings(table)
        self.is_admin = is_admin
        self.now_ms = now_ms
        self.reader = aws_reader.AwsReader(self.settings.current, clients_for=clients_for, installed=installed)
        self.status = aws_reader.Status(self.settings.current, installed=installed, source=source)
        self.settings.on_change += [self.reader.clear, self.status.clear]

    def on(self) -> bool:
        return self.settings.on()

    def health(self) -> dict:
        """For /api/health and trailbeam doctor: the status, never raising."""
        return self.status.get(self.now_ms())

    def view(self) -> dict:
        return {"settings": self.settings.current(), "status": self.status.get(self.now_ms())}

    def call(self, method: str, path: str, body: Optional[dict], who, base: Optional[str] = None) -> tuple[int, dict]:
        if not is_aws_path(path) or method not in METHODS:
            raise ApiProblem(404, f"no such endpoint: {method} {path}")
        if not self.is_admin(who):
            raise ApiProblem(403, "this needs an admin")
        if method == "GET":
            return 200, self.view()
        check_text(body)
        try:
            change = aws_settings.check_change(body)
        except aws_settings.SettingsProblem as e:
            raise ApiProblem(400, str(e)) from None
        self.settings.save(change, _by(who), self.now_ms())
        return 200, self.view()


def for_app(app) -> ServerAws:
    """The AWS parts of server.App, on its store's settings table (a test double's in memory)."""

    def is_admin(who) -> bool:
        return app.access.mode() != "accounts" or getattr(who, "role", None) == "admin"

    return ServerAws(aws_settings.tables_for(app.db), is_admin, now_ms=lambda: app.now() // 1_000_000)
