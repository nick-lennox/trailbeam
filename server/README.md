# trailbeam-server

Trailbeam itself: it receives Trailbeam spans over OTLP/HTTP, keeps them in SQLite (or Postgres), and serves
per-message timelines for Amazon SQS and RabbitMQ through a JSON API and a web screen. It provides the
`trailbeam` command (`trailbeam start`, `trailbeam doctor`, `trailbeam users`, `trailbeam tokens`).

```sh
pip install "trailbeam[server]"            # or: pip install trailbeam-server
trailbeam start
```

Add `postgres` for a Postgres store, `sso` for single sign-on, `passkeys` for signing in with passkeys and `aws` for
AWS reads:
`pip install "trailbeam[server,postgres,sso,passkeys,aws]"`.

The client library, with the hooks for boto3, AWS Lambda and pika, is the `trailbeam` distribution
(`pip install trailbeam`). Until 0.4.0 both were one distribution; from 0.5.0 the server is this one, at the same
version, and installs into the same `trailbeam` package, so `import trailbeam.server` and
`python -m trailbeam.server` keep working.

From a clone of the repository, build the web screen first and install both packages together, so pip doesn't
fetch the client library from PyPI instead of `./python` (`trailbeam` 0.4.0 from PyPI still carries its own
`trailbeam/server/` and an old web screen):

```sh norun="installs from a clone; CI installs it in its python job"
(cd ui && npm ci && npm run build)   # the web screen, built into server/src/trailbeam/server/ui
pip install ./python './server[postgres]'
trailbeam start
```

Without the build, Trailbeam serves its server-rendered pages only; the pages that only the web screen draws say
how to build it. `trailbeam start` logs a warning for either problem.

Documentation: https://docs.trailbeam.app. Source: https://github.com/nick-lennox/trailbeam (`server/`, with the
web screen in `ui/`).
