# trailbeam-server

Trailbeam itself: it receives Trailbeam spans over OTLP/HTTP, keeps them in SQLite (or Postgres), and serves
per-message timelines for Amazon SQS and RabbitMQ through a JSON API and a web screen. It provides the
`trailbeam` command (`trailbeam start`, `trailbeam doctor`, `trailbeam users`, `trailbeam tokens`).

```sh
pip install "trailbeam[server]"            # or: pip install trailbeam-server
trailbeam start
```

Add `postgres` for a Postgres store and `sso` for single sign-on: `pip install "trailbeam[server,postgres,sso]"`.

The client library, with the hooks for boto3, AWS Lambda and pika, is the `trailbeam` distribution
(`pip install trailbeam`). Until 0.4.0 both were one distribution; from 0.5.0 the server is this one, at the same
version, and installs into the same `trailbeam` package, so `import trailbeam.server` and
`python -m trailbeam.server` keep working.

Documentation: https://docs.trailbeam.app. Source: https://github.com/nick-lennox/trailbeam (`server/`, with the
web screen in `ui/`).

License: [AGPL-3.0-only](https://github.com/nick-lennox/trailbeam/blob/main/server/LICENSE) from 0.5.0 (0.4.0 and earlier: Apache-2.0). The client library, `trailbeam`, stays
Apache-2.0. See [LICENSING.md](https://github.com/nick-lennox/trailbeam/blob/main/LICENSING.md).
