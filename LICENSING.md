# Licensing

Trailbeam is open source under two licences, from release 0.5.0 on:

- **The client libraries and everything you put in your own services or infrastructure are Apache License 2.0.**
- **Trailbeam's server and its web UI are GNU Affero General Public License v3.0 only (AGPL-3.0-only).**

Releases 0.3.0 and 0.4.0 were Apache 2.0 throughout, and they stay Apache 2.0: a licence on code you already have
can't be taken back. What changes is the licence of the server and the UI in 0.5.0 and later.

## Which part is under which licence

| Folder | Licence | What it is |
| --- | --- | --- |
| `python/` | Apache-2.0 | `trailbeam` on PyPI, the Python library (boto3, AWS Lambda and pika hooks) |
| `node/` | Apache-2.0 | `trailbeam` on npm, the Node.js library and `npx trailbeam start` |
| `java/` | Apache-2.0 | The Java library |
| `dotnet/` | Apache-2.0 | The .NET library |
| `go/` | Apache-2.0 | The Go library |
| `contract/` | Apache-2.0 | The span contract, its vectors and fixtures, and the API description (`openapi.yaml`) |
| `conformance/` | Apache-2.0 | The conformance kit and its drivers |
| `examples/` | Apache-2.0 | Examples |
| `demo/` | Apache-2.0 | The demo |
| `deploy/` | Apache-2.0 | The Helm chart (`deploy/helm/trailbeam/` has its own copy), replicas and monitoring files |
| `collector/` | Apache-2.0 | OpenTelemetry Collector configurations |
| `compat/` | Apache-2.0 | The compatibility matrix |
| `docker-compose.yaml`, `.env.example` | Apache-2.0 | The team install (marked with an SPDX line, as they sit at the root) |
| `server/` | AGPL-3.0-only | `trailbeam-server` on PyPI: the server, its API and the `trailbeam` command |
| `ui/` | AGPL-3.0-only | `@trailbeam/ui`, the web UI, built into the server |
| `python/tests/server/` | AGPL-3.0-only | Tests of the server |
| `python/tests/conformance/` | AGPL-3.0-only | The store conformance suite (tests of the server's store) |
| `python/bench/` | AGPL-3.0-only | Benchmarks of the server |
| `python/smoke/` | AGPL-3.0-only | Smoke, upgrade and backup checks of the server |
| everything else at the root | AGPL-3.0-only | The root `Dockerfile` (the server's image), `scripts/`, `.github/`, the docs |

The container image `ghcr.io/nick-lennox/trailbeam` is the server, so it is labelled AGPL-3.0-only. The Helm
chart and the Compose file that run it are Apache-2.0: you can copy and change them freely, and running the image
with them doesn't put them under the AGPL.

The names Trailbeam and the logo are not covered by either licence.

## What this means for you

**You install a library in your service** (`pip install trailbeam`, `npm install trailbeam`, the Java, .NET or Go
package). Nothing changes. The libraries are Apache 2.0 as before, and they don't contain or depend on any server
code: CI fails if a library file imports from the server or the UI. Your service's code is yours, whatever licence
it is under. Sending spans to a Trailbeam server, self-hosted or hosted, doesn't change that.

**You run the server unchanged**, for your team or company, in a container or with `pip install trailbeam-server`.
You can do that freely, as before. The AGPL's conditions apply when you *modify* the server and let other people
use it.

**You modify the server or the UI and let other people use it over a network.** Then the AGPL asks you to offer
those users the source code of your modified version, under the AGPL. Changing its configuration (flags,
environment variables, the Helm values) isn't modifying it. If you hand out copies of a modified server, the same
applies to the people who get them.

**You want to embed the server in a closed-source product, or offer a modified version without publishing your
changes.** Ask us for a commercial licence.

This section is a plain-language summary. The licence texts in each `LICENSE` file are what applies.

## How the repository is set up

- **Each folder in the table has a `LICENSE` file with the full text of its licence.** A file is under the licence
  of the nearest `LICENSE` file in its folder or above it.
- **The root `LICENSE` is the AGPL-3.0 text.** GitHub's licence badge and many licence scanners read only the root
  file. With the AGPL at the root, a scanner that stops there reads the stricter of the two licences, never a
  looser one than applies; the libraries' packages on PyPI, npm, Maven Central, NuGet and the Go module proxy carry
  their own `LICENSE` and Apache-2.0 metadata, which is what dependency scanners in your service read.
  The alternative, Apache 2.0 at the root, would have shown the libraries' licence on GitHub, but it would have told
  anyone who reads only the root file that the server is Apache 2.0, which is wrong from 0.5.0 on.
- **AGPL-3.0-*only*, not *or later*.** "Or later" would let a future AGPL version, written by the Free Software
  Foundation, apply to the server without us choosing it. With "only", the copyright holder decides if and when to
  move to a new version.
- **SPDX lines.** Files don't need a licence header: their folder decides. A file whose licence differs from its
  folder's says so in one of its first five lines:

  ```
  # SPDX-License-Identifier: AGPL-3.0-only
  ```

  `docker-compose.yaml` and `.env.example` are marked Apache-2.0 this way, and a few library tests that import the
  server to check what it makes of the library's spans are marked AGPL-3.0-only.
- **New files** go in the folder whose licence they should have. A new library file must not import the server
  (`trailbeam.server`) or the UI (`@trailbeam/ui`), or any AGPL file. Tests that need the server go in
  `python/tests/server/`, or start the server as a program (`python -m trailbeam.server`, the container image), which
  is allowed from Apache-2.0 code: it isn't an import.
- **The check.** `python3 scripts/licences.py check` checks all of this: every `LICENSE` text, the folders above,
  the licence in each package manifest and the image label, SPDX lines, and that no Apache-2.0 file imports an
  AGPL-3.0 one. CI runs it on every pull request (the *Changelog* job).
  `python3 scripts/licences.py which PATH` says which licence a file is under, and why.

The `trailbeam` Python library's `server`, `postgres` and `sso` extras install `trailbeam-server` (so that
`pip install "trailbeam[server]"` keeps working). They are optional: `pip install trailbeam` installs no server
code, and the library's code never imports it.
