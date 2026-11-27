/*
  Puts the shared test inputs in the package, so the hosted app tests itself on the same ones through its
  normal dependency: @trailbeam/ui/fixtures/contract-3.2-spans.json (the 32 spans the Node.js package
  recorded, from ../contract/fixtures), @trailbeam/ui/fixtures/contract-3.2-timelines.json (what
  Trailbeam's server returns for them, from test/make-fixtures.py), @trailbeam/ui/fixtures/purge-timelines.json
  (what it returns after a PurgeQueue call, contract 3.3), @trailbeam/ui/fixtures/sns-timelines.json (after an SNS
  fan-out, contract 3.4), @trailbeam/ui/fixtures/origin-timelines.json (EventBridge and S3 origins, contract 3.5),
  @trailbeam/ui/fixtures/put-event-timelines.json (recorded EventBridge PutEvents entries, contract 3.6),
  @trailbeam/ui/fixtures/body-views.json
  (what contract/body-view.md gives for them: each message's Body tab and the body.* searches), and
  @trailbeam/ui/fixtures/causes-parity.json (timelines and the cause groups both products must make of them,
  from test/make-causes-fixture.py), and @trailbeam/ui/fixtures/alerts-parity.json (alert rule cases and what both
  engines must make of them, from ../contract/fixtures/make-alerts-parity.py). Run by npm run build:lib.
*/
import { copyFileSync, mkdirSync } from "node:fs";

const out = new URL("../dist/fixtures/", import.meta.url);
mkdirSync(out, { recursive: true });
for (const f of ["contract-3.2-spans.json", "body-views.json", "alerts-parity.json"]) copyFileSync(new URL(`../../contract/fixtures/${f}`, import.meta.url), new URL(f, out));
for (const f of ["contract-3.2-timelines.json", "purge-timelines.json", "sns-timelines.json", "causes-parity.json", "origin-timelines.json", "put-event-timelines.json"]) copyFileSync(new URL(`../test/fixtures/${f}`, import.meta.url), new URL(f, out));
