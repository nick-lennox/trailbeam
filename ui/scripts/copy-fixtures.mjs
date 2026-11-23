/*
  Puts the shared test inputs in the package, so the hosted app tests itself on the same ones through its
  normal dependency: @trailbeam/ui/fixtures/contract-3.2-spans.json (the 32 spans the Node.js package
  recorded, from ../contract/fixtures), @trailbeam/ui/fixtures/contract-3.2-timelines.json (what
  Trailbeam's server returns for them, from test/make-fixtures.py), @trailbeam/ui/fixtures/purge-timelines.json
  (what it returns after a PurgeQueue call, contract 3.3) and @trailbeam/ui/fixtures/body-views.json
  (what contract/body-view.md gives for them: each message's Body tab and the body.* searches). Run by
  npm run build:lib.
*/
import { copyFileSync, mkdirSync } from "node:fs";

const out = new URL("../dist/fixtures/", import.meta.url);
mkdirSync(out, { recursive: true });
for (const f of ["contract-3.2-spans.json", "body-views.json"]) copyFileSync(new URL(`../../contract/fixtures/${f}`, import.meta.url), new URL(f, out));
for (const f of ["contract-3.2-timelines.json", "purge-timelines.json"]) copyFileSync(new URL(`../test/fixtures/${f}`, import.meta.url), new URL(f, out));
