/*
  Puts the shared test inputs in the package, so the hosted app tests itself on the same ones through its
  normal dependency: @trailbeam/ui/fixtures/contract-3.2-spans.json (the 32 spans the Node.js package
  recorded, from ../contract/fixtures) and @trailbeam/ui/fixtures/body-views.json (what
  contract/body-view.md gives for them: each message's Body tab and the body.* searches). Run by
  npm run build:lib.
*/
import { copyFileSync, mkdirSync } from "node:fs";

const out = new URL("../dist/fixtures/", import.meta.url);
mkdirSync(out, { recursive: true });
for (const f of ["contract-3.2-spans.json", "body-views.json"]) copyFileSync(new URL(`../../contract/fixtures/${f}`, import.meta.url), new URL(f, out));
