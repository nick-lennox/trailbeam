/*
  Puts the shared test inputs in the package, so the hosted app tests itself on the same ones through its
  normal dependency: @trailbeam/ui/fixtures/contract-3.2-spans.json (the 32 spans the Node.js package
  recorded, from ../contract/fixtures) and @trailbeam/ui/fixtures/contract-3.2-timelines.json (what
  Trailbeam's server returns for them, from test/make-fixtures.py). Run by npm run build:lib.
*/
import { copyFileSync, mkdirSync } from "node:fs";

const out = new URL("../dist/fixtures/", import.meta.url);
mkdirSync(out, { recursive: true });
copyFileSync(new URL("../../contract/fixtures/contract-3.2-spans.json", import.meta.url), new URL("contract-3.2-spans.json", out));
copyFileSync(new URL("../test/fixtures/contract-3.2-timelines.json", import.meta.url), new URL("contract-3.2-timelines.json", out));
