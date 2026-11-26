/*
  The package must load on React's server build, which has no createContext: the hosted app's route handlers and server
  components import it (through its index) for its helpers, and a context made when a module loads failed every one of
  them in `next build` (QV-399). Views pass what they share as props instead.
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";

const SRC = new URL("../src/", import.meta.url).pathname;
const files = (dir: string): string[] =>
  readdirSync(dir, { withFileTypes: true }).flatMap((d) => (d.isDirectory() ? files(join(dir, d.name)) : /\.tsx?$/.test(d.name) ? [join(dir, d.name)] : []));

test("no module of the package calls createContext", () => {
  const found = files(SRC).filter((f) => /\bcreateContext\b/.test(readFileSync(f, "utf8")));
  assert.deepEqual(found, [], "a context made at load breaks a server-side import of @trailbeam/ui");
});
