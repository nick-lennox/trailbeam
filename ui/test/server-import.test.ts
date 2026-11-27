/*
  The package must load on React's server build, which has no createContext: the hosted app's route handlers and server
  components import it (through its index) for its helpers, and a context made when a module loads failed every one of
  them in `next build` (QV-399). Views pass what they share as props instead.

  The design entry (@trailbeam/ui/design) has two contexts, the host's link component and the app theme. Their modules
  start with "use client", so a server build never runs them, and the main entry never imports them.
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";

const SRC = new URL("../src/", import.meta.url).pathname;
const files = (dir: string): string[] =>
  readdirSync(dir, { withFileTypes: true }).flatMap((d) => (d.isDirectory() ? files(join(dir, d.name)) : /\.tsx?$/.test(d.name) ? [join(dir, d.name)] : []));

const CLIENT_CONTEXTS = ["design/nav.tsx", "design/theme.tsx"];

test("no module of the package calls createContext, except the design entry's two client modules", () => {
  const found = files(SRC).filter((f) => /\bcreateContext\b/.test(readFileSync(f, "utf8"))).map((f) => f.slice(SRC.length)).sort();
  assert.deepEqual(found, CLIENT_CONTEXTS, "a context made at load breaks a server-side import of @trailbeam/ui");
  for (const f of CLIENT_CONTEXTS) assert.match(readFileSync(join(SRC, f), "utf8"), /^"use client";/, `${f} must be a client module`);
});

test("the main entry never imports the design entry's contexts", () => {
  const seen = new Set<string>();
  const walk = (file: string) => {
    if (seen.has(file)) return;
    seen.add(file);
    for (const m of readFileSync(file, "utf8").matchAll(/from "(\.{1,2}\/[^"]+)"/g)) walk(join(file, "..", m[1]));
  };
  walk(join(SRC, "index.ts"));
  const reached = [...seen].map((f) => f.slice(SRC.length)).filter((f) => f.startsWith("design/"));
  assert.deepEqual(reached, []);
});
