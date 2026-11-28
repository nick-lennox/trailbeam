/*
  One UI (QV-739): a change that can't be undone asks first, one way everywhere (the shared Confirm on a row or in a
  form, the themed Modal from a menu, never the browser's own box); no fixed colour that breaks the day theme; the
  Live button is one part. Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join } from "node:path";
import { removePasskeySentence } from "../src/account/AccountCard.tsx";

const ROOT = new URL("..", import.meta.url).pathname;
const files = (dir: string): string[] => readdirSync(join(ROOT, dir)).flatMap((f) => {
  const p = join(dir, f);
  return statSync(join(ROOT, p)).isDirectory() ? files(p) : /\.tsx?$/.test(f) ? [p] : [];
});
const read = (p: string) => readFileSync(join(ROOT, p), "utf8");
const all = [...files("src"), ...files("shell")];

test("nothing asks in the browser's own confirm box", () => {
  const hits = all.filter((p) => /window\.confirm\(|\bconfirm\(`|\bconfirm\("/.test(read(p)));
  assert.deepEqual(hits, []);
});

test("the passkey remove asks, and is off while a change is sent", () => {
  assert.equal(removePasskeySentence({ name: "Chrome on Mac" }), "Remove Chrome on Mac? It can't sign you in after this.");
  assert.equal(removePasskeySentence({ name: "" }), "Remove this passkey? It can't sign you in after this.");
  const src = read("src/account/AccountCard.tsx");
  assert.match(src, /onClick=\{\(\) => setRemoving\(k\.id\)\} disabled=\{passkeys\.busy\}/);
  assert.match(src, /<Confirm label=/);
});

test("the alert and channel removes and the dashboard list's delete use the shared parts", () => {
  assert.match(read("src/alerts/AlertViews.tsx"), /<Confirm label="Delete this alert"/);
  assert.match(read("src/alerts/AlertViews.tsx"), /<Confirm label=\{`Remove \$\{c\.name\}`\}/);
  assert.match(read("src/dashboards/DashboardsPage.tsx"), /<DeleteDashboard /);
  assert.match(read("src/dashboards/DashboardView.tsx"), /<DeleteDashboard /);
});

test("the modal's backdrop is the theme's scrim, defined for day and night", () => {
  assert.match(read("src/dashboards/ui.tsx"), /\bbg-scrim\b/);
  const tokens = read("src/design/tokens.css");
  assert.equal(tokens.match(/--color-scrim:/g)?.length, 3, "the default, night and day");
  assert.match(read("src/theme.css"), /--color-scrim:/);
});

test("no fixed black or navy wash on a dashboard", () => {
  for (const p of files("src/dashboards")) assert.doesNotMatch(read(p), /bg-black\/|bg-\[rgb\(/, p);
  const options = read("src/dashboards/SectionOptions.tsx");
  assert.match(options, /const LIGHT = "var\(--color-sky-ink\)", DARK = "var\(--color-sky\)"/);
});

test("the Live and Paused button is one part", () => {
  const drawn = all.filter((p) => /live-dot/.test(read(p)) && /\bLive\b/.test(read(p)) && /Paused/.test(read(p)));
  assert.deepEqual(drawn, ["src/explorer/parts.tsx"]);
  for (const p of ["src/dashboards/DashboardView.tsx", "shell/events.tsx", "shell/messages.tsx"]) assert.match(read(p), /<LiveButton /, p);
});

test("MessagesTable is gone: the Messages list is MessagesView", () => {
  for (const p of all) assert.doesNotMatch(read(p), /\bMessagesTable\b/, p);
});
