/*
  The design entry (@trailbeam/ui/design, src/design): the hosted app's design system, shared by both products. Links go
  through the host's link component, hover notes are Tooltips and never the browser's `title`, the pills keep an icon
  and a label, and the open-source shell can import the theme with the same font variables next/font sets.
  Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { createElement as h } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import {
  AppThemeProvider, Button, HealthPill, Logo, Mark, NavProvider, OutcomePill, PageHeader, SourceTag, StatCard, StatePill, TopBar, Tooltip,
  purgeText, stateLook, type LinkProps,
} from "../src/design/index.ts";

const DESIGN = new URL("../src/design/", import.meta.url).pathname;
const SHELL = new URL("../shell/", import.meta.url).pathname;
const html = (el: ReturnType<typeof h>) => renderToStaticMarkup(el);

test("a link is a plain <a> without a host, and the host's link component inside NavProvider", () => {
  assert.equal(html(h(Button, { href: "/x", variant: "primary", children: "Go" })).slice(0, 9), '<a href="');
  const HostLink = ({ children, ...rest }: LinkProps) => h("a", { ...rest, "data-host": "yes" }, children);
  const out = html(h(NavProvider, { Link: HostLink, children: [h(Button, { key: 1, href: "/x", children: "Go" }), h(StatCard, { key: 2, label: "Sent", value: 3, href: "/y" })] }));
  assert.equal((out.match(/data-host="yes"/g) ?? []).length, 2);
  assert.match(html(h(Button, { type: "submit", children: "Save" })), /^<button class="[^"]*" type="submit">Save<\/button>$/);
});

test("the theme provider is night by default", () => {
  assert.match(html(h(AppThemeProvider, { className: "flex", children: "x" })), /^<div data-app-theme="night" class="flex">x<\/div>$/);
});

test("no design component uses a native title for a hover note", () => {
  const offenders = readdirSync(DESIGN).filter((f) => f.endsWith(".tsx")).filter((f) => /\stitle=\{|\stitle="/.test(readFileSync(join(DESIGN, f), "utf8")));
  assert.deepEqual(offenders, []);
  const tags = html(h("div", null,
    h(SourceTag, { source: "inferred" }),
    h(StatePill, { m: { state: "deleted", in_dead_letter_queue: false, deliveries: 1, purge: { status: "purged", start: "2026-11-27T12:00:09.000Z" } } }),
  ));
  assert.doesNotMatch(tags, /\stitle=/);
  // a Tooltip's trigger is focusable where it has to be, so a keyboard reaches the note too
  assert.equal((tags.match(/tabindex="0"/g) ?? []).length, 2);
});

test("Tooltip renders its trigger only, until it is pointed at", () => {
  assert.equal(html(h(Tooltip, { content: "A note", className: "t", children: "mark" })), '<span class="t">mark</span>');
});

test("the pills always carry an icon and a label", () => {
  for (const el of [h(OutcomePill, { outcome: "dead_lettered" }), h(HealthPill, { health: "critical" }), h(StatePill, { m: { state: "processing", in_dead_letter_queue: false, deliveries: 1 } })]) {
    const out = html(el);
    assert.match(out, /<svg[^>]*aria-hidden="true"/);
    assert.match(out.replace(/<[^>]+>/g, ""), /\w/);
  }
  assert.equal(stateLook({ state: "x", in_dead_letter_queue: true, deliveries: 2 }).label, "In the DLQ");
  assert.equal(stateLook({ state: "pending", in_dead_letter_queue: false, deliveries: 0 }).label, "Not received");
  assert.equal(stateLook({ state: "pending", in_dead_letter_queue: false, deliveries: 2 }).label, "Not deleted");
  assert.equal(purgeText({ purge: { status: "possibly_purged", start: "2026-11-27T12:00:09.000Z" } }), "Possibly purged: a PurgeQueue call ran at 12:00:09.000 UTC");
  assert.equal(purgeText({ purge: { status: "purged", start: "2026-11-27T12:00:09.000Z" } }), "Purged by a PurgeQueue call at 12:00:09.000 UTC");
  assert.equal(purgeText({}), "");
});

test("the purge notes are whole sentences: the purged pill's note, and the words after a possibly purged pill", () => {
  const start = "2026-11-27T12:00:09.000Z";
  const possibly = html(h(StatePill, { m: { state: "pending", in_dead_letter_queue: false, deliveries: 1, purge: { status: "possibly_purged", start } } }));
  assert.match(possibly, />Possibly purged: a PurgeQueue call ran at 12:00:09\.000 UTC</);
  assert.doesNotMatch(possibly, /observed:|\(PurgeQueue/);
});

test("source tags are labels, not values: no mono, and the labels in full and in sentence case", () => {
  for (const source of ["otel", "package", "cloudtrail", "function_logs", "inferred"] as const) {
    assert.doesNotMatch(html(h(SourceTag, { source })), /font-mono/);
  }
  assert.match(html(h(SourceTag, { source: "package" })), />Trailbeam package</);
  assert.match(html(h(SourceTag, { source: "function_logs" })), />Function logs</);
  assert.match(html(h(SourceTag, { source: "inferred" })), /border-dashed/);
});

test("the default Logo follows the theme, so the night app shows it; sky stays for the landing page", () => {
  const day = html(h(Logo, {}));
  assert.match(day, /class="[^"]*text-ink[ "]/);
  assert.match(day, /fill="var\(--color-accent\)"/);
  assert.doesNotMatch(day, /text-sky[ "]/);
  assert.match(html(h(Logo, { tone: "sky" })), /text-sky-ink/);
  assert.match(html(h(Mark, {})), /text-ink[ "]/);
});

test("Button inverse is dark text on its light fill, whatever the theme", () => {
  const out = html(h(Button, { variant: "inverse", children: "Go" }));
  assert.match(out, /bg-night-ink text-night /);
  assert.doesNotMatch(out, /text-ink[ "]/);
});

test("TopBar takes the host's search, and plain leaves it and the freshness mark out", () => {
  const search = h("input", { "data-search": "" });
  assert.match(html(h(TopBar, { title: "Overview", search, fresh: true })), /data-search.*Updated 4s ago/);
  assert.doesNotMatch(html(h(TopBar, { title: "Overview", search, plain: true, fresh: true })), /data-search|Updated/);
  assert.match(html(h(PageHeader, { title: "Queues", subtitle: "All of them" })), /<h1 class="font-serif text-title-sm text-ink">Queues<\/h1>/);
  assert.match(html(h(TopBar, { title: "Overview" })), /<h1 class="[^"]*font-serif[^"]*">Overview<\/h1>/);
});

test("TopBar claims no freshness unless the caller passes it: the real time, or the sample pages' fixed one", () => {
  assert.doesNotMatch(html(h(TopBar, { title: "Tenants" })), /Updated|data-fresh/);
  assert.doesNotMatch(html(h(PageHeader, { title: "Tenants" })), /Updated/);
  for (const fresh of [false, null, ""]) assert.doesNotMatch(html(h(TopBar, { title: "T", fresh })), /Updated/);
  assert.match(html(h(TopBar, { title: "T", fresh: h("time", { dateTime: "PT12S" }, "12s ago") })), /Updated <time dateTime="PT12S">12s ago<\/time>/);
  assert.match(html(h(PageHeader, { title: "T", fresh: "1m ago" })), /Updated 1m ago/);
  assert.doesNotMatch(html(h(TopBar, { title: "T", fresh: "1m ago" })), /4s ago/);
});

test("the open-source shell imports the theme, with the font variables next/font sets in the hosted app", () => {
  const styles = readFileSync(join(SHELL, "styles.css"), "utf8"), fonts = readFileSync(join(SHELL, "fonts.css"), "utf8");
  assert.match(styles, /@import "\.\.\/src\/design\/tokens\.css";\s*@import "\.\.\/src\/theme\.css";/);
  assert.match(styles, /@import "\.\/fonts\.css";/);
  for (const v of ["--font-newsreader", "--font-instrument", "--font-plex-mono"]) assert.match(fonts, new RegExp(`${v}: "`));
  const tokens = readFileSync(join(DESIGN, "tokens.css"), "utf8");
  for (const v of ["--font-newsreader", "--font-instrument", "--font-plex-mono"]) assert.match(tokens, new RegExp(`var\\(${v}\\)`));
  assert.match(tokens, /\[data-app-theme="night"\] \{/);
  assert.doesNotMatch(tokens + fonts, /JetBrains/i);
});

test("the package exports the design entry and its CSS", () => {
  const pkg = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8"));
  assert.deepEqual(pkg.exports["./design"], { types: "./dist/design/index.d.ts", default: "./dist/design/index.js" });
  assert.equal(pkg.exports["./design.css"], "./src/design/design.css");
  assert.equal(pkg.exports["./tokens.css"], "./src/design/tokens.css");
  assert.ok(pkg.peerDependencies["lucide-react"]);
});

test("a Tooltip with no content is the plain span", () => {
  for (const content of [undefined, null, false, ""]) assert.equal(html(h(Tooltip, { content, className: "t", children: "mark" })), '<span class="t">mark</span>');
});
