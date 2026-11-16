/*
  The shared Tenants table, rendered to HTML on the server, for the Tenants page self-hosted Trailbeam and the
  hosted app both compute from the shared fixtures (../../contract/fixtures/tenants-expected.json). Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { TenantSearch, TenantsTable, fmtShare, fmtWait, tenantsFootnote } from "../src/tenants/Tenants.tsx";
import type { TenantStats } from "../src/tenants/types.ts";

const expected = JSON.parse(readFileSync(new URL("../../contract/fixtures/tenants-expected.json", import.meta.url), "utf8")) as { stats: (TenantStats & { name: string })[] };
const text = (html: string) => html.replace(/<[^>]+>/g, " ").replace(/&lt;/g, "<").replace(/\s+/g, " ");
const render = (s: TenantStats) => renderToStaticMarkup(createElement(TenantsTable, { rows: s.rows, href: (t: string) => `/?tenant=${encodeURIComponent(t)}` }));

test("every tenant of every expected range is drawn, in the API's order, with its numbers", () => {
  for (const s of expected.stats) {
    const html = render(s);
    const flat = text(html);
    let at = -1;
    for (const r of s.rows) {
      const i = flat.indexOf(` ${r.tenant} `, at + 1);
      assert.ok(i > at, `${s.name}: ${r.tenant} after the tenant before it`);
      const row = flat.slice(i, i + 80);
      assert.ok(row.includes(` ${r.sent} `) && row.includes(fmtShare(r)) && row.includes(r.p95_in_queue_ms === null ? " - " : fmtWait(r.p95_in_queue_ms)), `${s.name}: ${r.tenant}'s numbers: ${row}`);
      assert.ok(html.includes(`href="/?tenant=${encodeURIComponent(r.tenant)}"`));
      at = i;
    }
    assert.ok(!/undefined|NaN/.test(html), `${s.name}: no undefined or NaN`);
  }
});

test("formats: share, p95 and the footnote", () => {
  assert.equal(fmtShare({ share: 2 / 7, sent: 2 }), "29%");
  assert.equal(fmtShare({ share: 0.004, sent: 1 }), "<1%");
  assert.equal(fmtShare({ share: 0, sent: 0 }), "0%");
  assert.deepEqual([fmtWait(430), fmtWait(1500), fmtWait(12_000), fmtWait(90_000)], ["430 ms", "1.5 s", "12 s", "1.5 min"]);
  const whole = expected.stats[0];
  assert.match(tenantsFootnote(whole, "Last 24 hours"), /^Last 24 hours, from every message in the range\. Share is of all 7 observed sends, 1 of them without a tenant\./);
  assert.match(tenantsFootnote({ ...whole, capped: true }, "Last hour"), /the most recent 2,000 messages/);
});

test("the tenants views use role tokens, which theme.css defines, so a light host page can define its own", () => {
  const html = render(expected.stats[0]) + renderToStaticMarkup(createElement(TenantSearch, { suggest: async () => [], href: (t: string) => t }));
  const classes = new Set([...html.matchAll(/class="([^"]*)"/g)].flatMap((m) => m[1].split(/\s+/)));
  const used = new Set<string>();
  for (const c of classes) {
    const m = /^(?:[a-z]+:)*(?:text|bg|border)-((?:ink|line|surface|brand|series|warning|critical|sky|dawn|wait|ember|lost)[a-z0-9-]*?)(?:\/\d+)?$/.exec(c);
    if (m) used.add(m[1]);
  }
  assert.ok(used.size >= 8, `role tokens found: ${[...used]}`);
  const theme = readFileSync(new URL("../src/theme.css", import.meta.url), "utf8");
  for (const t of used) {
    assert.ok(!/^(sky|dawn|wait|ember|lost)/.test(t), `${t} is the night palette; use a role token`);
    assert.match(theme, new RegExp(`--color-${t}:`), `theme.css defines --color-${t}`);
  }
});
