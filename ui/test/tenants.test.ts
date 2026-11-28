/*
  The shared Tenants page (table, range control and page chrome), rendered to HTML on the server, for the Tenants page
  self-hosted Trailbeam and the hosted app both compute from the shared fixtures
  (../../contract/fixtures/tenants-expected.json), and the top bar's search on both. Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { P95_NOTE, TENANT_ENV, TenantSearch, fmtShare, fmtWait, noTenantsText, p95Note, tenantsFootnote } from "../src/tenants/Tenants.tsx";
import { MessageSearch, NavProvider, TenantsPage, TenantsTable, type LinkProps } from "../src/design/index.ts";
import { DEFAULT_TENANTS_RANGE, tenantsHref, tenantsTime, tenantsUpgrade } from "../src/tenants/range.ts";
import { custom, preset } from "../src/model/timerange.ts";
import { shellSearchHref } from "../shell/frame.tsx";
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
      assert.ok(row.includes(` ${r.sent} `) && row.includes(fmtShare(r)) && row.includes(r.p95_in_queue_ms === null ? " none " : fmtWait(r.p95_in_queue_ms)), `${s.name}: ${r.tenant}'s numbers: ${row}`);
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
  assert.doesNotMatch(tenantsFootnote({ ...whole, capped: true }, "Last hour"), /2,000/, "capped is always false: no capped wording (QV-247)");
  const foot = tenantsFootnote(whole, "Last hour");
  assert.ok(foot.includes(P95_NOTE));
  for (const s of ["two clocks", "SentTimestamp when the send wasn't observed", "more first receives than sends", "below 0 counts as 0", "within 1% of the exact p95"])
    assert.ok(P95_NOTE.includes(s), `p95 note says ${s}`);
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

test("no tenants: what a tenant is and how to set one, in plain words (QV-217)", () => {
  const head = "A tenant is your own customer or account ID, read from each message. ";
  const tail = " To split traffic by customer, set one of these in every service that sends or receives:";
  assert.equal(noTenantsText(10, "Last 24 hours"), `${head}10 messages were sent in the last 24 hours, none with one.${tail}`);
  assert.equal(noTenantsText(1, "Last hour"), `${head}1 message was sent in the last hour, none with one.${tail}`);
  assert.equal(noTenantsText(2500, "Last 7 days"), `${head}2,500 messages were sent in the last 7 days, none with one.${tail}`);
  assert.equal(noTenantsText(0, "Last 7 days"), `${head}No messages were sent in the last 7 days.${tail}`);
  assert.equal(TENANT_ENV, "export TRAILBEAM_TENANT_ATTRIBUTE=tenant        # a message attribute\nexport TRAILBEAM_TENANT_BODY_PATH=customer.id   # or a path in a JSON body");
  for (const s of [noTenantsText(10, "Last hour"), TENANT_ENV]) assert.ok(!/15\.2|span contract|trailbeam\.tenant/.test(s));
});

test("the tenant search box says what to type", () => {
  const html = renderToStaticMarkup(createElement(TenantSearch, { suggest: async () => [], href: (t: string) => t }));
  assert.match(html, /placeholder="Tenant \(customer ID\)"/);
});

test("a tenant links through the host's link component, from a prefix a server component can pass", () => {
  const rows = expected.stats[0].rows;
  const HostLink = ({ children, ...rest }: LinkProps) => createElement("a", { ...rest, "data-host": "yes" }, children);
  const html = renderToStaticMarkup(createElement(NavProvider, { Link: HostLink }, createElement(TenantsTable, { rows, href: "/app/acme/messages?tenant=" })));
  assert.equal((html.match(/data-host="yes"/g) ?? []).length, rows.length);
  for (const r of rows) assert.ok(html.includes(`href="/app/acme/messages?tenant=${encodeURIComponent(r.tenant)}"`), r.tenant);
});

test("the table follows the design rules: sans labels, hover notes as Tooltips, no dash for a missing p95", () => {
  const html = render(expected.stats[0]);
  assert.doesNotMatch(html, /\stitle="/, "no native title");
  for (const th of html.match(/<th [^>]*>/g) ?? []) assert.doesNotMatch(th, /font-mono/, `a label in mono: ${th}`);
  assert.equal((html.match(/<th /g) ?? []).length, 5);
  assert.equal(p95Note(1), "Over 1 first receive.");
  assert.equal(p95Note(1200), "Over 1,200 first receives.");
  assert.equal(p95Note(0), "No first receive observed.");
  assert.doesNotMatch(text(html), / - /);
});

test("the page chrome: the heading, the host's one range control, and the host's search", () => {
  const html = renderToStaticMarkup(createElement(TenantsPage, { control: createElement("button", { id: "host-range" }, "Past 1 day"), search: createElement("form", { id: "host-search" }), fresh: false }, createElement("p", null, "body")));
  assert.match(html, /<h1[^>]*>Tenants<\/h1>/);
  assert.ok(html.includes("Which of your customers are sending the traffic, and who is waiting longest."));
  assert.ok(html.includes('id="host-search"'));
  assert.equal((html.match(/id="host-range"/g) ?? []).length, 1, "one range control");
  assert.doesNotMatch(html, /Last 24 hours|Last hour/);
  assert.doesNotMatch(html, /Updated/, "no freshness claim when the host passes fresh={false}");
});

test("the range: the time picker's ids and names, a day by default, and the links from before QV-711 (QV-711)", () => {
  const NOW = Date.UTC(2026, 10, 27, 12, 0, 0);
  const at = (q: string) => { const p = new URLSearchParams(q); return tenantsTime((k) => p.get(k), NOW); };
  assert.equal(DEFAULT_TENANTS_RANGE, "1d");
  assert.deepEqual(at(""), preset("1d", NOW));
  assert.equal(at("").label, "Past 1 day");
  assert.deepEqual(at("range=4h"), preset("4h", NOW));
  assert.deepEqual(at("range=nonsense"), preset("1d", NOW));
  assert.deepEqual(at("from=2026-11-27T09:00:00Z&to=2026-11-27T10:00:00Z"), custom(NOW - 3 * 3_600_000, NOW - 2 * 3_600_000));
  // the open-source page's since_s and the hosted page's 24h
  assert.deepEqual(at("since_s=3600"), preset("1h", NOW));
  assert.deepEqual(at("since_s=86400"), preset("1d", NOW));
  assert.deepEqual(at("since_s=604800"), preset("7d", NOW));
  assert.deepEqual(at("range=24h"), preset("1d", NOW));
  assert.deepEqual(at("range=1h&since_s=604800"), preset("1h", NOW), "range= wins");
  // what an old link becomes
  assert.equal(tenantsUpgrade("?since_s=3600"), "?range=1h");
  assert.equal(tenantsUpgrade("?since_s=86400"), "");
  assert.equal(tenantsUpgrade("?range=24h"), "");
  assert.equal(tenantsUpgrade("?since_s=604800&x=1"), "?x=1&range=7d");
  assert.equal(tenantsUpgrade("?range=1h"), null);
  assert.equal(tenantsUpgrade(""), null);
  // the page's link
  assert.equal(tenantsHref("/tenants", preset("1d", NOW)), "/tenants");
  assert.equal(tenantsHref("/tenants", preset("7d", NOW)), "/tenants?range=7d");
  assert.equal(tenantsHref("/tenants?since_s=3600", preset("1h", NOW)), "/tenants?range=1h");
  assert.equal(tenantsHref("/tenants", custom(NOW - 3_600_000, NOW)), "/tenants?from=2026-11-27T11%3A00%3A00Z&to=2026-11-27T12%3A00%3A00Z");
});

test("the empty state's sentence reads the time picker's names and a fixed range", () => {
  assert.match(noTenantsText(3, "Past 1 day"), /3 messages were sent in the past 1 day, none with one\./);
  assert.match(noTenantsText(0, "Nov 27, 09:00 to 10:00"), /No messages were sent from Nov 27, 09:00 to 10:00\./);
});

test("the top bar's search: a GET form to the host's search, with the host's parameter name", () => {
  const hosted = renderToStaticMarkup(createElement(MessageSearch, { action: "/app/acme/messages" }));
  assert.match(hosted, /^<form role="search" [^>]*action="\/app\/acme\/messages"/);
  assert.match(hosted, /name="q"/);
  assert.match(hosted, /placeholder="Find a message by its ID, a trace ID or a tenant"/);
  const shell = renderToStaticMarkup(createElement(MessageSearch, { action: "/message", name: "id", placeholder: "Find a message by ID" }));
  assert.match(shell, /action="\/message"/);
  assert.match(shell, /name="id"/);
});

test("the open-source top bar: a message ID opens its timeline, tenant:<name> or a suggested name lists its messages", () => {
  assert.equal(shellSearchHref("  "), null);
  assert.equal(shellSearchHref("0b6f3c1e-msg"), "/message?id=0b6f3c1e-msg");
  assert.equal(shellSearchHref("tenant:acme corp"), "/?tenant=acme%20corp");
  assert.equal(shellSearchHref("Tenant: acme"), "/?tenant=acme");
  assert.equal(shellSearchHref("tenant:"), null);
  assert.equal(shellSearchHref("acme", ["acme", "acme-eu"]), "/?tenant=acme");
  assert.equal(shellSearchHref("acm", ["acme"]), "/message?id=acm");
});
