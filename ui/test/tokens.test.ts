/*
  The Tokens page's shared parts (src/tokens), rendered to HTML on the server: what each token row offers, the legacy
  shared token's line, the confirmation the first ingest token needs, expiry choices and the one-time token's wording.
  Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { CreateToken, OneTimeToken, TokensTable } from "../src/tokens/TokensViews.tsx";
import {
  DEFAULT_EXPIRY, EXPIRY, OPEN_INGEST_CONFIRM, revokeSentence, secretSentence, sharedSentence, shownPrefix,
} from "../src/tokens/Tokens.tsx";
import type { IngestToken, PersonalToken, SharedToken } from "../src/tokens/types.ts";

const ingest = (id: number, over: Partial<IngestToken> = {}): IngestToken => ({
  id, name: `svc-${id}`, prefix: `tbi_ab${id}x`, status: "active", created_at_ms: 1795400000000, created_by: "ada@example.com",
  last_used_ms: null, revoked_at_ms: null, ...over,
});
const personal = (id: number, over: Partial<PersonalToken> = {}): PersonalToken => ({
  id, name: `script-${id}`, prefix: `tbp_cd${id}y`, status: "active", created_at_ms: 1795400000000, expires_at_ms: null,
  last_used_ms: null, revoked_at_ms: null, ...over,
});
const SHARED: SharedToken = { set: true, label: "legacy shared token (from TRAILBEAM_TOKEN)", works_for: ["ingest", "metrics"] };
const noop = () => {};
const row = (html: string, id: number | string) => {
  const at = html.indexOf(`data-token="${id}"`);
  assert.ok(at >= 0, `row ${id}`);
  return html.slice(at, html.indexOf("</tr>", at));
};
const button = (r: string, label: string) => new RegExp(`<button[^>]*>${label}</button>`).test(r);

test("the legacy shared token is listed by its name, never its value, and can't be revoked here", () => {
  const html = renderToStaticMarkup(createElement(TokensTable, { kind: "ingest", tokens: [ingest(1)], shared: SHARED, onRevoke: noop }));
  const s = row(html, "shared");
  assert.ok(s.includes("legacy shared token (from TRAILBEAM_TOKEN)"));
  assert.ok(!button(s, "Revoke"), "set in the environment: no revoke");
  assert.match(sharedSentence(SHARED), /sending spans and \/metrics/);
  assert.match(sharedSentence(SHARED), /remove TRAILBEAM_TOKEN/);
  assert.ok(button(row(html, 1), "Revoke"));
  const unset = renderToStaticMarkup(createElement(TokensTable, { kind: "ingest", tokens: [], shared: { ...SHARED, set: false }, onRevoke: noop }));
  assert.ok(!unset.includes('data-token="shared"') && unset.includes("No ingest tokens yet."));
  assert.match(sharedSentence({ ...SHARED, set: false }), /isn't set/);
});

test("a token shows only its first 8 characters; revoked and expired ones stay, without a revoke", () => {
  const html = renderToStaticMarkup(createElement(TokensTable, {
    kind: "personal", onRevoke: noop,
    tokens: [personal(1), personal(2, { status: "revoked", revoked_at_ms: 1795500000000 }), personal(3, { status: "expired", expires_at_ms: 1795450000000 })],
  }));
  assert.equal(shownPrefix("tbp_cd1y"), "tbp_cd1y");
  assert.ok(row(html, 1).includes("tbp_cd1y"));
  assert.ok(button(row(html, 1), "Revoke"));
  for (const id of [2, 3]) assert.ok(!button(row(html, id), "Revoke"), `row ${id}`);
  assert.ok(row(html, 2).includes("Revoked") && row(html, 2).includes("2026-11-"));
  assert.ok(row(html, 3).includes("Expired"));
  assert.ok(row(html, 1).includes("never"), "no expiry, never used");
  assert.ok(html.includes("Expires") && !html.includes("by ada"), "personal tokens have an expiry column, no creator");
  const ing = renderToStaticMarkup(createElement(TokensTable, { kind: "ingest", tokens: [ingest(1)], onRevoke: noop }));
  assert.ok(!ing.includes("Expires") && ing.includes("by ada@example.com"));
});

test("a token being revoked has its button off", () => {
  const html = renderToStaticMarkup(createElement(TokensTable, { kind: "ingest", tokens: [ingest(1), ingest(2)], pending: 1, onRevoke: noop }));
  assert.match(row(html, 1), /<button[^>]*disabled=""[^>]*>Revoke<\/button>/);
  assert.doesNotMatch(row(html, 2), /disabled=""/);
});

test("the first ingest token on an open server needs its own confirmation; otherwise none", () => {
  const open = renderToStaticMarkup(createElement(CreateToken, { kind: "ingest", ingestOpen: true, onCreate: async () => {} }));
  assert.match(open, /<input[^>]*type="checkbox" required=""[^>]*name="close_open_ingest"/);
  assert.ok(open.includes(OPEN_INGEST_CONFIRM));
  const closed = renderToStaticMarkup(createElement(CreateToken, { kind: "ingest", onCreate: async () => {} }));
  assert.ok(!closed.includes("close_open_ingest") && !closed.includes("expires"), "ingest tokens don't expire");
});

test("a personal token works for 90 days unless asked, or until revoked", () => {
  const html = renderToStaticMarkup(createElement(CreateToken, { kind: "personal", onCreate: async () => {} }));
  assert.equal(DEFAULT_EXPIRY, 90);
  assert.match(html, /<option value="90" selected="">/);
  assert.match(html, /<option value="">Until revoked<\/option>/);
  assert.deepEqual(EXPIRY.map(([d]) => d), [30, 90, 365, null]);
  assert.ok(EXPIRY.every(([d]) => d === null || (d >= 1 && d <= 3650)), "the API takes 1 to 3650 days");
});

test("a new token is shown once, with how to use it", () => {
  const html = renderToStaticMarkup(createElement(OneTimeToken, { kind: "ingest", name: "orders", secret: "tbi_" + "a".repeat(40), onDismiss: noop }));
  assert.ok(html.includes(`value="tbi_${"a".repeat(40)}"`) && button(html, "Copy token"));
  assert.match(secretSentence("ingest", "orders"), /OTEL_EXPORTER_OTLP_HEADERS/);
  assert.match(secretSentence("personal", "laptop"), /Authorization: Bearer/);
  for (const k of ["ingest", "personal"] as const) assert.match(secretSentence(k, "x"), /isn't shown again/);
  assert.match(revokeSentence("ingest", { name: "orders" }), /spans are lost/);
  assert.match(revokeSentence("personal", { name: "laptop" }), /20 seconds/);
});

test("the tokens views follow the design rules: no native title, sans labels, mono only for values", () => {
  const html = renderToStaticMarkup(createElement(TokensTable, { kind: "personal", tokens: [personal(1, { last_used_ms: 1795400000000 })], onRevoke: noop }))
    + renderToStaticMarkup(createElement(CreateToken, { kind: "personal", onCreate: async () => {} }));
  assert.ok(!html.includes("title="), "hover notes are the app's Tooltip");
  for (const th of html.matchAll(/<th[^>]*>/g)) assert.doesNotMatch(th[0], /font-mono/, th[0]);
  for (const label of html.matchAll(/<label[^>]*>/g)) assert.doesNotMatch(label[0], /font-mono/, label[0]);
  assert.match(row(html, 1), /font-mono[^"]*">tbp_cd1y</, "the prefix is a value");
  assert.match(row(html, 1), /<span class="text-sm text-ink-3">never<\/span>/, "never is a word, not a value");
});
