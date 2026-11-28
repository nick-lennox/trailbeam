/*
  QV-367: the Needs attention views (src/causes), drawn from the groups in fixtures/causes-parity.json, the
  ones both Trailbeam's server (causes.py) and the hosted app (lib/app/causes.ts) make of its timelines.
  Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { CauseList, CauseMessagesTable, DeadLetterQueues } from "../src/causes/NeedsAttention.tsx";
import { causesNote, dlqSplit, tenantsText, trendOf, versionsText, whenFirst, whenLast } from "../src/causes/text.ts";
import type { CauseGroup, CauseMessage, DlqGroup } from "../src/causes/types.ts";

const fixture = JSON.parse(readFileSync(new URL("./fixtures/causes-parity.json", import.meta.url), "utf8")) as {
  nowMs: number; expected: { groups: CauseGroup[]; trends: Record<string, number[]> };
};
const { nowMs, expected: { groups, trends } } = fixture;
const html = (el: ReturnType<typeof createElement>) => renderToStaticMarkup(el);
const text = (s: string) => s.replace(/<[^>]+>/g, " ").replace(/&#x27;/g, "'").replace(/\s+/g, " ");

test("trendOf gives the fixture's trends (make-causes-fixture.py: the hour before nowMs)", () => {
  for (const g of groups) assert.deepEqual(trendOf(g.times, nowMs - 3_600_000, nowMs), trends[g.id], g.id);
});

test("the words: dead-letter split, first and last seen, versions, tenants", () => {
  assert.equal(dlqSplit({ counts: { dead_lettered: 0, stuck: 2, filtered: 0 }, dlqInferred: 0 }), "");
  assert.equal(dlqSplit({ counts: { dead_lettered: 1, stuck: 2, filtered: 0 }, dlqInferred: 0 }), "1 in the DLQ, 2 still in the queue");
  assert.equal(dlqSplit({ counts: { dead_lettered: 1, stuck: 0, filtered: 0 }, dlqInferred: 1 }), "1 in the DLQ (inferred from maxReceiveCount)");
  assert.equal(dlqSplit({ counts: { dead_lettered: 3, stuck: 0, filtered: 0 }, dlqInferred: 1 }), "3 in the DLQ, 1 inferred from maxReceiveCount");
  assert.equal(whenFirst(Date.UTC(2026, 10, 24, 9, 5), Date.UTC(2026, 10, 24, 12)), "09:05 UTC");
  assert.equal(whenFirst(Date.UTC(2026, 10, 3, 9, 5), Date.UTC(2026, 10, 24, 12)), "3 Nov");
  assert.equal(whenFirst(NaN, 0), "unknown");
  assert.equal(whenLast(nowMs - 30_000, nowMs), "now");
  assert.equal(whenLast(nowMs - 300_000, nowMs), "5m ago");
  assert.equal(whenLast(nowMs - 3 * 86_400_000, nowMs), "3d ago");
  assert.equal(versionsText([{ version: "1" }, { version: "2" }, { version: "3" }, { version: "4" }]), "seen in 2, 3, 4 and 1 earlier");
  assert.equal(tenantsText(["acme", "globex", "hooli"]), "acme +2");
  assert.match(causesNote({ capped: true, readLimit: 5000 }), /At least 5,000 messages are unfinished\. Causes are from the 5,000 oldest\.$/);
  assert.doesNotMatch(causesNote(), /CloudTrail/);
  assert.match(causesNote({ aws: true }), /CloudTrail/);
});

test("CauseList: one row per group, in order, each linking to its cause, with count and outcome", () => {
  const out = html(createElement(CauseList, { groups, nowMs, href: (g) => `/stuck?cause=${g.id}`, messageHref: (id) => `/message?id=${id}` }));
  const ids = [...out.matchAll(/data-cause="([0-9a-f]{12})"/g)].map((m) => m[1]);
  assert.deepEqual(ids, groups.map((g) => g.id));
  for (const g of groups) assert.ok(out.includes(`href="/stuck?cause=${g.id}"`), g.id);
  const first = groups[0];
  const row = text(out.split(`data-cause="${first.id}"`)[1].split("data-cause=")[0]);
  assert.match(row, /Dead-lettered/);
  assert.ok(row.includes(first.label));
  assert.ok(row.includes(`${dlqSplit(first)}.`), row);
  assert.match(row, new RegExp(` ${first.count} messages `));
  // the newest error text and the link to that message, in the expanded part
  assert.ok(row.includes(first.sampleError!));
  assert.ok(out.includes(`href="/message?id=${first.sampleMid}"`));
});

test("CauseList: the picked row is marked, slots sit beside the link, a firing rule marks the edge", () => {
  const g = groups[1];
  const out = html(createElement(CauseList, {
    groups: [g], nowMs, picked: g.id, href: () => "/x", messageHref: () => "/m",
    actions: (c) => createElement("button", { type: "button" }, `Alert on ${c.id}`), firing: () => true,
    outcome: (o) => createElement("b", null, `pill:${o}`),
  }));
  assert.match(out, /aria-current="true"/);
  assert.match(out, /border-l-critical/);
  assert.match(out, /<b>pill:dead_lettered<\/b>/);
  // the button is not inside the row's link (no button inside a link)
  const link = out.slice(out.indexOf('<a href="/x"'), out.indexOf("</a>"));
  assert.doesNotMatch(link, /<button/);
  assert.match(out, /<\/a><div[^>]*><button type="button">Alert on /);
});

test("CauseList: the tag, note and quiet slots (the hosted app's triage)", () => {
  const [a, b] = groups;
  const out = html(createElement(CauseList, {
    groups: [a, b], nowMs, href: () => "/x", messageHref: () => "/m",
    tag: (g) => (g.id === a.id ? createElement("span", { "data-tag": "" }, "Resolved") : null),
    note: (g) => (g.id === a.id ? "2 notes." : null),
    quiet: (g) => g.id === a.id,
  }));
  const rows = out.split("<li").slice(1);
  assert.match(rows[0], /data-tag="">Resolved</);
  assert.match(rows[0], /2 notes\./);
  assert.match(rows[0], /text-ink-2" title=/);
  assert.doesNotMatch(rows[0], /role="img"/);
  assert.doesNotMatch(rows[1], /data-tag|notes/);
  assert.match(rows[1], /role="img"/);
});

test("CauseList: the trend is the past 24 hours, as numbers for a screen reader", () => {
  const g: CauseGroup = { ...groups[0], times: [nowMs - 1000, nowMs - 2000, nowMs - 23 * 3_600_000, nowMs - 30 * 3_600_000] };
  const out = html(createElement(CauseList, { groups: [g], nowMs, href: () => "/x", messageHref: () => "/m" }));
  assert.match(out, /aria-label="Messages over the past 24 hours, in 9 parts of 2.7 h, oldest first: 1, 0, 0, 0, 0, 0, 0, 0, 2"/);
});

const msg = (id: string, cause: string, extra: Partial<CauseMessage> = {}): CauseMessage => ({
  message_id: id, state: "not_deleted", source_queue: "orders", current_queue: "orders", in_dead_letter_queue: false, dead_letter_queue_move: null,
  deliveries: 2, successful_deliveries: 0, consumers: [], first_seen: null, first_seen_clock: "host", last_seen: null, age_s: 600, age_across_clocks: false, cause, ...extra,
});

test("CauseMessagesTable: each message with its cause's label, linking to the cause; the picked cause says Above", () => {
  const [a, b] = groups;
  const ms = [msg("m-1", a.id), msg("m-2", b.id, { current_queue: "orders-dlq", in_dead_letter_queue: true, age_across_clocks: true })];
  const props = { messages: ms, groups, href: (id: string) => `/message?id=${id}`, causeHref: (g: CauseGroup) => `/stuck?cause=${g.id}` };
  const out = html(createElement(CauseMessagesTable, props));
  assert.ok(out.includes(`href="/stuck?cause=${a.id}"`));
  assert.ok(text(out).includes(b.label));
  assert.ok(out.includes('href="/message?id=m-2"'));
  assert.match(text(out), /10 min SQS clock/);
  const picked = text(html(createElement(CauseMessagesTable, { ...props, picked: a.id })));
  assert.match(picked, /Above/);
});

test("DeadLetterQueues: one row per queue, with its count, its source queues and an action slot", () => {
  const dlqs: DlqGroup[] = [
    { key: "1/r/orders-dlq", name: "orders-dlq", account: "1", region: "r", messageIds: ["a", "b"], sources: ["orders"], newest: nowMs - 120_000 },
    { key: "1/r/pay-dlq", name: "pay-dlq", account: null, region: null, messageIds: ["c"], sources: ["pay", "refunds"], newest: null },
  ];
  const out = text(html(createElement(DeadLetterQueues, { groups: dlqs, nowMs, action: (g) => `Redrive ${g.name}` })));
  assert.match(out, /orders-dlq 2 messages from orders last seen 2m ago Redrive orders-dlq/);
  assert.match(out, /pay-dlq 1 message from pay, refunds Redrive pay-dlq/);
});
