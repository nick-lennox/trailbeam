/*
  QV-586: Duplicates (`/duplicates`, shell/messages.tsx) on the shared Messages list. The query bar's queue: and reason:,
  the Why column, the ask to GET /api/duplicates, and links from the list before it. Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { createElement as h } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { MessagesView, WHY_COLUMN, listQuery, queryOfParams, type MessageRow } from "../src/explorer/index.ts";
import { anyTime, custom, preset } from "../src/model/index.ts";
import { DUPLICATES_PAGE, DuplicatesHeader, MESSAGES_PAGE, listOf, messagesApiQuery, messagesHref, messagesTime } from "../shell/messages.tsx";

const NOW = Date.UTC(2026, 11, 1, 12, 0, 0);
const H = 3_600_000;
const text = (html: string) => html.replace(/<[^>]+>/g, "").replace(/&#x27;|&rsquo;/g, "'").replace(/&quot;/g, '"');
const L = DUPLICATES_PAGE.lang;

const row = (over: Partial<MessageRow> = {}): MessageRow => ({
  message_id: "m-1", source_queue: "orders", current_queue: "orders", first_seen: "2026-12-01T11:00:00.000Z", last_seen: "2026-12-01T11:05:00.000Z",
  deliveries: 3, successful_deliveries: 2, consumers: [], reasons: ["received_more_than_once", "succeeded_more_than_once"], ...over,
} as MessageRow);

test("links from the list before open the same rows: reason, queue, range, from_ms, to_ms", () => {
  const params = { reason: "succeeded_more_than_once", queue: "orders", tenant: "acme", limit: "20" };
  // the filters the duplicates API takes, and no others
  assert.deepEqual(listOf(params, DUPLICATES_PAGE), { queue: "orders", reason: "succeeded_more_than_once" });
  assert.equal(queryOfParams(listOf(params, DUPLICATES_PAGE), L), "queue:orders reason:succeeded_more_than_once");
  // the old range control's presets, and its fixed window (both ends included)
  assert.deepEqual(messagesTime("?range=24h", NOW), preset("1d", NOW));
  assert.deepEqual(messagesTime("?range=7d", NOW), preset("7d", NOW));
  assert.deepEqual(messagesTime(`?reason=received_more_than_once&from_ms=${NOW - 2 * H}&to_ms=${NOW - H - 1}`, NOW), custom(NOW - 2 * H, NOW - H));
  // and the ask is what the old page sent
  assert.deepEqual(messagesApiQuery(listOf(params, DUPLICATES_PAGE), custom(NOW - 2 * H, NOW - H), { limit: "20", cursor: "c" }, DUPLICATES_PAGE),
    { queue: "orders", reason: "succeeded_more_than_once", from_ms: String(NOW - 2 * H), to_ms: String(NOW - H - 1), limit: "20", cursor: "c" });
  assert.deepEqual(messagesApiQuery({ reason: "received_more_than_once" }, anyTime(NOW), {}, DUPLICATES_PAGE), { reason: "received_more_than_once" });
});

test("the page's links stay on /duplicates", () => {
  assert.equal(messagesHref({}, anyTime(NOW), {}, DUPLICATES_PAGE), "/duplicates");
  assert.equal(messagesHref({ reason: "received_more_than_once", queue: "orders" }, preset("1h", NOW), { refresh: "15" }, DUPLICATES_PAGE),
    "/duplicates?queue=orders&reason=received_more_than_once&range=1h&refresh=15");
  // Messages' links don't take a reason
  assert.equal(messagesHref({ reason: "received_more_than_once" } as never, anyTime(NOW)), "/");
});

test("the query bar takes queue: and reason:, and says what it doesn't", () => {
  assert.deepEqual(listQuery("queue:orders reason:Received_More_Than_Once", L).params, { queue: "orders", reason: "received_more_than_once" });
  assert.equal(listQuery("reason:twice", L).error, "reason takes received_more_than_once or succeeded_more_than_once.");
  assert.equal(listQuery("tenant:acme", L).error, "There is no field called tenant. The list filters on queue or reason.");
  assert.equal(listQuery("body:x", L).error, "This list can't search message bodies.");
  assert.equal(listQuery("reason:received_more_than_once reason:succeeded_more_than_once", L).error, "The list takes one reason at a time.");
  // a bare word is a message ID, as on Messages
  assert.deepEqual(listQuery("m-1", L).params, { q: "m-1" });
  // Messages' bar doesn't take a reason
  assert.match(listQuery("reason:received_more_than_once", MESSAGES_PAGE.lang).error!, /no field called reason/);
});

test("the Why column says each reason with its count, in the table and the export", () => {
  assert.equal(WHY_COLUMN.text(row()), "received 3 times, handled 2 times");
  assert.equal(WHY_COLUMN.text(row({ reasons: ["received_more_than_once"] })), "received 3 times");
  assert.equal(text(renderToStaticMarkup(h("div", null, WHY_COLUMN.cell(row())))), "received 3 times, handled 2 times");
  const html = renderToStaticMarkup(h(MessagesView, {
    messages: [row()], query: "reason:received_more_than_once", onQuery: () => {}, lang: L, messageHref: (id: string) => `/message?id=${id}`, extra: DUPLICATES_PAGE.extra,
  }));
  assert.match(html, /<th[^>]*>Why<\/th>/);
  assert.match(text(html), /received 3 times, handled 2 times/);
  // every figure the old table showed: receives, handlings, first and last seen
  for (const head of ["Message ID", "State", "Queue", "Consumers", "Receives", "Handled", "First seen", "Last seen"]) assert.match(text(html), new RegExp(head));
});

test("the heading and intro, and the empty states", () => {
  const head = text(renderToStaticMarkup(h(DuplicatesHeader)));
  assert.match(head, /^Duplicates/);
  assert.match(head, /ChangeMessageVisibility/);
  assert.match(head, /visibility timeout expired/);
  const e = DUPLICATES_PAGE.empty({ reason: "received_more_than_once", queue: "orders" }, { empty: false })!;
  assert.equal(e.title, 'No duplicates for "received more than once".');
  assert.deepEqual(e.link, { href: "/duplicates?queue=orders", label: "Show every reason" });
  assert.equal(DUPLICATES_PAGE.empty({}, { empty: true })!.title, "No messages yet.");
  assert.equal(DUPLICATES_PAGE.empty({}, { empty: false })!.title, "No duplicates.");
});
