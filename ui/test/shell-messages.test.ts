/*
  QV-582 part 2: the open-source shell's Messages list (`/`, shell/messages.tsx) on the shared MessagesView. The URL's
  range and filters, links from before the shared list, and the ask to GET /api/messages. Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { createElement as h } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { MessagesView, listQuery, queryOfParams } from "../src/explorer/index.ts";
import { anyTime, custom, preset } from "../src/model/index.ts";
import { bodyNote, MESSAGES_LANGUAGE, messagesApiQuery, messagesHref, messagesTime, searchTime } from "../shell/messages.tsx";

const NOW = Date.UTC(2026, 10, 27, 12, 0, 0);
const H = 3_600_000;
const text = (html: string) => html.replace(/<[^>]+>/g, "").replace(/&#x27;|&rsquo;/g, "'");

test("the URL's range: any time when it names none, a preset, a typed one, or a fixed from and to", () => {
  assert.deepEqual(messagesTime("", NOW), anyTime(NOW));
  assert.deepEqual(messagesTime("?queue=orders", NOW), anyTime(NOW));
  assert.deepEqual(messagesTime("?range=4h", NOW), preset("4h", NOW));
  assert.equal(messagesTime("?range=45m", NOW).from, NOW - 45 * 60_000);
  assert.deepEqual(messagesTime("?from=2026-11-27T09:00:00Z&to=2026-11-27T10:00:00Z", NOW), custom(NOW - 3 * H, NOW - 2 * H));
  assert.deepEqual(messagesTime("?range=nonsense", NOW), anyTime(NOW));
});

test("links from before the shared list open what they named", () => {
  assert.deepEqual(messagesTime("?range=24h", NOW), preset("1d", NOW));
  assert.deepEqual(messagesTime("?range=1h", NOW), preset("1h", NOW));
  // from_ms and to_ms included both ends; either one alone was allowed
  assert.deepEqual(messagesTime(`?from_ms=${NOW - H}&to_ms=${NOW - 1}`, NOW), custom(NOW - H, NOW));
  assert.deepEqual(messagesTime(`?from_ms=${NOW - H}`, NOW), custom(NOW - H, NOW));
  assert.deepEqual(messagesTime(`?to_ms=${NOW - H - 1}`, NOW), custom(0, NOW - H));
  // a body search's "last seen within"
  assert.deepEqual(messagesTime("?body=ord-1003&since_s=604800", NOW), preset("7d", NOW));
  // the old ?body= of bare words reads as body:<text>, and is sent as such (the server reads both alike)
  const q = queryOfParams({ body: "ord-1003" }, MESSAGES_LANGUAGE);
  assert.equal(q, "body:ord-1003");
  assert.deepEqual(listQuery(q, MESSAGES_LANGUAGE).params, { body: "body:ord-1003" });
});

test("the ask: the filters, the range's from_ms and to_ms (both included), the page; any time sends no window", () => {
  assert.deepEqual(messagesApiQuery({ queue: "orders", tenant: "acme", version: "1.4.3" }, anyTime(NOW)), { queue: "orders", tenant: "acme", version: "1.4.3" });
  assert.deepEqual(messagesApiQuery({ queue: "orders" }, preset("1h", NOW), { limit: "50", cursor: "abc" }),
    { queue: "orders", from_ms: String(NOW - H), to_ms: String(NOW - 1), limit: "50", cursor: "abc" });
  // a body search reads one window: a day when the range is any time
  assert.deepEqual(searchTime(anyTime(NOW), true), preset("1d", NOW));
  assert.deepEqual(searchTime(anyTime(NOW), false), anyTime(NOW));
  assert.deepEqual(searchTime(preset("4h", NOW), true), preset("4h", NOW));
});

test("the page's link: the filters and the range, any time naming none", () => {
  assert.equal(messagesHref({}, anyTime(NOW)), "/");
  assert.equal(messagesHref({ queue: "orders", body: "body.a:1" }, preset("1d", NOW), { refresh: "15" }), "/?queue=orders&body=body.a%3A1&range=1d&refresh=15");
  assert.equal(messagesHref({}, custom(NOW - H, NOW), { cursor: "c" }), "/?from=2026-11-27T11%3A00%3A00.000Z&to=2026-11-27T12%3A00%3A00.000Z&cursor=c");
  // and it reads back as the same range
  assert.deepEqual(messagesTime(messagesHref({}, custom(NOW - H, NOW)).slice(1), NOW), custom(NOW - H, NOW));
});

test("the query bar takes what GET /api/messages takes, and says what it doesn't", () => {
  assert.equal(listQuery("state:dlq", MESSAGES_LANGUAGE).error, "There is no field called state. The list filters on queue, tenant, version or body.");
  assert.deepEqual(listQuery("22e15633-1b7c", MESSAGES_LANGUAGE).params, { q: "22e15633-1b7c" });
});

test("a body search says what it read, from the answer", () => {
  const t = text(renderToStaticMarkup(h("div", null, bodyNote({ stores_bodies: true, from_ms: NOW - 24 * H, to_ms: NOW, scanned: 5000, searched_from_ms: NOW - 2 * H, complete: false }, false))));
  assert.ok(t.includes("Read the bodies of 5,000 messages last seen between 2026-11-27 10:00:00 UTC and 2026-11-27 12:00:00 UTC. The next page reads older ones, back to 2026-11-26 12:00:00 UTC."), t);
  assert.ok(t.includes("TRAILBEAM_STORE_BODIES"));
  const none = text(renderToStaticMarkup(h("div", null, bodyNote({ stores_bodies: false, from_ms: 0, to_ms: NOW, scanned: 0, searched_from_ms: null, complete: true }, false))));
  assert.ok(none.includes("This server keeps no message bodies"));
});

test("an /api/messages answer drawn by the shared list: the open-source summary needs no times, so those columns are left out", () => {
  const html = renderToStaticMarkup(h(MessagesView, {
    query: "queue:orders", onQuery: () => {}, lang: MESSAGES_LANGUAGE, messageHref: (id: string) => `/message?id=${encodeURIComponent(id)}`, page: true,
    messages: [{
      message_id: "m-legacy", state: "deleted", source_queue: "orders", current_queue: "orders", in_dead_letter_queue: false, dead_letter_queue_move: null,
      deliveries: 1, successful_deliveries: 1, consumers: [{ service_name: "legacy-svc", client_id: null, service_version: null }],
      first_seen: "2026-11-27T11:00:00Z", first_seen_clock: "host", last_seen: "2026-11-27T11:00:01Z",
    }],
  }));
  assert.ok(html.includes('href="/message?id=m-legacy"'));
  assert.ok(!text(html).includes("In queue") && !text(html).includes("End to end"));
});
