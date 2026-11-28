/*
  QV-582: the Messages list built from the Event explorer's parts (src/messages/MessagesView.tsx): the query bar over a
  list API's filters (listQuery.ts), the explorer's results bar with Columns and Export, its column list, and its rows
  with the level bar. Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { createElement as h } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import {
  DEFAULT_MESSAGE_COLUMNS, MESSAGE_COLUMNS, MessagesView, listQuery, messageLevel, messageQueryLanguage, queryOfParams, traceIdOf, type MessageRow,
} from "../src/explorer/index.ts";

const OSS = messageQueryLanguage(["queue", "tenant", "version", "body"]);
const HOSTED = messageQueryLanguage(["queue", "tenant", "state", "version", "trace"]);
const TRACE = "4bf92f3577b34da6a3ce929d0e0e4736";

const row = (over: Partial<MessageRow> = {}): MessageRow => ({
  message_id: "22e15633-1b7c-4d6e-9a1f-0c3b8e29707e", state: "deleted", source_queue: "orders", current_queue: "orders", in_dead_letter_queue: false,
  dead_letter_queue_move: null, deliveries: 1, successful_deliveries: 1, consumers: [{ service_name: "orders-worker", client_id: "c1" }],
  first_seen: "2026-11-27T11:59:00.123456Z", first_seen_clock: "host", last_seen: "2026-11-27T11:59:01Z", ...over,
});
const draw = (messages: MessageRow[], over: Record<string, unknown> = {}) => renderToStaticMarkup(h(MessagesView, {
  messages, query: "", onQuery: () => {}, lang: OSS, messageHref: (id: string) => `/message?id=${id}`, ...over,
}));
const text = (html: string) => html.replace(/<[^>]+>/g, "").replace(/&#x27;|&rsquo;/g, "'").replace(/&quot;/g, '"').replace(/&amp;/g, "&");
const headers = (html: string) => [...(html.match(/<thead[\s\S]*?<\/thead>/)?.[0] ?? "").matchAll(/<th(?:\s[^>]*)?>(?:<span[^>]*>)?([^<]*)/g)].map((m) => m[1]);

test("a query becomes the list API's parameters, with the names both list pages used before", () => {
  assert.deepEqual(listQuery("queue:orders tenant:acme version:1.4.3", OSS), { params: { queue: "orders", tenant: "acme", version: "1.4.3" } });
  assert.deepEqual(listQuery("queue:orders body.order_id:ord-1003 -body:test", OSS).params, { queue: "orders", body: "body.order_id:ord-1003 -body:test" });
  assert.deepEqual(listQuery(`state:DLQ trace:${TRACE.toUpperCase()}`, HOSTED).params, { state: "dlq", trace: TRACE });
  // words with no field are looked up by the host: a message ID, a trace ID, a tenant
  assert.deepEqual(listQuery("m-1 queue:orders", HOSTED).params, { queue: "orders", q: "m-1" });
  // a term still being typed asks for nothing
  assert.deepEqual(listQuery("queue:", HOSTED).params, {});
  assert.equal(traceIdOf(`00-${TRACE}-00f067aa0ba902b7-01`), TRACE);
  assert.equal(traceIdOf("0".repeat(32)), null);
});

test("what a list API can't take is a mistake the bar says, before anything is asked", () => {
  const err = (q: string, lang = OSS) => listQuery(q, lang).error;
  assert.equal(err("state:dlq"), "There is no field called state. The list filters on queue, tenant, version or body.");
  assert.equal(err("body.order_id:1", HOSTED), "This list can't search message bodies.");
  assert.equal(err("-queue:orders"), "The list can't leave out a queue. Remove the minus.");
  assert.equal(err("queue:a queue:b"), "The list takes one queue at a time.");
  assert.equal(err("queue:a queue:a"), undefined);
  assert.equal(err("state:gone", HOSTED), "state takes open, processing, deleted or dlq.");
  assert.equal(err("trace:abc", HOSTED), "trace takes a trace ID: 32 hex digits.");
  assert.equal(err("-m-1", HOSTED), "The list can't leave out words. Remove the minus.");
  // the explorer's aliases
  assert.deepEqual(listQuery("service.version:2 trace_id:" + TRACE, HOSTED).params, { version: "2", trace: TRACE });
});

test("a link's parameters read back as the query: an old ?body= of bare words is body:<text>", () => {
  assert.equal(queryOfParams({ queue: "orders", body: 'ord-1003 "call me" -x body.a:1', tenant: "acme" }, OSS), 'queue:orders tenant:acme body:ord-1003 body:"call me" -body:x body.a:1');
  assert.deepEqual(listQuery(queryOfParams({ queue: "orders", body: "ord-1003" }, OSS), OSS).params, { queue: "orders", body: "body:ord-1003" });
  // fields the host's API doesn't take are not read from its URL
  assert.equal(queryOfParams({ state: "dlq", body: "x", q: "m-1" }, HOSTED), "state:dlq m-1");
  assert.equal(queryOfParams({ queue: "my queue" }, OSS), 'queue:"my queue"');
});

test("the rows are the explorer's: Message ID links to the timeline, a column no row fills is left out", () => {
  const html = draw([row()]);
  assert.deepEqual(headers(html), ["Message ID", "State", "Queue", "Consumers", "Receives", "Handled", "First seen", "Last seen"]);
  assert.ok(html.includes('href="/message?id=22e15633-1b7c-4d6e-9a1f-0c3b8e29707e"'));
  const t = text(html);
  for (const s of ["22e15633…9707e", "Deleted", "orders-worker", "2026-11-27 11:59:00.123", "1 message, most recently active first"]) assert.ok(t.includes(s), `missing ${s}: ${t}`);
  assert.match(html, /class="ev-row group"/);
  assert.ok(html.includes("data-results-bar") && t.includes("Columns") && t.includes("Export"));
  // the hosted list measures the times, so it has In queue and End to end
  const timed = draw([row({ in_queue_ms: 1200, end_to_end_ms: 2500, clock: "host" }), row({ message_id: "m-2", state: "not_deleted", in_queue_ms: 40_000, waiting: true, end_to_end_ms: null })]);
  assert.deepEqual(headers(timed), ["Message ID", "State", "Queue", "Consumers", "Receives", "Handled", "In queue", "End to end", "First seen", "Last seen"]);
  const tt = text(timed);
  for (const s of ["1.2s", "2.5s", "40.0s so far", "Not finished"]) assert.ok(tt.includes(s), `missing ${s}`);
  // no native title anywhere, and the rules on copy
  assert.ok(!/\stitle="/.test(timed), "hover notes are Tooltips");
  assert.ok(!/—|→| · /.test(tt), "no em dash, arrow or a · b");
});

test("the level bar: red in a dead-letter queue, amber when received more than once or due on the DLQ", () => {
  assert.equal(messageLevel(row({ in_dead_letter_queue: true, current_queue: "orders-dlq" })).level, "error");
  assert.equal(messageLevel(row({ deliveries: 3 })).level, "warn");
  assert.equal(messageLevel(row({ dead_letter_queue_move: "not_seen_on_dlq_yet" })).word, "Due on the dead-letter queue");
  assert.equal(messageLevel(row()).level, "info");
  const html = draw([row({ in_dead_letter_queue: true, current_queue: "orders-dlq", deliveries: 3, successful_deliveries: 0 })]);
  assert.match(html, /data-level="error"/);
  assert.match(html, /--bar:var\(--color-critical\)/);
  const t = text(html);
  assert.ok(t.includes("In the DLQ") && t.includes("orders-dlq (dead-letter queue of orders)") && t.includes("In a dead-letter queue"));
  assert.match(html, /font-medium text-warning-text">3</, "a message received more than once is marked");
});

test("nothing the open-source list shows is lost: SQS clock, purge, the DLQ move not seen yet, consumers none yet", () => {
  const t = text(draw([row({ first_seen_clock: "sqs", state: "not_deleted", consumers: [], deliveries: 4, successful_deliveries: 0, dead_letter_queue_move: "not_seen_on_dlq_yet",
    purge: { status: "possibly_purged", queue: "orders", start: "2026-11-27T12:00:09.000Z", end: "2026-11-27T12:00:09.100Z", window_end: "2026-11-27T12:01:09.100Z", span: { trace_id: TRACE, span_id: "00f067aa0ba902b7" } } })]));
  for (const s of ["SQS clock", "Possibly purged: a PurgeQueue call ran at 12:00:09.000 UTC", "reached maxReceiveCount; the move to the dead-letter queue isn't seen yet", "None yet"]) assert.ok(t.includes(s), `missing ${s}: ${t}`);
});

test("an empty page says the host's words; the host's own columns join on the right", () => {
  const empty = text(draw([], { empty: "No messages on orders." }));
  assert.ok(empty.includes("No messages on orders.") && empty.includes("0 messages"));
  const extra = [{ id: "why", label: "Why", cell: () => "received 2 times", text: () => "received 2 times" }];
  const html = draw([row()], { extra });
  assert.equal(headers(html).at(-1), "Why");
  assert.deepEqual(DEFAULT_MESSAGE_COLUMNS, MESSAGE_COLUMNS.map((c) => c.id));
});

test("the export writes the columns shown, every value in full", () => {
  let got = "";
  const m = row({ in_dead_letter_queue: true, current_queue: "orders-dlq", first_seen_clock: "sqs" });
  const cols = MESSAGE_COLUMNS.filter((c) => !c.fills || c.fills(m));
  const csvRow = cols.map((c) => c.text(m));
  assert.deepEqual(csvRow, ["22e15633-1b7c-4d6e-9a1f-0c3b8e29707e", "In the DLQ", "orders-dlq (dead-letter queue of orders)", "orders-worker", "1", "1", "2026-11-27 11:59:00.123 (SQS clock)", "2026-11-27 11:59:01"]);
  // the view's export goes through the host's download
  draw([m], { download: (_n: string, _t: string, d: BlobPart) => { got = String(d); } });
  assert.equal(got, "", "nothing is written until Export is chosen");
});

test("Handled has its own head note, and Receives' note is about receives only", () => {
  const tip = (id: string) => MESSAGE_COLUMNS.find((c) => c.id === id)?.tip;
  assert.equal(tip("handled"), "Receives that ended in a delete, or in a Lambda record that succeeded.");
  assert.doesNotMatch(String(tip("receives")), /Handled/);
});
