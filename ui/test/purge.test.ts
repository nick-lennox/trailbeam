/*
  Contract 3.3, section 18: what a recorded PurgeQueue call shows, on the timeline and in the lists, for timelines
  Trailbeam really returned (fixtures/purge-timelines.json, from make-fixtures.py). Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { TimelineView } from "../src/timeline/Timeline.tsx";
import { answerFor } from "../src/summary.ts";
import { MessagesTable, POSSIBLY_PURGED_TITLE, rowFacts, stateText, type MessageSummary } from "../src/messages/Messages.tsx";
import type { Timeline } from "../src/timeline/types.ts";

const { timelines } = JSON.parse(readFileSync(new URL("./fixtures/purge-timelines.json", import.meta.url), "utf8")) as { timelines: Timeline[] };
const byId = (id: string) => timelines.find((t) => t.message_id === id)!;
const render = (t: Timeline) => renderToStaticMarkup(createElement(TimelineView, { t, answer: answerFor(t) }));
const text = (html: string) => html.replace(/<[^>]+>/g, " ").replace(/&#x27;/g, "'").replace(/\s+/g, " ");
/** A list row from a timeline: the fields a MessageSummary shares with it. */
const row = (t: Timeline): MessageSummary => ({ ...t, consumers: [] }) as unknown as MessageSummary;

test("a purged message says so, with the purge's time, and the purge is a recorded step", () => {
  const t = byId("purge-sent-before");
  assert.equal(t.state, "purged");
  const html = text(render(t));
  assert.match(answerFor(t), /removed by a PurgeQueue call on orders\. No delete was seen\./);
  assert.match(html, /Purged \(observed: PurgeQueue at \d\d:\d\d:\d\d\.\d{3}\)/);
  assert.match(html, / Queue purged /);
  assert.match(html, /PurgeQueue · SQS accepted/);
  assert.match(html, /deletes every message in the queue/);
  assert.match(stateText(row(t)), /^purged \(observed: PurgeQueue at \d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{3}\)$/);
});

test("a message in the purge's window keeps its state and is marked possibly purged", () => {
  const t = byId("purge-sent-during");
  assert.equal(t.state, "not_deleted");
  const html = text(render(t));
  assert.match(html, /No delete seen possibly purged \(PurgeQueue at /);
  assert.match(html, /Queue purged \(may have removed it\)/);
  assert.match(answerFor(t), /not received yet/);
  assert.match(rowFacts(row(t))[0], /^no delete seen, possibly purged \(PurgeQueue at /);
  const table = renderToStaticMarkup(createElement(MessagesTable, { messages: [row(t)], href: (id) => `/m/${id}` }));
  assert.ok(table.includes('data-purge="possibly_purged"') && table.includes(POSSIBLY_PURGED_TITLE));
});

test("a message received after the window is not affected", () => {
  const t = byId("purge-received-after");
  assert.equal(t.purge, undefined);
  assert.ok(!t.events.some((e) => e.type === "queue_purged"));
  assert.ok(!/PurgeQueue|Queue purged|possibly purged/.test(text(render(t))));
  assert.equal(stateText(row(t)), "no delete seen");
});
