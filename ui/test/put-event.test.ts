/*
  A recorded EventBridge PutEvents entry (contract 3.6, section 25.3), drawn for timelines Trailbeam really returned
  (fixtures/put-event-timelines.json, from make-fixtures.py). Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { TimelineView } from "../src/timeline/Timeline.tsx";
import { answerFor } from "../src/summary.ts";
import type { Timeline } from "../src/timeline/types.ts";

const { timelines } = JSON.parse(readFileSync(new URL("./fixtures/put-event-timelines.json", import.meta.url), "utf8")) as { timelines: Timeline[] };
const byId = (id: string) => timelines.find((t) => t.message_id === id)!;
const render = (t: Timeline, href?: (id: string) => string) =>
  renderToStaticMarkup(createElement(TimelineView, { t, answer: answerFor(t), openSteps: ["e1"], messageHref: href }));
const text = (html: string) => html.replace(/<[^>]+>/g, " ").replace(/&#x27;/g, "'").replace(/\s+/g, " ");

test("a joined PutEvents entry is an observed first step, named by its bus, from the sender's process", () => {
  const t = byId("pe-orders");
  assert.deepEqual(t.events.map((e) => [e.type, e.inferred]), [["put_event", false], ["received", false], ["deleted", false]]);
  const html = text(render(t));
  assert.match(html, / e1 Put on EventBridge bus orders-bus /);
  assert.match(html, /Trailbeam pkg/);
  assert.doesNotMatch(html, /Arrived via an EventBridge rule/);
  assert.match(html, /orders-api/, "where: the process that called PutEvents");
  assert.match(html, /PutEvents · entry 0 · joined by event ID/);
  assert.match(html, /11710aed-b79e-4468-a20b-bb3c0c3b4860/);
  assert.match(html, /OrderPlaced/);
  assert.match(html, /Put on EventBridge bus orders-bus by PutEvents\./, "the server's reason");
  assert.ok(!/undefined|NaN/.test(html));
});

test("the entry lists every message it reached, linking the others", () => {
  const html = render(byId("pe-orders"), (id) => `/messages/${id}`);
  const reached = text(html.slice(html.indexOf("data-reached")));
  assert.match(reached, /Reached billing: pe-billing · orders: pe-orders \(this message\)/);
  assert.match(html, /href="\/messages\/pe-billing"/);
  assert.doesNotMatch(html, /href="\/messages\/pe-orders"/);
});

test("a sender-given Time is said to be the sender's, not EventBridge's", () => {
  const given = text(render(byId("pe-orders")));
  assert.match(given, /EventBridge delivery 400 ms \(from the sender's time\)/);
  assert.match(given, /Event time \(sender's\)/);
  assert.match(given, /measured from the sender's clock, not EventBridge's/);

  const own = text(render(byId("pe-default")));
  assert.match(own, / Put on EventBridge bus default /);
  assert.match(own, /EventBridge delivery 300 ms /);
  assert.doesNotMatch(own, /sender's/);
  assert.match(own, /Event time \(EventBridge\)/);
  assert.match(own, /both AWS clocks/);
});

test("a concurrent box names the entry by its short label", () => {
  for (const t of timelines) {
    const e = t.events.find((x) => x.type === "put_event")!;
    const grouped = { ...t, groups: [{ ...t.groups[0], group: 0, concurrent: true, observed_events: t.events.length, inferred_events: 0 }],
      events: t.events.map((x) => ({ ...x, group: 0 })) } as Timeline;
    assert.match(text(render(grouped)), new RegExp(` ${e.id} the PutEvents entry `));
  }
});
