/*
  QV-579 stage 2: the hosted Overview's views in @trailbeam/ui (src/now): a message's sequence diagram, the messages
  behind a part of the flow map, the queue lane and the "nothing yet" picture. They take their data as props and read
  no sample tables. Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync, readdirSync } from "node:fs";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import type { Ev } from "../src/model/events.ts";
import { MessageSequence, sequenceOf, type QueueFacts } from "../src/now/MessageSequence.tsx";
import { PartMessages, type PartMessage } from "../src/now/PartMessages.tsx";
import { QueueLane } from "../src/now/QueueLane.tsx";
import { Watching } from "../src/now/Watching.tsx";

const T0 = Date.UTC(2026, 10, 27, 14, 0, 0);
const ev = (event: Ev["event"], dt: number, o: Partial<Ev> = {}): Ev => ({
  key: `m1-${event}-${dt}`, ms: T0 + dt, event, status: event === "processing_error" ? "error" : "ok", queue: "orders", messageId: "m1-0000-aaaa",
  service: "orders-worker", producer: "checkout-api", consumer: "orders-worker", detail: "", source: "package", hasPage: true, live: true, ...o,
});
const delivered = [
  ev("send", 0, { sizeBytes: 812, service: "checkout-api" }),
  ev("receive", 140, { receiveCount: 1, waitMs: 140, attrs: { "batch.size": 3, "batch.index": 1 } }),
  ev("delete", 900, { durationMs: 700 }),
];
const READ: QueueFacts = { read: true, fifo: false, visibilityTimeoutS: 45, retentionS: 345_600, delayS: 0, maxReceiveCount: 3, dlq: "orders-dlq", triggers: null };
const html = (el: Parameters<typeof renderToStaticMarkup>[0]) => renderToStaticMarkup(el);

test("the views read no sample tables, no hosted modules and no workspace", () => {
  for (const f of readdirSync(new URL("../src/now/", import.meta.url))) {
    const src = readFileSync(new URL(`../src/now/${f}`, import.meta.url), "utf8");
    assert.doesNotMatch(src, /from "@\//, `${f} imports a hosted module`);
    assert.doesNotMatch(src, /lib\/(data|events|queuesim|inqueue|topology|partmessages)/, `${f} reads the sample`);
    assert.doesNotMatch(src, /useWs|useWorkspace/, `${f} reads the hosted workspace`);
  }
});

test("a delivered message's sequence: sent, received, deleted, with bars for each state", () => {
  const s = sequenceOf([...delivered].reverse(), READ);
  assert.deepEqual(s.steps.map((x) => x.name), ["Sent", "Receive #1", "Deleted"]);
  assert.deepEqual(s.parts.map((p) => p.role), ["Producer", "Queue", "Consumer"]);
  assert.deepEqual(s.bars.map((b) => `${b.kind}:${b.from}-${b.to}`), ["visible:0-1", "run:1-2", "hidden:1-2"]);
  const facts = Object.fromEntries(s.steps[1].facts.map(([k, v]) => [k, typeof v === "string" || typeof v === "number" ? v : html(v as never)]));
  assert.equal(facts["Visibility timeout"], "45 s");
  assert.match(String(facts["Position in batch"]), /2.*of 3/);
  assert.match(s.steps[1].say, /orders-worker received the message for the first time, 140ms after it was sent\./);
});

test("what the spans and the queue's settings did not give says so, never a made-up value", () => {
  const s = sequenceOf(delivered);
  const vis = s.steps[1].facts.find(([k]) => k === "Visibility timeout")!;
  assert.match(html(vis[1] as never), /30 s.*SQS&#x27;s default; the queue&#x27;s setting was not read/);
  assert.match(html(s.steps[1].facts.find(([k]) => k === "Delivered by")![1] as never), /Not recorded/);
  assert.match(html(s.queueAttrs.find(([k]) => k === "GetQueueAttributes")![1] as never), /Not read/);
});

test("a dead-lettered message gets the dead-letter queue as a fourth participant", () => {
  const s = sequenceOf([...delivered.slice(0, 2), ev("dead_letter", 40_000, { receiveCount: 3, source: "inferred" })], READ);
  assert.deepEqual(s.parts.map((p) => p.name), ["checkout-api", "orders", "orders-worker", "orders-dlq"]);
  assert.equal(s.steps[2].draw.kind === "arrow" && s.steps[2].draw.inferred, true);
});

test("MessageSequence draws the steps, offers the message page only when given its link, and uses no native title", () => {
  const out = html(createElement(MessageSequence, { event: delivered[2], history: delivered, queue: READ, messageHref: "/messages/m1-0000-aaaa" }));
  for (const w of ["SendMessage", "ReceiveMessage", "DeleteMessage", "Step", "Message attributes", "Queue attributes", "Open message"]) assert.ok(out.includes(w), w);
  assert.doesNotMatch(out, / title="/);
  const plain = html(createElement(MessageSequence, { event: delivered[2], history: delivered }));
  assert.ok(!plain.includes("Open message"));
  assert.ok(plain.includes("The body is not shown here."));
});

test("a host's own sequence (the hosted sample) is drawn as given", () => {
  const own = sequenceOf(delivered, READ);
  own.steps[0] = { ...own.steps[0], name: "Sent by the sample" };
  const out = html(createElement(MessageSequence, { event: delivered[0], history: delivered, sequence: own, payload: { order: 7 } }));
  assert.ok(out.includes("Sent by the sample"));
  assert.ok(out.includes("&quot;order&quot;: 7"));
});

test("PartMessages lists the rows it is given and links to the Event explorer through the host", () => {
  const row: PartMessage = { id: "m1-0000-aaaa", producer: "checkout-api", receiveCount: 1, sentMs: T0, lastMs: T0 + 900, outcome: "deleted", events: delivered };
  const asked: string[] = [];
  const out = html(createElement(PartMessages, { queue: "orders", part: "consumer", rows: [row], max: 3, eventsHref: (q) => (asked.push(q), `/events?q=${encodeURIComponent(q)}`) }));
  assert.deepEqual(asked, ["queue:orders event:receive"]);
  for (const w of ["m1-0000-", "checkout-api", "Deleted", "700ms", "Open in Event explorer"]) assert.ok(out.includes(w), w);
  assert.equal(html(createElement(PartMessages, { queue: "orders", part: "dlq", rows: [], max: 3, eventsHref: () => "" })), "");
});

test("the lane and the empty picture render without a browser", () => {
  const lane = html(createElement(QueueLane, {
    live: [], gone: [], consumer: "orders-worker", dlq: "orders-dlq", max: 3, fifo: false, visible: 0, inFlight: 0, flow: 0, windowS: 60, exact: true, t: 0, dead: 0,
    selected: null, onSelect: () => {}, onGroup: () => {},
  }));
  assert.match(lane, /aria-label="The queue as a lane, drawn to scale"/);
  const w = html(createElement(Watching, { title: "No messages in orders now" }, "Messages show here as their spans arrive."));
  assert.ok(w.includes("watch-beam") && w.includes("No messages in orders now"));
});
