/*
  Contract 3.4, section 21: what an SNS fan-out shows on the timeline, for timelines Trailbeam really returned
  (fixtures/sns-timelines.json, from make-fixtures.py). Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { TimelineView } from "../src/timeline/Timeline.tsx";
import { answerFor } from "../src/summary.ts";
import type { Timeline } from "../src/timeline/types.ts";

const { timelines } = JSON.parse(readFileSync(new URL("./fixtures/sns-timelines.json", import.meta.url), "utf8")) as { timelines: Timeline[] };
const byId = (id: string) => timelines.find((t) => t.message_id === id)!;
const href = (id: string) => `/message?id=${encodeURIComponent(id)}`;
const render = (t: Timeline, messageHref?: (id: string) => string) =>
  renderToStaticMarkup(createElement(TimelineView, { t, answer: answerFor(t), messageHref }));
const text = (html: string) => html.replace(/<[^>]+>/g, " ").replace(/&#x27;/g, "'").replace(/\s+/g, " ");

test("a publish joined through the envelope is a recorded step with the topic, the publisher and the SNS delivery time", () => {
  const t = byId("sns-orders");
  assert.deepEqual(t.events.map((e) => e.type), ["published", "received", "deleted"]);
  const html = text(render(t));
  assert.match(html, / Published to topic orders-topic /);
  assert.match(html, /orders-api/);
  assert.match(html, /Publish Joined by the envelope SNS delivery 40 ms/);
  assert.match(html, /SentTimestamp \(SQS\) − Timestamp \(SNS\), both AWS clocks/);
  assert.match(html, /arn:aws:sns:us-east-1:111122223333:orders-topic/);
  assert.match(html, /After e1 \(the publish\)/);
});

test("a publish lists the messages it reached, by queue, linking the others when the host gives links", () => {
  const html = render(byId("sns-orders"), href);
  assert.match(text(html), /Reached billing: sns-billing ?; orders: sns-orders \(this message\)/);
  assert.match(html, /href="\/message\?id=sns-billing"/);
  assert.doesNotMatch(html, /href="\/message\?id=sns-orders"/);
  assert.doesNotMatch(render(byId("sns-orders")), /href="\/message\?id=sns-billing"/);
});

test("raw delivery joins by the publish ID and has no SNS delivery time", () => {
  const html = text(render(byId("sns-billing"), href));
  assert.match(html, /Publish Joined by the publish ID/);
  assert.match(html, /trailbeam\.publish_id pub-1/);
  assert.doesNotMatch(html, /SNS delivery \d/);
  assert.match(html, /Reached billing: sns-billing \(this message\) ?; orders: sns-orders/);
});

test("a receive whose envelope names a topic with no recorded publish arrived via SNS, observed, with no time", () => {
  const t = byId("sns-arrived");
  assert.deepEqual(t.events.map((e) => e.type), ["arrived_via_sns", "received"]);
  const html = text(render(t));
  assert.match(html, /Arrived via SNS topic audit-topic/);
  assert.match(html, /no time/);
  assert.match(html, /SNS envelope/);
  assert.match(html, /Reported on receive 1 SNS delivery 25 ms/);
  assert.match(html, /Before e2 \(receive 1\)/);
  assert.match(html, /publisher runs no Trailbeam package/);
  assert.doesNotMatch(html, /inferred</);
});
