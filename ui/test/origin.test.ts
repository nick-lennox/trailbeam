/*
  Contract 3.5, section 22: the inferred first step of a message an EventBridge rule or an S3 notification sent, for
  timelines Trailbeam really returned (fixtures/origin-timelines.json, from make-fixtures.py). Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { TimelineView } from "../src/timeline/Timeline.tsx";
import { answerFor } from "../src/summary.ts";
import type { Timeline } from "../src/timeline/types.ts";

const { timelines } = JSON.parse(readFileSync(new URL("./fixtures/origin-timelines.json", import.meta.url), "utf8")) as { timelines: Timeline[] };
const byId = (id: string) => timelines.find((t) => t.message_id === id)!;
const render = (t: Timeline) => renderToStaticMarkup(createElement(TimelineView, { t, answer: answerFor(t) }));
const text = (html: string) => html.replace(/<[^>]+>/g, " ").replace(/&#x27;/g, "'").replace(/\s+/g, " ");

test("an EventBridge event body gives an inferred first step with the source, detail-type and delivery time", () => {
  const t = byId("eb-order");
  assert.deepEqual(t.events.map((e) => [e.type, e.inferred]), [["arrived_via_eventbridge", true], ["received", false], ["deleted", false]]);
  const html = text(render(t));
  assert.match(html, / Arrived via an EventBridge rule: OrderPlaced /);
  assert.match(html, /inferred · EventBridge event body/);
  assert.match(html, /EventBridge · com\.example\.orders/);
  assert.match(html, /body of delivery 1 · delivery 1\.2 s|body of delivery 1 · delivery 1200 ms/);
  assert.match(html, /6a7e8feb-b491-4cf7-a9f1-bf3703467718/);
});

test("an S3 notification names the event, bucket and key; the test message says what it is", () => {
  const up = text(render(byId("s3-upload")));
  assert.match(up, / S3 ObjectCreated:Put in uploads /);
  assert.match(up, /inferred · S3 event body/);
  assert.match(up, /red\+flower\.jpg/);
  assert.match(up, /0055AED6DCD90281E5/);
  const tst = text(render(byId("s3-test")));
  assert.match(tst, / S3 test message for bucket uploads /);
  assert.doesNotMatch(tst, /ObjectCreated/);
});
