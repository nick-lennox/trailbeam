/*
  QV-341: the headline of a dead-lettered message counts its receives on the source queue only. SQS's move is
  seen from the receive on the dead-letter queue, so that receive comes after the move, not before it. 0.4.0
  counted every receive and said "after 3 receives" here, and "after 4" for a maxReceiveCount of 3.
  The timeline is Trailbeam's own (fixtures/contract-3.2-timelines.json, from make-fixtures.py). Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { TimelineView } from "../src/timeline/Timeline.tsx";
import { answerFor } from "../src/summary.ts";
import type { Timeline, TimelineEvent } from "../src/timeline/types.ts";

const { timelines } = JSON.parse(readFileSync(new URL("./fixtures/contract-3.2-timelines.json", import.meta.url), "utf8")) as { timelines: Timeline[] };
const moved = timelines.find((t) => t.message_id === "m-pay-2")!;
const text = (t: Timeline) => renderToStaticMarkup(createElement(TimelineView, { t, answer: answerFor(t) })).replace(/<[^>]+>/g, " ").replace(/\s+/g, " ");

test("the recorded DLQ timeline: 2 receives on payments, the move, then delivery 3 on payments-dlq", () => {
  assert.ok(moved.in_dead_letter_queue);
  assert.equal(moved.deliveries, 3);
  assert.deepEqual(moved.events.filter((e) => e.type === "received").map((e) => e.queue), ["payments", "payments", "payments-dlq"]);
  const a = answerFor(moved);
  assert.equal(a, "Moved to the dead-letter queue payments-dlq after 2 observed receives on payments.");
  assert.ok(text(moved).includes(a), "the headline is drawn on the card");
});

test("maxReceiveCount 3 (the QV-334 report): after 3 receives, not 4", () => {
  // The recorded timeline with one more receive on payments before the move, so receives 1-3 are on payments
  // and delivery 4 is the one from payments-dlq.
  const first = moved.events.find((e) => e.type === "received" && e.delivery === 2)!;
  const at = moved.events.findIndex((e) => e.type === "moved_to_dead_letter_queue");
  const third = { ...first, id: `${first.id}-3`, delivery: 3 } as TimelineEvent;
  const renumbered = moved.events.map((e) => (e.type === "received" && e.queue === "payments-dlq" ? { ...e, delivery: 4 } : e));
  const t: Timeline = { ...moved, deliveries: 4, events: [...renumbered.slice(0, at), third, ...renumbered.slice(at)] };
  const a = answerFor(t);
  assert.equal(a, "Moved to the dead-letter queue payments-dlq after 3 observed receives on payments.");
  assert.doesNotMatch(a, /after 4/);
});
