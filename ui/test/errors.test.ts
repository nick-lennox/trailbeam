/*
  Lambda error text and errors grouped by cause (contract 15.5), on the timelines Trailbeam returns for the 32
  contract 3.2 spans the Node.js package recorded (fixtures/contract-3.2-timelines.json, from make-fixtures.py
  and contract/fixtures/contract-3.2-spans.json). The hosted app runs the same spans through its own engine
  and checks it gets these timelines. Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { TimelineView } from "../src/timeline/Timeline.tsx";
import { answerFor } from "../src/summary.ts";
import { errorCauses } from "../src/errors.ts";
import type { Timeline } from "../src/timeline/types.ts";

const { timelines } = JSON.parse(readFileSync(new URL("./fixtures/contract-3.2-timelines.json", import.meta.url), "utf8")) as { timelines: Timeline[] };
const by = new Map(timelines.map((t) => [t.message_id, t]));
const render = (t: Timeline) => renderToStaticMarkup(createElement(TimelineView, { t, answer: answerFor(t) }));
const text = (html: string) => html.replace(/<[^>]+>/g, " ").replace(/&#x27;/g, "'").replace(/\s+/g, " ");

test("the fixture: every message of the 32 recorded spans", () => {
  assert.deepEqual([...by.keys()], ["m-bill-1", "m-bill-2", "m-fifo-1", "m-fifo-2", "m-legacy", "m-orders-1", "m-orders-2", "m-orphan", "m-pay-1", "m-pay-2"]);
});

test("errors grouped by cause: the redacted text makes two customers' failures one cause", () => {
  const causes = errorCauses(by.get("m-bill-1")!);
  assert.deepEqual(causes.map((c) => [c.errorType, c.message, c.outcome, c.count, c.deliveries, c.steps]), [
    ["TypeError", "card declined for [REDACTED]", "failed_exception", 2, [1, 3], ["e2", "e8"]],
    ["Error", "Stripe timed out after 10000 ms", "failed_exception", 1, [2], ["e5"]],
  ]);
  assert.ok(causes[0].first < causes[0].last);
});

test("no causes where no handler failed", () => {
  for (const t of timelines) if (!t.events.some((e) => e.type === "lambda_outcome" && e.outcome !== "success")) assert.deepEqual(errorCauses(t), [], t.message_id);
});

test("the error text is on each failed step, and the causes at the foot of the timeline", () => {
  const html = text(render(by.get("m-bill-1")!));
  const steps = html.slice(0, html.indexOf("Handler errors, by cause"));
  const foot = html.slice(html.indexOf("Handler errors, by cause"));
  assert.equal(steps.split("card declined for [REDACTED]").length - 1, 2, "both TypeError steps show their text");
  assert.equal(steps.split("Stripe timed out after 10000 ms").length - 1, 1);
  assert.match(foot, /3 failed outcomes in 2 causes/);
  assert.match(foot, /TypeError 2 times, deliveries 1, 3 ?, .* UTC ?, steps e2, e8 card declined for \[REDACTED\]/);
  assert.match(foot, / Error Once, delivery 2 ?, .* UTC ?, step e5 Stripe timed out/);
});

test("every contract 3.2 timeline draws every step, with no undefined or NaN", () => {
  for (const t of timelines) {
    const html = text(render(t));
    for (const e of t.events) assert.ok(html.includes(` ${e.id} `), `${t.message_id}: ${e.id}`);
    assert.ok(!/undefined|NaN/.test(html), t.message_id);
    if (!errorCauses(t).length) assert.ok(!html.includes("Handler errors, by cause"), t.message_id);
  }
});
