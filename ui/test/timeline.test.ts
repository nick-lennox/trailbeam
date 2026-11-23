/*
  The shared timeline, rendered to HTML on the server, for timelines Trailbeam really returned
  (fixtures/timelines.json, from make-fixtures.py). Every step must appear, in the API's order, and the
  one-sentence summary must say what happened. Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { TimelineView } from "../src/timeline/Timeline.tsx";
import { answerFor } from "../src/summary.ts";
import type { Timeline } from "../src/timeline/types.ts";

const { timelines } = JSON.parse(readFileSync(new URL("./fixtures/timelines.json", import.meta.url), "utf8")) as { timelines: Timeline[] };
const render = (t: Timeline) => renderToStaticMarkup(createElement(TimelineView, { t, answer: answerFor(t) }));
const text = (html: string) => html.replace(/<[^>]+>/g, " ").replace(/&#x27;/g, "'").replace(/\s+/g, " ");

test("every step of every recorded timeline is drawn, in the API's order", () => {
  assert.ok(timelines.length >= 3);
  for (const t of timelines) {
    const html = text(render(t));
    let at = -1;
    for (const e of t.events) {
      const i = html.indexOf(` ${e.id} `, at + 1);
      assert.ok(i > at, `${t.message_id}: step ${e.id} (${e.type}) is drawn after the step before it`);
      at = i;
    }
    assert.ok(!/undefined|NaN/.test(html), `${t.message_id}: no undefined or NaN in the page`);
  }
});

test("an inferred step is marked inferred, and a message without a group still shows", () => {
  const t = timelines.find((x) => x.events.some((e) => e.inferred))!;
  const html = render(t);
  assert.match(html, /inferred/);
  const lone = t.events.find((e) => e.group === null);
  if (lone) assert.ok(text(html).includes(` ${lone.id} `), "a step placed between groups is drawn");
});

test("the summary says what happened", () => {
  const by = Object.fromEntries(timelines.map((t) => [t.deliveries, answerFor(t)]));
  assert.equal(by[0], "Sent to trailbeam-smoke and not received yet.");
  assert.equal(by[2], "Received 2 times by trailbeam-smoke-worker, then deleted.");
});

test("between steps there is only the rail, and the record keeps the order evidence (QV-212)", () => {
  const t = timelines.find((x) => x.events.some((e) => e.type === "visibility_changed" && e.delivery === 1))!;
  const html = render(t);
  const page = text(html);
  assert.ok(!/placed after|↓/.test(page), "no connector text between steps");
  assert.ok(!/visibility change of \d/.test(page), "no clipped 'visibility change of 1'");
  assert.match(html, /data-connector=/, "the rail between steps is still drawn");
  const vc = t.events.find((e) => e.type === "visibility_changed")!;
  if (t.events.some((e) => e.after.some((a) => a.event === vc.id))) {
    assert.ok(page.includes(`After ${vc.id} (the visibility change on delivery 1)`), "the record names the visibility change in full");
  }
});
