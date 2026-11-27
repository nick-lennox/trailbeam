/*
  QV-580: self-hosted Trailbeam's GET /api/timeline-events makes the Event explorer's events with a Python port of
  eventsOfTimeline (server timeline_events.py). fixtures/timeline-events-parity.json holds what the port was given for
  each message (timeline, queue URLs, the spans' 3.2 attributes, the kept body) and what the endpoint answered; the
  shared function must make the same events of the same inputs, in the same order. Made by
  test/make-timeline-events-fixture.py. Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { eventsOfTimeline, type Ev, type SpanMeta } from "../src/model/index.ts";

type Input = { timeline: Record<string, unknown>; urls: Record<string, string>; metas: SpanMeta[];
  body: { value: unknown; traceId: string; spanId: string } | null };
const f = JSON.parse(readFileSync(new URL("./fixtures/timeline-events-parity.json", import.meta.url), "utf8")) as {
  fromMs: number; toMs: number; inputs: Input[];
  answer: { from_ms: number; to_ms: number; events: Ev[]; messages: number; capped: boolean; queues: string[] };
};

/* JSON, as a page receives it: no undefined fields */
const plain = <T,>(x: T): T => JSON.parse(JSON.stringify(x));

test("eventsOfTimeline makes the server's events of the same timelines, newest first", () => {
  const found: { e: Ev; i: number }[] = [];
  for (const m of f.inputs)
    for (const e of eventsOfTimeline(m.timeline, { urls: m.urls, metas: m.metas, body: m.body ?? undefined }))
      if (e.ms >= f.fromMs && e.ms <= f.toMs) found.push({ e, i: found.length });
  const events = found.sort((a, b) => b.e.ms - a.e.ms || b.i - a.i).map((x) => x.e);
  assert.deepEqual(plain(events), f.answer.events);
});

test("the fixture covers inferred events, kept bodies, tenants and body sizes", () => {
  const evs = f.answer.events;
  for (const k of ["send", "receive", "delete", "processing_error", "visibility_expired", "dead_letter"]) assert.ok(evs.some((e) => e.event === k), k);
  assert.ok(evs.some((e) => e.source === "inferred"));
  assert.ok(evs.some((e) => e.body !== undefined), "a kept body");
  assert.ok(evs.some((e) => e.tenant), "a tenant");
  assert.ok(evs.some((e) => e.sizeBytes !== undefined), "a body size");
  assert.ok(evs.some((e) => e.attrs?.["queue.region"]), "a queue's region from its URL");
  assert.equal(f.answer.messages, f.inputs.length);
});
