/*
  QV-580 part 3: the open-source shell's /events page on the shared explorer (shell/events.tsx). The URL's range,
  links from before the shared explorer, the ask to GET /api/timeline-events for the chosen range, and the server's
  recorded answer drawn by the shared explorer. Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createElement as h } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { EventExplorer } from "../src/explorer/index.ts";
import { custom, preset } from "../src/model/index.ts";
import { DEFAULT_EVENTS_RANGE, eventsHref, eventsQueryOf, eventsTime, explorerData, timelineEventsQuery } from "../shell/events.tsx";

const NOW = Date.UTC(2026, 10, 27, 12, 0, 0);
const H = 3_600_000;

test("the URL's range: an hour when it names none, a preset, a typed one, or a fixed from and to", () => {
  assert.equal(DEFAULT_EVENTS_RANGE, "1h");
  assert.deepEqual(eventsTime("", NOW), preset("1h", NOW));
  assert.deepEqual(eventsTime("?range=4h", NOW), preset("4h", NOW));
  assert.equal(eventsTime("?range=45m", NOW).from, NOW - 45 * 60_000);
  assert.deepEqual(eventsTime("?from=2026-11-27T09:00:00Z&to=2026-11-27T10:00:00Z", NOW), custom(NOW - 3 * H, NOW - 2 * H));
  // a range that can't be read is the default
  assert.deepEqual(eventsTime("?range=nonsense", NOW), preset("1h", NOW));
});

test("links from before the shared explorer open what they named", () => {
  assert.deepEqual(eventsTime("?range=24h", NOW), preset("1d", NOW));
  assert.deepEqual(eventsTime("?range=7d", NOW), preset("7d", NOW));
  assert.deepEqual(eventsTime(`?from_ms=${NOW - 3 * H}&to_ms=${NOW - 2 * H}`, NOW), custom(NOW - 3 * H, NOW - 2 * H));
  assert.equal(eventsQueryOf("kind:send queue:orders"), "event:send queue:orders");
  assert.equal(eventsQueryOf("-kind:change_visibility"), "-event:visibility_change");
  // a word that only ends in kind: is left alone
  assert.equal(eventsQueryOf("body.kind:x"), "body.kind:x");
});

test("the page's link names the query and the range; Copy link's pins a range that ends now", () => {
  assert.equal(eventsHref("", preset("1d", NOW)), "/events?range=1d");
  assert.equal(eventsHref(" queue:orders ", preset("1h", NOW)), "/events?q=queue%3Aorders&range=1h");
  assert.equal(eventsHref("", custom(NOW - H, NOW)), "/events?from=2026-11-27T11%3A00%3A00Z&to=2026-11-27T12%3A00%3A00Z");
  assert.deepEqual(eventsTime(eventsHref("", custom(NOW - H, NOW)).slice(7), NOW + 5 * H), custom(NOW - H, NOW));
});

test("the chosen range is what the server is asked for, not a fixed hour", () => {
  // the API's window includes both ends, the range's end is not in it
  assert.deepEqual(timelineEventsQuery(preset("7d", NOW)), { from_ms: String(NOW - 7 * 24 * H), to_ms: String(NOW - 1) });
  assert.deepEqual(timelineEventsQuery(custom(NOW - 3 * H, NOW - 2 * H)), { from_ms: String(NOW - 3 * H), to_ms: String(NOW - 2 * H - 1) });
});

test("the server's recorded answer, drawn by the shared explorer: inferred events, tenant and body size included", () => {
  const fx = JSON.parse(readFileSync(new URL("./fixtures/timeline-events-parity.json", import.meta.url), "utf8"));
  const data = explorerData(fx.answer, fx.nowMs);
  assert.equal(data.events.length, fx.answer.events.length);
  assert.equal(data.live, true);
  assert.equal(data.bodies, "shown");
  assert.equal(data.capped, fx.answer.capped);
  const html = renderToStaticMarkup(h(EventExplorer, {
    query: "", setQuery: () => {}, time: custom(fx.fromMs, fx.toMs + 1), data, messageHref: (id: string) => `/message?id=${id}`, page: true,
  }));
  const kinds = new Set(fx.answer.events.map((e: { event: string }) => e.event));
  for (const k of ["visibility_expired", "dead_letter", "visibility_change"]) assert.ok(kinds.has(k), `the fixture has a ${k} event`);
  assert.match(html, /Visibility timeout expired/);
  assert.match(html, /Moved to dead-letter queue/);
  const head = html.match(/<thead[\s\S]*?<\/thead>/)?.[0] ?? "";
  assert.match(head, />Tenant</);
  assert.match(head, />Body size</);
});
