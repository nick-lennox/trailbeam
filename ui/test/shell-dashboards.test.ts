/*
  QV-591 part 3: the shell's Dashboards page (shell/dashboards.tsx) on the server's /api/dashboards: the views' links
  as this server's pages, the one read a dashboard is drawn from, the list's alerts from the server's answer, and the
  changes sent as contract/openapi.yaml takes them. Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { AWS_OFF_NOTE, STARTERS, type DashboardDef } from "../src/dashboards/index.ts";
import { custom, DAY, HOUR, preset } from "../src/model/index.ts";
import { ApiError } from "../shell/chrome.tsx";
import { actionsFor, alertsOf, apiActions, dashboardTime, dashboardsHref, dataWindow, measuresPath, problem } from "../shell/dashboards.tsx";

const NOW = 1_795_400_000_000;
const def = (over: Partial<DashboardDef> = {}): DashboardDef => ({ variables: [], sections: [], widgets: [], ...over });

test("the views' links are this server's pages", () => {
  assert.equal(dashboardsHref(""), "/overview");
  assert.equal(dashboardsHref("/dashboards"), "/dashboards");
  assert.equal(dashboardsHref("/dashboards/a1b2"), "/dashboards?id=a1b2");
  assert.equal(dashboardsHref("/dashboards/a1b2?range=4h"), "/dashboards?range=4h&id=a1b2");
  assert.equal(dashboardsHref("/messages?queue=orders.fifo"), "/queue?name=orders.fifo");
  assert.equal(dashboardsHref("/messages"), "/");
  assert.equal(dashboardsHref("/messages/m-1"), "/message?id=m-1");
  assert.equal(dashboardsHref("/events?q=status%3Aerror&range=1h"), "/events?q=status%3Aerror&range=1h");
});

test("a dashboard opens at its link's range, else its own time, else the past hour", () => {
  assert.equal(dashboardTime("?range=4h", { time: "1d" }, NOW).preset, "4h");
  assert.equal(dashboardTime("", { time: "1d" }, NOW).preset, "1d");
  assert.equal(dashboardTime("", { time: null }, NOW).preset, "1h");
  const fixed = dashboardTime(`?from=${NOW - 2 * HOUR}&to=${NOW - HOUR}`, { time: null }, NOW);
  assert.deepEqual([fixed.from, fixed.to], [NOW - 2 * HOUR, NOW - HOUR]);
});

test("one read covers the range, every widget's own range and the past day, up to 30 days", () => {
  assert.deepEqual(dataWindow(preset("1h", NOW), def(), NOW), { from: NOW - DAY, to: NOW });
  assert.deepEqual(dataWindow(preset("7d", NOW), def(), NOW), { from: NOW - 7 * DAY, to: NOW });
  const own = def({ widgets: [{ ...STARTERS[0].make().widgets[0], time: "7d" }] });
  assert.deepEqual(dataWindow(preset("1h", NOW), own, NOW), { from: NOW - 7 * DAY, to: NOW });
  // a range in the past is read up to its end, unless a widget keeps its own range, which ends now
  const past = custom(NOW - 10 * DAY, NOW - 9 * DAY);
  assert.deepEqual(dataWindow(past, def(), NOW), { from: NOW - 10 * DAY, to: NOW - 9 * DAY });
  assert.deepEqual(dataWindow(past, own, NOW), { from: NOW - 10 * DAY, to: NOW });
  const long = def({ widgets: [{ ...STARTERS[0].make().widgets[0], time: "30d" }] });
  assert.deepEqual(dataWindow(past, long, NOW), { from: NOW - 30 * DAY, to: NOW });
  assert.equal(measuresPath("a b", { from: 1, to: 2 }), "/api/dashboards/a%20b/measures?from=1&to=2&n=1&data=1");
  assert.equal(measuresPath("x", { from: 1, to: 2 }, false), "/api/dashboards/x/measures?from=1&to=2&n=1");
});

test("the list's alerts are the server's, for widgets still on the dashboard", () => {
  const d = { id: "d1", name: "Queues", def: STARTERS[0].make(), starred: false, lockedBy: null, lockedById: null, createdBy: "Ada", updatedBy: "Ada", updatedAt: NOW };
  const w = d.def.widgets.find((x) => x.rule)!;
  const alert = { tone: "critical" as const, value: "3", limit: "above 0", queue: "orders" };
  const states = alertsOf([d, { ...d, id: "d2" }], [
    { now: NOW, widgets: [{ id: w.id, alert }, { id: "gone", alert }, { id: "s", skipped: "Ask for it on its own." }, { id: "q", alert: null }] },
    null,
  ]);
  assert.deepEqual(states, [{ dashboardId: "d1", widgetId: w.id, title: w.title, ...alert, since: NOW }]);
});

test("a 404 is a dashboard gone, or dashboards off on this server", () => {
  assert.equal(problem(new ApiError(404, "Trailbeam answered 404: This dashboard no longer exists.")), "This dashboard no longer exists.");
  assert.match(problem(new ApiError(404, "Trailbeam answered 404: no such endpoint")), /TRAILBEAM_DASHBOARDS=1/);
  assert.equal(problem(new ApiError(500, "Trailbeam answered 500: boom")), "Trailbeam answered 500: boom");
});

test("changes are sent as the API takes them, and a refusal is the server's sentence", async () => {
  const sent: { method: string; url: string; body: unknown }[] = [];
  let answer: [number, unknown] = [200, {}];
  globalThis.fetch = (async (url: string, init: RequestInit) => {
    sent.push({ method: init.method ?? "GET", url, body: init.body ? JSON.parse(String(init.body)) : undefined });
    return new Response(JSON.stringify(answer[1]), { status: answer[0] });
  }) as typeof fetch;
  answer = [201, { dashboard: { id: "n1" } }];
  assert.deepEqual(await apiActions.create("Health", "queue-health"), { ok: true, id: "n1" });
  const made = sent.at(-1)!.body as { name: string; def: DashboardDef };
  assert.equal(made.name, "Health");
  assert.equal(made.def.widgets.length, STARTERS[0].make().widgets.length);
  await apiActions.create("Pasted", "", '{"widgets": []}');
  assert.deepEqual(sent.at(-1)!.body, { name: "Pasted", imported: '{"widgets": []}' });
  await apiActions.create("Blank", "");
  assert.deepEqual(sent.at(-1)!.body, { name: "Blank" });
  answer = [200, {}];
  await apiActions.save("d1", { name: "New" });
  await apiActions.star("d1", true);
  answer = [200, { lockedBy: "Ada" }];
  assert.deepEqual(await apiActions.lock("d1", true), { ok: true, lockedBy: "Ada" });
  answer = [201, { dashboard: { id: "d9" } }];
  assert.deepEqual(await apiActions.duplicate("d1"), { ok: true, id: "d9" });
  answer = [200, { removed: "d1" }];
  await apiActions.remove("d1");
  assert.deepEqual(sent.slice(3).map((s) => [s.method, s.url, s.body]), [
    ["PATCH", "/api/dashboards/d1", { name: "New" }],
    ["PATCH", "/api/dashboards/d1", { starred: true }],
    ["POST", "/api/dashboards/d1/lock", { lock: true }],
    ["POST", "/api/dashboards/d1/duplicate", undefined],
    ["DELETE", "/api/dashboards/d1", undefined],
  ]);
  answer = [400, { error: "this dashboard is locked" }];
  assert.deepEqual(await apiActions.remove("d1"), { ok: false, error: "This dashboard is locked." });
  // on a server that does not read AWS, the starter sent is the one built from the spans (QV-772)
  answer = [201, { dashboard: { id: "n2" } }];
  await actionsFor(false).create("Health", "queue-health");
  assert.ok((sent.at(-1)!.body as { def: DashboardDef }).def.widgets.some((w) => w.text === AWS_OFF_NOTE));
  await actionsFor(true).create("Health", "queue-health");
  assert.ok(!(sent.at(-1)!.body as { def: DashboardDef }).def.widgets.some((w) => w.type === "note"));
});
