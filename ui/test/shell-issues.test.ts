/*
  QV-708: Issues in the shell (shell/issues.tsx): /issues and /issues/<cause ID> drawn with the shared Issues views
  on GET /api/causes, in the shared frame with Issues marked, and /stuck sent there with its queue, age, range and
  cause. Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createElement as h } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import type { CausePage } from "../src/causes/types.ts";

const NOW = Date.UTC(2026, 10, 28, 12, 0);

function withLocation<T>(pathname: string, search: string, run: () => Promise<T>): Promise<T> {
  const g = globalThis as { location?: unknown; window?: unknown };
  const before = [g.location, g.window];
  g.location = { search, pathname, origin: "http://localhost" };
  g.window = { location: g.location };
  return run().finally(() => { [g.location, g.window] = before; });
}

test("a link's age, queue and range; with none, the server's age and the list's own range", async () => {
  const { readIssuesUrl } = await import("../shell/issues.tsx");
  assert.deepEqual(readIssuesUrl("", NOW), { age: null, badAge: false, queue: null, range: null });
  const u = readIssuesUrl("?age=15m&queue=orders&range=4h", NOW);
  assert.equal(u.age, 900);
  assert.equal(u.queue, "orders");
  assert.equal(u.range?.preset, "4h");
  assert.equal(readIssuesUrl("?older_than_s=60", NOW).age, 60, "the API's own name for it, as old links have");
  assert.deepEqual([readIssuesUrl("?age=soon", NOW).age, readIssuesUrl("?age=soon", NOW).badAge], [null, true]);
});

test("the API read: every message up to the limit, at the link's age, one cause's for an issue's page", async () => {
  const { causesAsk } = await import("../shell/issues.tsx");
  assert.equal(causesAsk({ age: null, queue: null }), "/api/causes?limit=1000");
  assert.equal(causesAsk({ age: 900, queue: "orders" }), "/api/causes?limit=1000&older_than_s=900&queue=orders");
  assert.equal(causesAsk({ age: null, queue: null }, "abc"), "/api/causes?limit=1000&cause=abc");
});

test("an issue's page is /issues/<cause ID>, as the hosted app's is", async () => {
  const { issueIdOf, issuesHref } = await import("../shell/issues.tsx");
  assert.equal(issueIdOf("/issues/3f2a9c1d7e4b"), "3f2a9c1d7e4b");
  assert.equal(issueIdOf("/issues/3f2a9c1d7e4b/"), "3f2a9c1d7e4b");
  assert.equal(issueIdOf("/issues"), null);
  assert.equal(issueIdOf("/issues/a/b"), null);
  assert.equal(issuesHref({ age: null, range: null }, null, "a b"), "/issues/a%20b");
  assert.equal(issuesHref({ age: 3600, range: null }, "orders"), "/issues?age=1h&queue=orders");
});

test("/stuck goes to Issues with its queue, age, range and cause", async () => {
  const { stuckTarget } = await import("../shell/issues.tsx");
  assert.equal(stuckTarget("", NOW), "/issues");
  assert.equal(stuckTarget("?cause=bf87b57963d7", NOW), "/issues/bf87b57963d7");
  assert.equal(stuckTarget("?queue=orders&older_than_s=60&range=24h", NOW), "/issues?age=1m&range=1d&queue=orders");
  assert.equal(stuckTarget("?range=1h", NOW), "/issues?range=1h");
  const from = NOW - 7200_000, to = NOW - 3600_000;
  assert.equal(stuckTarget(`?from_ms=${from}&to_ms=${to}`, NOW), "/issues?from=2026-11-28T10%3A00%3A00Z&to=2026-11-28T11%3A00%3A00Z");
  assert.equal(stuckTarget(`?from_ms=${from}`, NOW), "/issues?from=2026-11-28T10%3A00%3A00Z&to=2026-11-28T12%3A00%3A00Z", "open-ended: up to now");
  assert.equal(stuckTarget("?older_than_s=soon&limit=50", NOW), "/issues", "what can't be read, and Limit, are left behind");
});

test("Issues is in the shared frame, Issues marked in the sidebar where Stuck was", async () => {
  await withLocation("/issues", "", async () => {
    const { IssuesPage } = await import("../shell/issues.tsx");
    const html = renderToStaticMarkup(h(IssuesPage));
    assert.match(html, /<a[^>]*href="\/issues"[^>]*aria-current="page"|<a[^>]*aria-current="page"[^>]*href="\/issues"/, "Issues is the page marked");
    assert.doesNotMatch(html, />Stuck</);
    assert.match(html, /<h1[^>]*>Issues<\/h1>/);
    assert.match(html, /Find a message by its ID/, "the top bar's search");
  });
  await withLocation("/issues/bf87b57963d7", "", async () => {
    const { IssuePage } = await import("../shell/issues.tsx");
    const html = renderToStaticMarkup(h(IssuePage));
    assert.match(html, /<a[^>]*href="\/issues"[^>]*aria-current="page"|<a[^>]*aria-current="page"[^>]*href="\/issues"/, "an issue is under Issues");
  });
});

test("the list draws /api/causes's groups as the shared IssueList: one row per cause, linked to its page, no old Stuck words", async () => {
  const f = JSON.parse(readFileSync(new URL("./fixtures/viewmodel-parity.json", import.meta.url), "utf8"));
  const a = f.answers.python;
  const page = {
    older_than_s: 300, from_ms: null, to_ms: null, capped: false, read_limit: 5000, total: a.cause_messages.length, in_dead_letter_queue: 0,
    groups: a.causes, dead_letter_queues: [], cause: null, listed: a.cause_messages.length, messages: a.cause_messages,
  } as CausePage;
  await withLocation("/issues", "?queue=payments", async () => {
    const { IssuesBody, readIssuesUrl } = await import("../shell/issues.tsx");
    const u = readIssuesUrl("?queue=payments", f.nowMs);
    const html = renderToStaticMarkup(h(IssuesBody, {
      u, got: { page, at: f.nowMs, asked: "/api/causes?limit=1000&queue=payments", storeEmpty: false },
      alerts: { state: { kind: "off" }, rules: {}, load: async () => {} } as never, redrive: { view: null, reload: () => {} }, changed: () => {},
    }));
    for (const g of a.causes) assert.match(html, new RegExp(`href="/issues/${g.id}\\?queue=payments"`), g.id);
    assert.match(html, /Filter issues by cause, queue, service or tenant/, "the shared list's filter box");
    assert.match(html, /aria-label="Queue: payments\. Show every queue"[^>]*href="\/issues"|href="\/issues"[^>]*aria-label="Queue: payments\. Show every queue"/, "the queue chip");
    assert.match(html, /A message counts once it is 5 minutes old without a delete\. The server sets this with --stuck-after/);
    // one time control: the list's range, and no Older than or Limit fields
    assert.doesNotMatch(html, /Older than|name="limit"|name="older_than_s"/);
    // the old page's words
    assert.doesNotMatch(html, /retrying|Earlier deliveries|latest failed delivery|Stuck messages/);
    assert.doesNotMatch(html, /\stitle="/, "hover notes are the design system's Tooltip");
    assert.match(html, /href="\/api\/causes\?limit=1000&amp;queue=payments"/, "the same data as JSON");
  });
});
