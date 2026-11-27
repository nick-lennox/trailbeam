/*
  QV-583 part 2: the Issues views in @trailbeam/ui/issues (the list, an issue's page and their parts) and the charts
  in @trailbeam/ui/charts, moved from the hosted app so both products draw the same Issues pages. Built here from the
  recorded open-source answer in the parity fixture. Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { createElement as h } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import {
  AgeChoice, IssueDetail, IssueGone, IssueList, QueueChip, issueFigures, issueRow, issueSummary, sharedBy, shortId, span, withQueue,
} from "../src/issues/index.ts";
import { TimeSeries, fmt } from "../src/charts/index.ts";
import { issuesOfPage, type Issue } from "../src/model/index.ts";

const f = JSON.parse(readFileSync(new URL("./fixtures/viewmodel-parity.json", import.meta.url), "utf8"));
const NOW: number = f.nowMs;
const issues: Issue[] = issuesOfPage({ groups: f.answers.python.causes, messages: f.answers.python.cause_messages }, NOW);
const EMPTY = { title: "No open issues", body: "Every message your services reported was deleted." };
const list = (over: Record<string, unknown> = {}) => renderToStaticMarkup(h(IssueList, {
  rows: issues.map((i) => issueRow(i)), now: NOW, filterName: "Open", emptyText: EMPTY, defaultRange: "1h", ...over,
}));

test("the fixture has issues to draw", () => {
  assert.ok(issues.length >= 2, String(issues.length));
});

test("the list has a row per issue, each a link to its page, with the host's tags and actions in their slots", () => {
  const html = list({
    issueHref: (id: string) => `/issues/${id}?older_than_s=300`,
    tags: Object.fromEntries(issues.map((i) => [i.id, h("span", { className: "slot-tag" }, "Assigned")])),
    actions: Object.fromEntries(issues.map((i) => [i.id, h("button", { className: "slot-action" }, "Triage")])),
  });
  for (const i of issues) assert.ok(html.includes(`href="/issues/${i.id}?older_than_s=300"`), i.id);
  assert.equal(html.match(/slot-tag/g)?.length, issues.length);
  assert.equal(html.match(/slot-action/g)?.length, issues.length);
  const messages = issues.reduce((a, i) => a + i.count, 0);
  assert.match(html, new RegExp(`<b[^>]*>${issues.length}</b> issues with <b[^>]*>${messages}</b> failing messages`));
  assert.match(html, /aria-label="Filter issues"/);
  // a row's own link from hrefs wins over issueHref
  assert.ok(list({ hrefs: { [issues[0].id]: "/kept?status=open" } }).includes(`href="/kept?status=open"`));
});

test("with no issues, the list says what the host's empty text says", () => {
  const html = list({ rows: [] });
  assert.ok(html.includes(EMPTY.title) && html.includes(EMPTY.body));
});

test("an issue's page: its figures, the flow, what to check, the messages linked to their pages, and the slots", () => {
  const issue = issues[0];
  const html = renderToStaticMarkup(h(IssueDetail, {
    issue, now: NOW, listHref: "/issues?older_than_s=300", queueHref: withQueue("/issues?older_than_s=300", issue.queue), messageHref: (id: string) => `/messages/${id}`,
    tags: h("span", { className: "slot-tag" }), actions: h("button", { className: "slot-action" }), panel: h("section", { className: "slot-panel" }),
  }));
  assert.ok(html.includes(">Issues</a>") && html.includes(`href="/issues?older_than_s=300&amp;queue=${encodeURIComponent(issue.queue)}"`));
  for (const fig of issueFigures(issue, NOW)) assert.ok(html.includes(fig.label), fig.label);
  assert.ok(html.includes(`The flow through ${issue.queue}`));
  assert.ok(html.includes("What to check"));
  for (const m of issue.messages) assert.ok(html.includes(`href="/messages/${m.id}"`), m.id);
  for (const s of ["slot-tag", "slot-action", "slot-panel"]) assert.ok(html.includes(s), s);
  assert.ok(html.includes("Copy summary"));
});

test("the example message is drawn from the host's slot, and without it the steps are listed", () => {
  const issue = issues.find((i) => i.shape?.example && i.shape.steps.length) ?? issues[0];
  if (!issue.shape?.example) return;
  const page = (example?: unknown) => renderToStaticMarkup(h(IssueDetail, {
    issue, now: NOW, listHref: "/issues", queueHref: "/issues", messageHref: (id: string) => `/messages/${id}`, example: example as never,
  }));
  const drawn = page(h("div", { className: "slot-example" }));
  assert.ok(drawn.includes("slot-example") && drawn.includes(shortId(issue.shape.example)));
  assert.ok(drawn.includes(sharedBy(issue, true)));
  const listed = page();
  assert.ok(!listed.includes("slot-example"));
  if (issue.shape.steps.length) assert.ok(listed.includes(sharedBy(issue, false)));
});

test("an issue page with nothing to draw says why", () => {
  const none = renderToStaticMarkup(h(IssueGone, { kind: "none", listHref: "/issues", age: "5 minutes" }));
  assert.match(none, /No unfinished message older than 5 minutes has this cause now/);
  assert.match(none, /See every open issue/);
  const done = renderToStaticMarkup(h(IssueGone, { kind: "finished", listHref: "/issues", age: "5 minutes", panel: h("section", { className: "slot-panel" }) }));
  assert.match(done, /Every message with this cause has finished/);
  assert.match(done, /slot-panel/);
});

test("issueRow keeps an issue's figures, its triage status, and when each message was last seen", () => {
  const i = issues[0];
  const r = issueRow(i, { status: "resolved", regressed: true });
  assert.equal(r.status, "resolved");
  assert.equal(r.regressed, true);
  assert.equal(r.points.length, i.lifespans.length);
  assert.ok(!("messages" in r) && !("lifespans" in r));
  assert.equal(issueRow(i).status, "open");
});

test("withQueue sets or takes off ?queue= and keeps the rest of the link", () => {
  assert.equal(withQueue("/issues?older_than_s=300", "orders"), "/issues?older_than_s=300&queue=orders");
  assert.equal(withQueue("/issues?queue=orders&range=1h", null), "/issues?range=1h");
  assert.equal(withQueue("/issues?queue=orders", null), "/issues");
  assert.equal(withQueue("/issues#top", "a b"), "/issues?queue=a+b#top");
});

test("issueSummary and span write what the hosted page wrote", () => {
  const lines = issueSummary({ title: "TimeoutError", queue: "orders", consumers: ["billing"], count: 2, inDlq: 1, notFinished: 1, firstSeen: Date.UTC(2026, 0, 1), lastSeen: Date.UTC(2026, 0, 1, 1) });
  assert.deepEqual(lines, ["TimeoutError", "Queue orders, consumer billing", "2 messages: 1 in a dead-letter queue, 1 not finished", "First seen 2026-01-01 00:00:00 UTC, last seen 2026-01-01 01:00:00 UTC"]);
  assert.equal(span(96_000), "96 s");
  assert.equal(span((13 * 24 + 23) * 3600_000), "13 days 23 h");
});

test("the age choices keep an age from the link that isn't a usual one, and the queue chip shows every queue", () => {
  const html = renderToStaticMarkup(h(AgeChoice, { now: 120, href: (s: number) => `/issues?older_than_s=${s}` }));
  assert.match(html, /href="\/issues\?older_than_s=120" aria-current="true"[^>]*>2 min</);
  assert.match(html, /href="\/issues\?older_than_s=300"/);
  const chip = renderToStaticMarkup(h(QueueChip, { queue: "orders", href: "/issues" }));
  assert.match(chip, /aria-label="Queue: orders. Show every queue"/);
});

test("the chart draws nothing before it is measured, and writes figures in their unit", () => {
  assert.equal(renderToStaticMarkup(h(TimeSeries, { lines: [{ name: "a", color: "red", points: [1, 2] }], from: 0, to: 60_000, unit: "count" })).includes("<svg"), false);
  assert.equal(fmt(12_500, "count"), "12.5k");
  assert.equal(fmt(125, "s"), "2m 5s");
  assert.equal(fmt(Infinity, "s"), "Growing");
});

test("the views' copy: no native title, no em dash, no spaced dot fragments", () => {
  for (const dir of ["../src/issues/", "../src/charts/"]) {
    const at = new URL(dir, import.meta.url);
    for (const file of readdirSync(at)) {
      const src = readFileSync(new URL(file, at), "utf8");
      // a native title on an HTML or SVG tag; Panel's title prop is its heading
      assert.doesNotMatch(src, /<[a-z][\w-]*\s[^>]*?\btitle=/, file);
      assert.doesNotMatch(src, /—/, file);
      assert.doesNotMatch(src, / · /, file);
    }
  }
});
