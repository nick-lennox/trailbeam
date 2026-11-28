/* QV-727: the Overview's links to Issues (a queue's issues, its dead-lettered messages, a cause) keep the page's range. */
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { custom, preset } from "../src/model/index.ts";
import { overviewIssuesHref } from "../shell/overview.tsx";
import { readIssuesUrl } from "../shell/issues.tsx";

const NOW = Date.UTC(2026, 10, 28, 12);

test("a range that ends now goes to Issues as its preset, the past hour included", () => {
  const t = preset("1h", NOW);
  assert.equal(overviewIssuesHref(t, "orders"), "/issues?range=1h&queue=orders");
  assert.equal(overviewIssuesHref(t, null, "c 1"), "/issues/c%201?range=1h");
  assert.equal(overviewIssuesHref(preset("1d", NOW), null), "/issues?range=1d");
  assert.equal(readIssuesUrl("?range=1h&queue=orders", NOW).range?.preset, "1h");
});

test("a fixed range goes to Issues as its times, and Issues reads them back", () => {
  const t = custom(NOW - 3 * 3_600_000, NOW - 3_600_000);
  const href = overviewIssuesHref(t, "orders");
  assert.equal(href, "/issues?from=2026-11-28T09%3A00%3A00Z&to=2026-11-28T11%3A00%3A00Z&queue=orders");
  const r = readIssuesUrl(href.slice(href.indexOf("?")), NOW).range;
  assert.deepEqual([r?.from, r?.to], [t.from, t.to]);
});

test("every link the Overview makes to Issues carries the range", () => {
  const src = readFileSync(new URL("../shell/overview.tsx", import.meta.url), "utf8");
  assert.doesNotMatch(src, /["`]\/issues[?/"`]/, "no /issues link is written by hand");
  for (const key of ["issues", "deadLettered", "cause"]) assert.match(src, new RegExp(`\\b${key}: .*overviewIssuesHref\\(u\\.time`), key);
  assert.match(src, /causeHref: \(id\) => overviewIssuesHref\(u\.time, null, id\)/);
});
