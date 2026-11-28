/*
  QV-765: Stuck (`/stuck`, shell/stuck.tsx) is drawn in the shared frame (shell/frame.tsx) like every other page: the
  sidebar with its items and Stuck marked, and the top bar's search; not the old header frame. Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { createElement as h } from "react";
import { renderToStaticMarkup } from "react-dom/server";

test("Stuck is in the shared frame: the sidebar, Stuck marked, and the top bar's search", async () => {
  const g = globalThis as { location?: unknown; window?: unknown };
  const before = [g.location, g.window];
  g.location = { search: "", pathname: "/stuck", origin: "http://localhost" };
  g.window = { location: g.location };
  try {
    const { StuckPage } = await import("../shell/stuck.tsx");
    const html = renderToStaticMarkup(h(StuckPage));
    for (const [href, label] of [["/overview", "Overview"], ["/events", "Event explorer"], ["/", "Messages"], ["/duplicates", "Duplicates"], ["/stuck", "Stuck"], ["/tenants", "Tenants"]]) {
      assert.match(html, new RegExp(`href="${href}"[^>]*>[\\s\\S]*?${label}`), `the sidebar has ${label}`);
    }
    assert.match(html, /<a[^>]*href="\/stuck"[^>]*aria-current="page"|<a[^>]*aria-current="page"[^>]*href="\/stuck"/, "Stuck is the page marked");
    assert.match(html, /Find a message by its ID/, "the top bar's search");
    assert.match(html, /<h1[^>]*>Stuck messages<\/h1>/);
    // the old header frame's links and box are gone
    assert.doesNotMatch(html, />Events</);
    assert.doesNotMatch(html, /placeholder="Message ID"/);
  } finally {
    [g.location, g.window] = before;
  }
});
