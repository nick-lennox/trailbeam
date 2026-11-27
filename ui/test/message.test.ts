/*
  The message page (row 3 of the one-UI plan): the shared MessagePage chrome around the restyled timeline and the Body
  tab, the same in both products, with slots for what only one has today (redrive, the cause and its triage note).
  Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createElement as h } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { MessageFrame, MessagePage, TimelineView } from "../src/design/index.ts";
import { BodyPanel, answerFor, type MessageBody, type MessageHrefs, type Timeline } from "../src/index.ts";
import { shellMessageHrefs } from "../shell/message.tsx";

const { timelines } = JSON.parse(readFileSync(new URL("./fixtures/timelines.json", import.meta.url), "utf8")) as { timelines: Timeline[] };
const extra = ["sns", "put-event", "origin", "purge", "contract-3.2"].flatMap((f) =>
  (JSON.parse(readFileSync(new URL(`./fixtures/${f}-timelines.json`, import.meta.url), "utf8")) as { timelines: Timeline[] }).timelines);
const all = [...timelines, ...extra];
const text = (html: string) => html.replace(/<[^>]+>/g, " ").replace(/&#x27;/g, "'").replace(/&amp;/g, "&").replace(/\s+/g, " ");

const href: MessageHrefs = {
  messages: "/m",
  queue: (q) => `/m?queue=${q}`,
  tenant: (n) => `/m?tenant=${n}`,
  message: (id) => `/msg/${id}`,
  tab: (k) => (k === "body" ? "/msg/x?tab=body" : "/msg/x"),
};
const t0 = timelines.find((t) => t.deliveries > 0)!;
const page = (p: Partial<Parameters<typeof MessagePage>[0]> = {}) => renderToStaticMarkup(h(MessagePage, { t: t0, href, tab: "timeline", ...p }));
const body: MessageBody = {
  message_id: t0.message_id, kind: "body", size: 20, offloaded_size: null, tenant: "acme", tenant_source: "message_attribute",
  body: { body: '{"order":1}', format: "json", from_kind: "send", service: "orders-api", capture_status: "captured", redacted_count: 0, envelope: null } as never,
};

test("the timeline keeps the design system's rules for every recorded timeline: role tokens, Tooltips, no arrows or fragments", () => {
  assert.ok(extra.length >= 5, String(extra.length));
  for (const t of all) {
    for (const html of [renderToStaticMarkup(h(TimelineView, { t, answer: answerFor(t), openSteps: t.events.map((e) => e.id) })), page({ t })]) {
      assert.doesNotMatch(html, / title="|<title>/, `${t.message_id}: hover notes are the Tooltip, never title`);
      assert.doesNotMatch(html, /[→←↗↑×]/, `${t.message_id}: no arrows`);
      assert.doesNotMatch(html, /uppercase|tracking-/, `${t.message_id}: no tracked capitals`);
      assert.doesNotMatch(text(html), / · |—| – /, `${t.message_id}: no a · b fragments, no em dashes`);
      assert.doesNotMatch(html, /(?:text|bg|border|from|to)-(?:sky|dawn|trail|ember|lost)\b/, `${t.message_id}: role tokens, not the landing page's sky`);
      assert.doesNotMatch(html, /var\(--color-(?:sky|trail|ember|lost)/, `${t.message_id}: the nodes take role tokens too`);
      assert.doesNotMatch(html, /JetBrains/);
    }
  }
});

test("labels are in the sans face: a step's source tag and the record's field names are never mono", () => {
  const html = renderToStaticMarkup(h(TimelineView, { t: t0, answer: answerFor(t0), openSteps: t0.events.map((e) => e.id) }));
  for (const m of html.matchAll(/<span[^>]*class="([^"]*)"[^>]*>(Trailbeam package|inferred)</g)) assert.doesNotMatch(m[1], /font-mono/, m[2]);
  for (const m of html.matchAll(/<dt class="([^"]*)"/g)) assert.doesNotMatch(m[1], /font-mono/);
  assert.match(html, /font-mono[^"]*">\d\d:\d\d:\d\d\.\d{3}</, "times are values, in the mono face");
});

test("the page: breadcrumb, the answer as the serif heading, the state pill, then the timeline without its own heading", () => {
  const html = page();
  const words = text(html);
  assert.ok(words.indexOf("Messages") < words.indexOf(t0.source_queue), "Messages, then the queue");
  assert.match(html, /href="\/m"/);
  assert.match(html, new RegExp(`href="/m\\?queue=${t0.source_queue}"`));
  assert.match(html, new RegExp(`<h1 class="[^"]*font-serif text-title-sm[^"]*">${answerFor(t0).replace(/[.()]/g, "\\$&")}</h1>`));
  assert.equal((html.match(/<h1/g) ?? []).length, 1);
  assert.doesNotMatch(html, /<h3/, "the timeline's own heading is left out in the page");
  assert.match(html, /Copy message ID/);
  assert.match(html, /Copy link/);
  assert.match(html, /aria-label="Steps, in proven order"/);
});

test("no body given: no tabs, only the timeline; with one, Timeline and Body, the open one marked", () => {
  assert.doesNotMatch(page(), /Message views/);
  const tl = page({ body: "B" });
  assert.match(tl, /aria-label="Message views"/);
  assert.match(tl, /href="\/msg\/x" aria-current="page"/);
  assert.match(tl, /href="\/msg\/x\?tab=body"/);
  assert.match(tl, /data-tab="timeline"/);
  const b = renderToStaticMarkup(h(MessagePage, { t: t0, href, tab: "body", body: h(BodyPanel, { view: body, access: "Anyone" }), bodyNote: "Owners and admins only" }));
  assert.match(b, /href="\/msg\/x\?tab=body" aria-current="page"/);
  assert.match(b, /data-tab="body"/);
  assert.doesNotMatch(b, /data-tab="timeline"/);
  assert.match(b, /\{\s*&quot;order&quot;: 1\s*\}/, "the body is drawn");
  assert.match(b, /aria-label="Owners and admins only"/, "the lock says who may read it");
});

test("the slots: actions in the state row, the cause above the timeline on its tab only, the host's notices before the timeline's", () => {
  const withNotice = { ...t0, notices: [{ code: "own", message: "From the timeline." }] } as Timeline;
  const html = page({ t: withNotice, actions: h("button", { "data-redrive": "" }, "Redrive whole DLQ"), cause: h("p", { "data-triage": "" }, "Cause: timeouts"),
    notices: [{ code: "redriven", message: "Redriven." }] });
  assert.ok(html.indexOf("data-redrive") < html.indexOf("Steps, in proven order"));
  assert.ok(html.indexOf("data-triage") < html.indexOf('data-tab="timeline"'), "the cause sits above the timeline");
  assert.ok(html.indexOf("Redriven.") < html.indexOf("From the timeline."), "the host's notices come first");
  assert.equal(html.split("From the timeline.").length - 1, 1, "a notice is drawn once, by the page");
  const onBody = page({ tab: "body", body: "B", cause: h("p", { "data-triage": "" }, "x") });
  assert.doesNotMatch(onBody, /data-triage/, "the cause belongs to the Timeline tab");
});

test("the tenant links to its messages when the host has a tenant link; the message group is shown", () => {
  const html = page({ tenant: "acme", groupId: "g-1" });
  assert.match(html, /href="\/m\?tenant=acme"/);
  assert.match(text(html), /Message group g-1/);
  assert.doesNotMatch(page({ tenant: "acme", href: { ...href, tenant: undefined } }), /tenant=acme/);
});

test("the frame for the page's other states: the top bar, the breadcrumb, the host's words", () => {
  const html = renderToStaticMarkup(h(MessageFrame, { href, id: "abc", search: h("form", { "data-search": "" }), children: "Loading the timeline" }));
  assert.ok(html.indexOf("data-search") < html.indexOf("Messages"));
  assert.match(text(html), /Messages abc Loading the timeline/);
});

test("the shell's links: Messages at /, a queue's messages, the tabs on /message", () => {
  const s = shellMessageHrefs("m 1");
  assert.equal(s.messages, "/");
  assert.equal(s.queue("orders"), "/?queue=orders");
  assert.equal(s.tab("timeline"), "/message?id=m%201");
  assert.equal(s.tab("body"), "/message?id=m%201&tab=body");
  assert.equal(s.message!("x"), "/message?id=x");
});

test("the Body tab keeps the design system's rules: role tokens, a serif heading, no dot fragments", () => {
  const html = renderToStaticMarkup(h(BodyPanel, { view: body, access: "Anyone" }));
  assert.match(html, /font-serif/);
  assert.doesNotMatch(text(html), / · /);
  assert.doesNotMatch(html, /sky-/);
});
