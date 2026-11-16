/*
  The Body tab and body.* search on the shared expected outputs (../../contract/fixtures/body-views.json,
  written from the recorded contract 3.2 spans by make-body-views.py). The hosted app checks its bodyView
  against the same file. Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { BodyPanel } from "../src/body/BodyPanel.tsx";
import { NO_BODY_TEXT } from "../src/body/rules.ts";
import { bodyMatches, parseBodyQuery, parsedBody } from "../src/body/search.ts";
import type { MessageBody } from "../src/body/types.ts";

type Case = { name: string; views: Record<string, MessageBody> };
const fixture = JSON.parse(readFileSync(new URL("../../contract/fixtures/body-views.json", import.meta.url), "utf8")) as {
  cases: Case[]; terms: { body: string; format: string; query: string; matches: boolean }[];
};
const text = (html: string) => html.replace(/<[^>]+>/g, " ").replace(/&#x27;/g, "'").replace(/&quot;/g, '"').replace(/&amp;/g, "&").replace(/\s+/g, " ");
const render = (v: MessageBody) => renderToStaticMarkup(createElement(BodyPanel, { view: v, access: "ACCESS-NOTE", hints: { not_opted_in: "HINT-STORE" } }));

test("each term case gives what the spec says", () => {
  assert.ok(fixture.terms.length >= 30);
  for (const t of fixture.terms) assert.equal(bodyMatches(parsedBody(t.body, t.format), parseBodyQuery(t.query)), t.matches, `${t.query} on ${t.body}`);
});

test("a body search takes body terms only", () => {
  for (const q of ["queue:orders", "acme", "body", "-", ""]) assert.throws(() => parseBodyQuery(q), q);
});

test("every view the server gives for the recorded spans is drawn, with its reason or its body", () => {
  let bodies = 0, none = 0;
  for (const c of fixture.cases) for (const v of Object.values(c.views)) {
    const html = render(v);
    const t = text(html);
    assert.ok(!/undefined|NaN|null/.test(t), `${c.name} ${v.message_id}: ${t}`);
    assert.ok(t.includes("ACCESS-NOTE"), "says who can read bodies");
    if (v.kind === "none") {
      none++;
      assert.ok(t.includes(NO_BODY_TEXT[v.reason].replace(/'/g, "'")), `${v.message_id}: ${v.reason}`);
      assert.equal(t.includes("HINT-STORE"), v.reason === "not_opted_in");
      if (v.size !== null) assert.ok(t.includes(`${v.size} bytes`));
    } else {
      bodies++;
      const parsed = JSON.parse(v.body.body) as { customer: { id: string } };
      assert.ok(t.includes(parsed.customer.id), "the body is drawn");
      assert.ok(t.includes(v.body.from_kind === "send" ? "From the send by" : "From a receive by"));
    }
  }
  assert.ok(bodies >= 5 && none >= 10);
});

test("the notes under producer not instrumented", () => {
  const v = fixture.cases.find((c) => c.name === "consumer only")!.views["m-pay-1"];
  assert.ok(text(render(v)).includes("Capture is off for this queue in the consumer's configuration too."));
  const legacy: MessageBody = { message_id: "x", kind: "none", reason: "producer_not_instrumented", size: null, sends: 0, receives: 2, send_status: null, receive_status: null };
  assert.ok(text(render(legacy)).includes("The consumer's package is also from before contract revision 3.2."));
  const odd: MessageBody = { ...legacy, reason: "unknown", sends: 1, send_status: "captured" };
  assert.ok(text(render(odd)).includes("The send's capture status is captured."));
});
