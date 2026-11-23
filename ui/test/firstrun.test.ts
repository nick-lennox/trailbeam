/*
  The first-run screen (design QV-146): what the waiting line says for each /api/health answer, the screen in
  State A (waiting) and State B (the first message is in), and the empty states of the other pages. Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { EXAMPLE, FirstRun, environment, highlight, type FirstRunProps } from "../src/firstrun/FirstRun.tsx";
import { NO_SPANS_AFTER_MS, announcement, isDefaultEndpoint, retainedFor, storeHasSpans, storeIsEmpty, waitingStatus, type Health } from "../src/firstrun/status.ts";
import { EmptyState, MessagesTable, duplicateWhy, fmtAge, fmtTime, type MessageSummary } from "../src/messages/Messages.tsx";
import { duplicatesEmpty, lookupEmpty, messagesEmpty, stuckEmpty, tenantsEmpty, without } from "../src/messages/empty.tsx";

/** The page's words: block elements and cells end a phrase, inline ones (code, spans) don't; curly quotes as straight. */
const text = (html: string) => html.replace(/<\/?(p|h1|h2|div|section|button|output|th|td|pre|li)\b[^>]*>/g, " ").replace(/<[^>]+>/g, "").replace(/\u2019/g, "'").replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&quot;/g, '"').replace(/&#x27;/g, "'").replace(/&amp;/g, "&").replace(/\s+/g, " ").trim();
const health = (over: { last_row?: number; ingest?: Health["ingest"]; auth?: "off" | "token"; retention?: { default: string; passes: number; last_pass?: { deleted_spans: number } } }): Health => ({
  status: "ok", version: "0.3.0",
  ingest: { requests: 0, inserted: 0, ignored: 0, rejected: 0, ...over.ingest },
  data: { last_row: over.last_row ?? 0 },
  config: { auth: over.auth ?? "off", retention: over.retention ?? { default: "7d", passes: 0 } },
});
const render = (p: Partial<FirstRunProps>) => renderToStaticMarkup(createElement(FirstRun, {
  endpoint: "http://localhost:4318/v1/traces", auth: "off", status: { kind: "waiting", checkedAgoS: 2 }, version: "0.3.0",
  runtime: "python", onRuntime: () => {}, ...p,
}));

test("the store is empty only when health says last_row 0", () => {
  assert.equal(storeIsEmpty(health({})), true);
  assert.equal(storeIsEmpty(health({ last_row: 3 })), false);
  assert.equal(storeIsEmpty({ status: "ok", version: "0.3.0" }), false); // no session: unknown, not empty
  assert.equal(storeIsEmpty({ data: { last_row: null } }), false); // store unreachable
  assert.equal(storeHasSpans(health({ last_row: 1 })), true);
  assert.equal(storeHasSpans(health({})), false);
});

test("the waiting line's reason comes from the ingest counters", () => {
  assert.deepEqual(waitingStatus(health({}), 1000), { kind: "waiting", checkedAgoS: 0 });
  assert.deepEqual(waitingStatus(health({}), NO_SPANS_AFTER_MS - 1), { kind: "waiting", checkedAgoS: 0 });
  assert.deepEqual(waitingStatus(health({}), NO_SPANS_AFTER_MS), { kind: "no-spans" });
  // a request came, so it isn't "nothing arrived", even after two minutes
  assert.equal(waitingStatus(health({ ingest: { requests: 1 } }), NO_SPANS_AFTER_MS).kind, "waiting");
  assert.deepEqual(waitingStatus(health({ ingest: { requests: 2, ignored: 5 } }), 0), { kind: "ignored", ignored: 5 });
  assert.deepEqual(waitingStatus(health({ ingest: { requests: 2, ignored: 5, rejected: 3 } }), 0), { kind: "rejected", rejected: 3 });
  // something was inserted (and since purged): no failure to report
  assert.equal(waitingStatus(health({ ingest: { requests: 2, ignored: 5, inserted: 1 } }), 0).kind, "waiting");
});

test("retention variant: only when a purge emptied the store", () => {
  const purged = (def: string, deleted: number) => ({ default: def, passes: 3, last_pass: { deleted_spans: deleted } });
  // a fresh server has run its first pass and deleted nothing: the normal first-run screen
  assert.equal(retainedFor(health({ retention: { default: "7d", passes: 1, last_pass: { deleted_spans: 0 } } })), null);
  assert.equal(retainedFor(health({ retention: purged("7d", 12) })), "7 days");
  assert.equal(retainedFor(health({ retention: purged("1d", 1) })), "1 day");
  assert.equal(retainedFor(health({ retention: purged("12h", 1) })), "12 hours");
  assert.equal(retainedFor(health({ retention: purged("forever", 1) })), null);
  assert.equal(retainedFor(health({ last_row: 9, retention: purged("7d", 1) })), null);
  // stored since start and gone, the last pass having found nothing more
  assert.equal(retainedFor(health({ ingest: { inserted: 4 }, retention: purged("7d", 0) })), "7 days");
});

test("State A: the three steps, the endpoint and the waiting line", () => {
  const html = render({});
  const t = text(html);
  for (const s of [
    "Get started", "Send your first SQS message to Trailbeam",
    "Add the package to one service, point it here, and make one SQS call. The message shows up on this page by itself.",
    "Waiting for your first message", "Checked 2 s ago · no spans yet",
    "01", "Install the package", "pip install trailbeam", "Python 3.10 or later, with boto3.",
    "02", "Point it at this Trailbeam", "The package sends its spans over OTLP/HTTP to this endpoint:",
    "This is the package's default, so a service on this machine can skip TRAILBEAM_OTLP_TRACES_ENDPOINT",
    "This Trailbeam has no access token, so services send without one.",
    "export TRAILBEAM_OTLP_TRACES_ENDPOINT=http://localhost:4318/v1/traces", "export OTEL_SERVICE_NAME=orders-worker",
    "03", "Make one SQS call", "trailbeam.boto.install()", 'QueueUrl="<queue URL>"',
  ]) assert.ok(t.includes(s), `missing: ${s}\n${t}`);
  assert.ok(html.includes("<h1") && (html.match(/<h2/g) ?? []).length === 3, "one h1, a h2 per step");
  assert.ok(html.includes("<output") && html.includes("http://localhost:4318/v1/traces</output>"));
  assert.ok(!t.includes("OTEL_EXPORTER_OTLP_HEADERS"), "no token header when the server has no token");
  assert.ok(t.includes("Spans go out in batches every few seconds, and any still waiting go out when the process exits."));
  assert.ok(!t.includes("forceFlush"), "forceFlush is the Node.js API; the Python package has no such function");
  assert.ok(!t.includes("Show setup"), "nothing hidden behind a click");
  // the runtime switch is a tab list that controls both code windows
  assert.match(html, /role="tablist"/);
  assert.match(html, /role="tab" aria-selected="true"[^>]*>Python</);
  const controls = /aria-controls="([^"]+)"/.exec(html)![1].split(" ");
  assert.equal(controls.length, 2);
  for (const id of controls) assert.ok(html.includes(`id="${id}"`), `controls ${id}`);
  for (const name of ["Copy the install command", "Copy the endpoint", "Copy the environment variables", "Copy the example"]) assert.ok(html.includes(`aria-label="${name}"`), name);
  assert.ok(html.includes('aria-live="polite"'));
  assert.ok(!/undefined|NaN/.test(html));
});

test("State A, Node.js, a token and another origin", () => {
  const t = text(render({ runtime: "node", auth: "token", endpoint: "http://10.0.0.5:4318/v1/traces" }));
  for (const s of [
    "npm install trailbeam", "Node.js 20 or later", "trailbeam.instrument(new SQSClient({}))", "await trailbeam.forceFlush();",
    "This is the address your browser used.", "http://host.docker.internal:4318/v1/traces",
    "This Trailbeam needs its access token on every export.", "Trailbeam doesn't show it here.",
    'export OTEL_EXPORTER_OTLP_HEADERS="Authorization=Bearer%20<token>"', "Replace <token> with the access token.",
    "export TRAILBEAM_OTLP_TRACES_ENDPOINT=http://10.0.0.5:4318/v1/traces",
  ]) assert.ok(t.includes(s), `missing: ${s}`);
  assert.ok(!t.includes("pip install"));
  assert.ok(t.includes("In a long-running service you don't need forceFlush: spans go out in batches every few seconds."));
  assert.equal(isDefaultEndpoint("http://127.0.0.1:4318/v1/traces"), true);
  assert.equal(isDefaultEndpoint("https://localhost:4318/v1/traces"), false);
});

test("copied text is exactly the code: no prompt, no trailing newline, no hash key", () => {
  assert.equal(environment("http://x:1/v1/traces", "off"), "export TRAILBEAM_OTLP_TRACES_ENDPOINT=http://x:1/v1/traces\nexport OTEL_SERVICE_NAME=orders-worker");
  assert.ok(!environment("e", "token").includes("RECEIPT_HASH_KEY") && !environment("e", "token").endsWith("\n"));
  for (const r of ["python", "node"] as const) {
    const html = renderToStaticMarkup(createElement("pre", null, highlight(EXAMPLE[r].code)));
    assert.equal(text(html).replace(/\s+/g, " "), EXAMPLE[r].code.replace(/\s+/g, " "), `${r}: the highlighted code reads the same as the copied code`);
  }
});

test("the waiting line with a reason", () => {
  const cases: [FirstRunProps["status"], string, string?][] = [
    [{ kind: "no-spans" }, "No spans have arrived in 2 minutes."],
    [{ kind: "ignored", ignored: 4 }, "Trailbeam received 4 spans, but none came from the Trailbeam package"],
    [{ kind: "rejected", rejected: 2 }, "Trailbeam rejected 2 spans that don't match its span contract.", "This server is 0.3.0."],
    [{ kind: "unreachable" }, "Can't reach Trailbeam's API. Trying again every 10 s."],
    [{ kind: "waiting", checkedAgoS: null, hidden: true }, "Paused while this tab is hidden"],
  ];
  for (const [status, s, also] of cases) {
    const t = text(render({ status }));
    assert.ok(t.includes("Waiting for your first message") && t.includes(s) && (!also || t.includes(also)), `${status.kind}: ${t.slice(0, 400)}`);
    assert.ok(!t.includes("no spans yet") || status.kind === "waiting" && !status.hidden);
  }
  assert.ok(!text(render({ status: { kind: "no-spans" } })).includes("401"));
  assert.ok(text(render({ status: { kind: "no-spans" }, auth: "token" })).includes("If its log shows 401"));
  // the live region doesn't change with the poll counter
  assert.equal(announcement({ kind: "waiting", checkedAgoS: 1 }), announcement({ kind: "waiting", checkedAgoS: 7 }));
});

test("State B: the first message, a link to its timeline, the setup collapsed", () => {
  const first = { message_id: "m-1", source_queue: "orders", consumer: null };
  const html = render({ status: { kind: "received", first } });
  const t = text(html);
  assert.ok(t.includes("First message received") && t.includes("m-1 · orders · sent, not received yet"), t);
  assert.ok(!t.includes("Waiting for your first message"));
  assert.ok(html.includes('href="/message?id=m-1"') && t.includes("Open its timeline"));
  assert.match(t, /Setup\s*·\s*Python\s*·\s*http:\/\/localhost:4318\/v1\/traces Show setup again/);
  assert.match(html, /grid-rows-\[0fr\]" inert="" aria-hidden="true"/, "the steps are collapsed and out of reach");
  assert.equal(announcement({ kind: "received", first }), "First message received: m-1. Open its timeline.");
  const named = text(render({ status: { kind: "received", first: { ...first, consumer: "orders-worker" } } }));
  assert.ok(named.includes("m-1 · orders · orders-worker"));
});

test("retention variant: its own heading, the setup collapsed", () => {
  const t = text(render({ retainedFor: "7 days" }));
  assert.ok(t.includes("No spans in the last 7 days") && t.includes("Trailbeam keeps spans for 7 days and has deleted older ones."));
  assert.ok(t.includes("Show setup") && !t.includes("Show setup again"));
});

const msg: MessageSummary = {
  message_id: "m-2", state: "not_deleted", source_queue: "orders", current_queue: "orders-dlq", in_dead_letter_queue: true,
  dead_letter_queue_move: "observed", deliveries: 3, successful_deliveries: 0, consumers: [{ service_name: "orders-worker", client_id: "c1" }],
  first_seen: "2026-11-16T18:05:37.429123Z", first_seen_clock: "sqs", last_seen: "2026-11-16T18:06:00Z", age_s: 400,
};

test("the message list", () => {
  const html = renderToStaticMarkup(createElement(MessagesTable, { messages: [msg], href: (id: string) => `/message?id=${id}`, extra: { title: "Age", cell: (m: MessageSummary) => fmtAge(m.age_s) } }));
  const t = text(html);
  for (const s of ["m-2", "not deleted", "orders-dlq (DLQ, from orders)", "3 (0 ok)", "orders-worker", "2026-11-16 18:05:37.429 SQS's clock", "2026-11-16 18:06:00", "Age", "7 min"]) assert.ok(t.includes(s), `missing ${s}: ${t}`);
  assert.ok(html.includes('href="/message?id=m-2"'));
  assert.equal(fmtTime(null), null);
  assert.deepEqual([fmtAge(30), fmtAge(600), fmtAge(9000), fmtAge(200_000), fmtAge(null)], ["30 s", "10 min", "2.5 h", "2.3 days", "?"]);
});

const empty = (e: Parameters<typeof EmptyState>[0] | null) => (e ? text(renderToStaticMarkup(createElement(EmptyState, e))) : "");
const link = (e: Parameters<typeof EmptyState>[0] | null) => e?.link?.href;

test("empty states while no span is stored point to setup", () => {
  assert.equal(empty(stuckEmpty(300, true)), "No messages yet. A message shows up here when it's older than 300 s and has no final delete. Set up your first service");
  assert.equal(empty(duplicatesEmpty({}, true)), "No messages yet. Duplicates show up once a service sends spans. Set up your first service");
  assert.equal(empty(tenantsEmpty()), "No messages yet. Tenants show up once a service sends spans with a tenant set. Set up your first service");
  assert.equal(empty(lookupEmpty("abc", true)), "No message with the ID abc has reached Trailbeam. No service has sent spans here yet. Set up your first service");
  assert.equal(empty(messagesEmpty({ queue: "q" }, { empty: true })), "No messages yet. No service has sent spans here yet. Set up your first service");
  for (const e of [stuckEmpty(1, true), duplicatesEmpty({}, true), tenantsEmpty(), lookupEmpty("x", true)]) assert.equal(link(e), "/");
});

test("empty states once spans exist", () => {
  assert.equal(empty(stuckEmpty(300, false)), "No stuck messages. Every message first seen more than 300 s ago has a final delete.");
  assert.equal(empty(duplicatesEmpty({}, false)), "No duplicate deliveries. No message was received more than once or handled more than once.");
  const byReason = duplicatesEmpty({ reason: "succeeded_more_than_once", queue: "q" }, false);
  assert.equal(empty(byReason), 'No duplicate deliveries for "handled more than once". Show every reason');
  assert.equal(link(byReason), "/duplicates?queue=q");
  const q = messagesEmpty({ queue: "orders", tenant: "acme", limit: "10", cursor: "c" }, { empty: false });
  assert.ok(empty(q).startsWith("No messages on orders. Trailbeam has seen messages, none on this queue."));
  assert.equal(link(q), "/?tenant=acme&limit=10", "clears only the queue (and the cursor)");
  const ten = messagesEmpty({ tenant: "acme" }, { empty: false });
  assert.ok(empty(ten).startsWith("No messages for the tenant acme.") && link(ten) === "/");
  assert.equal(empty(messagesEmpty({ body: " x " }, { empty: false, storesBodies: true })), "No message body matches x");
  assert.equal(messagesEmpty({ body: "x" }, { empty: false, storesBodies: false }), null);
  assert.ok(empty(lookupEmpty("abc", false)).includes("Message IDs are the MessageId SQS returns from SendMessage, not a receipt handle."));
  assert.equal(without("/", { a: "1", b: "" }, "a"), "/");
});

test("Duplicates: the Why cell gives each reason with its count from the row", () => {
  const row = { ...msg, deliveries: 3, successful_deliveries: 2, reasons: ["received_more_than_once", "succeeded_more_than_once"] };
  const html = renderToStaticMarkup(createElement("p", null, duplicateWhy(row)));
  assert.equal(html, '<p><span>received <span class="font-mono tabular-nums">3</span> times</span>, <span>handled <span class="font-mono tabular-nums">2</span> times</span></p>');
});
