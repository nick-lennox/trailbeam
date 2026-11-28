/*
  The People page's shared parts (src/people), rendered to HTML on the server: the controls each person gets, the
  last-admin rule, two roles only, and the wording of one-time links. Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { AddPerson, OneTimeLink, PeopleTable } from "../src/people/PeopleViews.tsx";
import {
  LAST_ADMIN, ROLES, emailedSentence, fmtWhen, isLastAdmin, linkButton, linkKind, linkSentence, notSentSentence, removeSentence, removeSsoNote, signInAdmins, unsentSentence,
} from "../src/people/People.tsx";
import type { Person } from "../src/people/types.ts";

const person = (id: number, over: Partial<Person> = {}): Person => ({
  id, email: `p${id}@example.com`, name: `P${id}`, role: "member", status: "active", created_at_ms: 1795400000000 + id, last_sign_in_ms: null, you: false, ...over,
});
const noop = () => {};
const table = (people: Person[], pending: number | null = null) =>
  renderToStaticMarkup(createElement(PeopleTable, { people, pending, onRole: noop, onDisabled: noop, onRemove: noop, onLink: noop }));
/** The HTML of one person's row. */
const row = (html: string, id: number) => {
  const at = html.indexOf(`data-person="${id}"`);
  assert.ok(at >= 0, `row ${id}`);
  const end = html.indexOf("</li>", at);
  return html.slice(at, end);
};
const disabledButton = (r: string, label: string) => new RegExp(`<button[^>]*disabled=""[^>]*>${label}</button>`).test(r);
const button = (r: string, label: string) => new RegExp(`<button[^>]*>${label}</button>`).test(r);

test("two roles only: admin and member, no viewer", () => {
  assert.deepEqual(ROLES.map(([v]) => v), ["admin", "member"]);
  const html = table([person(1, { role: "admin", you: true })]) + renderToStaticMarkup(createElement(AddPerson, { onAdd: async () => {} }));
  assert.doesNotMatch(html, /viewer/i);
  assert.equal([...html.matchAll(/<option value="(\w+)"/g)].map((m) => m[1]).join(","), "admin,member,admin,member");
  assert.match(html, /<option value="member" selected="">/, "a new person is a member unless asked");
});

test("the last admin who can sign in can't be demoted, disabled or removed", () => {
  const ada = person(1, { role: "admin", you: true });
  const ben = person(2);
  const html = table([ada, ben]);
  const a = row(html, 1);
  assert.ok(isLastAdmin([ada, ben], ada) && !isLastAdmin([ada, ben], ben));
  assert.match(a, /<select[^>]*disabled=""/, "role locked");
  assert.ok(disabledButton(a, "Disable") && disabledButton(a, "Remove"), a);
  assert.ok(a.includes(LAST_ADMIN), "the reason is in the row, not only on hover");
  assert.ok(!html.includes("title="), "no native title");
  const b = row(html, 2);
  assert.doesNotMatch(b, /disabled=""/, "a member's controls are on");

  // a second admin frees both; a disabled admin doesn't count, an invited one does (as on the server)
  const two = [ada, person(2, { role: "admin" })];
  assert.doesNotMatch(row(table(two), 1), /disabled=""/);
  assert.ok(isLastAdmin([ada, person(2, { role: "admin", status: "disabled" })], ada));
  assert.ok(!isLastAdmin([ada, person(2, { role: "admin", status: "invited" })], ada));
  assert.deepEqual(signInAdmins([ada, person(3, { role: "admin", status: "locked" })]).map((p) => p.id), [1, 3]);
});

test("each status gets its own controls and words", () => {
  const people = [person(1, { role: "admin", you: true }), person(2, { status: "invited" }), person(3, { status: "locked", last_sign_in_ms: Date.UTC(2026, 10, 24, 6, 2) }), person(4, { status: "disabled" })];
  const html = table(people);
  assert.ok(button(row(html, 2), "Make a new invite link") && linkKind(people[1]) === "invite");
  assert.ok(button(row(html, 3), "Make a reset link") && linkKind(people[2]) === "reset");
  assert.ok(button(row(html, 4), "Enable") && !/link<\/button>/.test(row(html, 4)), "no link for a disabled person (the server answers 409)");
  assert.match(row(html, 2), />Invited</);
  assert.match(row(html, 3), />Locked for now</);
  assert.match(row(html, 3), />2026-11-24 06:02 UTC</);
  assert.match(row(html, 2), />never</);
  assert.match(row(html, 1), /> \(you\)</);
  assert.equal(linkButton(people[0]), "Make a reset link");
  assert.ok(!/undefined|NaN/.test(html));
});

test("a person whose change is being sent has their controls off", () => {
  const r = row(table([person(1, { role: "admin" }), person(2, { role: "admin" })], 2), 2);
  assert.match(r, /<select[^>]*disabled=""/);
  assert.ok(disabledButton(r, "Disable") && disabledButton(r, "Remove") && disabledButton(r, "Make a reset link"));
});

test("one-time links say what they do, how long they last, and that they are shown once", () => {
  const expires = Date.UTC(2026, 11, 1, 6, 0);
  const invite = linkSentence({ name: "Ben", email: "ben@example.com" }, { kind: "invite", url: "http://x/set-password#code=abc", expires_at_ms: expires });
  assert.equal(invite, "Ben (ben@example.com)'s invite link: it lets them choose a password and sign in. It works once, until 2026-12-01 06:00 UTC. "
    + "Send it to them privately: until it is used, anyone with it can sign in as them. It isn't shown again; make a new link if it is lost.");
  const reset = linkSentence({ name: "ben@example.com", email: "ben@example.com" }, { kind: "reset", url: "u", expires_at_ms: expires });
  assert.match(reset, /^ben@example\.com's password reset link: it lets them choose a new password, and signs them out everywhere else\./);
  const html = renderToStaticMarkup(createElement(OneTimeLink, { person: { name: "Ben", email: "ben@example.com" }, link: { kind: "invite", url: "http://x/set-password#code=abc", expires_at_ms: expires }, onDismiss: noop }));
  assert.match(html, /value="http:\/\/x\/set-password#code=abc"/);
  assert.match(html, /readOnly=""|readonly=""/);
  assert.ok(button(html, "Copy link") && button(html, "Done"));
  assert.equal(fmtWhen(expires), "2026-12-01 06:00 UTC");
});

test("an emailed link is not shown, and an email that failed says why and shows the link to copy", () => {
  const expires = Date.UTC(2026, 11, 1, 6, 0);
  const ben = { name: "Ben", email: "ben@example.com" };
  const link = { kind: "invite" as const, expires_at_ms: expires };
  assert.equal(emailedSentence(ben, link, "ben@example.com"),
    "Ben's invite link was emailed to ben@example.com. It works once, until 2026-12-01 06:00 UTC. Make a new link if it doesn't arrive.");
  assert.match(emailedSentence({ name: "ben@example.com", email: "ben@example.com" }, { ...link, kind: "reset" }, "ben@example.com"),
    /^Their password reset link was emailed to ben@example\.com\./);
  const sent = renderToStaticMarkup(createElement(OneTimeLink, { person: ben, link, email: { sent: true, to: "ben@example.com", error: null }, onDismiss: noop }));
  assert.ok(!sent.includes("<input") && !button(sent, "Copy link") && button(sent, "Done"));
  const failed = { sent: false, to: "ben@example.com", error: "The mail server refused ben@example.com." };
  assert.equal(unsentSentence(failed), "The email to ben@example.com wasn't sent: The mail server refused ben@example.com. Copy the link and send it to them yourself.");
  const html = renderToStaticMarkup(createElement(OneTimeLink, { person: ben, link: { ...link, url: "http://x/set-password#code=abc" }, email: failed, onDismiss: noop }));
  assert.ok(html.includes("wasn&#x27;t sent") && button(html, "Copy link"));
  assert.match(html, /value="http:\/\/x\/set-password#code=abc"/);
  // through the proxy the server shows and emails nothing (QV-712, QV-755): the box says so, not "emailed"
  const none = renderToStaticMarkup(createElement(OneTimeLink, { person: ben, link, email: null, onDismiss: noop }));
  assert.ok(none.includes(notSentSentence(ben).replace(/'/g, "&#x27;")) && !none.includes("was emailed") && button(none, "Done"));
  const form = renderToStaticMarkup(createElement(AddPerson, { onAdd: async () => {}, sendsEmail: true }));
  assert.ok(button(form, "Add and email invite"));
});

test("removing asks first, and says what stops working", () => {
  assert.equal(removeSentence({ name: "Ben", email: "ben@example.com", you: false }),
    "Remove Ben? Their sign-ins, personal API tokens and links stop working. This can't be undone.");
  assert.match(removeSentence({ name: "Ada", email: "a@example.com", you: true }), /^Remove your own account\? You are signed out/);
});

test("removing someone who signs in with single sign-on warns that it doesn't keep them out (QV-460)", () => {
  const ben = { name: "Ben", email: "ben@example.com", you: false };
  assert.equal(removeSsoNote(ben), null, "no single sign-on: no warning");
  assert.equal(removeSsoNote({ ...ben, single_sign_on: false }), null);
  assert.equal(removeSsoNote({ ...ben, single_sign_on: true }),
    "Ben signs in with single sign-on, so removing them doesn't keep them out: at their next sign-in they come back "
    + "as a new person with the default role. To keep them out, disable them instead.");
  assert.match(removeSsoNote({ name: "", email: "ben@example.com", you: false, single_sign_on: true })!, /^ben@example\.com signs in/);
  const me = { name: "Ada", email: "a@example.com", you: true };
  assert.equal(removeSentence(me), "Remove your own account? You are signed out, and can't sign in again.");
  assert.equal(removeSentence({ ...me, single_sign_on: true }), "Remove your own account? You are signed out.",
    "they can sign in again, so don't say they can't");
  assert.match(removeSsoNote({ ...me, single_sign_on: true })!, /^You sign in with single sign-on, so removing your account doesn't keep you out/);
  // the confirmation shows the note, in a role token, only when there is one
  const src = readFileSync(new URL("../src/people/PeopleViews.tsx", import.meta.url), "utf8");
  assert.match(src, /\{removeSsoNote\(p\) && <span className="[^"]*text-warning-text"[^>]*>\{removeSsoNote\(p\)\}<\/span>\}/);
});

test("the people views use the design system's role tokens, which tokens.css defines", () => {
  const html = table([person(1, { role: "admin", you: true }), person(2, { status: "disabled" }), person(3, { status: "invited" })])
    + renderToStaticMarkup(createElement(AddPerson, { onAdd: async () => {} }))
    + renderToStaticMarkup(createElement(OneTimeLink, { person: person(2), link: { kind: "reset", url: "u", expires_at_ms: 0 }, onDismiss: noop }));
  const classes = new Set([...html.matchAll(/class="([^"]*)"/g)].flatMap((m) => m[1].split(/\s+/)));
  const theme = readFileSync(new URL("../src/design/tokens.css", import.meta.url), "utf8");
  let n = 0;
  for (const c of classes) {
    const m = /^(?:[a-z]+:)*(?:text|bg|border)-((?:ink|line|surface|brand|series|warning|critical|sky|dawn|wait|ember|lost)[a-z0-9-]*?)(?:\/\d+)?$/.exec(c);
    if (!m) continue;
    n++;
    assert.ok(!/^(sky|dawn|wait|ember|lost)/.test(m[1]), `${m[1]} is the night palette; use a role token`);
    assert.match(theme, new RegExp(`--color-${m[1]}:`), `tokens.css defines --color-${m[1]}`);
  }
  assert.ok(n >= 8, `role tokens found: ${n}`);
});
