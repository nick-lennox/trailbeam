/*
  The account screen (QV-587): the ways in it lists for what /api/me says, and the shared AccountCard with and
  without passkeys. Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { accountWays, deviceName, withoutPerson } from "../src/account/text.ts";
import { AccountCard, type AccountCardProps } from "../src/account/AccountCard.tsx";
import type { SignInIdentity } from "../src/account/types.ts";

const text = (html: string) => html.replace(/<\/?(p|h2|h3|div|li)\b[^>]*>/g, " ").replace(/<[^>]+>/g, "").replace(/&#x27;|’/g, "'").replace(/\s+/g, " ").trim();
const gh: SignInIdentity = { provider: "github", label: "GitHub", on: true, email: "ana@example.com", last_sign_in_ms: Date.UTC(2026, 10, 27, 9, 30) };

test("a password and an account at a provider", () => {
  const ways = accountWays({ via: "session", admin: false, password: true, identities: [gh] });
  assert.deepEqual(ways.map((w) => w.title), ["Password", "GitHub"]);
  assert.match(ways[0].detail, /ask an admin for a reset link/);
  assert.equal(ways[1].value, "ana@example.com");
  assert.equal(ways[1].detail, "Last used 2026-11-27 09:30 UTC.");
  assert.ok(!ways[1].off);
  // an admin makes their own reset link
  assert.match(accountWays({ via: "session", admin: true, password: true, identities: [] })[0].detail, /People page/);
});

test("no password: the provider first, then how to get one", () => {
  const sso = { ...gh, provider: "sso" as const, label: null, on: false, last_sign_in_ms: null };
  const ways = accountWays({ via: "session", admin: false, password: false, identities: [sso] });
  assert.deepEqual(ways.map((w) => [w.title, !!w.off]), [["Single sign-on", true], ["Password", false]]);
  assert.equal(ways[0].detail, "Not used yet.");
  assert.match(ways[1].detail, /You don't have a password/);
  assert.match(accountWays({ via: "session", admin: false, password: false, identities: [] })[0].detail, /invite or reset link/);
});

test("signed in by a proxy, the proxy is the way in", () => {
  const ways = accountWays({ via: "proxy", admin: true, password: false, identities: [gh] });
  assert.equal(ways.length, 1);
  assert.match(ways[0].detail, /proxy in front of this Trailbeam/);
});

const card = (p: Partial<AccountCardProps>) => renderToStaticMarkup(createElement(AccountCard, { user: { name: "Ana", email: "ana@example.com", role: "Member" }, ...p }));

test("the card: who, the ways in, and the off one said in its row", () => {
  const html = card({ ways: accountWays({ via: "session", admin: false, password: true, identities: [{ ...gh, on: false, label: null }] }) });
  const t = text(html);
  assert.ok(t.includes("Signed in as Ana, ana@example.com, member"), t);
  assert.ok(t.includes("Ways to sign in") && t.includes("Password") && t.includes("GitHub") && t.includes("Off"), t);
  assert.ok(t.includes("doesn't offer this way in now"), "said in the row, not only on hover");
  assert.ok(!t.includes("Passkeys"), "no passkeys part without passkeys");
  assert.doesNotMatch(html, / title="/);
  assert.match(html, /font-mono[^"]*">ana@example.com/, "the email is a value, in mono");
  assert.equal(text(card({ user: null })), "Sign-in Loading");
});

test("the card's passkeys: the list, add and remove", () => {
  const html = card({ passkeys: { keys: [{ id: "k1", name: "Chrome on Mac", createdAt: "2026-11-20T10:00:00Z" }, { id: "k2", createdAt: 0 }], onAdd() {}, onRemove() {}, busy: true, error: "The passkey wasn't saved." } });
  const t = text(html);
  assert.ok(t.includes("Passkeys") && t.includes("Add a passkey") && t.includes("Chrome on Mac added 2026-11-20") && t.includes("Passkey added 1970-01-01"), t);
  assert.ok(html.includes('aria-label="Remove Chrome on Mac"') && html.includes('role="alert"'));
  assert.match(html, /<button[^>]*disabled=""[^>]*>.*Add a passkey/s, "off while an add is running");
});

test("a new passkey's name", () => {
  assert.equal(deviceName("Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Chrome/131.0 Safari/537.36"), "Chrome on Mac");
  assert.equal(deviceName("Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 Version/17.0 Mobile Safari/604.1"), "Safari on iPhone");
  assert.equal(deviceName("Mozilla/5.0 (Windows NT 10.0) Chrome/131.0 Safari/537.36 Edg/131.0"), "Edge on Windows");
  assert.equal(deviceName("curl/8"), "This device");
});

test("signed in without a person, the card says how instead of loading for ever (QV-767)", () => {
  // the README team install: TRAILBEAM_TOKEN, users and roles off
  const off = withoutPerson({ via: "shared_cookie", accounts: false });
  assert.equal(text(card({ user: null, signedIn: off.signedIn })), "Sign-in Signed in with this server's shared access token (TRAILBEAM_TOKEN), not as a person.");
  assert.match(off.accounts, /off on this server.*TRAILBEAM_ACCOUNTS=1.*trailbeam users create --accounts --email <email> --role admin/);
  // an admin made with them on may exist: say to create one only if there is none (QV-791)
  assert.doesNotMatch(off.accounts, /first admin/);
  // on, with no admin yet: no need to turn them on
  assert.doesNotMatch(withoutPerson({ via: "shared_token", accounts: true }).accounts, /TRAILBEAM_ACCOUNTS/);
  assert.match(withoutPerson({ via: "none", accounts: false }).signedIn, /needs no sign-in/);
});
