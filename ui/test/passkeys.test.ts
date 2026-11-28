/*
  The Account page's passkey helpers (shell/passkeys.ts): base64url both ways, the server's options into what
  navigator.credentials.create takes, the credential it makes back into JSON, the name a new passkey gets, and what
  the page says when the browser's part fails. Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { browserProblem, creationOptions, deviceName, fromB64url, registrationJSON, toB64url } from "../shell/passkeys.ts";

const bytes = (b: ArrayBuffer) => [...new Uint8Array(b)];

test("base64url round-trips every byte, without padding", () => {
  const all = new Uint8Array(256).map((_, i) => i);
  const s = toB64url(all);
  assert.ok(!/[+/=]/.test(s));
  assert.deepEqual(bytes(fromB64url(s)), [...all]);
  assert.equal(toB64url(new Uint8Array([0xfb, 0xff])), "-_8");
  assert.deepEqual(bytes(fromB64url("-_8")), [0xfb, 0xff]);
  assert.equal(toB64url(new Uint8Array(0)), "");
});

test("the server's options become the browser's, binary fields as bytes", () => {
  const o = creationOptions({
    rp: { id: "trailbeam.example.com", name: "Trailbeam" },
    user: { id: toB64url(new TextEncoder().encode("7")), name: "ada@example.com", displayName: "Ada" },
    challenge: toB64url(new Uint8Array([1, 2, 3])),
    pubKeyCredParams: [{ type: "public-key", alg: -7 }],
    excludeCredentials: [{ id: toB64url(new Uint8Array([9, 9])), type: "public-key", transports: ["internal"] }],
    authenticatorSelection: { residentKey: "required", userVerification: "required" },
  });
  assert.deepEqual(bytes(o.challenge as ArrayBuffer), [1, 2, 3]);
  assert.deepEqual(bytes(o.user.id as ArrayBuffer), [55]);
  assert.equal(o.user.name, "ada@example.com");
  assert.deepEqual(bytes(o.excludeCredentials![0].id as ArrayBuffer), [9, 9]);
  assert.equal(o.authenticatorSelection?.residentKey, "required");
  assert.deepEqual(creationOptions({ rp: { name: "T" }, user: { id: "", name: "a", displayName: "a" }, challenge: "", pubKeyCredParams: [] }).excludeCredentials, []);
});

test("the made passkey goes back as JSON the server reads", () => {
  const buf = (...b: number[]) => new Uint8Array(b).buffer;
  const j = registrationJSON({
    id: "AQI", rawId: buf(1, 2), type: "public-key",
    response: { clientDataJSON: buf(3), attestationObject: buf(4, 5), getTransports: () => ["internal", "hybrid"] },
    getClientExtensionResults: () => ({ credProps: { rk: true } }),
  });
  assert.deepEqual(j, {
    id: "AQI", rawId: "AQI", type: "public-key",
    response: { clientDataJSON: "Aw", attestationObject: "BAU", transports: ["internal", "hybrid"] },
    clientExtensionResults: { credProps: { rk: true } },
  });
  const bare = registrationJSON({ id: "x", rawId: buf(), type: "public-key", response: { clientDataJSON: buf(), attestationObject: buf() } });
  assert.deepEqual(bare.response.transports, []);
  assert.deepEqual(bare.clientExtensionResults, {});
});

test("a new passkey is named after the browser and the device", () => {
  const mac = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Safari/537.36";
  assert.equal(deviceName(mac), "Chrome on Mac");
  assert.equal(deviceName("Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 Version/18.0 Mobile Safari/604.1"), "Safari on iPhone");
  assert.equal(deviceName("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0 Safari/537.36 Edg/131.0"), "Edge on Windows");
  assert.equal(deviceName("Mozilla/5.0 (X11; Linux x86_64; rv:133.0) Gecko/20100101 Firefox/133.0"), "Firefox on Linux");
  assert.equal(deviceName("curl/8"), "This device");
});

test("what the page says when the browser's part fails", () => {
  const err = (name: string) => Object.assign(new Error("x"), { name });
  assert.equal(browserProblem(err("NotAllowedError")), "The passkey wasn't saved. Your browser may have canceled it.");
  assert.equal(browserProblem(err("InvalidStateError")), "This device has a passkey for your account already.");
  assert.match(browserProblem(err("SecurityError")), /public URL/);
  assert.equal(browserProblem(null), "The passkey wasn't saved. Your browser may have canceled it.");
  for (const s of [browserProblem(err("SecurityError")), browserProblem(err("InvalidStateError"))]) {
    assert.ok(!/[—→·]/.test(s), s);  // the copy rules: no em dashes, arrows or "a · b" fragments
  }
});
