/*
  QV-398: what the Overview still derives in src/overview/derive.ts (the legacy Overview table and its tests went
  with QV-710; the Overview is OverviewFlows, tested in now-flows.test.ts). Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { fmtBytes, lambdaDeletes } from "../src/overview/derive.ts";
import type { Flow } from "../src/queue/selfhosted.ts";

const svc = (name: string, n: number, lambda = false) => ({ name, n, lambda });
const flow = (name: string, o: Partial<Flow> = {}): Flow => ({
  key: `123456789012/eu-west-1/${name}`, queue: name, name, url: null, account: "123456789012", region: "eu-west-1", lastSeen: 0,
  sent: 0, received: 0, deleted: 0, visibilityChanges: 0, producers: [], consumers: [], avgBytes: null, dlq: null, ...o,
});

test("Lambda deletes: every consumer Lambda reads By Lambda, some is a count with + Lambda", () => {
  assert.equal(lambdaDeletes(flow("payments.fifo", { consumers: [svc("ledger", 1002, true)] })), "all");
  assert.equal(lambdaDeletes(flow("orders", { consumers: [svc("orders-worker", 4388)] })), "none");
  assert.equal(lambdaDeletes(flow("x", { consumers: [svc("w", 3), svc("fn", 4, true)] })), "mixed");
  assert.equal(lambdaDeletes(flow("x", { consumers: [svc("w", 3), svc("fn", 0, true)] })), "none", "a consumer that received nothing doesn't count");
  assert.equal(lambdaDeletes(flow("audit")), "none", "no consumer at all");
});

test("sizes: bytes, then KB, MB and GB in binary steps", () => {
  assert.equal(fmtBytes(812), "812 B");
  assert.equal(fmtBytes(1536), "1.5 KB");
  assert.equal(fmtBytes(5 * 1024 ** 2), "5.0 MB");
  assert.equal(fmtBytes(3 * 1024 ** 3), "3.00 GB");
});
