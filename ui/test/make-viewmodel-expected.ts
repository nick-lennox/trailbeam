/*
  Writes `expected` in fixtures/viewmodel-parity.json: what the shared view-model functions make of open-source
  Trailbeam's recorded answers (test/viewmodel.ts). Run after make-viewmodel-fixture.py and the hosted app's
  scripts/record-viewmodel-answers.ts; test/viewmodel.test.ts then checks the hosted answers give the same.

    node --import tsx test/make-viewmodel-expected.ts
*/
import { readFileSync, writeFileSync } from "node:fs";
import { viewModels, type Answers } from "./viewmodel.ts";

const file = new URL("./fixtures/viewmodel-parity.json", import.meta.url);
const f = JSON.parse(readFileSync(file, "utf8")) as { nowMs: number; fromMs: number; answers: { python: Answers }; expected?: unknown };
f.expected = viewModels(f.answers.python, f.nowMs, f.fromMs);
writeFileSync(file, `${JSON.stringify(f, null, 1)}\n`);
console.log("expected:", Object.entries(f.expected as Record<string, unknown[] | object>).map(([k, v]) => `${k} ${Array.isArray(v) ? v.length : "1"}`).join(", "));
