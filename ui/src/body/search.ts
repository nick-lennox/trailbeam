/*
  body.* search (contract/body-view.md): body:text finds text anywhere in the body, and
  body.<path>:<value> compares the values at a JSON path. The hosted app's explorer runs these on each
  event's body; self-hosted Trailbeam runs the same rules in Python (server/bodies.py). Both are
  checked against the term cases in contract/fixtures/body-views.json.
*/

export const isBodyField = (name: string) => /^body(\.|\[|$)/i.test(name);

/** body.Records[0].key -> ["Records", "0", "key"] */
export const bodyPath = (field: string) => field.slice(4).replace(/\[(\d+)\]/g, ".$1").split(".").filter(Boolean);

/** Every scalar at a path. An array on the way is searched element by element; a number picks one. Only a body's own keys count. */
export function bodyValues(node: unknown, path: string[]): (string | number | boolean)[] {
  if (node === null || node === undefined) return [];
  if (Array.isArray(node)) return path.length && /^\d+$/.test(path[0]) ? bodyValues(node[+path[0]], path.slice(1)) : node.flatMap((x) => bodyValues(x, path));
  if (!path.length) return typeof node === "object" ? [] : [node as string | number | boolean];
  if (typeof node !== "object" || !Object.hasOwn(node, path[0])) return [];
  return bodyValues((node as Record<string, unknown>)[path[0]], path.slice(1));
}

const UNIT: Record<string, number> = { ms: 1, s: 1000, m: 60_000, b: 1, kb: 1024, mb: 1024 ** 2 };
export const NUM = /^(>=|<=|>|<)?(\d+(?:\.\d+)?)(ms|s|m|b|kb|mb)?$/i;
export const RANGE = /^(\d+(?:\.\d+)?)(ms|s|m|b|kb|mb)?\.\.(\d+(?:\.\d+)?)(ms|s|m|b|kb|mb)?$/i;

/** A number against a query value such as >=3, 30s or 1kb..4kb; null if the value is not numeric. */
export function numHit(x: number, value: string): boolean | null {
  const r = RANGE.exec(value);
  if (r) return x >= +r[1] * (UNIT[(r[2] ?? "").toLowerCase()] ?? 1) && x < +r[3] * (UNIT[(r[4] ?? "").toLowerCase()] ?? 1);
  const m = NUM.exec(value);
  if (!m) return null;
  const n = +m[2] * (UNIT[(m[3] ?? "").toLowerCase()] ?? 1);
  return m[1] === ">" ? x > n : m[1] === ">=" ? x >= n : m[1] === "<" ? x < n : m[1] === "<=" ? x <= n : x === n;
}

/** Text, ignoring case; * is a wildcard. */
export function textHit(v: unknown, value: string): boolean {
  const s = String(v ?? "").toLowerCase();
  const want = value.toLowerCase();
  if (want.includes("*")) return new RegExp(`^${want.split("*").map((p) => p.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).join(".*")}$`).test(s);
  return s === want;
}

/** A stored body as search reads it: parsed JSON, or the text itself. */
export function parsedBody(body: string, format: string | null): unknown {
  if (format !== "text") { try { return JSON.parse(body); } catch { /* not JSON after all */ } }
  return body;
}

/** One body term against a parsed body. field: body or body.<path>; an empty value matches (it is still being typed). */
export function bodyHit(body: unknown, field: string, value: string): boolean {
  if (!value) return true;
  const path = bodyPath(field);
  if (!path.length) return JSON.stringify(body).toLowerCase().includes(value.toLowerCase());
  return bodyValues(body, path).some((v) => (typeof v === "number" ? numHit(v, value) ?? textHit(v, value) : textHit(v, value)));
}

export type BodyTerm = { field: string; value: string; neg: boolean };

/** The words for a term ?body= can't read, the same on the server (server/bodies.py). */
export const badBodyTerm = (raw: string) => `Can't read \`${raw}\`. Use text, or \`body.<path>:<value>\`.`;

/**
  A query of body terms, as self-hosted Trailbeam's ?body= takes it. Plain text with no field (ord-1003, or
  "call me" in quotes) is body:<text>. Throws on a term for another field (queue:orders).
*/
export function parseBodyQuery(q: string): BodyTerm[] {
  const terms: BodyTerm[] = [];
  for (const m of q.matchAll(/(?:[^\s"]+|"[^"]*"?)+/g)) {
    const raw = m[0];
    const neg = raw.startsWith("-") && raw.length > 1;
    const term = neg ? raw.slice(1) : raw;
    const i = term.indexOf(":");
    const name = i > 0 && !term.startsWith('"') ? term.slice(0, i) : "";
    if (!name) {
      const value = term.replace(/"/g, "");
      if (value) terms.push({ field: "body", value, neg });
      continue;
    }
    if (!isBodyField(name)) throw new Error(badBodyTerm(raw));
    terms.push({ field: `body${name.slice(4)}`, value: term.slice(i + 1).replace(/"/g, ""), neg });
  }
  if (!terms.length) throw new Error("the body search is empty");
  return terms;
}

/** Terms on one field are alternatives, different fields must all match, and a leading minus excludes. */
export function bodyMatches(body: unknown, terms: BodyTerm[]): boolean {
  const byField = new Map<string, BodyTerm[]>();
  for (const t of terms) {
    if (t.neg) { if (bodyHit(body, t.field, t.value)) return false; continue; }
    byField.set(t.field, [...(byField.get(t.field) ?? []), t]);
  }
  for (const ts of byField.values()) if (!ts.some((t) => bodyHit(body, t.field, t.value))) return false;
  return true;
}
