/*
  The Messages list's query: the Event explorer's query bar (QueryInput) over a list API's filters. A list is filtered on
  the server, so the query is not matched in the browser: each term becomes a parameter of the host's list API, and a
  term that API can't take is a mistake the bar says before anything is asked.

    queue:orders          one queue (queue=)
    tenant:acme           one tenant (tenant=)
    state:dlq             one state: open, processing, deleted or dlq (state=, the hosted list's state filter)
    reason:received_more_than_once   a duplicate's reason: received_more_than_once or succeeded_more_than_once (reason=, Duplicates)
    version:1.4.3         messages a consumer on that service.version received (version=)
    trace:<trace ID>      messages with a span in that trace (trace=)
    body:text, body.<path>:value, -body...: the body search (body=, self-hosted Trailbeam), as ?body= takes it
    bare words            a message ID, a trace ID or a tenant to look up (q=); the host says what it finds

  Each host passes the fields its API takes (messageQueryLanguage). The parameters have the names both list pages used
  before the shared list, so their links still open what they named. Pure.
*/
import { isBodyField } from "../body/search.ts";
import type { Term } from "../model/query.ts";

/** A field the Messages list can filter on; each is a parameter of the same name, body is the body search's terms. */
export type ListField = "queue" | "tenant" | "state" | "reason" | "version" | "trace" | "body";

/** What the list's state filter offers, as the hosted list's state filter (lib/app/message-list.ts LIST_STATES) names them. */
export const LIST_STATES: { key: string; label: string }[] = [
  { key: "open", label: "Not deleted" },
  { key: "processing", label: "Being handled" },
  { key: "deleted", label: "Deleted" },
  { key: "dlq", label: "In the DLQ" },
];

/** What Duplicates' reason filter takes (GET /api/duplicates reason=), as DUPLICATE_REASONS names them. */
export const LIST_REASONS: { key: string; label: string }[] = [
  { key: "received_more_than_once", label: "received more than once" },
  { key: "succeeded_more_than_once", label: "handled more than once" },
];

/** The list API's parameters a query gives: one per field, and q for the words to look up. */
export type ListQuery = Partial<Record<ListField | "q", string>>;

type FieldInfo = { name: string; label: string; group: string };
const INFO: Record<Exclude<ListField, "body">, FieldInfo> = {
  queue: { name: "queue", label: "Queue", group: "Message" },
  tenant: { name: "tenant", label: "Tenant", group: "Message" },
  state: { name: "state", label: "State", group: "Message" },
  reason: { name: "reason", label: "Reason", group: "Message" },
  version: { name: "version", label: "Consumer version", group: "Consumer" },
  trace: { name: "trace", label: "Trace ID", group: "Trace" },
};
/* names the bar takes for a field, as the explorer and the hosted search write them */
const ALIAS: Record<string, string> = { "service.version": "version", trace_id: "trace" };
const ORDER: ListField[] = ["queue", "tenant", "state", "reason", "version", "trace", "body"];

/*
  A trace ID as OpenTelemetry writes it, 32 hex digits in any case (not all zeros), or a W3C traceparent header
  (00-<trace ID>-<span ID>-<flags>); lowercased, as spans store them. The hosted list's traceIdOf.
*/
export function traceIdOf(text: string | null | undefined): string | null {
  const t = (text ?? "").trim().replace(/^trace(?:_id)?:\s*/i, "");
  const m = /^(?:[0-9a-f]{2}-)?([0-9a-f]{32})(?:-[0-9a-f]{16}-[0-9a-f]{2})?$/i.exec(t);
  if (!m || /^0+$/.test(m[1])) return null;
  return m[1].toLowerCase();
}

const quote = (v: string) => (/[\s()]/.test(v) || v === "" ? `"${v}"` : v);
const list = (names: string[]) => (names.length < 2 ? names.join("") : `${names.slice(0, -1).join(", ")} or ${names[names.length - 1]}`);

/** The query bar's language over the fields `fields` (what the host's list API takes). */
export function messageQueryLanguage(fields: readonly ListField[]) {
  const take = ORDER.filter((f) => fields.includes(f));
  const body = take.includes("body");
  const defs: FieldInfo[] = take.filter((f): f is Exclude<ListField, "body"> => f !== "body").map((f) => INFO[f]);
  const names = [...defs.map((d) => d.name), ...(body ? ["body"] : [])];
  const fieldName = (name: string): string | undefined => {
    const low = name.toLowerCase();
    if (isBodyField(name)) return body ? `body${name.slice(4)}` : undefined;
    const f = ALIAS[low] ?? low;
    return defs.some((d) => d.name === f) ? f : undefined;
  };
  const def = (name: string): FieldInfo | undefined => {
    const f = fieldName(name);
    if (!f) return undefined;
    return f.startsWith("body") ? { name: f, label: f === "body" ? "Body" : f, group: "Message body" } : defs.find((d) => d.name === f);
  };
  const fieldsFor = (_live?: boolean): FieldInfo[] => defs;

  /** The query's terms, each with its field and, if the list can't take it, what is wrong. */
  function parse(q: string, _live?: boolean): Term[] {
    const terms: Term[] = [];
    const seen = new Map<string, string>();
    for (const m of q.matchAll(/(?:[^\s"]+|"[^"]*"?)+/g)) {
      const raw = m[0];
      const neg = raw.startsWith("-") && raw.length > 1;
      const word = neg ? raw.slice(1) : raw;
      const i = word.indexOf(":");
      const t: Term = { raw, start: m.index!, end: m.index! + raw.length, neg, value: word.replace(/"/g, "") };
      if (i > 0 && !word.startsWith('"')) {
        const name = word.slice(0, i);
        t.value = word.slice(i + 1).replace(/"/g, "");
        const f = fieldName(name);
        if (!f) {
          t.error = isBodyField(name)
            ? "This list can't search message bodies."
            : `There is no field called ${name}. The list filters on ${list(names)}.`;
        } else {
          t.field = f;
          if (f.startsWith("body")) { terms.push(t); continue; } // the body search takes a minus, and several terms
          if (neg) t.error = `The list can't leave out a ${f}. Remove the minus.`;
          else if (seen.has(f) && seen.get(f) !== t.value) t.error = `The list takes one ${f} at a time.`;
          else if (f === "state" && t.value && !LIST_STATES.some((s) => s.key === t.value.toLowerCase()))
            t.error = `state takes ${list(LIST_STATES.map((s) => s.key))}.`;
          else if (f === "reason" && t.value && !LIST_REASONS.some((r) => r.key === t.value.toLowerCase()))
            t.error = `reason takes ${list(LIST_REASONS.map((r) => r.key))}.`;
          else if (f === "trace" && t.value && !traceIdOf(t.value)) t.error = "trace takes a trace ID: 32 hex digits.";
          if (!t.error && t.value) seen.set(f, t.value);
        }
      } else if (neg) {
        t.error = "The list can't leave out words. Remove the minus.";
      }
      terms.push(t);
    }
    return terms;
  }

  return { fields: take, def, fieldsFor, parse };
}

export type MessageQueryLanguage = ReturnType<typeof messageQueryLanguage>;

/**
  The list API's parameters for a query, or the first mistake in it. Words with no field are joined into q, for
  the host to look up. Body terms go to body as they were typed.
*/
export function listQuery(q: string, lang: MessageQueryLanguage): { params: ListQuery; error?: string } {
  const terms = lang.parse(q);
  const bad = terms.find((t) => t.error);
  if (bad) return { params: {}, error: bad.error };
  const params: ListQuery = {};
  const words: string[] = [];
  const bodies: string[] = [];
  for (const t of terms) {
    if (!t.field) { if (t.value) words.push(t.value); continue; }
    if (t.field.startsWith("body")) { if (t.value) bodies.push(t.raw); continue; }
    if (!t.value) continue; // still being typed
    const f = t.field as ListField;
    params[f] = f === "state" || f === "reason" ? t.value.toLowerCase() : f === "trace" ? traceIdOf(t.value)! : t.value;
  }
  if (words.length) params.q = words.join(" ");
  if (bodies.length) params.body = bodies.join(" ");
  return { params };
}

/**
  The query a list's URL parameters stand for: the inverse of listQuery, for a page opened from a link. A ?body= from
  before the shared list may hold bare words (text anywhere in the body): they become body:<text>.
*/
export function queryOfParams(params: ListQuery, lang: MessageQueryLanguage): string {
  const out: string[] = [];
  for (const f of lang.fields) {
    const v = params[f]?.trim();
    if (!v) continue;
    if (f === "body") {
      for (const m of v.matchAll(/(?:[^\s"]+|"[^"]*"?)+/g)) {
        const raw = m[0];
        const neg = raw.startsWith("-") && raw.length > 1 ? "-" : "";
        const word = neg ? raw.slice(1) : raw;
        const i = word.indexOf(":");
        out.push(i > 0 && !word.startsWith('"') ? raw : `${neg}body:${quote(word.replace(/"/g, ""))}`);
      }
      continue;
    }
    out.push(`${f}:${quote(v)}`);
  }
  if (params.q?.trim()) out.push(params.q.trim());
  return out.join(" ");
}
