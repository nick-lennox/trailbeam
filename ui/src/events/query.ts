/*
  The Event explorer's query language, on the fields GET /api/events filters by. Terms are field:value, joined with AND:

    queue:orders            the steps on one queue (its name)
    service:orders-worker   the steps whose span's service.name is this
    kind:delete             send, receive, delete, change_visibility or outcome (a Lambda outcome); several kind terms are
                            alternatives, and -kind:receive leaves a kind out
    message_id:m-1          one message's steps; a bare word is a message ID too

  It keeps only what spans carry and the API can filter on. The hosted explorer's earlier fields that spans don't carry
  (tenant, payload size, processing time) or that the API doesn't filter on yet are named in an error, so a saved link
  that uses them says why it shows more than it did.
*/
import { STEP_KINDS, type StepKind } from "./types.ts";

export type QueryField = "queue" | "service" | "kind" | "message_id";
export const QUERY_FIELDS: QueryField[] = ["queue", "service", "kind", "message_id"];

export type Term = { raw: string; start: number; end: number; neg: boolean; field: QueryField | null; value: string; error?: string };

/** What the query asks the API for, the terms it was read from, and the first thing wrong with it. */
export type ParsedQuery = { terms: Term[]; params: Partial<Record<QueryField, string>>; error?: string };

export const KIND_LABELS: Record<StepKind, string> = {
  send: "Send",
  receive: "Receive",
  delete: "Delete",
  change_visibility: "Visibility change",
  outcome: "Lambda outcome",
};

const FIELD_ALIASES: Record<string, QueryField> = {
  queue: "queue", service: "service", service_name: "service", kind: "kind", event: "kind", step: "kind", message_id: "message_id", id: "message_id",
};
const KIND_ALIASES: Record<string, StepKind> = {
  send: "send", sent: "send", receive: "receive", received: "receive", delete: "delete", deleted: "delete",
  change_visibility: "change_visibility", visibility: "change_visibility", visibility_change: "change_visibility", outcome: "outcome", lambda: "outcome",
};
/* fields the earlier, sample-data explorer had: why each isn't here now */
const NOT_RECORDED = ["tenant", "body_size", "size", "processing_time", "duration", "handler_duration", "time_in_queue", "producer", "consumer", "source"];
const NOT_FILTERED = ["status", "result", "outcome", "receive_count", "version", "service_version", "error_type", "error_code"];

const unquote = (s: string) => s.replace(/"/g, "");

function fieldError(name: string): string {
  const low = name.toLowerCase();
  if (NOT_RECORDED.includes(low)) return `Spans don't record ${low} yet, so the explorer can't filter on it.`;
  if (NOT_FILTERED.includes(low)) return `The explorer can't filter on ${low} yet. Filter on queue, service, kind or message_id.`;
  if (/^body(\.|$)/.test(low)) return "The explorer doesn't search message bodies. A message's Body tab shows its stored body.";
  if (/^(message|queue|mapping)\./.test(low)) return `Spans don't record ${name}, so the explorer can't filter on it.`;
  return `There is no field called ${name}. Filter on queue, service, kind or message_id.`;
}

/** Read a query. The terms keep their place in the text, for editing it. */
export function parseQuery(q: string): ParsedQuery {
  const terms: Term[] = [];
  const re = /(?:[^\s"]+|"[^"]*"?)+/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(q))) {
    const raw = m[0];
    const neg = raw.startsWith("-") && raw.length > 1;
    const body = neg ? raw.slice(1) : raw;
    const i = body.indexOf(":");
    const at = { raw, start: m.index, end: m.index + raw.length, neg };
    if (i <= 0 || body.startsWith('"')) {
      terms.push({ ...at, field: "message_id", value: unquote(body) });
      continue;
    }
    const name = body.slice(0, i);
    const value = unquote(body.slice(i + 1));
    const field = FIELD_ALIASES[name.toLowerCase()];
    if (!field) { terms.push({ ...at, field: null, value, error: fieldError(name) }); continue; }
    const t: Term = { ...at, field, value };
    if (field === "kind") {
      const k = KIND_ALIASES[value.toLowerCase()];
      if (value && !k) t.error = `kind is one of ${STEP_KINDS.join(", ")}, not “${value}”.`;
      else if (k) t.value = k;
    } else if (value.includes("*")) t.error = `${field} takes a whole value: the explorer doesn't match with *.`;
    else if (neg) t.error = `The explorer can't leave out a ${field === "message_id" ? "message" : field} yet: only -kind: works.`;
    terms.push(t);
  }

  const params: ParsedQuery["params"] = {};
  let error = terms.find((t) => t.error)?.error;
  for (const f of ["queue", "service", "message_id"] as const) {
    const values = [...new Set(terms.filter((t) => t.field === f && !t.neg && !t.error && t.value).map((t) => t.value))];
    if (values.length > 1) error ??= `The explorer filters on one ${f === "message_id" ? "message" : f} at a time, not ${values.length}.`;
    else if (values.length) params[f] = values[0];
  }
  const kinds = terms.filter((t) => t.field === "kind" && !t.error && t.value);
  const wanted = new Set(kinds.filter((t) => !t.neg).map((t) => t.value));
  const left = new Set(kinds.filter((t) => t.neg).map((t) => t.value));
  if (wanted.size || left.size) {
    const chosen = STEP_KINDS.filter((k) => (wanted.size ? wanted.has(k) : true) && !left.has(k));
    if (!chosen.length) error ??= "The query leaves out every kind of step, so it would match nothing.";
    else if (chosen.length < STEP_KINDS.length) params.kind = chosen.join(",");
  }
  return error ? { terms, params, error } : { terms, params };
}

/** The kinds the query keeps, in STEP_KINDS order: every kind when it names none. */
export function kindsOf(parsed: ParsedQuery): StepKind[] {
  return parsed.params.kind ? (parsed.params.kind.split(",") as StepKind[]) : [...STEP_KINDS];
}

const quote = (v: string) => (/[\s"]/.test(v) ? `"${v.replace(/"/g, "")}"` : v);
const drop = (q: string, keep: (t: Term) => boolean) =>
  parseQuery(q).terms.filter(keep).map((t) => t.raw).join(" ");

/** Replace every term on a field with field:value, or remove them when value is null. */
export function setField(q: string, field: QueryField, value: string | null): string {
  const rest = drop(q, (t) => t.field !== field);
  return value === null ? rest : `${rest} ${field}:${quote(value)}`.trim();
}

/** The kind legend's click: show only this kind, or every kind again when it is already the only one shown. */
export function onlyKind(q: string, kind: StepKind): string {
  const kinds = kindsOf(parseQuery(q));
  return setField(q, "kind", kinds.length === 1 && kinds[0] === kind ? null : kind);
}
