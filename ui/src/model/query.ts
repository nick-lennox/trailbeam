import type { Ev } from "./events.ts";
import { bodyHit, bodyPath, bodyValues, isBodyField as isBody, numHit, textHit, NUM, RANGE } from "../body/search.ts";

/* The Event explorer's query language. Terms are field:value, joined with AND.
   Several terms on the same field are alternatives. A leading minus excludes.
   * is a wildcard. Numbers compare with >, >=, < and <=, and a..b is a range
   that includes a and stops before b. Bare text matches a message ID or the detail.

   The message body is searched by JSON path: body.customer:cus_acme_01,
   body.amount:>=10000, body.Records.eventName:ObjectCreated*. A path through an
   array matches if any element does; body.Records.0.key picks one. body:text
   finds text anywhere in the body. Only a body that Trailbeam stored can match: the
   queue needs payload capture on, and the search sees the body as stored, after
   redaction. On a workspace's own data, bodies reach the explorer only for owners
   and admins, and each sits on the one event whose span carried it.

   Pure: what an event's attributes and body are comes from QueryLookups. By default they are the ones it carries
   (Ev.attrs, Ev.body); a host with sample events passes its own (queryLanguage).

   Every field that can be filtered on or shown as a column is in FIELD_DEFS.
   Attributes keep their SQS and Lambda names behind a prefix, for example
   queue.VisibilityTimeout:30s or message.ApproximateReceiveCount:>=3. */

export type Group = "Event" | "Message attributes" | "Queue attributes" | "Event source mapping";
export type Kind = "text" | "number" | "duration" | "bytes";
/* buckets: for a measured quantity, the edges (in ms or bytes) its filter groups values by.
   sampleOnly: the sample data has it, a workspace's own events never do. Spans carry no queue settings or event
   source mapping, and the explorer doesn't read them from AWS, so on own data such a filter would match nothing:
   it is hidden there, and a query that names it gets an error (live-events.ts says what own events carry). */
export type FieldDef = { name: string; label: string; group: Group; kind: Kind; facet?: boolean; buckets?: number[]; sampleOnly?: boolean; get: (e: Ev) => string | number | undefined };

/** Where an event's attributes and stored body are read from. */
export type QueryLookups = { attr: (e: Ev, name: string) => string | number | undefined; body: (e: Ev) => unknown };

/** The attributes and body a recorded event carries (eventsOfTimeline puts them there). */
export const RECORDED: QueryLookups = { attr: (e, name) => e.attrs?.[name], body: (e) => e.body };

/* the paths seen in stored bodies, for suggestions while typing */
export function bodyPathsOf(bodies: unknown[]): string[] {
  const seen = new Set<string>();
  const walk = (node: unknown, at: string) => {
    if (Array.isArray(node)) node.forEach((x) => walk(x, at));
    else if (node && typeof node === "object") for (const [k, v] of Object.entries(node)) walk(v, at ? `${at}.${k}` : k);
    else if (at) seen.add(at);
  };
  bodies.forEach((b) => walk(b, ""));
  return [...seen].sort();
}

export type Field = string;
export const GROUPS: Group[] = ["Event", "Message attributes", "Queue attributes", "Event source mapping"];
export type Term = { raw: string; start: number; end: number; neg: boolean; field?: Field; value: string; error?: string };
const quote = (v: string) => (/[\s()]/.test(v) ? `"${v}"` : v);
export const has = (terms: Term[], field: Field, value: string) => terms.some((t) => !t.neg && t.field === field && t.value === value);

const dist = (a: string, b: string) => {
  const d = Array.from({ length: a.length + 1 }, (_, i) => [i, ...Array(b.length).fill(0)]);
  for (let j = 1; j <= b.length; j++) d[0][j] = j;
  for (let i = 1; i <= a.length; i++) for (let j = 1; j <= b.length; j++) d[i][j] = Math.min(d[i - 1][j] + 1, d[i][j - 1] + 1, d[i - 1][j - 1] + (a[i - 1] === b[j - 1] ? 0 : 1));
  return d[a.length][b.length];
};

/** The query language over events whose attributes and bodies `look` reads. */
export function queryLanguage(look: QueryLookups = RECORDED) {
  const { attr } = look;
  const bodyOf = look.body;
  const A = (name: string, group: Group, kind: Kind, facet = true): FieldDef => ({ name, label: name.slice(name.indexOf(".") + 1), group, kind, facet, get: (e) => attr(e, name) });
  const S = (name: string, group: Group, kind: Kind): FieldDef => ({ ...A(name, group, kind), sampleOnly: true });

  const FIELD_DEFS: FieldDef[] = [
    { name: "status", label: "Status", group: "Event", kind: "text", facet: true, get: (e) => e.status },
    { name: "event", label: "Event", group: "Event", kind: "text", facet: true, get: (e) => e.event },
    /* the messaging system (QV-308): aws.sqs or rabbitmq; offered only when some event in range is not SQS's (filledColumns) */
    { name: "system", label: "System", group: "Event", kind: "text", facet: true, get: (e) => e.system ?? "aws.sqs" },
    { name: "queue", label: "Queue", group: "Event", kind: "text", facet: true, get: (e) => e.queue },
    { name: "service", label: "Service", group: "Event", kind: "text", facet: true, get: (e) => e.service },
    { name: "producer", label: "Producer", group: "Event", kind: "text", facet: true, get: (e) => e.producer },
    { name: "consumer", label: "Consumer", group: "Event", kind: "text", facet: true, get: (e) => e.consumer },
    { name: "tenant", label: "Tenant", group: "Event", kind: "text", facet: true, get: (e) => e.tenant },
    { name: "source", label: "Source", group: "Event", kind: "text", facet: true, get: (e) => e.source },
    { name: "receive_count", label: "Receive count", group: "Event", kind: "number", facet: true, get: (e) => e.receiveCount },
    { name: "message_id", label: "Message ID", group: "Event", kind: "text", get: (e) => e.messageId },
    /* Measured quantities. Each is its own column and its own filter, grouped into ranges. */
    { name: "body_size", label: "Body size", group: "Event", kind: "bytes", facet: true, buckets: [1024, 4096, 16384, 65536], get: (e) => e.sizeBytes },
    { name: "processing_time", label: "Processing time", group: "Event", kind: "duration", facet: true, buckets: [100, 500, 1000, 5000], get: (e) => e.durationMs },
    { name: "time_in_queue", label: "Time in queue", group: "Event", kind: "duration", facet: true, buckets: [100, 1000, 10_000, 60_000], get: (e) => e.waitMs },
    A("message.ApproximateReceiveCount", "Message attributes", "number"),
    A("message.MessageGroupId", "Message attributes", "text"),
    A("message.tenant", "Message attributes", "text"),
    A("message.BodySize", "Message attributes", "bytes", false),
    A("queue.account", "Queue attributes", "text"),
    A("queue.region", "Queue attributes", "text"),
    S("queue.FifoQueue", "Queue attributes", "text"),
    S("queue.VisibilityTimeout", "Queue attributes", "duration"),
    S("queue.MessageRetentionPeriod", "Queue attributes", "number"),
    S("queue.DelaySeconds", "Queue attributes", "duration"),
    S("queue.ReceiveMessageWaitTimeSeconds", "Queue attributes", "duration"),
    S("queue.MaximumMessageSize", "Queue attributes", "bytes"),
    S("queue.ContentBasedDeduplication", "Queue attributes", "text"),
    S("queue.RedrivePolicy.maxReceiveCount", "Queue attributes", "number"),
    S("queue.RedrivePolicy.deadLetterTargetArn", "Queue attributes", "text"),
    S("mapping.BatchSize", "Event source mapping", "number"),
    S("mapping.MaximumBatchingWindowInSeconds", "Event source mapping", "duration"),
    S("mapping.ReportBatchItemFailures", "Event source mapping", "text"),
  ];
  const FIELDS = FIELD_DEFS.map((f) => f.name);
  /** The fields the events can have: all of them on the sample data, those not sampleOnly on a workspace's own. */
  const fieldsFor = (live = false) => (live ? FIELD_DEFS.filter((f) => !f.sampleOnly) : FIELD_DEFS);
  const DEF = new Map(FIELD_DEFS.map((f) => [f.name.toLowerCase(), f]));
  /* earlier names, still accepted in a query or a saved column list */
  const ALIAS: Record<string, string> = { size: "body_size", duration: "processing_time", handler_duration: "processing_time" };
  /* body and body.<path> are fields too, made on demand: any path can be asked for. Keys keep their case. */
  const BODY = new Map<string, FieldDef>();
  /* body.* paths and matching are open-source @trailbeam/ui's, the same rules self-hosted Trailbeam searches with */
  function bodyDef(name: string): FieldDef {
    const field = `body${name.slice(4)}`;
    if (!BODY.has(field)) {
      const path = bodyPath(field);
      /* the whole body as one line of JSON, or the first value at a path */
      BODY.set(field, { name: field, label: path.length ? field : "Body", group: "Event", kind: "text", get: (e) => {
        const body = bodyOf(e);
        if (!path.length) return body === undefined ? undefined : JSON.stringify(body);
        const v = bodyValues(body, path)[0];
        return typeof v === "boolean" ? String(v) : v;
      } });
    }
    return BODY.get(field)!;
  }
  const def = (name: string) => (isBody(name) ? bodyDef(name) : DEF.get(ALIAS[name.toLowerCase()] ?? name.toLowerCase()));

  /* live: the query runs on a workspace's own events, which never have a sampleOnly field */
  function parse(q: string, live = false): Term[] {
    const terms: Term[] = [];
    const re = /(?:[^\s"]+|"[^"]*"?)+/g;
    let m: RegExpExecArray | null;
    while ((m = re.exec(q))) {
      const raw = m[0];
      const neg = raw.startsWith("-") && raw.length > 1;
      const body = neg ? raw.slice(1) : raw;
      const i = body.indexOf(":");
      const t: Term = { raw, start: m.index, end: m.index + raw.length, neg, value: body.replace(/"/g, "") };
      if (i > 0 && !body.startsWith('"')) {
        const name = body.slice(0, i);
        t.value = body.slice(i + 1).replace(/"/g, "");
        const d = def(name);
        if (d) {
          t.field = d.name;
          /* the earlier name for this event kind, still accepted */
          if (d.name === "event" && t.value.toLowerCase() === "handler_error") t.value = "processing_error";
        } else {
          const low = name.toLowerCase();
          const near = [...FIELDS].sort((a, b) => dist(low, a.toLowerCase()) - dist(low, b.toLowerCase()))[0];
          t.error = `There is no field called ${name}.${dist(low, near.toLowerCase()) <= 3 ? ` Did you mean ${near}?` : ""}`;
        }
        if (d && d.kind !== "text" && t.value && !NUM.test(t.value) && !RANGE.test(t.value)) t.error = `${d.name} takes a number, for example ${d.name}:>=2.`;
        if (d?.sampleOnly && live) t.error = `${d.name} isn't recorded on your own events, so it would match nothing.`;
      }
      terms.push(t);
    }
    return terms;
  }

  const get = (e: Ev, f: Field): string | number | undefined => def(f)?.get(e);

  /* A value as it is written in a query and listed in a facet: 30000 ms is 30s, 262144 bytes is 256KB. */
  function token(f: Field, v: string | number): string {
    const k = def(f)?.kind;
    if (typeof v !== "number") return v;
    if (k === "duration") return v % 1000 === 0 ? `${v / 1000}s` : `${v}ms`;
    if (k === "bytes") return v % 1024 === 0 && v >= 1024 ? `${v / 1024}KB` : `${v}B`;
    return String(v);
  }

  /* A quantity's ranges, in order: the query value for each and the words a filter shows.
     Edges 1024 and 4096 bytes give <1KB, 1KB..4KB, >=4KB. */
  function ranges(f: FieldDef): { value: string; label: string; lo: number; hi: number }[] {
    const e = f.buckets ?? [];
    const words = (n: number) => token(f.name, n).replace(/(\d)([a-zA-Z])/, "$1 $2");
    return [
      { value: `<${token(f.name, e[0])}`, label: `Under ${words(e[0])}`, lo: -Infinity, hi: e[0] },
      ...e.slice(1).map((hi, i) => ({ value: `${token(f.name, e[i])}..${token(f.name, hi)}`, label: `${words(e[i])} to ${words(hi)}`, lo: e[i], hi })),
      { value: `>=${token(f.name, e[e.length - 1])}`, label: `${words(e[e.length - 1])} or more`, lo: e[e.length - 1], hi: Infinity },
    ];
  }

  function hit(e: Ev, t: Term): boolean {
    if (!t.field) {
      const s = t.value.toLowerCase();
      return !s || e.messageId.includes(s) || e.detail.toLowerCase().includes(s);
    }
    if (isBody(t.field)) {
      const body = bodyOf(e);
      if (body === undefined) return false; // no stored body: payload capture is off for this queue
      return bodyHit(body, t.field, t.value);
    }
    const d = def(t.field)!;
    const v = d.get(e);
    if (!t.value) return true; // still being typed
    if (d.kind !== "text") return v === undefined ? false : numHit(v as number, t.value) ?? false;
    return textHit(v, t.value);
  }

  /* skip: ignore positive terms on this field, used to count a facet's own values. */
  function matches(e: Ev, terms: Term[], skip?: Field): boolean {
    const byField = new Map<string, Term[]>();
    for (const t of terms) {
      if (t.error) continue;
      if (t.neg) { if (hit(e, t)) return false; continue; }
      if (!t.field) { if (!hit(e, t)) return false; continue; }
      if (t.field === skip) continue;
      byField.set(t.field, [...(byField.get(t.field) ?? []), t]);
    }
    for (const ts of byField.values()) if (!ts.some((t) => hit(e, t))) return false;
    return true;
  }

  function toggle(q: string, field: Field, value: string): string {
    const t = parse(q).find((x) => !x.neg && x.field === field && x.value === value);
    if (t) return (q.slice(0, t.start) + q.slice(t.end)).replace(/\s+/g, " ").trim();
    return `${q.trim()} ${field}:${quote(value)}`.trim();
  }

  /* Replace every term on a field with one value. Used when the map sets the filter. */
  function setOnly(q: string, fields: Field[], field: Field, value: string): string {
    const kept = parse(q).filter((t) => !(t.field && fields.includes(t.field))).map((t) => t.raw);
    return [...kept, `${field}:${quote(value)}`].join(" ");
  }

  return { FIELD_DEFS, FIELDS, GROUPS, fieldsFor, def, parse, get, token, ranges, matches, has, toggle, setOnly };
}

export type QueryLanguage = ReturnType<typeof queryLanguage>;
