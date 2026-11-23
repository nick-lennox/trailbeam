/*
  The list pages: Messages (`/`, as a list), Duplicates and Stuck, drawn with the shared MessagesTable. Each is a
  plain GET form over its API, so the URL is the page's state and Next page is a link, as on the HTML pages.
*/
import { useEffect, useState, type ReactNode } from "react";
import {
  DUPLICATE_REASONS, EmptyState, duplicateWhy, MessagesTable, duplicatesEmpty, fmtAge, fmtThreshold, messagesEmpty, parseBodyQuery, storeIsEmpty, stuckEmpty,
  type Column, type EmptyStateProps, type MessageSummary,
} from "../src/index.ts";
import { Frame, Json, getHealth, getJSON, messageHref } from "./chrome.tsx";

type Page = {
  messages: MessageSummary[];
  next_cursor: string | null;
  older_than_s?: number;
  body_search?: { stores_bodies: boolean; from_ms: number; to_ms: number; scanned: number; searched_from_ms: number | null; complete: boolean };
};

type Field = { name: string; label: string; kind: "text" | "number" | [string, string][]; placeholder?: string; disabled?: boolean };

const pick = (params: Record<string, string>, names: string[]) => Object.fromEntries(names.filter((n) => params[n]).map((n) => [n, params[n]]));
const qs = (p: Record<string, string>) => new URLSearchParams(Object.entries(p).filter(([, v]) => v !== "")).toString();
const ms = (v: number | null) => (v === null ? "-" : new Date(v).toISOString().replace("T", " ").replace(/\.\d+Z$/, " UTC"));

function Filters({ action, values, fields }: { action: string; values: Record<string, string>; fields: Field[] }) {
  const input = "rounded-full border border-sky-line bg-sky-2 px-3 py-1 font-mono text-caption text-sky-ink";
  return (
    <form method="get" action={action} className="mt-4 flex flex-wrap items-end gap-3 text-sm text-sky-ink-2">
      {fields.map((f) => (
        <label key={f.name} className="flex flex-col gap-1">
          <span>{f.label}</span>
          {Array.isArray(f.kind) ? (
            <select name={f.name} defaultValue={values[f.name] ?? ""} className={input}>
              {f.kind.map(([v, t]) => <option key={v} value={v}>{t}</option>)}
            </select>
          ) : (
            <input name={f.name} type={f.kind} defaultValue={values[f.name] ?? ""} placeholder={f.placeholder} disabled={f.disabled}
              className={`${input} ${f.kind === "number" ? "w-24" : f.placeholder ? "w-72" : "w-44"} placeholder:text-sky-ink-3 disabled:cursor-not-allowed disabled:opacity-60`} />
          )}
        </label>
      ))}
      <button className="rounded-full border border-sky-line px-4 py-1 text-sky-ink hover:border-sky-ink">Apply</button>
    </form>
  );
}

type ListState = { kind: "loading" } | { kind: "error"; message: string } | { kind: "ok"; page: Page; storeEmpty: boolean };

type ListProps = {
  /** the page's path, for the form and Next page */
  action: string;
  /** the API path, under /api */
  api: string;
  /** what goes to the API from the URL */
  params: Record<string, string>;
  fields: Field[];
  /** the form's values when the URL has none */
  defaults?: Record<string, string>;
  extra?: Column;
  /** above the list, from the answer (body search notes) */
  notes?: (page: Page) => ReactNode;
  empty: (page: Page, storeEmpty: boolean) => EmptyStateProps | null;
  /** the query for Next page, from the answer; the URL's filters with the cursor by default */
  next?: (page: Page) => Record<string, string>;
  /** after "Times are in UTC." in the footer */
  foot?: ReactNode;
  /** told each answer, for what the page around the list says */
  onPage?: (page: Page) => void;
  /** under the filters, before any answer */
  below?: ReactNode;
  /** what's wrong with the URL's filters, said instead of asking the API */
  invalid?: ReactNode;
};

export function MessageList({ action, api, params, fields, defaults = {}, extra, notes, empty, next, foot, onPage, below, invalid }: ListProps) {
  const [state, setState] = useState<ListState>({ kind: "loading" });
  const query = qs(params);
  useEffect(() => {
    if (invalid) return;
    let live = true;
    getJSON<Page>(`/api/${api}${query ? `?${query}` : ""}`)
      .then(async (page) => {
        // an empty list says something different while no service has sent anything (design, section 5.1)
        const storeEmpty = page.messages.length === 0 ? await getHealth(5000).then(storeIsEmpty, () => false) : false;
        if (!live) return;
        setState({ kind: "ok", page, storeEmpty });
        onPage?.(page);
      })
      .catch((e: Error) => live && setState({ kind: "error", message: e.message }));
    return () => { live = false; };
  }, [api, query]);

  const values = { ...defaults, ...params };
  return (
    <>
      <Filters key={qs(values)} action={action} values={values} fields={fields} />
      {below}
      <div className="mt-5">
        {invalid ? <p className="text-sky-ink-2" role="alert">{invalid}</p> : <>
        {state.kind === "loading" && <p className="text-sky-ink-3">Loading messages</p>}
        {state.kind === "error" && <p className="text-sky-ink-2" role="alert">{state.message}</p>}
        {state.kind === "ok" && (
          <>
            {notes?.(state.page)}
            {state.page.messages.length === 0 ? (
              (() => { const e = empty(state.page, state.storeEmpty); return e && <EmptyState {...e} />; })()
            ) : (
              <div className="border-y border-sky-line md:overflow-x-auto md:rounded-[16px] md:border md:bg-sky-2/55">
                <MessagesTable messages={state.page.messages} href={messageHref} extra={extra} />
              </div>
            )}
            <p className="mt-3 text-sm text-sky-ink-3">
              {state.page.next_cursor && (
                <><a className="text-dawn hover:underline" href={`${action}?${qs({ ...(next ? next(state.page) : params), cursor: state.page.next_cursor })}`}>Next page</a> · </>
              )}
              Times are in UTC. {foot && <>{foot} </>}<Json href={`/api/${api}${query ? `?${query}` : ""}`} />
            </p>
          </>
        )}
        </>}
      </div>
    </>
  );
}

/** Inline `code` in a sentence: the words the parsers write (badBodyTerm in src/body/search.ts). */
const withCode = (s: string) => s.split("`").map((part, i) => (i % 2 ? <code key={i} className="font-mono text-caption">{part}</code> : part));

/** What ?body= can't read, said before any request: the same rules the server applies. */
function bodyQueryProblem(body: string | undefined): ReactNode {
  if (!body?.trim()) return null;
  try { parseBodyQuery(body); return null; } catch (e) { return withCode((e as Error).message); }
}

const NoBodies = () => (
  <p className="mt-3 max-w-[80ch] text-sm text-sky-ink-2">
    This server keeps no message bodies, so there&rsquo;s nothing to search. To keep them, set <code className="font-mono text-caption">TRAILBEAM_STORE_BODIES</code> on the server and <code className="font-mono text-caption">TRAILBEAM_CAPTURE_BODY_QUEUES</code> in the package.{" "}
    <a className="text-dawn hover:underline" href="https://github.com/nick-lennox/trailbeam#message-bodies">Message bodies</a>
  </p>
);

const WINDOWS: [string, string][] = [["3600", "an hour"], ["86400", "a day"], ["604800", "a week"], ["2592000", "30 days"]];

/** The Messages list: recent messages, filtered by queue or tenant, or with body= those whose kept body matches. */
export function MessagesList({ params, heading = "h1" }: { params: Record<string, string>; heading?: "h1" | "h2" }) {
  const searching = Boolean(params.body?.trim());
  const H = heading;
  // /api/health's store_bodies; [] when the server keeps none (without a session, health doesn't say: leave the field on)
  const [noBodies, setNoBodies] = useState(false);
  useEffect(() => {
    let live = true;
    getHealth(5000).then((h) => live && setNoBodies(Array.isArray(h.config?.store_bodies) && h.config.store_bodies.length === 0), () => {});
    return () => { live = false; };
  }, []);
  const fields: Field[] = [{ name: "body", label: "Body", kind: "text", placeholder: "ord-1003 or body.order_id:ord-1003", disabled: noBodies }, { name: "queue", label: "Queue", kind: "text" }, { name: "tenant", label: "Tenant", kind: "text" }, { name: "limit", label: "Limit", kind: "number" }];
  if (searching) fields.splice(2, 0, { name: "since_s", label: "Last seen within", kind: WINDOWS });
  return (
    <section>
      <H className={heading === "h1" ? "text-[1.75rem] text-sky-ink" : "text-[1.25rem] text-sky-ink"}>{searching ? "Messages whose body matches" : "Recent messages"}</H>
      <MessageList action="/" api="messages" fields={fields} defaults={searching ? { since_s: "86400" } : {}}
        below={noBodies && <NoBodies />} invalid={bodyQueryProblem(params.body)}
        params={pick(params, ["limit", "queue", "tenant", "body", "cursor", ...(searching ? ["since_s", "from_ms", "to_ms"] : [])])}
        notes={(p) => p.body_search && (
          <div className="mb-4 grid gap-2 text-[15px] leading-[1.5] text-sky-ink-2">
            <p className="max-w-[80ch] text-sm text-sky-ink-3">Searches only the bodies this server keeps: those of queues in <code className="font-mono text-caption">TRAILBEAM_STORE_BODIES</code>, as the package redacted them. Anyone who can open this page can read them.</p>
            {!p.body_search.stores_bodies ? (
              !noBodies && <NoBodies />
            ) : p.body_search.scanned > 0 && (
              <p className="text-sm text-sky-ink-3">Read the bodies of <span className="font-mono">{p.body_search.scanned.toLocaleString("en-US")}</span> messages last seen between {ms(p.body_search.searched_from_ms)} and {ms(p.body_search.to_ms)}{p.body_search.complete ? "." : `; the next page reads older ones, back to ${ms(p.body_search.from_ms)}.`}</p>
            )}
          </div>
        )}
        empty={(p, storeEmpty) => messagesEmpty(params, { empty: storeEmpty, storesBodies: p.body_search?.stores_bodies })}
        next={(p) => {
          if (!p.body_search) return pick(params, ["limit", "queue", "tenant"]);
          const { since_s: _, cursor: __, ...rest } = params;
          return { ...rest, from_ms: String(p.body_search.from_ms), to_ms: String(p.body_search.to_ms) };
        }} />
    </section>
  );
}

export function DuplicatesPage() {
  const params = Object.fromEntries(new URLSearchParams(location.search));
  useEffect(() => { document.title = "Duplicate deliveries · Trailbeam"; }, []);
  const reasons: [string, string][] = [["", "any reason"], ...Object.entries(DUPLICATE_REASONS)];
  return (
    <Frame>
      <h1 className="text-[1.75rem] text-sky-ink">Duplicate deliveries</h1>
      <p className="mt-1 max-w-[80ch] text-sky-ink-2">Messages SQS returned more than once, or that more than one consumer handled. A second receive is expected after your own <code className="font-mono text-caption">ChangeMessageVisibility</code> or a failed attempt. It is worth a look when the visibility timeout expired while a consumer was still working, or when a message was handled twice.</p>
      <MessageList action="/duplicates" api="duplicates" params={pick(params, ["limit", "queue", "reason", "cursor"])}
        fields={[{ name: "reason", label: "Reason", kind: reasons }, { name: "queue", label: "Queue", kind: "text" }, { name: "limit", label: "Limit", kind: "number" }]}
        extra={{ title: "Why", cell: duplicateWhy }}
        empty={(_, storeEmpty) => duplicatesEmpty(params, storeEmpty)} />
    </Frame>
  );
}

export function StuckPage() {
  const params = Object.fromEntries(new URLSearchParams(location.search));
  const [olderThan, setOlderThan] = useState<number | null>(null);
  const asked = params.older_than_s !== undefined && /^\d+$/.test(params.older_than_s) ? Number(params.older_than_s) : undefined;
  const threshold = olderThan ?? asked;
  useEffect(() => { document.title = "Stuck messages · Trailbeam"; }, []);
  return (
    <Frame>
      <h1 className="text-[1.75rem] text-sky-ink">Stuck messages</h1>
      <p className="mt-1 max-w-[80ch] text-sky-ink-2">
        Messages first seen more than {threshold === undefined ? "?" : fmtThreshold(threshold)} ago that no consumer has deleted yet, oldest first. Change the age with Older than, or the server default with <code className="font-mono text-caption">--stuck-after</code> (<code className="font-mono text-caption">TRAILBEAM_STUCK_AFTER_S</code>).
      </p>
      <MessageList action="/stuck" api="stuck" params={pick(params, ["older_than_s", "limit", "queue", "cursor"])}
        defaults={olderThan !== null ? { older_than_s: String(olderThan) } : {}}
        fields={[{ name: "older_than_s", label: "Older than (s)", kind: "number" }, { name: "queue", label: "Queue", kind: "text" }, { name: "limit", label: "Limit", kind: "number" }]}
        extra={{ title: "Age", align: "right", brief: (m) => `${fmtAge(m.age_s)} old${m.age_across_clocks ? " (two clocks)" : ""}`, cell: (m) => (
          <span className="font-mono text-caption tabular-nums">{fmtAge(m.age_s)}{m.age_across_clocks && <span className="text-sky-ink-3" title="this server's clock minus SQS's SentTimestamp: two clocks"> two clocks</span>}</span>
        ) }}
        foot={<>A message that a recorded PurgeQueue call removed is not listed here; one whose last step was during a purge (or within 60 s after it) is listed, marked possibly purged. A message can also leave SQS without a delete when its queue&rsquo;s retention period (<code className="font-mono text-caption">MessageRetentionPeriod</code>) ends; SQS reports nothing when that happens, so Trailbeam cannot see it and the message stays here until Trailbeam&rsquo;s own retention removes it. Age is this server&rsquo;s clock now minus the send&rsquo;s time on the sender&rsquo;s clock, so skew between the two clocks shows up in it. If Trailbeam never saw the send, it counts from SQS&rsquo;s <code className="font-mono text-caption">SentTimestamp</code>, and the age says so; with neither, from the first receive, on the consumer&rsquo;s clock.</>}
        onPage={(p) => p.older_than_s !== undefined && setOlderThan(p.older_than_s)}
        empty={(p, storeEmpty) => stuckEmpty(p.older_than_s ?? 0, storeEmpty)} />
    </Frame>
  );
}
