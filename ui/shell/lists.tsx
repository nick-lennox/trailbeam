/*
  The list pages: Messages (`/`, as a list), Duplicates and Stuck, drawn with the shared MessagesTable. Each is a
  plain GET form over its API, so the URL is the page's state and Next page is a link, as on the HTML pages.
*/
import { useEffect, useState, type FormEvent, type ReactNode } from "react";
import {
  DUPLICATE_REASONS, EmptyState, duplicateWhy, MessagesTable, duplicatesEmpty, fmtAge, fmtThreshold, messagesEmpty, parseBodyQuery, storeIsEmpty, stuckEmpty,
  RANGE_PARAMS, RANGE_PRESETS, REFRESH_S, apiQuery, formQuery, pinnedQuery, rangeText, rangeWindow, readRange, refreshOn, toInputUtc,
  type Column, type EmptyStateProps, type MessageSummary, type TimeRange,
} from "../src/index.ts";
import { CopyLink, Frame, Json, getHealth, getJSON, messageHref } from "./chrome.tsx";

type Page = {
  messages: MessageSummary[];
  next_cursor: string | null;
  older_than_s?: number;
  body_search?: { stores_bodies: boolean; from_ms: number; to_ms: number; scanned: number; searched_from_ms: number | null; complete: boolean };
};

type Field = { name: string; label: string; kind: "text" | "number" | [string, string][]; placeholder?: string; disabled?: boolean };

/** A list's time range: what the window is on (`what`, for the sentences) and whether its API takes to_ms. */
type RangeOpt = { what: "last seen" | "first seen"; upper: boolean };

const pick = (params: Record<string, string>, names: string[]) => Object.fromEntries(names.filter((n) => params[n]).map((n) => [n, params[n]]));
const qs = (p: Record<string, string>) => new URLSearchParams(Object.entries(p).filter(([, v]) => v !== "")).toString();
const ms = (v: number | null) => (v === null ? "-" : new Date(v).toISOString().replace("T", " ").replace(/\.\d+Z$/, " UTC"));
const withQuery = (path: string, p: Record<string, string>) => { const q = qs(p); return q ? `${path}?${q}` : path; };
const input = "rounded-full border border-sky-line bg-sky-2 px-3 py-1 font-mono text-caption text-sky-ink";

/** The range control: presets, all time, or custom (two UTC times; Stuck takes only a start). */
function RangeFields({ range, opt }: { range: TimeRange; opt: RangeOpt }) {
  const initial = range.kind === "preset" ? range.preset : range.kind;
  const [kind, setKind] = useState<string>(initial === "all" ? "" : initial);
  const custom: { from_ms?: number; to_ms?: number } = range.kind === "custom" ? range : {};
  return (
    <>
      <label className="flex flex-col gap-1">
        <span>{opt.what === "first seen" ? "First seen" : "Last seen"}</span>
        <select name="range" value={kind} onChange={(e) => setKind(e.target.value)} className={input}>
          <option value="">any time</option>
          {RANGE_PRESETS.map(([k, label]) => <option key={k} value={k}>{label.toLowerCase()}</option>)}
          <option value="custom">custom</option>
        </select>
      </label>
      {kind === "custom" && <>
        <label className="flex flex-col gap-1">
          <span>From (UTC)</span>
          <input name="from" type="datetime-local" defaultValue={custom.from_ms !== undefined ? toInputUtc(custom.from_ms) : ""} className={input} />
        </label>
        {opt.upper && (
          <label className="flex flex-col gap-1">
            <span>To (UTC)</span>
            <input name="to" type="datetime-local" defaultValue={custom.to_ms !== undefined ? toInputUtc(custom.to_ms) : ""} className={input} />
          </label>
        )}
      </>}
    </>
  );
}

function Filters({ action, values, fields, range, refresh }: { action: string; values: Record<string, string>; fields: Field[]; range?: { r: TimeRange; opt: RangeOpt }; refresh: boolean }) {
  // the form's fields become the URL (formQuery: a custom range turns into from_ms/to_ms); refresh stays as it is
  const submit = (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    const q = formQuery([...new FormData(e.currentTarget).entries()].map(([k, v]) => [k, String(v)] as [string, string]));
    location.assign(withQuery(action, { ...q, ...(refresh && { refresh: String(REFRESH_S) }) }));
  };
  return (
    <form method="get" action={action} onSubmit={submit} className="mt-4 flex flex-wrap items-end gap-3 text-sm text-sky-ink-2">
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
      {range && <RangeFields range={range.r} opt={range.opt} />}
      <button className="rounded-full border border-sky-line px-4 py-1 text-sky-ink hover:border-sky-ink">Apply</button>
    </form>
  );
}

/** Ask again every REFRESH_S seconds while on (and the tab is showing); on/off is kept in the URL as refresh=15. */
function useRefresh(initial: boolean): [boolean, (on: boolean) => void, number] {
  const [on, setOn] = useState(initial);
  const [tick, setTick] = useState(0);
  useEffect(() => {
    if (!on) return;
    const id = setInterval(() => { if (!document.hidden) setTick((t) => t + 1); }, REFRESH_S * 1000);
    const back = () => { if (!document.hidden) setTick((t) => t + 1); };
    document.addEventListener("visibilitychange", back);
    return () => { clearInterval(id); document.removeEventListener("visibilitychange", back); };
  }, [on]);
  const set = (v: boolean) => {
    setOn(v);
    const u = new URL(location.href);
    if (v) u.searchParams.set("refresh", String(REFRESH_S)); else u.searchParams.delete("refresh");
    history.replaceState(history.state, "", u);
  };
  return [on, set, tick];
}

type ListState = { kind: "loading" } | { kind: "error"; message: string } | { kind: "ok"; page: Page; storeEmpty: boolean; at: number; asked: string };

type ListProps = {
  /** the page's path, for the form and Next page */
  action: string;
  /** the API path, under /api */
  api: string;
  /** what goes to the API from the URL (with `range`, the URL's range=/from_ms/to_ms too: they become from_ms/to_ms) */
  params: Record<string, string>;
  fields: Field[];
  /** the form's values when the URL has none */
  defaults?: Record<string, string>;
  extra?: Column;
  /** the list has a time range control */
  range?: RangeOpt;
  /** above the list, from the answer (body search notes) */
  notes?: (page: Page) => ReactNode;
  empty: (page: Page, storeEmpty: boolean) => EmptyStateProps | null;
  /** the query for Next page, from the answer; the URL's filters with the cursor by default */
  next?: (page: Page) => Record<string, string>;
  /** the query Copy link pins, from the answer; by default the URL's, with the range made absolute (pinnedQuery) */
  pin?: (page: Page) => Record<string, string>;
  /** after "Times are in UTC." in the footer */
  foot?: ReactNode;
  /** told each answer, for what the page around the list says */
  onPage?: (page: Page) => void;
  /** under the filters, before any answer */
  below?: ReactNode;
  /** what's wrong with the URL's filters, said instead of asking the API */
  invalid?: ReactNode;
};

const clock = (t: number) => new Date(t).toISOString().slice(11, 19);

export function MessageList({ action, api, params, fields, defaults = {}, extra, range, notes, empty, next, pin, foot, onPage, below, invalid }: ListProps) {
  const [state, setState] = useState<ListState>({ kind: "loading" });
  const read = range ? readRange(params) : ({ kind: "all" } as TimeRange);
  const r: TimeRange = "error" in read ? { kind: "all" } : read;
  const problem = invalid ?? ("error" in read ? read.error : null);
  const [refresh, setRefresh, tick] = useRefresh(refreshOn(params));
  const key = qs(params);
  useEffect(() => {
    if (problem) return;
    let live = true;
    // a preset range ends now: worked out again for each ask, so a refresh moves the window along
    const now = Date.now();
    const { refresh: _, ...rest } = params;
    const q = qs(range ? apiQuery(params, r, now, range.upper) : rest);
    const asked = `/api/${api}${q ? `?${q}` : ""}`;
    getJSON<Page>(asked)
      .then(async (page) => {
        // an empty list says something different while no service has sent anything (design, section 5.1)
        const storeEmpty = page.messages.length === 0 ? await getHealth(5000).then(storeIsEmpty, () => false) : false;
        if (!live) return;
        setState({ kind: "ok", page, storeEmpty, at: now, asked });
        onPage?.(page);
      })
      // a failed refresh says so, in place of the rows it could not bring up to date
      .catch((e: Error) => live && setState({ kind: "error", message: e.message }));
    return () => { live = false; };
  }, [api, key, tick, Boolean(problem)]);

  const own = Object.fromEntries(Object.entries(params).filter(([k]) => !(RANGE_PARAMS as readonly string[]).includes(k)));
  const values = { ...defaults, ...(range ? own : params) };
  const keep: Record<string, string> = refresh ? { refresh: String(REFRESH_S) } : {};
  // Next page keeps the window of the rows on screen, as absolute times, so its rows follow on from these
  const windowAt = (at: number) => (range ? Object.fromEntries(Object.entries(rangeWindow(r, at, { upper: range.upper })).map(([k, v]) => [k, String(v)])) : {});
  const emptyFor = (page: Page, storeEmpty: boolean) => {
    const e = empty(page, storeEmpty);
    if (!e || storeEmpty || r.kind === "all") return e;
    const sentence = <>Only messages {range!.what} {rangeText(r)} are listed.</>;
    return { ...e, detail: <>{e.detail && <>{e.detail} </>}{sentence}</>, link: e.link ?? { href: withQuery(action, { ...own, ...keep, cursor: "" }), label: "Show any time" } };
  };
  return (
    <>
      <Filters key={qs(values)} action={action} values={values} fields={fields} range={range && { r, opt: range }} refresh={refresh} />
      <div className="mt-3 flex flex-wrap items-center gap-4 text-sm text-sky-ink-2">
        <label className="inline-flex items-center gap-2">
          <input type="checkbox" checked={refresh} onChange={(e) => setRefresh(e.target.checked)} />
          Refresh every {REFRESH_S} s
        </label>
        {state.kind === "ok" && <>
          <span className="text-sky-ink-3">As of {clock(state.at)} UTC</span>
          <CopyLink link={() => withQuery(action, pin ? pin(state.page) : pinnedQuery(params, r, state.at, range?.upper ?? true))} />
        </>}
      </div>
      {below}
      <div className="mt-5">
        {problem ? <p className="text-sky-ink-2" role="alert">{problem}</p> : <>
        {state.kind === "loading" && <p className="text-sky-ink-3">Loading messages</p>}
        {state.kind === "error" && <p className="text-sky-ink-2" role="alert">{state.message}</p>}
        {state.kind === "ok" && (
          <>
            {notes?.(state.page)}
            {state.page.messages.length === 0 ? (
              (() => { const e = emptyFor(state.page, state.storeEmpty); return e && <EmptyState {...e} />; })()
            ) : (
              <div className="border-y border-sky-line md:overflow-x-auto md:rounded-[16px] md:border md:bg-sky-2/55">
                <MessagesTable messages={state.page.messages} href={messageHref} extra={extra} />
              </div>
            )}
            <p className="mt-3 text-sm text-sky-ink-3">
              {state.page.next_cursor && (
                <><a className="text-dawn hover:underline" href={withQuery(action, { ...(next ? next(state.page) : range ? own : params), ...windowAt(state.at), ...keep, cursor: state.page.next_cursor })}>Next page</a> · </>
              )}
              Times are in UTC. {foot && <>{foot} </>}<Json href={state.asked} />
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
        params={pick(params, ["limit", "queue", "tenant", "body", "cursor", "refresh", "from_ms", "to_ms", ...(searching ? ["since_s"] : ["range"])])}
        range={searching ? undefined : { what: "last seen", upper: true }}
        pin={searching ? (p) => {
          // a body search pins the window it read (the answer's from_ms/to_ms), whatever since_s said
          const { since_s: _, refresh: __, range: ___, ...rest } = params;
          return p.body_search ? { ...rest, from_ms: String(p.body_search.from_ms), to_ms: String(p.body_search.to_ms) } : rest;
        } : undefined}
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
      <MessageList action="/duplicates" api="duplicates" params={pick(params, ["limit", "queue", "reason", "cursor", "refresh", "range", "from_ms", "to_ms"])}
        range={{ what: "last seen", upper: true }}
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
      <MessageList action="/stuck" api="stuck" params={pick(params, ["older_than_s", "limit", "queue", "cursor", "refresh", "range", "from_ms"])}
        range={{ what: "first seen", upper: false }}
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
