/*
  The Messages list (`/`, row 4 of the one-UI plan): the shared MessagesView (@trailbeam/ui/explorer), built from the Event
  explorer's parts, in the shell's frame, on GET /api/messages. The URL is the page's state:
    queue, tenant, version   one each, as the query bar's queue:, tenant: and version: terms
    body                     the body search's terms (body:<text>, body.<path>:<value>), as the bar's body terms
    range=1h|4h|1d|...       a range that ends now, on when a message was last seen; with none, any time
    from, to                 a fixed range, as Copy link and Next page pin it (ISO 8601 in UTC, or Unix milliseconds)
    refresh=15               ask again every 15 s (Live)
    cursor, limit            the page, as the API takes them
  Links from before the shared list open what they named: range=24h, from_ms and to_ms, and a body search's since_s.
  A body search reads one window (a day unless the range says otherwise), so with Any time it searches the last day.
  Words with no field in the query bar are a message ID: the page opens that message.
*/
import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { Pause, Play } from "lucide-react";
import {
  EmptyState, listQuery, messageQueryLanguage, messagesEmpty, queryOfParams, storeIsEmpty, type ListQuery, type MessageSummary,
} from "../src/index.ts";
import { Tooltip } from "../src/design/index.ts";
import { MessagesView, TimePicker } from "../src/explorer/index.ts";
import { ANY_TIME, anyTime, custom, follow, parseRange, pinned, preset, PRESETS, type TimeRange } from "../src/model/index.ts";
import { CopyLink, getHealth, getJSON, messageHref } from "./chrome.tsx";
import { ShellFrame } from "./frame.tsx";
import { qs, useRefresh } from "./lists.tsx";

/** What GET /api/messages answers (contract/openapi.yaml, MessagePage). */
export type MessagePage = {
  messages: MessageSummary[];
  next_cursor: string | null;
  body_search?: { stores_bodies: boolean; from_ms: number; to_ms: number; scanned: number; searched_from_ms: number | null; complete: boolean };
};

/** The filters GET /api/messages takes, as the query bar's language. */
export const MESSAGES_LANGUAGE = messageQueryLanguage(["queue", "tenant", "version", "body"]);
const FILTERS = ["queue", "tenant", "version", "body"] as const;

const ms = (v: string | null) => (!v ? NaN : /^\d+$/.test(v) ? Number(v) : /^\d{4}-\d{2}-\d{2}T/.test(v) ? Date.parse(/Z$|[+-]\d{2}:?\d{2}$/.test(v) ? v : `${v}Z`) : NaN);
const SINCE: Record<string, string> = { "3600": "1h", "86400": "1d", "604800": "7d", "2592000": "30d" };

/** The URL's range at `now`: a fixed from and to (or the older from_ms, to_ms), a range that ends now, or any time. */
export function messagesTime(search: string, now: number): TimeRange {
  const p = new URLSearchParams(search);
  const from = ms(p.get("from") ?? p.get("from_ms")), to = ms(p.get("to") ?? p.get("to_ms"));
  // an older link may give only one end: the other is the start of history, or now
  if (Number.isFinite(from) || Number.isFinite(to)) {
    const f = Number.isFinite(from) ? from : 0, t = Number.isFinite(to) ? to + (p.has("to_ms") ? 1 : 0) : now;
    if (f < t) return custom(f, t);
  }
  const r = p.get("range") === "24h" ? "1d" : p.get("range") ?? (p.get("body") ? SINCE[p.get("since_s") ?? ""] : undefined);
  if (r && PRESETS.some((x) => x.id === r)) return preset(r, now);
  const typed = r ? parseRange(r, now) : null;
  return typed?.preset ? typed : anyTime(now);
}

/** The window the list is asked for: a body search reads one window, a day when the range is any time. */
export const searchTime = (t: TimeRange, body: boolean): TimeRange => (body && t.preset === ANY_TIME ? preset("1d", t.to) : t);

/** GET /api/messages' query: the filters, the range's from_ms and to_ms (both included), the page. */
export function messagesApiQuery(list: ListQuery, t: TimeRange, page: { cursor?: string; limit?: string } = {}): Record<string, string> {
  const out: Record<string, string> = {};
  for (const f of FILTERS) if (list[f]) out[f] = list[f]!;
  if (t.preset !== ANY_TIME) { out.from_ms = String(Math.floor(t.from)); out.to_ms = String(Math.ceil(t.to) - 1); }
  if (page.limit) out.limit = page.limit;
  if (page.cursor) out.cursor = page.cursor;
  return out;
}

/** The page's link: the filters, the range (none for any time), and the rest as given. */
export function messagesHref(list: ListQuery, t: TimeRange, rest: Record<string, string> = {}): string {
  const p: Record<string, string> = {};
  for (const f of FILTERS) if (list[f]) p[f] = list[f]!;
  if (t.preset && t.preset !== ANY_TIME) p.range = t.preset;
  else if (!t.preset) { p.from = new Date(t.from).toISOString(); p.to = new Date(t.to).toISOString(); }
  const q = qs({ ...p, ...rest });
  return q ? `/?${q}` : "/";
}

const listOf = (params: Record<string, string>): ListQuery => Object.fromEntries(FILTERS.filter((f) => params[f]?.trim()).map((f) => [f, params[f].trim()]));

const C = ({ children }: { children: ReactNode }) => <code className="font-mono text-caption">{children}</code>;
const utc = (v: number | null) => (v === null ? "unknown" : new Date(v).toISOString().replace("T", " ").replace(/\.\d+Z$/, " UTC"));

const NoBodies = () => (
  <p className="max-w-[80ch] border-b border-line px-5 py-2.5 text-[13px] text-ink-2" data-no-bodies>
    This server keeps no message bodies, so a body search finds nothing. To keep them, set <C>TRAILBEAM_STORE_BODIES</C> on the server and <C>TRAILBEAM_CAPTURE_BODY_QUEUES</C> in the package.{" "}
    <a className="text-brand hover:underline" href="https://github.com/nick-lennox/trailbeam#message-bodies">Message bodies</a>
  </p>
);

/** What a body search read, under the query bar. */
export function bodyNote(b: NonNullable<MessagePage["body_search"]>, noBodies: boolean): ReactNode {
  return (
    <div className="mt-1.5 grid gap-1 font-sans text-[13px] text-ink-3">
      <p className="max-w-[80ch]">Searches only the bodies this server keeps: those of queues in <C>TRAILBEAM_STORE_BODIES</C>, as the package redacted them. Anyone who can open this page can read them.</p>
      {b.stores_bodies && b.scanned > 0 && (
        <p>Read the bodies of <span className="font-mono">{b.scanned.toLocaleString("en-US")}</span> messages last seen between {utc(b.searched_from_ms)} and {utc(b.to_ms)}{b.complete ? "." : `. The next page reads older ones, back to ${utc(b.from_ms)}.`}</p>
      )}
      {!b.stores_bodies && !noBodies && <p>This server keeps no message bodies, so there&rsquo;s nothing to search.</p>}
    </div>
  );
}

type State = { kind: "loading" } | { kind: "error"; message: string } | { kind: "ok"; page: MessagePage; storeEmpty: boolean; at: number; asked: string };

const BUTTON = "inline-flex h-10 items-center rounded-lg border border-line-strong bg-surface px-3 text-[13px] text-ink-2 hover:text-ink";

/** The list: the whole page (`/`), or `embedded` in a card under the first-run screen once the first message arrives. */
export function MessagesPanel({ embedded = false }: { embedded?: boolean }) {
  const params = useMemo(() => Object.fromEntries(new URLSearchParams(embedded ? "" : location.search)), [embedded]);
  const list = useMemo(() => listOf(params), [params]);
  const query = useMemo(() => queryOfParams(list, MESSAGES_LANGUAGE), [list]);
  const [time, setTime] = useState<TimeRange>(() => messagesTime(embedded ? "" : location.search, Date.now()));
  const { limit } = params;
  // a new range starts again from the newest page
  const [cursor, setCursor] = useState<string | undefined>(params.cursor);
  const pickTime = (t: TimeRange) => { setCursor(undefined); setTime(t); };
  const [live, setLive, tick] = useRefresh(params.refresh === "15");
  const following = live && !!time.preset && !cursor;
  const [state, setState] = useState<State>({ kind: "loading" });
  // /api/health's store_bodies: [] when the server keeps none (without a session, health doesn't say: leave the search on)
  const [noBodies, setNoBodies] = useState(false);
  useEffect(() => {
    let on = true;
    getHealth(5000).then((h) => on && setNoBodies(Array.isArray(h.config?.store_bodies) && h.config.store_bodies.length === 0), () => {});
    return () => { on = false; };
  }, []);

  useEffect(() => { if (tick && following) setTime((t) => follow(t, Date.now())); }, [tick]);
  const asks = searchTime(time, !!list.body);
  const asked = `/api/messages?${qs(messagesApiQuery(list, asks, { cursor, limit }))}`.replace(/\?$/, "");
  // a range that ends now moves on each tick, which changes the ask; any time asks the same again
  const again = asks.preset === ANY_TIME ? tick : 0;
  const n = useRef(0);
  useEffect(() => {
    const me = ++n.current;
    const at = Date.now();
    getJSON<MessagePage>(asked)
      .then(async (page) => {
        // an empty list says something different while no service has sent anything (design QV-146, section 5.1)
        const storeEmpty = page.messages.length === 0 ? await getHealth(5000).then(storeIsEmpty, () => false) : false;
        if (me === n.current) setState({ kind: "ok", page, storeEmpty, at, asked });
      })
      .catch((e: Error) => me === n.current && setState({ kind: "error", message: e.message }));
  }, [asked, again]);

  // the URL follows the range (a range that ends now keeps its link as it moves up)
  const keep = { ...(limit && { limit }), ...(live && { refresh: "15" }) };
  const link = messagesHref(list, time, { ...keep, ...(cursor && { cursor }) });
  useEffect(() => { if (!embedded) history.replaceState(history.state, "", link); }, [link, embedded]);

  const onQuery = (q: string) => {
    const { params: next } = listQuery(q, MESSAGES_LANGUAGE);
    // a word with no field is a message ID: open it (the message page says when there's none)
    if (next.q) { location.assign(messageHref(next.q)); return; }
    location.assign(messagesHref(next, time, keep));
  };

  const ok = state.kind === "ok" ? state : null;
  const page = ok?.page;
  const b = page?.body_search;
  // Next page keeps the window of the rows on screen, so its rows follow on from these: a body search's, or the one asked
  const nextWindow = (p: MessagePage): TimeRange =>
    p.body_search ? custom(p.body_search.from_ms, p.body_search.to_ms + 1) : asks.preset === ANY_TIME ? asks : pinned(asks);
  // Copy link pins the same rows: any time is up to when they were asked
  const pinFor = (p: MessagePage, at: number): TimeRange => (!p.body_search && asks.preset === ANY_TIME ? custom(0, at) : nextWindow(p));
  const pager = page && (
    <>
      {cursor && <a className="text-brand hover:underline" href={messagesHref(list, time, keep)}>Newest</a>}
      {page.next_cursor && <a className="text-brand hover:underline" href={messagesHref(list, nextWindow(page), { ...keep, cursor: page.next_cursor })}>Next page</a>}
    </>
  );

  const emptyState = () => {
    if (!ok) return state.kind === "error" ? <p role="alert">{state.message}</p> : <p>Loading messages</p>;
    const e = messagesEmpty(params, { empty: ok.storeEmpty, storesBodies: b?.stores_bodies });
    if (!e) return <p>No messages.</p>;
    if (ok.storeEmpty || asks.preset === ANY_TIME) return <EmptyState {...e} />;
    const sentence = <>Only messages last seen {asks.preset ? `in the ${asks.label.toLowerCase()}` : `between ${utc(asks.from)} and ${utc(asks.to)}`} are listed.</>;
    return <EmptyState {...e} detail={<>{e.detail && <>{e.detail} </>}{sentence}</>} link={e.link ?? { href: messagesHref(list, anyTime(Date.now()), keep), label: "Show any time" }} />;
  };

  const queues = useMemo(() => [...new Set(page?.messages.flatMap((m) => [m.source_queue, m.current_queue]).filter(Boolean) ?? [])].sort(), [page]);
  const versions = useMemo(() => [...new Set(page?.messages.flatMap((m) => m.consumers.map((c) => c.service_version ?? "")).filter(Boolean) ?? [])].sort(), [page]);

  const controls = (
    <div className="flex h-10 shrink-0 items-center gap-2">
      <TimePicker any time={time} setTime={pickTime} now={Date.now()} />
      {ok && <CopyLink design link={() => messagesHref(list, pinFor(ok.page, ok.at), { ...(limit && { limit }), ...(cursor && { cursor }) })} />}
      <Tooltip content={!time.preset ? "A fixed range in the past does not update" : cursor ? "A later page does not update" : following ? "Stop asking for new messages" : "Ask for new messages every 15 s"} className="inline-flex">
        <button type="button" onClick={() => setLive(!live)} disabled={!time.preset || !!cursor} aria-pressed={following}
          className={`${BUTTON} w-[92px] justify-center gap-2 disabled:opacity-50 disabled:hover:text-ink-2`}>
          {following ? <><span className="live-dot size-1.5 rounded-full bg-good" /> Live <Pause size={13} className="text-ink-3" /></> : <>Paused <Play size={13} className="text-ink-3" /></>}
        </button>
      </Tooltip>
    </div>
  );

  const view = (
    <MessagesView page={!embedded} messages={page?.messages ?? []} query={query} onQuery={onQuery} lang={MESSAGES_LANGUAGE} messageHref={messageHref}
      values={(f) => (f === "queue" ? queues : f === "version" ? versions : undefined)}
      controls={controls} notice={noBodies && <NoBodies />} queryNote={b && bodyNote(b, noBodies)} pager={pager} empty={emptyState()}
      summary={ok && page!.messages.length > 0 && <>
        <b className="font-mono font-medium text-ink">{page!.messages.length.toLocaleString("en-US")}</b> {page!.messages.length === 1 ? "message" : "messages"}
        {b ? " whose body matches, most recently seen first" : ", most recently active first"}{cursor ? ", further back" : ""}
      </>} />
  );
  const foot = (
    <p className={`shrink-0 text-xs text-ink-3 ${embedded ? "mt-3" : "border-t border-line px-5 py-2"}`}>
      Times are in UTC{ok && <>, as of {new Date(ok.at).toISOString().slice(11, 19)}</>}. The same data as JSON:{" "}
      <a className="font-mono text-brand hover:underline" href={ok?.asked ?? asked}>/api/messages</a>
    </p>
  );
  if (embedded) return <section><h2 className="mb-3 font-serif text-[1.25rem] text-ink">Recent messages</h2>{view}{foot}</section>;
  return <div className="flex h-screen flex-col"><h1 className="sr-only">{list.body ? "Messages whose body matches" : "Messages"}</h1>{view}{foot}</div>;
}

export function MessagesPage() {
  useEffect(() => { document.title = "Messages · Trailbeam"; }, []);
  return <ShellFrame path="/"><MessagesPanel /></ShellFrame>;
}
