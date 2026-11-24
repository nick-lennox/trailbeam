/*
  The Event explorer (/events): every recorded step in a time range, from GET /api/events, drawn with the shared view
  (../src/events). The URL is the page's state, as on the lists:
    q=<query>            the explorer's query (queue:, service:, kind:, message_id:)
    range=1h|24h|7d      a window ending now (24h when the URL names none), moved along by each refresh
    from_ms, to_ms       a fixed window, as Copy link pins it
    refresh=15           ask again every 15 s
*/
import { useEffect, useMemo, useState } from "react";
import {
  EventExplorer, RANGE_PRESETS, REFRESH_S, pinnedQuery, rangeText, rangeWindow, readRange, refreshOn,
  type EventPage, type TimeRange,
} from "../src/index.ts";
import { ApiError, CopyLink, Header, getJSON, messageHref } from "./chrome.tsx";
import { clock, pick, qs, useRefresh, withQuery } from "./lists.tsx";

const DEFAULT: TimeRange = { kind: "preset", preset: "24h" };
const DAY_MS = 86_400_000;

/** The URL's range as the explorer's window at `now`. The API needs both ends: a missing start is a day before the end. */
export function eventsWindow(r: TimeRange, now: number): { from_ms: number; to_ms: number } {
  const w = rangeWindow(r, now, { pinNow: true });
  const to_ms = w.to_ms ?? now;
  return { from_ms: w.from_ms ?? Math.max(0, to_ms - DAY_MS), to_ms };
}

/** The range the page shows: the URL's, with "any time" (no range in the URL) read as the last 24 hours. */
export function eventsRange(params: Record<string, string>): TimeRange | { error: string } {
  const r = readRange(params);
  return "error" in r || r.kind !== "all" ? r : DEFAULT;
}

const set = (changes: Record<string, string | null>) => {
  const u = new URL(location.href);
  for (const [k, v] of Object.entries(changes)) if (v === null || v === "") u.searchParams.delete(k); else u.searchParams.set(k, v);
  history.replaceState(history.state, "", u);
};

export function EventsPage() {
  const params = Object.fromEntries(new URLSearchParams(location.search));
  const own = pick(params, ["q", "range", "from_ms", "to_ms", "refresh"]);
  const read = eventsRange(own);
  const r: TimeRange = "error" in read ? DEFAULT : read;
  const problem = "error" in read ? read.error : null;
  const [query, setQuery] = useState(own.q ?? "");
  const [refresh, setRefresh, tick] = useRefresh(refreshOn(own));
  useEffect(() => { document.title = "Events · Trailbeam"; }, []);

  // the window is worked out again on each refresh, so a range that ends now moves along
  const [at, setAt] = useState(() => Date.now());
  useEffect(() => { if (tick) setAt(Date.now()); }, [tick]);
  const win = useMemo(() => eventsWindow(r, at), [at, JSON.stringify(r)]);
  const rangeKey = JSON.stringify(r);

  const onQuery = (q: string) => { setQuery(q); set({ q: q.trim() || null }); };
  const load = (q: Record<string, string>) => getJSON<EventPage>(`/api/events?${qs(q)}`).catch((e: Error) => {
    // a server from before the explorer (a newer page cached in front of an older server)
    throw e instanceof ApiError && e.status === 404 ? new Error("This Trailbeam server has no /api/events yet. Upgrade it to use the Event explorer.") : e;
  });
  const preset = r.kind === "preset" ? r.preset : "custom";
  const go = (value: string) => location.assign(withQuery("/events", { ...(query.trim() && { q: query.trim() }), range: value, ...(refresh && { refresh: String(REFRESH_S) }) }));

  const controls = (
    <div className="flex h-10 shrink-0 flex-wrap items-center gap-3 text-sm text-sky-ink-2">
      <label className="inline-flex items-center gap-2">
        <span className="sr-only">Time range</span>
        <select value={preset} onChange={(e) => go(e.target.value)} className="rounded-full border border-sky-line bg-sky-2 px-3 py-1 text-sky-ink">
          {RANGE_PRESETS.map(([k, label]) => <option key={k} value={k}>{label}</option>)}
          {preset === "custom" && <option value="custom" disabled>{rangeText(r).replace(/^between /, "")}</option>}
        </select>
      </label>
      <label className="inline-flex items-center gap-2" title={r.kind === "preset" ? undefined : "A fixed range in the past has nothing new to show"}>
        <input type="checkbox" checked={refresh} disabled={r.kind !== "preset"} onChange={(e) => setRefresh(e.target.checked)} />
        Refresh every {REFRESH_S} s
      </label>
      <span className="text-sky-ink-3">As of {clock(at)} UTC</span>
      <CopyLink link={() => withQuery("/events", pinnedQuery({ ...own, q: query.trim() }, r, at))} />
    </div>
  );

  return (
    <div className="mx-auto flex h-screen max-w-[1400px] flex-col px-4 md:px-8">
      <Header />
      <h1 className="sr-only">Events</h1>
      {problem ? (
        <p className="mt-6 text-sky-ink-2" role="alert">{problem} <a className="text-dawn hover:underline" href="/events">Show the last 24 hours</a></p>
      ) : (
        <div className="my-4 flex min-h-0 flex-1 flex-col overflow-hidden rounded-[16px] border border-sky-line bg-sky-2/55">
          <EventExplorer query={query} onQuery={onQuery} window={win} rangeKey={rangeKey} rangeText={rangeText(r)} load={load}
            messageHref={messageHref} controls={controls}
            foot={<>Times are in UTC. Inferred steps, such as an expired visibility timeout, are on each message&rsquo;s timeline. <a className="font-mono text-caption text-dawn hover:underline" href={`/api/events?${qs({ from_ms: String(win.from_ms), to_ms: String(win.to_ms) })}`}>/api/events</a></>} />
        </div>
      )}
    </div>
  );
}
