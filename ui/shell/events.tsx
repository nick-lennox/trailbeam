/*
  The Event explorer (/events, row 2 of the one-UI plan): the shared explorer (@trailbeam/ui/explorer) in the shell's
  frame (frame.tsx), as the hosted app draws it. Its events are GET /api/timeline-events for the chosen range, so the
  ones a timeline infers (an expired visibility timeout, a move to the dead-letter queue, a Lambda invocation with no
  outcome) are listed too. When the server read fewer messages than the range had, an empty query's chart is every
  recorded step in the range, from GET /api/events' histogram. The URL is the page's state, as in the hosted app:
    q=<query>            the explorer's query (event:, queue:, service:, tenant:, body.<path>:, ...)
    range=1h|4h|1d|...   a range that ends now (an hour when the URL names none, as on the hosted explorer), moved along
                         while Live is on; a span dragged on the chart becomes a fixed range
    from, to             a fixed range, as Copy link pins it (ISO 8601 in UTC, or Unix milliseconds)
  Links from before the shared explorer still open what they named: range=24h, from_ms and to_ms, and kind: queries.
*/
import { useEffect, useMemo, useRef, useState } from "react";
import { Pause, Play } from "lucide-react";
import { NothingReporting, Tooltip, TopBar } from "../src/design/index.ts";
import { EventExplorer, stepChart, stepChartQuery, TimePicker, type ExplorerData, type StepChartDoc } from "../src/explorer/index.ts";
import { DEFAULT_RANGE, follow, pinned, rangeFromParams, withRange, type Ev, type TimeRange } from "../src/model/index.ts";
import { storeIsEmpty } from "../src/firstrun/status.ts";
import { ApiError, CopyLink, getHealth, getJSON, messageHref } from "./chrome.tsx";
import { ShellFrame, ShellSearch } from "./frame.tsx";
import { qs, useRefresh } from "./lists.tsx";

/** The range the page opens on when its URL names none: an hour, as on the hosted explorer and the Overview. */
export const DEFAULT_EVENTS_RANGE = DEFAULT_RANGE;

/** The URL's range at `now`, reading the names links used before the shared explorer too. */
export function eventsTime(search: string, now: number): TimeRange {
  const p = new URLSearchParams(search);
  const get = (k: string) => {
    if (k === "from" || k === "to") return p.get(k) ?? p.get(`${k}_ms`);
    if (k === "range") return p.get("range") === "24h" ? "1d" : p.get("range");
    return p.get(k);
  };
  return rangeFromParams(get, now, DEFAULT_EVENTS_RANGE);
}

/** A query from before the shared explorer, in its words: kind: is event:, and change_visibility is visibility_change. */
export const eventsQueryOf = (q: string) =>
  q.replace(/(^|[\s(-])kind:/gi, "$1event:").replace(/\bevent:change_visibility\b/gi, "event:visibility_change");

/** The page's link for a query and a range. The range is always named, so a link says what it shows. */
export const eventsHref = (query: string, t: TimeRange) =>
  withRange(query.trim() ? `/events?q=${encodeURIComponent(query.trim())}` : "/events", t, true);

/** The part of GET /api/timeline-events' answer the page reads (contract/openapi.yaml, TimelineEventPage). */
type TimelineEventPage = { from_ms: number; to_ms: number; events: Ev[]; messages: number; capped: boolean; bodies_capped: boolean };

/** GET /api/timeline-events' query for [from, to): its window includes both ends. */
export const timelineEventsQuery = (t: Pick<TimeRange, "from" | "to">) => ({ from_ms: String(Math.floor(t.from)), to_ms: String(Math.ceil(t.to) - 1) });

/** The answer as what the explorer reads. Bodies come to anyone this server lets in, as on the Body tab. */
export function explorerData(page: TimelineEventPage, now: number): ExplorerData {
  return { events: page.events, now, live: true, capped: page.capped, bodies: "shown", bodiesCapped: page.bodies_capped };
}

type State = { kind: "loading" } | { kind: "error"; message: string } | { kind: "ok"; data: ExplorerData };

/** What /events shows while the server has never stored a span: the Overview's first-run state, not an explorer of zeros. */
export function EventsFirstRun() {
  return (
    <>
      <TopBar title="Event explorer" pad="px-6" plain search={<ShellSearch />} />
      <div className="px-6 pt-5 pb-10">
        <NothingReporting actions={<a className="rounded-control border border-line bg-surface px-3 py-1.5 text-sm text-ink hover:border-line-strong" href="/">See how to connect a service</a>}>
          This page lists every event of every message, from its send to its delete, once a service of yours is connected.
        </NothingReporting>
      </div>
    </>
  );
}

const BUTTON = "inline-flex h-10 items-center rounded-lg border border-line-strong bg-surface px-3 text-[13px] text-ink-2 hover:text-ink";
const loadSteps = (from: number, to: number) => getJSON<StepChartDoc>(`/api/events?${qs(stepChartQuery(from, to))}`).then(stepChart);

export function EventsPage() {
  const [time, setTime] = useState<TimeRange>(() => eventsTime(location.search, Date.now()));
  const [query, setQuery] = useState(() => eventsQueryOf(new URLSearchParams(location.search).get("q") ?? ""));
  // Live is on unless the URL says refresh=0; only a range that ends now can follow new events
  const [live, setLive, tick] = useRefresh(new URLSearchParams(location.search).get("refresh") !== "0");
  const following = live && !!time.preset;
  const [state, setState] = useState<State>({ kind: "loading" });
  // a server that has never stored a span shows the first-run state; checked again on each refresh, so the
  // explorer comes once the first span is in
  const [empty, setEmpty] = useState(false);
  useEffect(() => { document.title = "Event explorer, Trailbeam"; }, []);
  useEffect(() => {
    if (state.kind !== "ok" || state.data.events.length > 0) { setEmpty(false); return; }
    let on = true;
    getHealth(5000).then((h) => on && setEmpty(storeIsEmpty(h)), () => on && setEmpty(false));
    return () => { on = false; };
  }, [state]);

  // each refresh moves a range that ends now up to the clock
  useEffect(() => { if (tick && following) setTime((t) => follow(t, Date.now())); }, [tick]);

  // ask again whenever the range moves; an answer to an older ask is dropped
  const asked = useRef(0);
  useEffect(() => {
    const n = ++asked.current;
    const now = Date.now();
    getJSON<TimelineEventPage>(`/api/timeline-events?${qs(timelineEventsQuery(time))}`, 60_000)
      .then((page) => { if (n === asked.current) setState({ kind: "ok", data: explorerData(page, now) }); })
      .catch((e: Error) => {
        if (n !== asked.current) return;
        // a server from before the shared explorer (a newer page cached in front of an older server)
        const message = e instanceof ApiError && e.status === 404 ? "This Trailbeam server has no /api/timeline-events yet. Upgrade it to use the Event explorer." : e.message;
        setState({ kind: "error", message });
      });
  }, [time.from, time.to]);

  // the URL follows the query and the chosen range (a range that ends now keeps the same link as it moves up)
  const link = eventsHref(query, time);
  useEffect(() => {
    const t = setTimeout(() => {
      const u = new URL(link, location.origin);
      if (!live) u.searchParams.set("refresh", "0");
      history.replaceState(history.state, "", `${u.pathname}${u.search}`);
    }, 250);
    return () => clearTimeout(t);
  }, [link, live]);

  const now = state.kind === "ok" ? state.data.now : Date.now();
  const json = useMemo(() => `/api/timeline-events?${qs(timelineEventsQuery(time))}`, [time.from, time.to]);

  const controls = (
    <div className="flex h-10 shrink-0 items-center gap-2">
      <TimePicker time={time} setTime={setTime} now={Date.now()} />
      <CopyLink link={() => eventsHref(query, pinned(time))} />
      <Tooltip content={!time.preset ? "A fixed range in the past does not update" : following ? "Pause new events" : "Resume new events"} className="inline-flex">
        <button type="button" onClick={() => setLive(!live)} disabled={!time.preset} aria-pressed={following}
          className={`${BUTTON} w-[92px] justify-center gap-2 disabled:opacity-50 disabled:hover:text-ink-2`}>
          {following ? <><span className="live-dot size-1.5 rounded-full bg-good" /> Live <Pause size={13} className="text-ink-3" /></> : <>Paused <Play size={13} className="text-ink-3" /></>}
        </button>
      </Tooltip>
    </div>
  );

  return (
    <ShellFrame path="/events">
      <div className="flex h-screen flex-col">
        {!empty && <h1 className="sr-only">Event explorer</h1>}
        {empty ? (
          <EventsFirstRun />
        ) : state.kind === "ok" ? (
          <EventExplorer page query={query} setQuery={setQuery} time={time} setTime={setTime} data={state.data} messageHref={messageHref}
            controls={controls} loadStepChart={loadSteps} />
        ) : (
          <div className="flex items-start gap-3 border-b border-line px-5 py-3">
            <p className="flex-1 self-center text-sm text-ink-2" role={state.kind === "error" ? "alert" : undefined}>
              {state.kind === "loading" ? "Loading events" : state.message}
            </p>
            {controls}
          </div>
        )}
        {!empty && <p className="shrink-0 border-t border-line px-5 py-2 text-xs text-ink-3">
          Times are in UTC, as of {new Date(now).toISOString().slice(11, 19)}. The same data as JSON:{" "}
          <a className="font-mono text-brand hover:underline" href={json}>/api/timeline-events</a>
        </p>}
      </div>
    </ShellFrame>
  );
}
