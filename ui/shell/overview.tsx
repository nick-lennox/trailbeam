/*
  The Overview at /overview, drawn with the shared OverviewFlows (@trailbeam/ui/design), the hosted app's Overview, in
  the shell's frame (frame.tsx). It reads GET /api/flows for the range, GET /api/causes for why messages didn't
  finish in it, and GET /api/queues/{queue}/now?minutes= for each queue's messages now and what AWS says about it
  (overview/flows-data.ts). AWS reads are off by default (QV-589): then health is from the spans alone (spanHealth).
  A queue named in the AWS settings has its depth, age, settings and health from SQS and CloudWatch, as in the hosted
  app. Either way the card's figures include the range's totals from spans: Sends, Receives and Deletes, visibility
  changes, the average body and receives on the dead-letter queue.
  The URL is the page's state: the Event explorer's range=<preset> or from=&to= (the page before QV-579's from_ms=&to_ms=
  still opens), and flow=. The range is chosen with the explorer's TimePicker. A range that ends now asks again every 15 s
  while the tab is showing and updates the figures in place; a fixed range is read once. Copy link pins the exact window. /queues, the queue list QV-360 planned, is this page.
*/
import { useEffect, useMemo, useRef, useState } from "react";
import {
  LAMBDA_DELETES_HINT, REFRESH_S, flowsHref, flowsWindow, fmtBytes, lambdaDeletes, overviewRangeOf, pinnedFlowsHref, queueQuery, queueRangeOf,
  readFlowsUrl, selfHostedFlowsData, timeKey, storeIsEmpty, overviewNowPath, anyAwsRead, type CausePage, type Flow, type FlowsResponse, type FlowsUrl, type QueueNowResponse,
  type QueueNowAnswer,
} from "../src/index.ts";
import { fmtNum, partMessagesOf, phrase, preset, type Ev, type QueueLive, type TimeRange } from "../src/model/index.ts";
import {
  OverviewFlows, PartMessages, Since, TopBar, Tooltip, UiLink, type OverviewCopy, type OverviewFlowsData, type OverviewFlowsHrefs, type PartMessagesAt,
} from "../src/design/index.ts";
import { ApiError, CopyLink, getHealth, getJSON } from "./chrome.tsx";
import { eventsHref, timelineEventsQuery } from "./events.tsx";
import { qs } from "./lists.tsx";
import { ShellFrame, ShellSearch } from "./frame.tsx";
import { issuesHref } from "./issues.tsx";

const DOCS = "https://github.com/nick-lennox/trailbeam/blob/main/docs/api.md#list-flows";
const atOf = (f: Pick<Flow, "account" | "region">) => (f.account ? `${f.account}:${f.region ?? ""}` : null);
const clock = (ms: number) => new Date(ms).toISOString().slice(11, 16);

/** The server's default retention in ms ("7d", "36h"), or null for forever or unknown. */
function retentionMs(text: string | undefined): number | null {
  const m = /^(\d+)([smhd])$/.exec(text ?? "");
  if (!m) return null;
  return Number(m[1]) * { s: 1_000, m: 60_000, h: 3_600_000, d: 86_400_000 }[m[2] as "s" | "m" | "h" | "d"];
}

const COPY: OverviewCopy = {
  noHealth: (n) => (n === 1 ? "1 queue, with no data on its health" : `${fmtNum(n)} queues, with no data on their health`),
  spansOnly: ". Health is from your services' spans only",
  // the server's reason when it sends one (reads off, the queue not named, what AWS answered); a server before QV-589 sends none
  noFigures: (live) => live?.detail ?? "Trailbeam makes no AWS calls, so it can't see this queue's depth, its oldest message or its settings. Its figures here are from your services' spans.",
};

type Loaded = { flows: Flow[]; data: OverviewFlowsData; window: { fromMs: number; toMs: number; clipped: boolean }; aws: boolean };
type State =
  | { kind: "loading" }
  | { kind: "ok"; at: number; got: Loaded; stale?: { message: string; at: number } }
  | { kind: "empty-store" }
  | { kind: "unsupported" }
  | { kind: "error"; message: string };

/** One flow's "In ‹queue› now" and AWS parts over the past `minutes`, or null when it couldn't be read: the rest of the page still draws. */
const nowOf = (f: Flow, minutes: number) =>
  getJSON<QueueNowResponse>(overviewNowPath(f.key, minutes)).then((r): [string, QueueNowAnswer | null] => [f.key, r], () => [f.key, null]);

/** The range's totals for one queue, from its spans: what the open-source Overview's table showed. */
function QueueTotals({ f, range }: { f: Flow; range: string }) {
  const l = lambdaDeletes(f);
  const rows: [string, React.ReactNode][] = [
    ["Sends", fmtNum(f.sent)],
    ["Receives", fmtNum(f.received)],
    ["Deletes", l === "all"
      ? <Tooltip content={LAMBDA_DELETES_HINT} className="font-sans text-ink-3">By Lambda</Tooltip>
      : <>{fmtNum(f.deleted)}{l === "mixed" && <Tooltip content={LAMBDA_DELETES_HINT} className="ml-1 font-sans text-ink-3">and Lambda&rsquo;s</Tooltip>}</>],
    ["Visibility changes", fmtNum(f.visibilityChanges)],
    ["Average body", f.avgBytes === null ? <span className="font-sans text-ink-3">No data</span> : fmtBytes(f.avgBytes)],
    ["Received on its DLQ", f.dlq ? fmtNum(f.dlq.received) : <span className="font-sans text-ink-3">No DLQ seen</span>],
  ];
  return (
    <div className="border-t border-line px-4 py-3.5 text-xs">
      <p className="text-ink-3">From spans, in {range}</p>
      <dl className="mt-2 grid grid-cols-2 gap-x-5 gap-y-2 whitespace-nowrap">
        {rows.map(([k, v]) => (
          <div key={k} className="flex items-baseline justify-between gap-3">
            <dt className="text-ink-3">{k}</dt>
            <dd className="shrink-0 font-mono text-[12.5px] text-ink tabular-nums">{v}</dd>
          </div>
        ))}
      </dl>
    </div>
  );
}

/** The most messages the rows under a part are read from: GET /api/timeline-events' limit, most recently active first. */
const PART_LIMIT = 500;
type PartEvents = { key: string; events: Ev[]; capped: boolean } | { key: string; error: string };

/**
 * The messages behind a selected producer, consumer or dead-letter queue (the shared PartMessages), from recorded
 * spans: the range's events as the Event explorer reads them (GET /api/timeline-events), one row per message. Read
 * again when the range moves, without closing a row that is open.
 */
function ShellPartMessages({ at, queue, window: w, time, live }: { at: PartMessagesAt; queue: string; window: { fromMs: number; toMs: number }; time: TimeRange; live: QueueLive | undefined }) {
  const key = `${Math.floor(w.fromMs)}-${w.toMs}`;
  const [got, setGot] = useState<PartEvents | null>(null);
  useEffect(() => {
    let on = true;
    getJSON<{ events: Ev[]; capped: boolean }>(`/api/timeline-events?${qs({ ...timelineEventsQuery({ from: w.fromMs, to: w.toMs }), limit: String(PART_LIMIT) })}`)
      .then((r) => on && setGot({ key, events: r.events, capped: r.capped }), (e: Error) => on && setGot({ key, error: e.message }));
    return () => { on = false; };
  }, [key]);
  const name = at.part === "producer" ? at.producer : at.part === "consumer" ? at.consumer : undefined;
  const rows = useMemo(() => (got && "events" in got ? partMessagesOf(got.events, { queue, part: at.part, name }) : []), [got, queue, at.part, name]);
  if (at.part !== "dlq" && !name) return null;
  const note = "border-t border-line px-5 py-3 text-xs text-ink-3";
  if (!got) return <p className={note}>Reading its messages in {phrase(time)}.</p>;
  if ("error" in got) return <p className={note} role="alert">Couldn&rsquo;t read its messages: {got.error}</p>;
  if (!rows.length) return <p className={note}>No message {at.part === "producer" ? "it sent here" : at.part === "consumer" ? "it received from here" : "moved to the dead-letter queue"} has an event in {phrase(time)}.</p>;
  return (
    <>
      <PartMessages queue={queue} part={at.part} producer={at.producer} rows={rows} max={at.max} eventsHref={(q) => eventsHref(q, time)}
        facts={live && { read: live.status === "ok", fifo: live.fifo, visibilityTimeoutS: live.visibilityTimeoutS, retentionS: live.retentionS, delayS: live.delayS ?? null, maxReceiveCount: live.maxReceiveCount, dlq: live.dlq, triggers: null }} />
      {got.capped && <p className={note}>Read from the {fmtNum(PART_LIMIT)} most recently active messages in the range.</p>}
    </>
  );
}

/**
 * Issues (or one issue, with `id`) at the Overview's range, so a count clicked is the count the page opens on: a range
 * that ends now as its preset, a fixed one as its times. The Issues list's default is the past day, so the past hour is
 * named too.
 */
export const overviewIssuesHref = (time: TimeRange, queue: string | null, id?: string) => issuesHref({ age: null, range: time }, queue, id);

export function OverviewPage() {
  const [u, setU] = useState<FlowsUrl>(() => readFlowsUrl(new URLSearchParams(location.search), Date.now()));
  const [state, setState] = useState<State>({ kind: "loading" });
  const [keptMs, setKeptMs] = useState<number | null | undefined>(undefined);
  const [tick, setTick] = useState(0);
  const last = useRef<State>(state);
  last.current = state;

  useEffect(() => {
    document.title = "Overview, Trailbeam";
    getHealth(5000).then((h) => {
      setKeptMs(retentionMs(h.config?.retention?.default));
      if (storeIsEmpty(h)) setState({ kind: "empty-store" });
    }, () => setKeptMs(null));
  }, []);

  useEffect(() => {
    if (keptMs === undefined || last.current.kind === "empty-store") return;
    let live = true;
    const at = Date.now();
    const w = flowsWindow(u.time, at, keptMs);
    const win = `from_ms=${Math.floor(w.fromMs)}&to_ms=${w.toMs}`;
    Promise.all([
      getJSON<FlowsResponse>(`/api/flows?${win}`),
      getJSON<CausePage>(`/api/causes?${win}&limit=1`).catch(() => null),
    ]).then(async ([r, causes]) => {
      const now = Object.fromEntries(await Promise.all(r.flows.map((f) => nowOf(f, w.minutes))));
      if (!live) return;
      const data = selfHostedFlowsData({ range: overviewRangeOf(u.time), window: w, flows: r.flows, causes, now, causeHref: (id) => overviewIssuesHref(u.time, null, id) });
      setState({ kind: "ok", at, got: { flows: r.flows, data, window: w, aws: anyAwsRead(now) } });
    }, (e: Error) => {
      if (!live) return;
      if (e instanceof ApiError && e.status === 501) return setState({ kind: "unsupported" });
      const prev = last.current;
      setState(prev.kind === "ok" ? { ...prev, stale: { message: e.message, at } } : { kind: "error", message: e.message });
    });
    return () => { live = false; };
  }, [tick, keptMs, timeKey(u.time)]);

  // a range that ends now asks again every 15 s while the tab is showing, and on coming back to it
  const endsNow = !!u.time.preset;
  useEffect(() => {
    if (!endsNow) return;
    const again = () => { if (!document.hidden) setTick((t) => t + 1); };
    const id = setInterval(again, REFRESH_S * 1000);
    document.addEventListener("visibilitychange", again);
    return () => { clearInterval(id); document.removeEventListener("visibilitychange", again); };
  }, [endsNow]);

  const setTime = (time: TimeRange) => {
    const next = { ...u, time };
    history.pushState(history.state, "", flowsHref(next));
    setU(next);
  };
  // the browser's Back and Forward move between ranges chosen here
  useEffect(() => {
    const back = () => setU(readFlowsUrl(new URLSearchParams(location.search), Date.now()));
    window.addEventListener("popstate", back);
    return () => window.removeEventListener("popstate", back);
  }, []);

  const flows = state.kind === "ok" ? state.got.flows : [];
  const byLabel = new Map(flows.map((f) => [f.queue, f]));
  const nameOf = (label: string) => byLabel.get(label)?.name ?? label;
  const range = phrase(u.time);
  const asked = state.kind === "ok" ? state.got.window : { fromMs: u.time.from, toMs: u.time.to };
  const hrefs: OverviewFlowsHrefs = {
    range: (id) => flowsHref({ ...u, time: preset(id === "24h" ? "1d" : id, Date.now()) }),
    queue: ({ name }) => {
      const f = byLabel.get(name);
      return `/queue?${queueQuery({ name: f?.name ?? name, at: f ? atOf(f) : null, range: queueRangeOf(u.time, asked) })}`;
    },
    issues: (queue) => overviewIssuesHref(u.time, nameOf(queue)),
    cause: (c) => c.href ?? overviewIssuesHref(u.time, null),
    events: (query) => eventsHref(query, u.time),
    deadLettered: ({ queue }) => overviewIssuesHref(u.time, nameOf(queue)),
    message: (id) => `/message?id=${encodeURIComponent(id)}`,
  };
  const sidebar = flows.map((f) => ({ name: f.queue, health: state.kind === "ok" ? state.got.data.info[f.queue]?.health ?? null : null }));

  if (state.kind !== "ok" && state.kind !== "empty-store") {
    return (
      <ShellFrame path="/overview">
        <TopBar title="Overview" pad="px-6" plain search={<ShellSearch />} />
        <div className="px-6 pt-5 pb-10 text-sm text-ink-2" role={state.kind === "error" ? "alert" : undefined}>
          {state.kind === "loading" && <p className="text-ink-3">Reading the queues your services reported in {range}.</p>}
          {state.kind === "unsupported" && <p>This server&rsquo;s store can&rsquo;t list flows. See <a className="text-brand hover:underline" href={DOCS}>GET /api/flows</a> for the stores that can.</p>}
          {state.kind === "error" && <>
            <p>Couldn&rsquo;t read the queues: {state.message}</p>
            <button type="button" onClick={() => { setState({ kind: "loading" }); setTick((t) => t + 1); }} className="mt-3 rounded-control border border-line bg-surface px-3 py-1 text-ink hover:border-line-strong">Try again</button>
          </>}
        </div>
      </ShellFrame>
    );
  }

  const got = state.kind === "ok" ? state.got : null;
  const json = got ? `/api/flows?from_ms=${Math.floor(got.window.fromMs)}&to_ms=${got.window.toMs}` : "/api/flows";
  // spans are stored, but none in the range: the shared view's "No data yet" would say nothing reports here
  if (got && got.flows.length === 0) {
    return (
      <ShellFrame path="/overview">
        <TopBar title="Overview" pad="px-6" plain search={<ShellSearch />} />
        <div className="px-6 pt-5 pb-10 text-sm text-ink-2">
          <p>No queue had a span from your services in {range}.</p>
          {u.time.to - u.time.from < 86_400_000 && <p className="mt-2"><UiLink className="text-brand hover:underline" href={flowsHref({ ...u, time: preset("1d", Date.now()) })}>Show the past day</UiLink></p>}
        </div>
      </ShellFrame>
    );
  }

  return (
    <ShellFrame path="/overview" queues={sidebar}>
      <OverviewFlows
        key={timeKey(u.time)}
        time={{ range: u.time, now: state.kind === "ok" ? state.at : Date.now(), onChange: setTime }}
        data={got?.data ?? selfHostedFlowsData({ range: overviewRangeOf(u.time), window: { fromMs: 0, toMs: 0, minutes: 1 }, flows: [], causes: null, now: {}, causeHref: () => overviewIssuesHref(u.time, null) })}
        flow={u.flow}
        fresh={state.kind === "ok" ? <Since ms={state.at} /> : undefined}
        onFlow={(flow) => { const next = { ...u, flow }; setU(next); history.replaceState(history.state, "", flowsHref(next)); }}
        copy={COPY}
        hrefs={hrefs}
        slots={{
          search: <ShellSearch />,
          emptyActions: <a className="rounded-control border border-line bg-surface px-3 py-1.5 text-sm text-ink hover:border-line-strong" href="/">See how to connect a service</a>,
          actions: <CopyLink link={() => pinnedFlowsHref(u, state.kind === "ok" ? state.at : Date.now())} />,
          partMessages: (at) => got && <ShellPartMessages key={`${at.queue}:${at.part}:${at.producer ?? at.consumer ?? ""}`} at={at} queue={nameOf(at.queue)} window={got.window} time={u.time} live={got.data.info[at.queue]?.live} />,
          queueFacts: (label) => { const f = byLabel.get(label); return f ? <QueueTotals f={f} range={range} /> : null; },
          foot: got && <>
            {got.aws
              ? <p>Counts are of the spans your services reported between {clock(got.window.fromMs)} and {clock(got.window.toMs)} UTC. Depth, the oldest message and each queue&rsquo;s settings are from SQS and CloudWatch, for the queues named in the server&rsquo;s AWS settings.</p>
              : <p>Counts are of the spans your services reported between {clock(got.window.fromMs)} and {clock(got.window.toMs)} UTC. Trailbeam reads nothing from AWS for these queues, so it can&rsquo;t see queue depth or the oldest message: open a queue&rsquo;s page for what it can see, and the SQS console for the rest.</p>}
            {got.window.clipped && <p className="mt-1">Spans before {new Date(got.window.fromMs).toISOString().slice(0, 16).replace("T", " ")} UTC aren&rsquo;t kept on this server.</p>}
            {state.kind === "ok" && state.stale && <p className="mt-1" role="status">Couldn&rsquo;t refresh at {clock(state.stale.at)} UTC: {state.stale.message}. These figures are from {clock(state.at)} UTC.</p>}
            <p className="mt-1">The same data as JSON: <a className="font-mono text-caption text-brand hover:underline" href={json}>/api/flows</a></p>
          </>,
        }}
      />
    </ShellFrame>
  );
}

/** /queues: the queue list QV-360 planned is the Overview, so it redirects there with the range kept when the Overview has it. */
export function QueuesRedirect() {
  useEffect(() => {
    const u = readFlowsUrl(new URLSearchParams(location.search), Date.now());
    location.replace(flowsHref({ time: u.time, flow: undefined }));
  }, []);
  return <ShellFrame path="/overview"><p className="px-6 pt-6 text-sm text-ink-2">The queue list is the <a className="text-brand hover:underline" href="/overview">Overview</a>.</p></ShellFrame>;
}
