/*
  The queue page at /queue?name=<queue> (QV-360 design §2, §8; the queue list is the Overview, overview.tsx), drawn with the
  shared QueuePage (@trailbeam/ui/design) in the shell's frame (frame.tsx), with ShellSearch in its top bar. Everything the page shows is in its URL: range=<preset> or from=<ms>&to=<ms>, sel=<from>-<to>
  (the selected minutes), split=version, and at=<account>:<region> when two queues share a name. They read
  /api/queues/{queue}/series, /api/flows and /api/stuck on the same origin. With AWS reads on (QV-589) /series carries
  what AWS says about the queue: the page draws its settings, CloudWatch band and Lambda triggers from it, and for a
  queue AWS was read for, "In ‹queue› now" from /api/queues/{queue}/now under the band, as the hosted page does. A
  queue not read says why in the server's words, with a link to the AWS settings for admins.
*/
import { useEffect, useState } from "react";
import {
  QUEUE_PRESETS, flowFor, fromSelfHosted, queueQuery, queueWindow, readQueueUrl, utc,
  type Flow, type FlowsResponse, type QueueHrefs, type QueuePageData, type QueueRange, type QueueUrl, type SeriesResponse,
} from "../src/index.ts";
import { QueueNowLive, QueuePage, TopBar } from "../src/design/index.ts";
import type { QueueNowResponse } from "../src/index.ts";
import { TimePicker } from "../src/explorer/index.ts";
import { custom, preset, type TimeRange } from "../src/model/index.ts";
import type { QueueAwsView } from "../src/queue/types.ts";
import { ApiError, getHealth, getJSON, useMe } from "./chrome.tsx";
import { eventsHref } from "./events.tsx";
import { ShellFrame, ShellSearch } from "./frame.tsx";

export const queueHref = (name: string, at?: string | null) => `/queue?${queueQuery({ name, at: at ?? null })}`;
const atOf = (f: Pick<Flow, "account" | "region">) => (f.account ? `${f.account}:${f.region ?? ""}` : null);
const SPAN_REFRESH_MS = 30_000;
const STUCK_LIMIT = 500;

/** The server's default retention in ms ("7d", "36h"), or null for forever or unknown. */
function retentionMs(text: string | undefined): number | null {
  const m = /^(\d+)([smhd])$/.exec(text ?? "");
  if (!m) return null;
  return Number(m[1]) * { s: 1_000, m: 60_000, h: 3_600_000, d: 86_400_000 }[m[2] as "s" | "m" | "h" | "d"];
}
const daysText = (ms: number) => (ms % 86_400_000 === 0 ? `${ms / 86_400_000} day${ms === 86_400_000 ? "" : "s"}` : `${Math.round(ms / 3_600_000)} hours`);

type Loaded =
  | { kind: "loading" }
  | { kind: "ok"; data: QueuePageData; now: QueueNowResponse | null; consumer: string }
  | { kind: "unknown" }
  | { kind: "ambiguous"; flows: Flow[] }
  | { kind: "error"; message: string };

/** The page's range as the time picker's: a preset ends now, a fixed range is from and to. */
export const queueTime = (r: QueueRange, now: number): TimeRange => (r.kind === "preset" ? preset(r.preset, now) : custom(r.fromMs, r.toMs));
/** The time picker's range as the page's URL has it: one of its presets, else a fixed from and to (a typed 45m too). */
export const queueRangeOfTime = (t: TimeRange): QueueRange => {
  const p = QUEUE_PRESETS.find(([k]) => k === t.preset);
  return p ? { kind: "preset", preset: p[0] } : { kind: "fixed", fromMs: Math.floor(t.from), toMs: Math.ceil(t.to) };
};

/** The page's one range control: the time picker every page with a range has (QV-711). A new range loads the page again. */
function Picker({ u }: { u: QueueUrl }) {
  const go = (t: TimeRange) => location.assign(`/queue?${queueQuery({ name: u.name, at: u.at, split: u.split, range: queueRangeOfTime(t) })}`);
  return <TimePicker time={queueTime(u.range, Date.now())} setTime={go} now={Date.now()} />;
}

export function QueueShellPage() {
  const params = new URLSearchParams(location.search);
  const u0 = readQueueUrl((k) => params.get(k));
  const [sel, setSel] = useState(u0.sel);
  const [split, setSplit] = useState(u0.split);
  const [state, setState] = useState<Loaded>({ kind: "loading" });
  const [keptMs, setKeptMs] = useState<number | null>(null);
  const [awsOn, setAwsOn] = useState<boolean | undefined>(undefined);
  const me = useMe();
  const [tick, setTick] = useState(0);
  const u: QueueUrl = { ...u0, sel, split };

  // the URL is the page's state: sel= and split= are written back as they change
  const write = (next: Partial<QueueUrl>) => {
    const q = queueQuery({ ...u, ...next });
    history.replaceState(history.state, "", `/queue?${q}`);
  };

  useEffect(() => {
    document.title = `${u0.name || "Queue"}, Trailbeam`;
    getHealth(5000).then((h) => { setKeptMs(retentionMs(h.config?.retention?.default)); setAwsOn(!!h.config?.aws?.on); }, () => setAwsOn(false));
  }, []);

  useEffect(() => {
    if (!u0.name || awsOn === undefined) return;
    let live = true;
    const now = Date.now();
    const w = queueWindow(u0.range, now);
    const kept = keptMs !== null ? now - keptMs : null;
    const fromMs = kept !== null && w.fromMs < kept ? kept : w.fromMs; // a typed range is clipped to what's kept
    const asked = u0.at ? `${u0.at.split(":")[0]}/${u0.at.split(":")[1] ?? ""}/${u0.name}` : u0.name;
    const win = `from_ms=${fromMs}&to_ms=${w.toMs}`;
    Promise.all([
      getJSON<SeriesResponse>(`/api/queues/${encodeURIComponent(asked)}/series?${win}&split=version`),
      getJSON<FlowsResponse>(`/api/flows?${win}`).catch(() => null),
      getJSON<{ messages: unknown[]; next_cursor: string | null }>(`/api/stuck?queue=${encodeURIComponent(u0.name)}&limit=${STUCK_LIMIT}`).catch(() => null),
    ]).then(async ([series, flows, stuck]) => {
      const flow = flows ? flowFor(flows.flows, series.queue) : null;
      if (series.series.length === 0 && !flow) {
        // never seen at all, or only outside the range? /api/flows reads an hourly rollup, so a long window is cheap
        const ever = await getJSON<FlowsResponse>(`/api/flows?since_s=${400 * 86_400}`).catch(() => null);
        if (ever && !flowFor(ever.flows, series.queue)) return live && setState({ kind: "unknown" });
      }
      // "In ‹queue› now" for a queue AWS was read for, as the hosted page draws it; the page still draws without it
      const now = series.aws?.status === "ok" ? await getJSON<QueueNowResponse>(`/api/queues/${encodeURIComponent(series.queue.key)}/now`).catch(() => null) : null;
      if (!live) return;
      const data = fromSelfHosted({ series, flow, stuck: stuck && { count: stuck.messages.length, more: !!stuck.next_cursor }, endsNow: w.endsNow, awsOn });
      setState({ kind: "ok", data, now, consumer: flow?.consumers.length === 1 ? flow.consumers[0].name : "" });
    }).catch(async (e: Error) => {
      if (e instanceof ApiError && e.status === 400 && /more than one queue/.test(e.message)) {
        const ever = await getJSON<FlowsResponse>(`/api/flows?since_s=${400 * 86_400}`).catch(() => null);
        return live && setState({ kind: "ambiguous", flows: (ever?.flows ?? []).filter((f) => f.name === u0.name) });
      }
      if (live) setState({ kind: "error", message: e.message });
    });
    return () => { live = false; };
  }, [tick, keptMs, awsOn]);

  // the span charts ask again every 30 s while the range ends now (§4.4), and the tab is showing
  useEffect(() => {
    if (u0.range.kind !== "preset") return;
    const id = setInterval(() => { if (!document.hidden) setTick((t) => t + 1); }, SPAN_REFRESH_MS);
    return () => clearInterval(id);
  }, []);

  if (!u0.name) {
    return (
      <ShellFrame path="/queue">
        <TopBar search={<ShellSearch />} />
        <p className="px-8 pt-6 text-sm text-ink-2">Choose a queue from the <a className="text-brand hover:underline" href="/overview">Overview</a>.</p>
      </ShellFrame>
    );
  }

  if (state.kind === "ambiguous") {
    return (
      <ShellFrame path="/queue">
        <TopBar search={<ShellSearch />} />
        <div className="px-8 pt-5 pb-12">
          <nav aria-label="Breadcrumb" className="text-sm text-ink-3"><a href="/overview" className="hover:text-ink">Overview</a></nav>
          <h1 className="mt-3 break-all font-serif text-title-sm text-ink">{u0.name}</h1>
          <p className="mt-2 text-sm text-ink-2">More than one queue is called <span className="font-mono text-caption text-ink">{u0.name}</span>. Which one?</p>
          <ul className="mt-4 overflow-hidden rounded-card border border-line bg-surface shadow-card">
            {state.flows.map((f) => (
              <li key={f.key} className="border-b border-line last:border-b-0">
                <a className="flex flex-wrap items-baseline gap-x-4 gap-y-1 px-4 py-2.5 text-sm hover:bg-sunken" href={queueHref(f.name, atOf(f))}>
                  <span className="text-ink-3">Account <span className="font-mono text-caption text-brand">{f.account ?? "none"}</span></span>
                  <span className="text-ink-3">Region <span className="font-mono text-caption text-brand">{f.region ?? "none"}</span></span>
                  <span className="ml-auto text-xs text-ink-3">Last seen <span className="font-mono text-caption">{utc(f.lastSeen, { date: true })}</span> UTC</span>
                </a>
              </li>
            ))}
          </ul>
        </div>
      </ShellFrame>
    );
  }

  const href: QueueHrefs = {
    queues: "/overview",
    queue: (name) => queueHref(name),
    // only "Sent" works on this server's lists today: they take a window but no sort or state filter (design §6)
    messages: ({ queue, fromMs, toMs, view }) => (view === "sent" ? `/?queue=${encodeURIComponent(queue)}&from_ms=${fromMs}&to_ms=${toMs - 1}` : null),
    stuck: (queue) => `/issues?queue=${encodeURIComponent(queue)}`,
    range: (preset) => `/queue?${queueQuery({ name: u.name, at: u.at, split: u.split, range: { kind: "preset", preset } })}`,
  };
  const now = Date.now();
  // a range reaching back past what the server keeps (a preset too) is read from the oldest span kept
  const clipped = keptMs !== null && queueWindow(u0.range, now).fromMs < now - keptMs;
  const rangeNote = keptMs === null ? null : clipped ? `Spans before ${utc(now - keptMs, { date: true })} UTC aren't kept on this server.` : `Kept for ${daysText(keptMs)} on this server`;
  const json = `/api/queues/${encodeURIComponent(u0.name)}/series`;
  // as the server decides (aws_api.for_app): anyone before accounts exist, else an admin
  const admin = !!me && (me.auth !== "accounts" || me.user?.role === "admin");
  // the server's sentence on why it didn't read the queue, and where an admin changes that
  const awsWhy = (a: QueueAwsView) => <>{a.detail ?? "AWS wasn't read for this queue."}{admin && a.status !== "failed" && <> <a className="text-brand hover:underline" href="/settings">Open the AWS settings</a></>}</>;
  const inNow = state.kind === "ok" && state.now?.live?.status === "ok" ? state.now : null;
  return (
    <ShellFrame path="/queue">
      <QueuePage
        name={u0.name}
        data={state.kind === "ok" ? state.data : null}
        error={state.kind === "error" ? state.message : null}
        onRetry={() => { setState({ kind: "loading" }); setTick((t) => t + 1); }}
        unknown={state.kind === "unknown"}
        href={href}
        picker={<Picker u={u} />}
        rangeNote={rangeNote}
        sel={sel}
        onSelect={(s) => { setSel(s); write({ sel: s }); }}
        split={split}
        onSplit={(on) => { setSplit(on); write({ split: on }); }}
        search={<ShellSearch />}
        awsWhy={awsWhy}
        afterCloudWatch={inNow && state.kind === "ok" && (
          <div className="scroll-mt-16" id="in-queue-now">
            <QueueNowLive queue={u0.name} view={inNow} live={inNow.live ?? null} heldTotal={inNow.held.length} consumer={state.consumer}
              fifo={inNow.live?.fifo ?? u0.name.endsWith(".fifo")} triggers={inNow.aws?.triggers ?? null}
              hrefs={{ message: (id) => `/message?id=${encodeURIComponent(id)}`, events: (q) => eventsHref(q, preset("1h", Date.now())) }} />
          </div>
        )}
        footer={<>Times are in UTC. The same data as JSON: <a className="font-mono text-caption text-brand hover:underline" href={json}>{json}</a></>}
      />
    </ShellFrame>
  );
}
