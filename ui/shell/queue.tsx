/*
  The queue page at /queue?name=<queue> and the queue list at /queues (QV-360 design §2, §8), drawn with the
  shared QueuePage. Everything the page shows is in its URL: range=<preset> or from=<ms>&to=<ms>, sel=<from>-<to>
  (the selected minutes), split=version, and at=<account>:<region> when two queues share a name. They read
  /api/queues/{queue}/series, /api/flows and /api/stuck on the same origin.
*/
import { useEffect, useState } from "react";
import {
  QUEUE_PRESETS, QueuePage, flowFor, fmtRate, fromSelfHosted, queueQuery, queueWindow, readQueueUrl, utc,
  type Flow, type FlowsResponse, type QueueHrefs, type QueuePageData, type QueuePreset, type QueueUrl, type SeriesResponse,
} from "../src/index.ts";
import { ApiError, Frame, Json, getHealth, getJSON } from "./chrome.tsx";

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
  | { kind: "ok"; data: QueuePageData }
  | { kind: "unknown" }
  | { kind: "ambiguous"; flows: Flow[] }
  | { kind: "error"; message: string };

function Picker({ u, keptMs }: { u: QueueUrl; keptMs: number | null }) {
  const value = u.range.kind === "preset" ? u.range.preset : "fixed";
  const go = (v: string) => {
    if (v === "fixed") return;
    location.assign(`/queue?${queueQuery({ name: u.name, at: u.at, split: u.split, range: { kind: "preset", preset: v as QueuePreset } })}`);
  };
  return (
    <label className="inline-flex items-center gap-2 text-sm text-ink-2">
      <span className="sr-only">Time range</span>
      <select value={value} onChange={(e) => go(e.target.value)}
        className="rounded-full border border-line bg-surface px-3 py-1 text-sm text-ink">
        {QUEUE_PRESETS.map(([k, label, ms]) => (
          <option key={k} value={k} disabled={keptMs !== null && ms > keptMs}>{label}{keptMs !== null && ms > keptMs ? " (not kept)" : ""}</option>
        ))}
        {u.range.kind === "fixed" && <option value="fixed">{utc(u.range.fromMs, { date: true })} to {utc(u.range.toMs, { date: true })} UTC</option>}
      </select>
    </label>
  );
}

export function QueueShellPage() {
  const params = new URLSearchParams(location.search);
  const u0 = readQueueUrl((k) => params.get(k));
  const [sel, setSel] = useState(u0.sel);
  const [split, setSplit] = useState(u0.split);
  const [state, setState] = useState<Loaded>({ kind: "loading" });
  const [keptMs, setKeptMs] = useState<number | null>(null);
  const [tick, setTick] = useState(0);
  const u: QueueUrl = { ...u0, sel, split };

  // the URL is the page's state: sel= and split= are written back as they change
  const write = (next: Partial<QueueUrl>) => {
    const q = queueQuery({ ...u, ...next });
    history.replaceState(history.state, "", `/queue?${q}`);
  };

  useEffect(() => {
    document.title = `${u0.name || "Queue"} · Trailbeam`;
    getHealth(5000).then((h) => setKeptMs(retentionMs(h.config?.retention?.default)), () => {});
  }, []);

  useEffect(() => {
    if (!u0.name) return;
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
      if (!live) return;
      const data = fromSelfHosted({ series, flow, stuck: stuck && { count: stuck.messages.length, more: !!stuck.next_cursor }, endsNow: w.endsNow });
      setState({ kind: "ok", data });
    }).catch(async (e: Error) => {
      if (e instanceof ApiError && e.status === 400 && /more than one queue/.test(e.message)) {
        const ever = await getJSON<FlowsResponse>(`/api/flows?since_s=${400 * 86_400}`).catch(() => null);
        return live && setState({ kind: "ambiguous", flows: (ever?.flows ?? []).filter((f) => f.name === u0.name) });
      }
      if (live) setState({ kind: "error", message: e.message });
    });
    return () => { live = false; };
  }, [tick, keptMs]);

  // the span charts ask again every 30 s while the range ends now (§4.4), and the tab is showing
  useEffect(() => {
    if (u0.range.kind !== "preset") return;
    const id = setInterval(() => { if (!document.hidden) setTick((t) => t + 1); }, SPAN_REFRESH_MS);
    return () => clearInterval(id);
  }, []);

  if (!u0.name) {
    return (
      <Frame>
        <p className="text-sky-ink-2">Choose a queue from the <a className="text-dawn hover:underline" href="/queues">queue list</a>.</p>
      </Frame>
    );
  }

  if (state.kind === "ambiguous") {
    return (
      <Frame>
        <h1 className="font-mono text-[1.875rem] text-sky-ink">{u0.name}</h1>
        <p className="mt-2 text-sky-ink-2">More than one queue is called <span className="font-mono">{u0.name}</span>. Which one?</p>
        <ul className="mt-3 border-t border-sky-line">
          {state.flows.map((f) => (
            <li key={f.key} className="border-b border-sky-line py-2">
              <a className="font-mono text-dawn hover:underline" href={queueHref(f.name, atOf(f))}>{[f.account ?? "no account", f.region ?? "no region"].join(" · ")}</a>
              <span className="ml-3 text-caption text-sky-ink-3">last seen {utc(f.lastSeen, { date: true })} UTC</span>
            </li>
          ))}
        </ul>
      </Frame>
    );
  }

  const href: QueueHrefs = {
    queues: "/queues",
    queue: (name) => queueHref(name),
    // only "Sent" works on this server's lists today: they take a window but no sort or state filter (design §6)
    messages: ({ queue, fromMs, toMs, view }) => (view === "sent" ? `/?queue=${encodeURIComponent(queue)}&from_ms=${fromMs}&to_ms=${toMs - 1}` : null),
    stuck: (queue) => `/stuck?queue=${encodeURIComponent(queue)}`,
    range: (preset) => `/queue?${queueQuery({ name: u.name, at: u.at, split: u.split, range: { kind: "preset", preset } })}`,
  };
  const now = Date.now();
  const clipped = keptMs !== null && u0.range.kind === "fixed" && u0.range.fromMs < now - keptMs;
  const rangeNote = keptMs === null ? null : clipped ? `Spans before ${utc(now - keptMs, { date: true })} UTC aren't kept on this server.` : `Kept for ${daysText(keptMs)} on this server`;
  return (
    <Frame>
      <QueuePage
        name={u0.name}
        data={state.kind === "ok" ? state.data : null}
        error={state.kind === "error" ? state.message : null}
        onRetry={() => { setState({ kind: "loading" }); setTick((t) => t + 1); }}
        unknown={state.kind === "unknown"}
        href={href}
        picker={<Picker u={u} keptMs={keptMs} />}
        rangeNote={rangeNote}
        sel={sel}
        onSelect={(s) => { setSel(s); write({ sel: s }); }}
        split={split}
        onSplit={(on) => { setSplit(on); write({ split: on }); }}
      />
      <p className="mt-6 text-sm text-sky-ink-3">
        Times are in UTC. <Json href={`/api/queues/${encodeURIComponent(u0.name)}/series`} />
      </p>
    </Frame>
  );
}

type ListState = { kind: "loading" } | { kind: "error"; message: string } | { kind: "ok"; r: FlowsResponse };

/** /queues: every queue a span named in the last day, with its traffic, each linking to its page. */
export function QueuesListPage() {
  const [state, setState] = useState<ListState>({ kind: "loading" });
  useEffect(() => {
    document.title = "Queues · Trailbeam";
    getJSON<FlowsResponse>("/api/flows").then((r) => setState({ kind: "ok", r }), (e: Error) => setState({ kind: "error", message: e.message }));
  }, []);
  const th = "px-3 py-2 text-left font-mono text-caption font-medium text-sky-ink-3";
  const td = "px-3 py-2.5 font-mono text-caption tabular-nums text-sky-ink-2";
  return (
    <Frame>
      <h1 className="text-[1.75rem] text-sky-ink">Queues</h1>
      <p className="mt-1 max-w-[80ch] text-sky-ink-2">Every queue your services sent to or received from in the last 24 hours. Open one to see whether work is arriving faster than it&rsquo;s finished.</p>
      <div className="mt-5">
        {state.kind === "loading" && <p className="text-sky-ink-3">Loading queues</p>}
        {state.kind === "error" && <p className="text-sky-ink-2" role="alert">{state.message}</p>}
        {state.kind === "ok" && state.r.flows.length === 0 && (
          <p className="text-sky-ink-2">No service reported a send or receive in the last 24 hours. A queue appears here when one does.</p>
        )}
        {state.kind === "ok" && state.r.flows.length > 0 && (() => {
          const mins = Math.max(1, (state.r.to_ms - state.r.from_ms) / 60_000);
          return (
            <div className="overflow-x-auto border-y border-sky-line">
              <table className="w-full min-w-[720px] text-[15px]">
                <thead><tr className="border-b border-sky-line">
                  <th className={th}>Queue</th><th className={th}>Senders</th><th className={th}>Consumers</th>
                  <th className={`${th} text-right`}>Sends/min</th><th className={`${th} text-right`}>Receives/min</th><th className={`${th} text-right`}>Deletes/min</th>
                  <th className={th}>DLQ (seen on a move)</th><th className={`${th} text-right`}>Last activity</th>
                </tr></thead>
                <tbody>
                  {state.r.flows.map((f) => (
                    <tr key={f.key} className="border-b border-sky-line/60 last:border-0">
                      <td className="px-3 py-2.5"><a className="font-mono text-sky-ink hover:underline" href={queueHref(f.name, f.queue !== f.name ? atOf(f) : null)}>{f.queue}</a></td>
                      <td className="px-3 py-2.5 text-sm text-sky-ink-2">{f.producers.map((p) => p.name).join(", ") || "-"}</td>
                      <td className="px-3 py-2.5 text-sm text-sky-ink-2">{f.consumers.map((c) => c.name + (c.lambda ? " (Lambda)" : "")).join(", ") || "-"}</td>
                      <td className={`${td} text-right`}>{fmtRate(f.sent / mins)}</td>
                      <td className={`${td} text-right`}>{fmtRate(f.received / mins)}</td>
                      <td className={`${td} text-right`}>{fmtRate(f.deleted / mins)}</td>
                      <td className={td}>{f.dlq ? <a className="hover:underline" href={queueHref(f.dlq.name)}>{f.dlq.name}</a> : "-"}</td>
                      <td className={`${td} text-right`}>{f.lastSeen ? `${utc(f.lastSeen, { date: true })} UTC` : "-"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          );
        })()}
        <p className="mt-3 text-sm text-sky-ink-3">Rates are averages over the last 24 hours, from your services&rsquo; spans. Trailbeam makes no AWS calls, so queue depth isn&rsquo;t here: the SQS console shows it. <Json href="/api/flows" /></p>
      </div>
    </Frame>
  );
}
