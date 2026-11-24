/*
  The Overview at /overview (QV-392 design), drawn with the shared Overview from GET /api/flows on the same
  origin. Its state is in the URL (overview/selfhosted.ts): range=1h|24h|7d or from_ms/to_ms, flow=<key>,
  idle=1, refresh=0 and q=. A range that ends now asks again every 15 s while the tab is showing; a refresh
  updates figures in place and never reorders rows. /queues, the queue list QV-360 planned, is this page.
*/
import { useEffect, useRef, useState, type FormEvent } from "react";
import {
  Overview, RANGE_PRESETS, REFRESH_S, fromInputUtc, overviewHref, overviewWindow, pinnedOverview, readOverviewUrl, selfHostedHrefs, storeIsEmpty,
  toInputUtc, widerPresets, type Flow, type FlowsResponse, type OverviewState, type OverviewUrl,
} from "../src/index.ts";
import { ApiError, CopyLink, Frame, Json, getHealth, getJSON } from "./chrome.tsx";

const DOCS = "https://github.com/nick-lennox/trailbeam/blob/main/docs/api.md#list-flows";
const input = "rounded-full border border-sky-line bg-sky-2 px-3 py-1 font-mono text-caption text-sky-ink";

/** The server's default retention in ms ("7d", "36h"), or null for forever or unknown. */
function retentionMs(text: string | undefined): number | null {
  const m = /^(\d+)([smhd])$/.exec(text ?? "");
  if (!m) return null;
  return Number(m[1]) * { s: 1_000, m: 60_000, h: 3_600_000, d: 86_400_000 }[m[2] as "s" | "m" | "h" | "d"];
}

/** The lists' presets plus custom (two UTC times); no "all time" here. */
function RangeControl({ u }: { u: OverviewUrl }) {
  const [kind, setKind] = useState<string>(u.range.kind === "preset" ? u.range.preset : "custom");
  const custom: { from_ms?: number; to_ms?: number } = u.range.kind === "custom" ? u.range : {};
  const go = (next: OverviewUrl) => location.assign(overviewHref(next));
  const apply = (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    const f = new FormData(e.currentTarget);
    const from_ms = fromInputUtc(String(f.get("from") ?? ""));
    const to = fromInputUtc(String(f.get("to") ?? ""));
    go({ ...u, range: { kind: "custom", from_ms, to_ms: to === undefined ? undefined : to + 59_999 } });
  };
  return (
    <form onSubmit={apply} className="flex flex-wrap items-center gap-2 text-sm text-sky-ink-2">
      <label className="inline-flex items-center gap-2">
        <span className="sr-only">Time range</span>
        <select value={kind} className={input} onChange={(e) => {
          setKind(e.target.value);
          if (e.target.value !== "custom") go({ ...u, range: { kind: "preset", preset: e.target.value as "1h" | "24h" | "7d" } });
        }}>
          {RANGE_PRESETS.map(([k, label]) => <option key={k} value={k}>{label}</option>)}
          <option value="custom">Custom</option>
        </select>
      </label>
      {kind === "custom" && <>
        <label className="inline-flex items-center gap-1"><span>From</span>
          <input name="from" type="datetime-local" defaultValue={custom.from_ms !== undefined ? toInputUtc(custom.from_ms) : ""} className={input} /></label>
        <label className="inline-flex items-center gap-1"><span>to</span>
          <input name="to" type="datetime-local" defaultValue={custom.to_ms !== undefined ? toInputUtc(custom.to_ms) : ""} className={input} /></label>
        <span className="text-caption text-sky-ink-3">UTC</span>
        <button className="rounded-full border border-sky-line px-3 py-0.5 text-sky-ink hover:border-sky-ink">Apply</button>
      </>}
    </form>
  );
}

export function OverviewPage() {
  const params = Object.fromEntries(new URLSearchParams(location.search));
  const read = readOverviewUrl(params);
  const [u, setU] = useState<OverviewUrl | null>("error" in read ? null : read);
  const [flows, setFlows] = useState<Flow[]>([]);
  const [state, setState] = useState<OverviewState>({ kind: "loading" });
  const [keptMs, setKeptMs] = useState<number | null | undefined>(undefined);
  const [tick, setTick] = useState(0);
  const [now, setNow] = useState(() => Date.now());
  const last = useRef<number | null>(null);

  // the URL is the page's state: flow=, idle=, refresh= and q= are written back as they change
  const write = (next: OverviewUrl) => { setU(next); history.replaceState(history.state, "", overviewHref(next)); };

  useEffect(() => {
    document.title = "Overview · Trailbeam";
    getHealth(5000).then((h) => {
      setKeptMs(retentionMs(h.config?.retention?.default));
      if (storeIsEmpty(h)) setState({ kind: "empty-store" });
    }, () => setKeptMs(null));
  }, []);

  useEffect(() => {
    if (!u || keptMs === undefined || state.kind === "empty-store") return;
    let live = true;
    const at = Date.now();
    const w = overviewWindow(u.range, at);
    const kept = keptMs !== null ? at - keptMs : null;
    const fromMs = kept !== null && w.fromMs < kept ? kept : w.fromMs; // a start before what's kept is clipped
    getJSON<FlowsResponse>(`/api/flows?from_ms=${fromMs}&to_ms=${w.toMs}`).then((r) => {
      if (!live) return;
      last.current = at;
      setNow(at);
      setFlows(r.flows);
      setState({ kind: "ok", updatedAt: at });
    }, (e: Error) => {
      if (!live) return;
      if (e instanceof ApiError && e.status === 501) return setState({ kind: "unsupported" });
      setState(last.current ? { kind: "error", message: e.message, staleAt: last.current, at } : { kind: "error", message: e.message });
    });
    return () => { live = false; };
  }, [tick, keptMs, u?.range]);

  // a range that ends now asks again every 15 s while the tab is showing, unless paused (refresh=0)
  const endsNow = u ? overviewWindow(u.range, now).endsNow : false;
  useEffect(() => {
    if (!u || !endsNow || !u.refresh) return;
    const id = setInterval(() => { if (!document.hidden) setTick((t) => t + 1); }, REFRESH_S * 1000);
    const back = () => { if (!document.hidden) setTick((t) => t + 1); };
    document.addEventListener("visibilitychange", back);
    return () => { clearInterval(id); document.removeEventListener("visibilitychange", back); };
  }, [endsNow, u?.refresh]);

  if (!u) {
    return <Frame><h1 className="text-xl text-sky-ink">Overview</h1><p className="mt-3 text-sky-ink-2" role="alert">{"error" in read ? read.error : ""}</p></Frame>;
  }

  const w = overviewWindow(u.range, now);
  const clipped = keptMs != null && w.fromMs < now - keptMs;
  const from = clipped ? now - keptMs! : w.fromMs;
  const range = { ...w, fromMs: from };
  const t = (ms: number) => new Date(ms).toISOString().slice(11, 16);
  const json = `/api/flows?from_ms=${range.fromMs}&to_ms=${range.toMs}`;
  return (
    <Frame>
      <Overview
        flows={flows}
        range={range}
        rangeControl={<RangeControl u={u} />}
        rangeNote={clipped ? `Spans before ${new Date(now - keptMs!).toISOString().slice(0, 16).replace("T", " ")} UTC aren't kept on this server.` : undefined}
        copyLink={<CopyLink link={() => pinnedOverview(u, Date.now())} />}
        open={u.flow}
        onOpen={(key) => write({ ...u, flow: key ?? "" })}
        showIdle={u.idle}
        onShowIdle={() => write({ ...u, idle: true })}
        query={u.q}
        onQuery={(q) => write({ ...u, q })}
        state={state}
        refresh={endsNow ? { on: u.refresh, toggle: () => write({ ...u, refresh: !u.refresh }), now: () => setTick((x) => x + 1) } : null}
        onRetry={() => { setState({ kind: "loading" }); setTick((x) => x + 1); }}
        widen={widerPresets(u)}
        href={selfHostedHrefs(u, now)}
        unsupportedHref={DOCS}
        foot={state.kind === "ok" || state.kind === "error" ? (
          <>
            <p>Counts are of the spans your services reported between {t(range.fromMs)} and {t(range.toMs)} UTC. Trailbeam makes no AWS calls, so it can&rsquo;t see queue depth or the oldest message: open a queue&rsquo;s page for what it can see, and the SQS console for the rest.</p>
            <p className="mt-1"><Json href={json} /></p>
          </>
        ) : undefined}
      />
    </Frame>
  );
}

/** /queues: the queue list QV-360 planned is the Overview, so it redirects there with the range kept. */
export function QueuesRedirect() {
  useEffect(() => {
    const p = new URLSearchParams(location.search);
    const keep = new URLSearchParams();
    for (const k of ["range", "from_ms", "to_ms"]) { const v = p.get(k); if (v) keep.set(k, v); }
    location.replace(keep.size ? `/overview?${keep}` : "/overview");
  }, []);
  return <Frame><p className="text-sky-ink-2">The queue list is the <a className="text-dawn hover:underline" href="/overview">Overview</a>.</p></Frame>;
}
