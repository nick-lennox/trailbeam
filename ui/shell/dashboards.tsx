/*
  Dashboards (/dashboards, row 13 of the one-UI plan): the shared views (@trailbeam/ui/dashboards/views) in the
  shell's frame (frame.tsx), on the server's /api/dashboards, which answers only while TRAILBEAM_DASHBOARDS is on.
    /dashboards                  every dashboard, and which of their widgets alert (each widget's `alert` from
                                 GET /api/dashboards/{id}/measures, worked out by the server)
    /dashboards?id=<id>          one dashboard; range=, or from= and to=, is its time range, as in the hosted app
  A dashboard is drawn from the events the server read for it (GET /api/dashboards/{id}/measures?data=1), as the
  hosted app draws it from the events it loaded: one read that covers the link's range, every widget's own range
  and at least the past day, again every 30 seconds while the dashboard follows the present. Changes are saved
  through the same API, as the hosted app's actions save them.
*/
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { dashboardMeasures, makeStarter, type Dashboard, type DashboardDef, type EventData } from "../src/dashboards/index.ts";
import { DashboardsEnvProvider, DashboardsPage, DashboardView, type DashboardActions, type DashboardsEnv, type Refused, type ServerAlerts } from "../src/dashboards/views.ts";
import { DAY, preset, queryLanguage, rangeFromParams, type TimeRange } from "../src/model/index.ts";
import { ApiError, getHealth, getJSON, messageHref, sendJSON } from "./chrome.tsx";
import { ShellFrame, shellQueueHref } from "./frame.tsx";
import { qs } from "./lists.tsx";

const ENGINE = dashboardMeasures();
const LANG = queryLanguage();
const MAX_SPAN = 30 * DAY;
const EVERY_MS = 30_000;

/** The server's Dashboard (contract/openapi.yaml): the hosted record and what this server keeps besides. */
export type ApiDashboard = Dashboard & { time: string | null; createdById: string | null; createdAt: number; version: number };
type WidgetMeasures = { id: string | null; alert: { tone: "good" | "warning" | "critical"; value: string; limit: string; queue: string } | null } | { id: string | null; skipped: string };
type MeasuresAnswer = { now: number; widgets: WidgetMeasures[]; data?: EventData };

/**
  The views' links on this server: the hosted paths they build, as this shell's pages. A dashboard is
  /dashboards?id=<id>, a queue's messages its queue page, a message its timeline, and the workspace's front page
  the Overview.
*/
export function dashboardsHref(path = ""): string {
  if (path === "") return "/overview";
  const [p, query = ""] = path.split("?");
  const s = new URLSearchParams(query);
  const one = /^\/dashboards\/([^/]+)$/.exec(p);
  if (one) { s.set("id", decodeURIComponent(one[1])); return `/dashboards?${s}`; }
  const msg = /^\/messages\/([^/]+)$/.exec(p);
  if (msg) return messageHref(decodeURIComponent(msg[1]));
  if (p === "/messages") return s.get("queue") ? shellQueueHref(s.get("queue")!) : "/";
  return path;
}

const said = (e: unknown): Refused => ({ ok: false, error: e instanceof Error ? e.message : String(e) });
const D = (id: string) => `/api/dashboards/${encodeURIComponent(id)}`;

/**
  The views' changes, saved through /api/dashboards. A starter is made here and sent as the new dashboard's
  definition; on a server that does not read AWS (aws false) it is the starter built from the spans alone (QV-772).
*/
export const actionsFor = (aws?: boolean): DashboardActions => ({
  async create(name, from, imported) {
    const def: DashboardDef | undefined = imported ? undefined : makeStarter(from, { aws });
    try {
      const r = await sendJSON<{ dashboard: ApiDashboard }>("POST", "/api/dashboards", { name, ...(imported ? { imported } : def ? { def } : {}) });
      return { ok: true, id: r.dashboard.id };
    } catch (e) { return said(e); }
  },
  async save(id, patch) {
    try { await sendJSON("PATCH", D(id), patch); return { ok: true }; } catch (e) { return said(e); }
  },
  async star(id, starred) {
    try { await sendJSON("PATCH", D(id), { starred }); return { ok: true }; } catch (e) { return said(e); }
  },
  async lock(id, lock) {
    try { const r = await sendJSON<{ lockedBy: string | null }>("POST", `${D(id)}/lock`, { lock }); return { ok: true, lockedBy: r.lockedBy }; } catch (e) { return said(e); }
  },
  async duplicate(id) {
    try { const r = await sendJSON<{ dashboard: ApiDashboard }>("POST", `${D(id)}/duplicate`); return { ok: true, id: r.dashboard.id }; } catch (e) { return said(e); }
  },
  async remove(id) {
    try { await sendJSON("DELETE", D(id)); return { ok: true }; } catch (e) { return said(e); }
  },
});
export const apiActions = actionsFor();

/** The time range a dashboard's link names, else the time the dashboard opens at, else the past hour. */
export function dashboardTime(search: string, d: Pick<ApiDashboard, "time">, now: number): TimeRange {
  const p = new URLSearchParams(search);
  return rangeFromParams((k) => p.get(k), now, d.time ?? "1h");
}

/** The one read a dashboard is drawn from: the link's range, every widget's own range and the past day, up to 30 days. */
export function dataWindow(time: TimeRange, def: DashboardDef, now: number): { from: number; to: number } {
  const own = def.widgets.flatMap((w) => (w.time ? [preset(w.time, now).from] : []));
  const to = own.length ? Math.max(time.to, now) : time.to;
  const from = Math.max(to - MAX_SPAN, Math.min(time.from, to - DAY, ...own));
  return { from: Math.floor(from), to: Math.ceil(to) };
}

export const measuresPath = (id: string, w: { from: number; to: number }, data = true) =>
  `${D(id)}/measures?${qs({ from: String(w.from), to: String(w.to), n: "1", ...(data ? { data: "1" } : {}) })}`;

/** The list's alerts, from what the server worked out for each dashboard's widgets over its own time. */
export function alertsOf(dashboards: Dashboard[], answers: (MeasuresAnswer | null)[]): ServerAlerts["states"] {
  return dashboards.flatMap((d, i) => (answers[i]?.widgets ?? []).flatMap((w) => {
    if ("skipped" in w || !w.alert || !w.id) return [];
    const widget = d.def.widgets.find((x) => x.id === w.id);
    return widget ? [{ dashboardId: d.id, widgetId: w.id, title: widget.title, ...w.alert, since: answers[i]!.now }] : [];
  }));
}

/** A 404 that is not about one dashboard: the server runs without TRAILBEAM_DASHBOARDS. */
const OFF = "Dashboards are off on this Trailbeam server. Start it with TRAILBEAM_DASHBOARDS=1 to use them.";
const GONE = "This dashboard no longer exists.";
export const problem = (e: unknown) =>
  e instanceof ApiError && e.status === 404 ? (e.message.includes(GONE) ? GONE : OFF) : e instanceof Error ? e.message : String(e);

const EMPTY: EventData = { live: true, events: [], now: Date.now(), queues: [], capped: false };

type State<T> = { kind: "loading" } | { kind: "error"; message: string } | { kind: "ok"; value: T };

function Note({ children, alert = false }: { children: string; alert?: boolean }) {
  return <p className="px-8 pt-6 text-sm text-ink-2" role={alert ? "alert" : undefined}>{children}</p>;
}

/** Ticks every 30 seconds while the tab is showing: the view draws it only while a dashboard follows the present. */
function Every({ onTick }: { onTick: () => void }) {
  const tick = useRef(onTick);
  tick.current = onTick;
  useEffect(() => {
    const id = setInterval(() => { if (!document.hidden) tick.current(); }, EVERY_MS);
    return () => clearInterval(id);
  }, []);
  return null;
}

/* aws: whether this server reads AWS (GET /api/health's config.aws.on); undefined while that is not known */
function useEnv(data: EventData, liveRefresh?: DashboardsEnv["liveRefresh"], aws?: boolean): DashboardsEnv {
  return useMemo(() => ({ engine: ENGINE, lang: LANG, data, href: dashboardsHref, actions: actionsFor(aws), liveRefresh, aws }), [data, liveRefresh, aws]);
}

function ListPage() {
  const [state, setState] = useState<State<{ dashboards: ApiDashboard[]; alerts: ServerAlerts["states"] }>>({ kind: "loading" });
  const [aws, setAws] = useState<boolean | undefined>(undefined);
  useEffect(() => {
    document.title = "Dashboards, Trailbeam";
    // whether this server reads AWS decides which starters it offers; unknown, they are the hosted app's
    getHealth(5000).then((h) => setAws(!!h.config?.aws?.on), () => setAws(undefined));
    const now = Date.now();
    getJSON<{ dashboards: ApiDashboard[] }>("/api/dashboards")
      .then(async ({ dashboards }) => {
        // each dashboard's alerts over its own time; one that can't be read has none to show
        const answers = await Promise.all(dashboards.map((d) =>
          getJSON<MeasuresAnswer>(measuresPath(d.id, dataWindow(preset(d.time ?? "1h", now), d.def, now), false), 60_000).catch(() => null)));
        setState({ kind: "ok", value: { dashboards, alerts: alertsOf(dashboards, answers) } });
      })
      .catch((e) => setState({ kind: "error", message: problem(e) }));
  }, []);
  const env = useEnv(EMPTY, undefined, aws);
  return (
    <ShellFrame path="/dashboards">
      {state.kind === "ok" ? (
        <DashboardsEnvProvider value={env}>
          <DashboardsPage dashboards={state.value.dashboards} server={{ states: state.value.alerts, error: null }} />
        </DashboardsEnvProvider>
      ) : <><h1 className="sr-only">Dashboards</h1><Note alert={state.kind === "error"}>{state.kind === "loading" ? "Loading dashboards" : state.message}</Note></>}
    </ShellFrame>
  );
}

function OnePage({ id }: { id: string }) {
  const [state, setState] = useState<State<ApiDashboard>>({ kind: "loading" });
  const [data, setData] = useState<EventData | null>(null);
  const [failed, setFailed] = useState<string | null>(null);
  const range = useMemo(() => { const p = new URLSearchParams(location.search); return { range: p.get("range") ?? undefined, from: p.get("from") ?? undefined, to: p.get("to") ?? undefined }; }, []);

  const asked = useRef(0);
  const load = useCallback((d: ApiDashboard) => {
    const n = ++asked.current;
    const now = Date.now();
    const w = dataWindow(dashboardTime(location.search, d, now), d.def, now);
    getJSON<MeasuresAnswer>(measuresPath(d.id, w), 60_000)
      .then((r) => { if (n === asked.current && r.data) { setData(r.data); setFailed(null); } })
      .catch((e) => { if (n === asked.current) setFailed(problem(e)); });
  }, []);

  useEffect(() => {
    getJSON<{ dashboard: ApiDashboard }>(D(id))
      .then(({ dashboard }) => { document.title = `${dashboard.name}, Trailbeam`; setState({ kind: "ok", value: dashboard }); load(dashboard); })
      .catch((e) => setState({ kind: "error", message: problem(e) }));
  }, [id, load]);

  const dashboard = state.kind === "ok" ? state.value : null;
  const live = useMemo(() => (dashboard ? <Every onTick={() => load(dashboard)} /> : undefined), [dashboard, load]);
  const env = useEnv(data ?? EMPTY, live);
  return (
    <ShellFrame path="/dashboards">
      {dashboard && data ? (
        <DashboardsEnvProvider value={env}>
          {failed && <Note alert>{failed}</Note>}
          <DashboardView dashboard={dashboard} range={range} />
        </DashboardsEnvProvider>
      ) : <><h1 className="sr-only">Dashboard</h1><Note alert={state.kind === "error" || !!failed}>{state.kind === "error" ? state.message : failed ?? "Loading the dashboard"}</Note></>}
    </ShellFrame>
  );
}

export function DashboardsShellPage() {
  const id = new URLSearchParams(location.search).get("id");
  return id ? <OnePage id={id} /> : <ListPage />;
}
