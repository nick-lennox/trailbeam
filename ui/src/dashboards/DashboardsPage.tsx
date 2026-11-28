"use client";

import { Fragment, useMemo, useState, type ReactNode } from "react";
import { ChevronDown, Lock, MoreHorizontal, Plus, Search, Star } from "lucide-react";
import { Button } from "../design/Button.tsx";
import { UiLink, useNav } from "../design/nav.tsx";
import { TH } from "../explorer/columns.ts";
import { useDashboardsEnv } from "./env.tsx";
import { fmtLimit, TONE_WORD, type Alert, type Tone } from "./measures.ts";
import { makeStarter, STARTERS, type Dashboard, type Widget } from "./types.ts";
import { Mini } from "./Mini.tsx";
import { FIELD, IconBtn, Labeled, Menu, MenuItem, Modal, StateMark, TONE_TEXT, TONE_VAR } from "./ui.tsx";

/*
  Every dashboard in the workspace, and which of their widgets are alerting.
  Most workspaces have a handful of dashboards, so this page is not a file
  list: it says whether any of them needs you before you open one.

  On a workspace's own data, alerts are the server's: what the scheduled check
  (lib/alerts) found and delivered, passed in as server. On sample data there
  is no check, so the rules are evaluated here, on the sample events.
*/
export type ServerAlerts = {
  states: { dashboardId: string; widgetId: string; title: string; tone: Tone; value: string; limit: string; queue: string; since: number }[];
  error: string | null;
  /** when the check last ran and whether it is late, in a sentence (the hosted app's checkStatus); none: nothing to say */
  check?: { text: string; late: boolean };
};
type Shown = { widget: Widget; widgetId: string; title: string; tone: Tone; value: string; limit: string; queue: string };
const fromRule = (a: Alert): Shown => ({ widget: a.widget, widgetId: a.widget.id, title: a.widget.title, tone: a.tone, value: a.value, limit: a.limit, queue: a.queue });

const SEC = "mt-7 mb-2.5 flex items-baseline gap-2 text-sm font-semibold text-ink";
const when = (ms: number) => {
  const d = new Date(ms), now = new Date();
  const hm = `${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(2, "0")}`;
  return d.toDateString() === now.toDateString() ? `today at ${hm}` : d.toLocaleDateString("en-GB", { day: "numeric", month: "short" });
};

export function DashboardsPage({ dashboards, server }: { dashboards: Dashboard[]; server?: ServerAlerts }) {
  const env = useDashboardsEnv();
  const { engine: E, actions } = env;
  const ws = env.href;
  const router = useNav();
  const [find, setFind] = useState("");
  const [creating, setCreating] = useState<string | null>(null);
  const [note, setNote] = useState<string | null>(null);
  const data = env.data;
  const [open, setOpen] = useState<string | null>(null);

  /* A widget alerts when the rule set on it is breached. On own data the server's check says so; its titles are read from the dashboard, so a rename shows at once. */
  const alerts = useMemo(
    () => dashboards.map((d) => ({ d, alerts: server
      ? server.states.flatMap((s): Shown[] => {
        const w = s.dashboardId === d.id ? d.def.widgets.find((x) => x.id === s.widgetId && x.rule) : undefined;
        return w ? [{ widget: w, widgetId: s.widgetId, title: w.title || s.title, tone: s.tone, value: s.value, limit: s.limit, queue: s.queue }] : [];
      })
      : d.def.widgets.map((w) => E.alertOf(w, d.def.variables, data)).filter((a): a is Alert => !!a).map(fromRule) })),
    [dashboards, data, server, E],
  );
  const firing = alerts.flatMap(({ d, alerts }) => alerts.filter((a) => a.tone !== "good").map((a) => ({ d, a }))).sort((x, y) => (x.a.tone === "critical" ? 0 : 1) - (y.a.tone === "critical" ? 0 : 1));
  const onBoards = new Set(firing.map((f) => f.d.id)).size;
  const byQueue = new Map<string, number>();
  for (const f of firing) if (f.a.queue) byQueue.set(f.a.queue, (byQueue.get(f.a.queue) ?? 0) + 1);
  const common = [...byQueue].sort((a, b) => b[1] - a[1])[0];
  const shown = dashboards.filter((d) => d.name.toLowerCase().includes(find.trim().toLowerCase()));
  const check = server?.check ?? null;
  const act = async (r: Promise<{ ok: boolean; error?: string }>) => { const x = await r; setNote(x.ok ? null : x.error ?? null); router.refresh(); };

  return (
    <div className="px-8 pt-7 pb-10">
      <div className="flex flex-wrap items-center gap-2">
        <h1 className="font-serif text-[22px] tracking-tight text-ink">Dashboards</h1>
        <span className="flex-1" />
        <label className="flex h-9 w-60 items-center gap-2 rounded-lg border border-line-strong bg-surface px-2.5 text-sm text-ink-3 focus-within:border-accent">
          <Search size={14} />
          <input value={find} onChange={(e) => setFind(e.target.value)} placeholder="Find a dashboard" aria-label="Find a dashboard" className="min-w-0 flex-1 bg-transparent text-ink !outline-none placeholder:text-ink-3" />
        </label>
        <Button variant="primary" size="sm" className="h-9" onClick={() => setCreating("")}><Plus size={14} /> New dashboard</Button>
      </div>
      {note && <p className="mt-2 text-[13px] text-critical-text">{note}</p>}

      {dashboards.length > 0 && (
        <>
          <h2 className={SEC}>Active alerts <span className="text-[13px] font-normal text-ink-3">{firing.length}</span>
            {check && <span className={`ml-auto text-xs font-normal ${check.late ? "text-critical-text" : "text-ink-3"}`}>{check.text}</span>}
          </h2>
          {server?.error && <p role="alert" className="-mt-1 mb-2.5 text-xs text-critical-text">The last check could not read this workspace&rsquo;s data: {server.error}</p>}
          {firing.length ? (
            /* One line per alert, read left to right as a statement: what is alerting and the limit it is past,
               then the figure now, then the queue behind it. Selecting a row opens the rest: the rule's
               thresholds, what it measures and over what time, and the way to its dashboard. */
            <div className="overflow-x-auto rounded-card border border-line bg-surface">
              <table className="w-full border-collapse">
                <thead><tr>{["Alert", "Now", "Queue", ""].map((h, i) => <th key={i} className={`${TH} first:pl-4 last:w-10`}>{h}</th>)}</tr></thead>
                <tbody>
                  {firing.map(({ d, a }) => {
                    const key = `${d.id}-${a.widgetId}`, on = open === key, rule = a.widget.rule, m = E.measure(a.widget.queries?.[0]?.measure ?? "");
                    const cell = "border-b border-line px-2.5 py-3 group-hover:bg-sunken";
                    const facts: [string, ReactNode][] = [
                      ["Severity", TONE_WORD[a.tone]],
                      ...(rule?.warn !== undefined && m ? [["Warning", <>{rule.below ? "below" : "above"} <span className="font-mono text-caption">{fmtLimit(rule.warn, m.unit)}</span></>] as [string, ReactNode]] : []),
                      ...(rule?.crit !== undefined && m ? [["Critical", <>{rule.below ? "below" : "above"} <span className="font-mono text-caption">{fmtLimit(rule.crit, m.unit)}</span></>] as [string, ReactNode]] : []),
                      ...(m ? [["Measure", m.label] as [string, string]] : []),
                      ...(a.widget.filter ? [["Filter", <span className="font-mono text-caption">{a.widget.filter}</span>] as [string, ReactNode]] : []),
                      ["Time window", <span className="font-mono text-caption">{a.widget.time ?? "1h"}</span>],
                    ];
                    return (
                      <Fragment key={key}>
                        <tr className="group cursor-pointer" aria-expanded={on} onClick={() => setOpen(on ? null : key)}>
                          <td className={`${cell} pl-4`}>
                            <span className="flex items-baseline gap-2.5 whitespace-nowrap">
                              <span className="size-2.5 shrink-0 translate-y-[-1px] rounded-[2px]" style={{ background: TONE_VAR[a.tone] }} />
                              <span className="text-[15px] font-semibold text-ink">{a.title}</span>
                              <span className="text-sm text-ink-2">{a.limit}</span>
                            </span>
                          </td>
                          <td className={`${cell} font-mono text-[17px] leading-6 font-medium whitespace-nowrap tabular-nums ${TONE_TEXT[a.tone]}`}>{a.value}</td>
                          <td className={`${cell} font-mono text-caption whitespace-nowrap text-ink-2`}>{a.queue || <span className="font-sans text-ink-3">none</span>}</td>
                          <td className={`${cell} pr-3 text-ink-3`}><ChevronDown size={16} className={`transition-transform ${on ? "rotate-180" : ""}`} aria-hidden /></td>
                        </tr>
                        {on && (
                          <tr>
                            <td colSpan={4} className="border-b border-line bg-sunken px-4 py-3.5">
                              <div className="flex flex-wrap items-start gap-x-10 gap-y-3">
                                <dl className="grid grid-cols-[auto_auto] gap-x-6 gap-y-1.5 text-[13px]">
                                  {facts.map(([k, v]) => (
                                    <Fragment key={k}><dt className="text-ink-3">{k}</dt><dd className="text-ink">{v}</dd></Fragment>
                                  ))}
                                </dl>
                                <UiLink href={ws(`/dashboards/${d.id}`)} className="ml-auto inline-flex h-8 items-center rounded-lg border border-line-strong bg-surface px-3 text-[13px] font-medium text-ink hover:border-ink-3">Open {d.name}</UiLink>
                              </div>
                            </td>
                          </tr>
                        )}
                      </Fragment>
                    );
                  })}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="rounded-card border border-line bg-surface px-4 py-3.5 text-sm text-ink-2">
              <StateMark tone="good">None</StateMark>
            </div>
          )}
        </>
      )}

      <h2 className={SEC}>Your dashboards <span className="text-[13px] font-normal text-ink-3">{dashboards.length}</span></h2>
      {shown.length ? (
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
          {shown.map((d) => {
            const mine = alerts.find((x) => x.d.id === d.id)!.alerts;
            const bad = mine.filter((a) => a.tone !== "good");
            return (
              <article key={d.id} className="relative flex flex-col gap-3 rounded-card border border-line bg-surface p-3.5 shadow-card hover:border-line-strong">
                <UiLink href={ws(`/dashboards/${d.id}`)} aria-label={`Open ${d.name}`} className="absolute inset-0 rounded-card" />
                <Mini def={d.def} tones={Object.fromEntries(mine.map((a) => [a.widgetId, a.tone]))} />
                <div className="flex items-center gap-1.5 text-[15px] font-semibold text-ink">
                  <span className="truncate">{d.name}</span>
                  <span className="relative z-10 -my-1 -mr-1.5 ml-auto flex items-center">
                    <IconBtn label={d.starred ? "Remove the star" : "Star this dashboard"} className={d.starred ? "!text-accent" : ""} onClick={() => act(actions.star(d.id, !d.starred))}><Star size={15} fill={d.starred ? "currentColor" : "none"} /></IconBtn>
                    <Menu button={(_, toggle) => <IconBtn label="Dashboard menu" onClick={toggle}><MoreHorizontal size={15} /></IconBtn>}>
                      {(close) => (
                        <>
                          <MenuItem onClick={() => { close(); act(actions.duplicate(d.id)); }}>Duplicate</MenuItem>
                          <MenuItem onClick={() => { close(); act(actions.lock(d.id, !d.lockedBy)); }}><Lock size={14} /> {d.lockedBy ? "Unlock" : "Lock"}</MenuItem>
                          <div className="my-1 h-px bg-line" />
                          <MenuItem danger disabled={!!d.lockedBy} onClick={() => { close(); if (window.confirm(`Delete ${d.name} and its ${d.def.widgets.length} widgets for everyone in this workspace? It cannot be undone.`)) act(actions.remove(d.id)); }}>Delete</MenuItem>
                        </>
                      )}
                    </Menu>
                  </span>
                </div>
                <div className="grid gap-1 text-[13px] text-ink-2">
                  {bad.length ? bad.slice(0, 4).map((a) => (
                    <div key={a.widgetId} className="flex min-w-0 items-center gap-2">
                      <span className="size-2 shrink-0 rounded-[2px]" style={{ background: TONE_VAR[a.tone] }} />
                      <span className="shrink-0 whitespace-nowrap">{a.title}</span>
                      <span className="min-w-0 flex-1 truncate font-mono text-caption text-ink-3">{a.queue}</span>
                      <b className={`shrink-0 font-mono text-caption font-medium ${TONE_TEXT[a.tone]}`}>{a.value}</b>
                    </div>
                  )) : mine.length ? <StateMark tone="good">No active alerts</StateMark> : null}
                </div>
                <div className="mt-auto flex items-center gap-2 border-t border-line pt-2.5 text-xs text-ink-3">
                  <span className="truncate">{d.def.widgets.length} {d.def.widgets.length === 1 ? "widget" : "widgets"}{d.updatedBy === "Trailbeam" ? "" : `, edited ${when(d.updatedAt)} by ${d.updatedBy}`}</span>
                  {d.lockedBy && <span className="ml-auto inline-flex shrink-0 items-center gap-1"><Lock size={11} /> Locked</span>}
                </div>
              </article>
            );
          })}
        </div>
      ) : (
        <div className="rounded-card border border-line bg-surface px-6 py-10 text-center text-sm text-ink-2">
          {dashboards.length ? <>No dashboard is called &ldquo;{find}&rdquo;.</> : "No dashboards yet."}
        </div>
      )}

      <h2 className={SEC}>Starters</h2>
      <div className="grid gap-3 md:grid-cols-3">
        {STARTERS.map((s) => (
          <button key={s.id} type="button" onClick={() => setCreating(s.id)} className="grid content-start gap-2 rounded-card border border-line bg-surface p-2.5 text-left hover:border-line-strong">
            <StarterMini id={s.id} />
            <b className="px-1 pb-1 text-sm font-semibold text-ink">{s.name}</b>
          </button>
        ))}
      </div>

      {creating !== null && <NewDashboard from={creating} onClose={() => setCreating(null)} onMade={(id) => router.push(ws(`/dashboards/${id}`))} />}
    </div>
  );
}

function StarterMini({ id }: { id: string }) {
  const { aws } = useDashboardsEnv();
  const def = useMemo(() => makeStarter(id, { aws })!, [id, aws]);
  return <Mini def={def} rows={10} />;
}

function NewDashboard({ from, onClose, onMade }: { from: string; onClose: () => void; onMade: (id: string) => void }) {
  const [name, setName] = useState(STARTERS.find((s) => s.id === from)?.name ?? "");
  const [start, setStart] = useState(from);
  const [json, setJson] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const { actions } = useDashboardsEnv();
  const create = async () => {
    setBusy(true);
    const r = await actions.create(name, start, json?.trim() || undefined);
    setBusy(false);
    if (r.ok) onMade(r.id); else setError(r.error);
  };
  return (
    <Modal
      title="New dashboard"
      onClose={onClose}
      width="max-w-[760px]"
      footer={
        <>
          <Button variant="ghost" size="sm" className="mr-auto" onClick={() => setJson(json === null ? "" : null)}>{json === null ? "Import JSON" : "Start from a starter"}</Button>
          <Button variant="secondary" size="sm" onClick={onClose}>Cancel</Button>
          <Button variant="primary" size="sm" disabled={busy || !name.trim()} className="disabled:opacity-50" onClick={create}>Create dashboard</Button>
        </>
      }
    >
      <div className="grid gap-4 px-5 pt-3.5 pb-5">
        <Labeled label="Name"><input autoFocus value={name} onChange={(e) => setName(e.target.value)} onKeyDown={(e) => e.key === "Enter" && name.trim() && create()} placeholder="Queue health, payments team" className={FIELD} /></Labeled>
        {json === null ? (
          <div className="grid gap-1.5">
            <span className="text-[13px] text-ink-2">Start from</span>
            <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
              {[{ id: "", name: "Blank", hint: "An empty grid." }, ...STARTERS].map((s) => (
                <button key={s.id} type="button" aria-pressed={start === s.id} onClick={() => { setStart(s.id); if (!name.trim() || STARTERS.some((x) => x.name === name)) setName(s.id ? s.name : ""); }} className={`grid content-start gap-2 rounded-card border bg-surface p-2 text-left ${start === s.id ? "border-brand ring-1 ring-brand" : "border-line hover:border-line-strong"}`}>
                  {s.id ? <StarterMini id={s.id} /> : <span className="grid h-[120px] place-items-center rounded-lg bg-canvas text-ink-3"><Plus size={18} /></span>}
                  <b className="px-1 text-sm font-semibold text-ink">{s.name}</b>
                  <span className="px-1 pb-1 text-xs leading-normal text-ink-3">{s.hint}</span>
                </button>
              ))}
            </div>
          </div>
        ) : (
          <Labeled label="Dashboard JSON" hint="Paste what Export JSON wrote.">
            <textarea value={json} onChange={(e) => setJson(e.target.value)} spellCheck={false} rows={10} className={`${FIELD} h-auto py-2 font-mono text-caption`} />
          </Labeled>
        )}
        {error && <p className="text-[13px] text-critical-text">{error}</p>}
      </div>
    </Modal>
  );
}
