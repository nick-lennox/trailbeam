"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ChevronDown, ChevronRight, Clock, Copy, GripVertical, Lock, Maximize, MoreHorizontal, Palette, Pause, Pin, Play, Plus, SquareArrowOutUpRight, Star, Undo2, X } from "lucide-react";
import { Button } from "../design/Button.tsx";
import { CopyText } from "../design/CopyText.tsx";
import { UiLink, useNav } from "../design/nav.tsx";
import { TimePicker } from "../explorer/TimePicker.tsx";
import type { QueryLanguage } from "../model/query.ts";
import { follow, pinned, PRESETS, rangeFromParams, stamp, withRange, type TimeRange } from "../model/timerange.ts";
import { TONE_WORD, type EventData } from "./measures.ts";
import { useDashboardsEnv } from "./env.tsx";
import { SETS, TYPE_INFO, WIDGET_TYPES, blank, uid, type Dashboard, type DashboardDef, type Section, type Widget, type WidgetType } from "./types.ts";
import { HoverCtx } from "../charts/index.ts";
import { Grid, firstFree, type Item } from "./Grid.tsx";
import { WidgetEditor } from "./WidgetEditor.tsx";
import { FLUSH, WidgetBody, exploreHref, type Ctx } from "./widgets.tsx";
import { FIELD, IconBtn, Menu, MenuItem, Modal, TONE_TEXT } from "./ui.tsx";
import { SectionOptions, sectionLook } from "./SectionOptions.tsx";

/*
  One dashboard. Widgets sit on the snap grid and can be moved and resized at
  any time; there is no edit mode. Every change is saved as it is made, and the
  last one can be undone. A locked dashboard can be read, filtered and zoomed,
  and nothing else.
*/

/* A section header's sizes. */
const SIZE = {
  sm: { row: "min-h-8 py-1", title: "text-[13px]" },
  md: { row: "min-h-10 py-1.5", title: "text-[15px]" },
  lg: { row: "min-h-14 py-2.5", title: "text-[22px] tracking-tight" },
  xl: { row: "min-h-[76px] py-4", title: "text-[32px] leading-tight tracking-tight" },
};
/* The dashed place where the next thing goes. */
const ADD = "flex h-[72px] w-full items-center justify-center gap-2 rounded-card border border-dashed border-line-strong text-sm font-medium text-ink-2 hover:border-ink-3 hover:bg-surface/60 hover:text-ink";
const MARK = "ml-1 inline-flex min-w-[26px] shrink items-center gap-1 overflow-hidden text-xs whitespace-nowrap text-ink-3";

/* The values a variable can take: what the events have for that field. */
function valuesOf(field: string, data: EventData, lang: QueryLanguage): string[] {
  if (field === "queue") return data.queues;
  const f = lang.def(field);
  return f ? [...new Set(data.events.map((e) => f.get(e)).filter((v): v is string | number => v !== undefined).map(String))].sort() : [];
}

/* range: the time range the page's link names (?range=, or ?from=&to=), kept in the URL as it changes, so a link shows the same span. */
export function DashboardView({ dashboard, range = {} }: { dashboard: Dashboard; range?: { range?: string; from?: string; to?: string } }) {
  const env = useDashboardsEnv();
  const { engine: E, lang, actions } = env;
  const ws = env.href;
  const router = useNav();
  const [def, setDefState] = useState<DashboardDef>(dashboard.def);
  const [name, setName] = useState(dashboard.name);
  const [renaming, setRenaming] = useState(false);
  const [starred, setStarred] = useState(dashboard.starred);
  const [lockedBy, setLockedBy] = useState(dashboard.lockedBy);
  const data = env.data;
  const [time, setTimeState] = useState<TimeRange>(() => rangeFromParams((k) => range[k as keyof typeof range], data.now));
  const [back, setBack] = useState<TimeRange | null>(null);
  const [live, setLive] = useState(true);
  const [hover, setHover] = useState<number | null>(null);
  /* the widget list, and the section a new widget goes into (null is above the sections) */
  const [tray, setTray] = useState<{ section: string | undefined } | null>(null);
  const [naming, setNaming] = useState<string | null>(null);
  const [styling, setStyling] = useState<string | null>(null);
  const [editing, setEditing] = useState<{ widget: Widget; isNew: boolean } | null>(null);
  const [undo, setUndo] = useState<{ def: DashboardDef; what: string } | null>(null);
  const [confirm, setConfirm] = useState(false);
  const [note, setNote] = useState<string | null>(null);
  const [full, setFull] = useState(false);
  const root = useRef<HTMLDivElement>(null);
  const locked = !!lockedBy;
  const following = live && !!time.preset;
  /* On a workspace's own data, following reloads the events; a range that ends now moves up with them. */
  useEffect(() => { if (following) setTimeState((t) => follow(t, data.now)); }, [data.now, following]);

  /* Save a little after the last change, so a drag is one write. */
  const pending = useRef<ReturnType<typeof setTimeout> | null>(null);
  const persist = useCallback((next: DashboardDef) => {
    if (pending.current) clearTimeout(pending.current);
    pending.current = setTimeout(async () => {
      const r = await actions.save(dashboard.id, { def: next });
      setNote(r.ok ? null : r.error);
    }, 500);
  }, [actions, dashboard.id]);
  const setDef = (next: DashboardDef, what?: string) => {
    if (what) setUndo({ def, what });
    setDefState(next);
    persist(next);
  };
  useEffect(() => { if (!undo) return; const t = setTimeout(() => setUndo(null), 8000); return () => clearTimeout(t); }, [undo]);

  /* the URL only: replaceState doesn't ask the server for the page again */
  const here = withRange(ws(`/dashboards/${dashboard.id}`), time);
  useEffect(() => { if (`${window.location.pathname}${window.location.search}` !== here) window.history.replaceState(null, "", here); }, [here]);
  const setTime = (t: TimeRange) => { if (!t.preset && time.preset) setBack(time); if (t.preset) setBack(null); setTimeState(t); };
  const setVar = (field: string, value: string) => setDef({ ...def, variables: def.variables.map((v) => (v.field === field ? { ...v, value } : v)) });
  const ctx: Ctx = useMemo(() => ({ vars: def.variables, time, setTime, ws, setVar, data, E, lang, bodyPaths: env.bodyPaths, sampleFlow: env.sampleFlow }), [def.variables, time, ws, data, E, lang, env.bodyPaths, env.sampleFlow]);

  useEffect(() => { const on = () => setFull(!!document.fullscreenElement); document.addEventListener("fullscreenchange", on); return () => document.removeEventListener("fullscreenchange", on); }, []);

  /* Paste a copied widget anywhere on the page. */
  useEffect(() => {
    const onPaste = (e: ClipboardEvent) => {
      if (locked || editing || (e.target as HTMLElement)?.closest?.("input, textarea, select")) return;
      try {
        const j = JSON.parse(e.clipboardData?.getData("text") ?? "") as Widget;
        if (E.problem(j)) return;
        const w: Widget = { ...j, id: uid(), section: undefined, layout: firstFree(itemsOf(undefined), j.layout.w, j.layout.h) };
        setDef({ ...def, widgets: [...def.widgets, w] }, `Pasted ${w.title}`);
      } catch {}
    };
    document.addEventListener("paste", onPaste);
    return () => document.removeEventListener("paste", onPaste);
  });

  const itemsOf = (section: string | undefined): Item[] => def.widgets.filter((w) => w.section === section).map((w) => ({ id: w.id, layout: w.layout }));
  const relayout = (items: Item[]) => setDef({ ...def, widgets: def.widgets.map((w) => { const it = items.find((i) => i.id === w.id); return it ? { ...w, layout: it.layout } : w; }) }, "Moved a widget");
  const add = (type: WidgetType) => {
    const t = TYPE_INFO[type], section = tray?.section;
    setTray(null);
    setEditing({ widget: { ...blank(type, firstFree(itemsOf(section), t.w, t.h)), section }, isNew: true });
  };
  const addSection = () => { const id = uid(); setDef({ ...def, sections: [...def.sections, { id, title: "New section" }] }); setNaming(id); };
  const patchSection = (id: string, patch: Partial<Section>) => setDef({ ...def, sections: def.sections.map((x) => (x.id === id ? { ...x, ...patch } : x)) });
  const save = (w: Widget) => {
    const isNew = !def.widgets.some((x) => x.id === w.id);
    setDef({ ...def, widgets: isNew ? [...def.widgets, w] : def.widgets.map((x) => (x.id === w.id ? w : x)) }, isNew ? `Added ${w.title}` : undefined);
    setEditing(null);
  };
  const remove = (w: Widget) => setDef({ ...def, widgets: def.widgets.filter((x) => x.id !== w.id) }, `Deleted ${w.title}`);
  const moveTo = (w: Widget, section: string | undefined) => setDef({ ...def, widgets: def.widgets.map((x) => (x.id === w.id ? { ...x, section, layout: firstFree(itemsOf(section), w.layout.w, w.layout.h) } : x)) }, `Moved ${w.title}`);
  const exportJson = () => {
    const a = document.createElement("a");
    a.href = URL.createObjectURL(new Blob([JSON.stringify({ name, ...def }, null, 2)], { type: "application/json" }));
    a.download = `${name.toLowerCase().replace(/[^a-z0-9]+/g, "-")}.json`;
    a.click();
    URL.revokeObjectURL(a.href);
  };

  const frame = (id: string, dragging: boolean) => {
    const w = def.widgets.find((x) => x.id === id);
    if (!w) return null;
    const alert = E.alertOf(w, def.variables, data);
    const pinned = w.follow === false && def.variables.length > 0;
    return (
      <section className={`group/w relative flex h-full flex-col overflow-hidden rounded-card border bg-surface ${dragging ? "border-brand shadow-raised" : "border-line shadow-card hover:border-line-strong"}`}>
        <header data-grid-drag className={`flex h-9 shrink-0 items-center gap-1.5 pr-1.5 pl-3 ${locked ? "" : "cursor-grab active:cursor-grabbing"}`}>
          {!locked && <GripVertical size={14} className="-mr-0.5 -ml-2 shrink-0 text-ink-3 opacity-0 group-hover/w:opacity-100" />}
          <h3 className="shrink-0 truncate text-[13px] font-semibold text-ink" style={{ maxWidth: "100%" }}>{w.title}</h3>
          {pinned && <span className={MARK} title="This widget does not follow the dashboard's variables"><Pin size={11} className="shrink-0" /><span className="truncate">Pinned</span></span>}
          {w.time && <span className={MARK}><Clock size={11} className="shrink-0" /><span className="truncate font-mono text-ink-2">{PRESETS.find((p) => p.id === w.time)?.label ?? w.time}</span></span>}
          {alert && alert.tone !== "good" && w.type !== "number" && <span className={`${MARK} ${TONE_TEXT[alert.tone]}`}>{TONE_WORD[alert.tone]}</span>}
          <span className="absolute top-[5px] right-1.5 flex gap-0.5 bg-surface pl-1.5 opacity-0 group-hover/w:opacity-100 focus-within:opacity-100">
            {w.type !== "note" && (
              <UiLink href={exploreHref(w, ctx)} aria-label="Open in Event explorer" title="Open in Event explorer" className="grid size-[26px] place-items-center rounded-md text-ink-3 hover:bg-sunken hover:text-ink"><SquareArrowOutUpRight size={14} /></UiLink>
            )}
            <Menu button={(_, toggle) => <IconBtn label="Widget menu" onClick={toggle}><MoreHorizontal size={14} /></IconBtn>}>
              {(close) => (
                <>
                  <MenuItem disabled={locked} onClick={() => { close(); setEditing({ widget: w, isNew: false }); }}>Edit</MenuItem>
                  <MenuItem onClick={() => { close(); navigator.clipboard?.writeText(JSON.stringify(w, null, 2)); setNote("Copied. Paste it on any dashboard."); setTimeout(() => setNote(null), 2500); }}><Copy size={14} /> Copy widget</MenuItem>
                  {!locked && def.sections.filter((g) => g.id !== w.section).map((g) => <MenuItem key={g.id} onClick={() => { close(); moveTo(w, g.id); }}>Move to {g.title}</MenuItem>)}
                  {!locked && w.section && <MenuItem onClick={() => { close(); moveTo(w, undefined); }}>Move out of the section</MenuItem>}
                  <div className="my-1 h-px bg-line" />
                  <MenuItem danger disabled={locked} onClick={() => { close(); remove(w); }}><X size={14} /> Delete widget</MenuItem>
                </>
              )}
            </Menu>
          </span>
        </header>
        <div className={`flex min-h-0 flex-1 flex-col ${FLUSH.has(w.type) ? "" : "px-3 pb-2.5"}`}><WidgetBody w={w} ctx={ctx} /></div>
        {!locked && <div data-grid-resize aria-hidden className="absolute right-1 bottom-1 size-2.5 cursor-nwse-resize rounded-br-[5px] border-r-2 border-b-2 border-line-strong opacity-0 group-hover/w:opacity-100" />}
      </section>
    );
  };

  const unused = E.GROUP_FIELDS.filter((f) => !def.variables.some((v) => v.field === f.id));
  const loose = itemsOf(undefined);

  return (
    <HoverCtx.Provider value={{ at: hover, set: setHover }}>
      <div ref={root} className={`px-8 pt-6 pb-10 ${full ? "min-h-screen overflow-y-auto bg-canvas" : ""}`}>
        <div className="flex flex-wrap items-center gap-2">
          {!full && <UiLink href={ws("/dashboards")} className="text-sm text-ink-3 hover:text-ink">Dashboards</UiLink>}
          {!full && <ChevronRight size={14} className="text-ink-3" />}
          {renaming ? (
            <input
              autoFocus
              defaultValue={name}
              aria-label="Dashboard name"
              onBlur={async (e) => {
                const v = e.target.value.trim();
                setRenaming(false);
                if (!v || v === name) return;
                setName(v);
                const r = await actions.save(dashboard.id, { name: v });
                if (!r.ok) setNote(r.error);
              }}
              onKeyDown={(e) => { if (e.key === "Enter") e.currentTarget.blur(); if (e.key === "Escape") setRenaming(false); }}
              className={`${FIELD} h-9 w-[320px] text-[22px]`}
            />
          ) : (
            <h1 className="text-[22px] tracking-tight text-ink" onDoubleClick={() => !locked && setRenaming(true)}>{name}</h1>
          )}
          <IconBtn label={starred ? "Remove the star" : "Star this dashboard"} className={starred ? "!text-accent" : ""} onClick={async () => { setStarred(!starred); await actions.star(dashboard.id, !starred); router.refresh(); }}>
            <Star size={16} fill={starred ? "currentColor" : "none"} />
          </IconBtn>
          {locked && <span className="inline-flex items-center gap-1 text-xs text-ink-3"><Lock size={11} /> Locked by {lockedBy}</span>}
          {note && <span className="text-[13px] text-ink-2">{note}</span>}
          <span className="flex-1" />
          <Button variant="ghost" size="sm" onClick={() => (full ? document.exitFullscreen() : root.current?.requestFullscreen())}><Maximize size={14} /> {full ? "Leave full screen" : "Full screen"}</Button>
          {!full && (
            <Menu button={(_, toggle) => <IconBtn label="Dashboard menu" onClick={toggle}><MoreHorizontal size={16} /></IconBtn>}>
              {(close) => (
                <>
                  <MenuItem disabled={locked} onClick={() => { close(); setRenaming(true); }}>Rename</MenuItem>
                  <MenuItem onClick={async () => { close(); const r = await actions.duplicate(dashboard.id); if (r.ok) router.push(ws(`/dashboards/${r.id}`)); else setNote(r.error); }}>Duplicate</MenuItem>
                  <MenuItem onClick={async () => { close(); const r = await actions.lock(dashboard.id, !locked); if (r.ok) setLockedBy(r.lockedBy); else setNote(r.error); }}><Lock size={14} /> {locked ? "Unlock" : "Lock"}</MenuItem>
                  <MenuItem onClick={() => { close(); exportJson(); }}>Export JSON</MenuItem>
                  <div className="my-1 h-px bg-line" />
                  <MenuItem danger disabled={locked} onClick={() => { close(); setConfirm(true); }}>Delete dashboard</MenuItem>
                </>
              )}
            </Menu>
          )}
          {!locked && !full && <Button variant="primary" size="sm" onClick={() => setTray({ section: undefined })}><Plus size={14} /> Add widget</Button>}
        </div>

        <div className="mt-3 flex flex-wrap items-center gap-2">
          {def.variables.map((v) => (
            <Menu key={v.name} align="left" width="w-64" button={(open, toggle) => (
              <button type="button" onClick={toggle} aria-expanded={open} className={`inline-flex h-10 items-center gap-2 rounded-lg border bg-surface pr-2 pl-3 text-[13px] hover:border-ink-3 ${open ? "border-accent" : "border-line-strong"}`}>
                <span className="text-ink-3">{v.name}</span>
                <span className="font-mono text-ink">{v.value === "*" ? "All" : v.value}</span>
                <ChevronDown size={14} className="text-ink-3" />
              </button>
            )}>
              {(close) => (
                <div className="max-h-[320px] overflow-y-auto">
                  <MenuItem onClick={() => { close(); setVar(v.field, "*"); }}>All</MenuItem>
                  {valuesOf(v.field, data, lang).map((val) => <MenuItem key={val} onClick={() => { close(); setVar(v.field, val); }}><span className="truncate font-mono text-caption">{val}</span></MenuItem>)}
                  {!locked && (<><div className="my-1 h-px bg-line" /><MenuItem danger onClick={() => { close(); setDef({ ...def, variables: def.variables.filter((x) => x.name !== v.name) }, `Removed the ${v.name} variable`); }}>Remove this variable</MenuItem></>)}
                </div>
              )}
            </Menu>
          ))}
          {!locked && !full && unused.length > 0 && (
            <Menu align="left" button={(_, toggle) => <Button variant="ghost" size="sm" onClick={toggle}><Plus size={14} /> Add variable</Button>}>
              {(close) => unused.map((f) => <MenuItem key={f.id} onClick={() => { close(); setDef({ ...def, variables: [...def.variables, { name: f.id, field: f.id, value: "*" }] }); }}>{f.label}</MenuItem>)}
            </Menu>
          )}
          <span className="flex-1" />
          {back && <Button variant="ghost" size="sm" onClick={() => { setTimeState(back); setBack(null); }}><Undo2 size={14} /> Back to {back.label}</Button>}
          <TimePicker time={time} setTime={setTime} now={data.now} />
          <CopyText path={() => withRange(ws(`/dashboards/${dashboard.id}`), pinned(time))} icon="link" label="Copy a link to this dashboard at this exact time range"
            className="h-10 rounded-lg border border-line-strong bg-surface px-3 text-[13px] text-ink-2">Copy link</CopyText>
          {data.live && following && env.liveRefresh}
          <button
            onClick={() => setLive(!live)}
            disabled={!time.preset}
            aria-pressed={following}
            title={!time.preset ? "A fixed range in the past does not update" : following ? `Updated ${stamp(data.now, { seconds: true })}. Pause updates` : "Resume updates"}
            className="inline-flex h-10 w-[92px] items-center justify-center gap-2 rounded-lg border border-line-strong bg-surface text-[13px] text-ink-2 hover:text-ink disabled:opacity-50 disabled:hover:text-ink-2"
          >
            {following ? (<><span className="live-dot size-1.5 rounded-full bg-good" /> Live <Pause size={13} className="text-ink-3" /></>) : (<>Paused <Play size={13} className="text-ink-3" /></>)}
          </button>
        </div>

        <div className="mt-4">
          {loose.length > 0 && <Grid items={loose} locked={locked} onChange={relayout} render={frame} />}
          {def.sections.map((g) => {
            const inside = itemsOf(g.id);
            const look = sectionLook(g);
            const boxed = !!look.bodyBg || !!g.border;
            const tool = look.headerBg || look.text ? "hover:bg-black/10" : "text-ink-2 hover:bg-sunken hover:text-ink";
            return (
              <section key={g.id} className={`mt-5 first:mt-0 ${boxed ? "rounded-panel p-3" : ""} ${g.border ? "border border-line-strong" : ""}`} style={{ background: look.bodyBg }}>
                <div className={`group/s relative flex items-center gap-2 text-ink ${SIZE[g.size ?? "md"].row} ${look.headerBg ? "rounded-lg px-2.5" : g.divider === false ? "" : "border-b border-line"}`} style={{ background: look.headerBg, color: look.text }}>
                  <button type="button" onClick={() => patchSection(g.id, { collapsed: !g.collapsed })} aria-expanded={!g.collapsed} aria-label={g.collapsed ? `Open ${g.title}` : `Close ${g.title}`} className={`-ml-1 grid size-6 shrink-0 place-items-center rounded-md opacity-70 hover:opacity-100 ${look.headerBg || look.text ? "hover:bg-black/10" : "hover:bg-sunken"}`}>
                    {g.collapsed ? <ChevronRight size={16} /> : <ChevronDown size={16} />}
                  </button>
                  <div className={`min-w-0 ${g.align === "center" ? "flex-1 px-56 text-center" : ""}`}>
                    {naming === g.id && !locked ? (
                      <input
                        autoFocus
                        defaultValue={g.title}
                        aria-label="Section name"
                        onFocus={(e) => e.currentTarget.select()}
                        onBlur={(e) => { const t = e.target.value.trim(); setNaming(null); if (t && t !== g.title) patchSection(g.id, { title: t }); }}
                        onKeyDown={(e) => { if (e.key === "Enter") e.currentTarget.blur(); if (e.key === "Escape") setNaming(null); }}
                        className={`${FIELD} h-7 w-64 px-2 text-[15px] font-semibold !text-ink`}
                      />
                    ) : (
                      <div className={`flex min-w-0 items-baseline gap-2 ${g.align === "center" ? "justify-center" : ""}`}>
                        <h2 className={`truncate ${g.weight === "regular" ? "font-normal" : "font-semibold"} ${SIZE[g.size ?? "md"].title}`} onDoubleClick={() => !locked && setNaming(g.id)}>{g.title}</h2>
                        {g.collapsed && <span className="shrink-0 text-[13px] whitespace-nowrap opacity-70">{inside.length} {inside.length === 1 ? "widget" : "widgets"}</span>}
                      </div>
                    )}
                    {g.description && (g.size ?? "md") !== "sm" && <p className={`truncate opacity-75 ${g.size === "lg" || g.size === "xl" ? "text-sm" : "text-xs"}`}>{g.description}</p>}
                  </div>
                  {!locked && !full && (
                    <span className={`flex shrink-0 items-center gap-0.5 ${g.align === "center" ? "absolute top-1/2 right-2 -translate-y-1/2" : "ml-auto"} ${styling === g.id ? "" : "opacity-0 group-hover/s:opacity-100 focus-within:opacity-100"}`}>
                      <button type="button" onClick={() => setTray({ section: g.id })} className={`inline-flex h-7 items-center gap-1.5 rounded-md px-2 text-[13px] font-medium ${tool}`}><Plus size={14} /> Add widget</button>
                      <button type="button" onClick={() => setStyling(styling === g.id ? null : g.id)} aria-expanded={styling === g.id} className={`inline-flex h-7 items-center gap-1.5 rounded-md px-2 text-[13px] font-medium ${tool}`}><Palette size={14} /> Customize</button>
                      <Menu button={(_, toggle) => <button type="button" aria-label="Section menu" onClick={toggle} className={`grid size-7 place-items-center rounded-md ${tool}`}><MoreHorizontal size={14} /></button>}>
                        {(close) => (
                          <>
                            <MenuItem disabled={def.sections[0].id === g.id} onClick={() => { close(); const i = def.sections.findIndex((x) => x.id === g.id); const next = [...def.sections]; [next[i - 1], next[i]] = [next[i], next[i - 1]]; setDef({ ...def, sections: next }); }}>Move up</MenuItem>
                            <MenuItem disabled={def.sections[def.sections.length - 1].id === g.id} onClick={() => { close(); const i = def.sections.findIndex((x) => x.id === g.id); const next = [...def.sections]; [next[i + 1], next[i]] = [next[i], next[i + 1]]; setDef({ ...def, sections: next }); }}>Move down</MenuItem>
                            <div className="my-1 h-px bg-line" />
                            <MenuItem danger onClick={() => { close(); setDef({ ...def, sections: def.sections.filter((x) => x.id !== g.id), widgets: def.widgets.filter((w) => w.section !== g.id) }, `Deleted the section ${g.title}`); }}>Delete the section and its {inside.length} {inside.length === 1 ? "widget" : "widgets"}</MenuItem>
                          </>
                        )}
                      </Menu>
                    </span>
                  )}
                  {styling === g.id && !locked && <SectionOptions section={g} onChange={(patch) => patchSection(g.id, patch)} onClose={() => setStyling(null)} />}
                </div>
                {!g.collapsed && (
                  <div className="pt-3">
                    {inside.length ? <Grid items={inside} locked={locked} onChange={relayout} render={frame} /> : !locked && (
                      <button type="button" onClick={() => setTray({ section: g.id })} className={ADD}><Plus size={15} /> Add widget</button>
                    )}
                  </div>
                )}
              </section>
            );
          })}
          {!locked && !full && (
            <div className={`mt-5 flex gap-3 ${def.widgets.length === 0 && def.sections.length === 0 ? "h-40" : "h-[72px]"}`}>
              <button type="button" onClick={() => setTray({ section: undefined })} className={`${ADD} h-full flex-1`}><Plus size={15} /> Add widget</button>
              <button type="button" onClick={addSection} className={`${ADD} h-full flex-1`}><Plus size={15} /> Add section</button>
            </div>
          )}
        </div>

        {undo && (
          <div className="fixed bottom-6 left-1/2 z-40 flex -translate-x-1/2 items-center gap-3 rounded-card border border-line bg-surface py-2 pr-2 pl-3.5 text-sm text-ink shadow-raised">
            {undo.what}
            <Button variant="secondary" size="sm" onClick={() => { setDefState(undo.def); persist(undo.def); setUndo(null); }}><Undo2 size={14} /> Undo</Button>
          </div>
        )}

        {tray && (
          <>
            <button aria-label="Close the widget list" className="fixed inset-0 z-40 cursor-default" onClick={() => setTray(null)} />
            <aside className="fixed top-3 right-3 bottom-3 z-50 flex w-80 flex-col overflow-hidden rounded-panel border border-line bg-surface shadow-panel">
              <div className="flex items-center px-4 pt-3.5 pb-2 text-[15px] font-semibold text-ink">Add widget<IconBtn label="Close" className="ml-auto" onClick={() => setTray(null)}><X size={16} /></IconBtn></div>
              <div className="min-h-0 flex-1 overflow-y-auto pb-2">
                {SETS.map((set) => (
                  <div key={set}>
                    <h5 className="px-4 pt-2.5 pb-1 text-xs font-medium text-ink-3">{set}</h5>
                    {WIDGET_TYPES.filter((t) => TYPE_INFO[t].set === set).map((t) => (
                      <button key={t} type="button" onClick={() => add(t)} className="mx-2 flex w-[calc(100%-16px)] items-center gap-3 rounded-control px-2 py-1.5 text-left hover:bg-sunken">
                        <span><b className="block text-sm font-medium text-ink">{TYPE_INFO[t].label}</b><span className="block text-xs text-ink-3">{TYPE_INFO[t].hint}</span></span>
                      </button>
                    ))}
                  </div>
                ))}
              </div>
              
            </aside>
          </>
        )}

        {editing && <WidgetEditor widget={editing.widget} isNew={editing.isNew} ctx={ctx} onSave={save} onClose={() => setEditing(null)} />}

        {confirm && (
          <Modal
            title={`Delete ${name}?`}
            onClose={() => setConfirm(false)}
            footer={
              <>
                <Button variant="secondary" size="sm" onClick={() => setConfirm(false)}>Cancel</Button>
                <button type="button" className="inline-flex h-8 items-center rounded-control bg-critical px-3 text-small font-medium text-canvas" onClick={async () => { const r = await actions.remove(dashboard.id); if (r.ok) router.push(ws("/dashboards")); else { setConfirm(false); setNote(r.error); } }}>Delete dashboard</button>
              </>
            }
          >
            <p className="px-5 pt-3 pb-5 text-sm text-ink-2">This deletes the dashboard and its {def.widgets.length} {def.widgets.length === 1 ? "widget" : "widgets"} for everyone in this workspace. It cannot be undone. Nothing in your queues changes.</p>
          </Modal>
        )}
      </div>
    </HoverCtx.Provider>
  );
}
