"use client";

import { useMemo, useState } from "react";
import { Copy, TriangleAlert } from "lucide-react";
import { Button } from "../design/index.ts";
import { ColumnChoices, QueryInput } from "../explorer/parts.tsx";
import { PRESETS } from "../model/timerange.ts";
import { ASKS, type DashboardMeasures } from "./measures.ts";
import { EVENT_COLUMNS, TABLE_COLUMNS, TYPE_INFO, type Widget } from "./types.ts";
import { FLUSH, WidgetBody, type Ctx } from "./widgets.tsx";
import { FIELD, Labeled, Modal } from "./ui.tsx";

/*
  The editor for one widget. The top is the widget itself with real data. Under
  it, the form and the JSON are two views of one definition: changing either
  changes the other, and the JSON is what gets saved.
*/

const HEADROOM = ["retention_headroom", "visibility_headroom", "near_dlq"];
const H4 = "text-[13px] font-semibold text-ink";

function MeasureSelect({ E, value, onChange, only, label = "Measure" }: { E: DashboardMeasures; value: string; onChange: (id: string) => void; only?: (id: string) => boolean; label?: string }) {
  return (
    <select aria-label={label} value={value} onChange={(e) => onChange(e.target.value)} className={FIELD}>
      {ASKS.map((ask) => {
        const ms = E.MEASURES.filter((m) => m.ask === ask && (!only || only(m.id)));
        return ms.length ? <optgroup key={ask} label={ask}>{ms.map((m) => <option key={m.id} value={m.id}>{m.label}</option>)}</optgroup> : null;
      })}
    </select>
  );
}

export function WidgetEditor({ widget, ctx, isNew, onSave, onClose }: { widget: Widget; ctx: Ctx; isNew: boolean; onSave: (w: Widget) => void; onClose: () => void }) {
  const { GROUP_FIELDS, MEASURES, NUMBER_FIELDS, measure, problem } = ctx.E;
  const [w, setW] = useState<Widget>(widget);
  const [tab, setTab] = useState<"form" | "json">("form");
  const [text, setText] = useState(() => JSON.stringify(widget, null, 2));
  const [copied, setCopied] = useState(false);
  const parsed = useMemo(() => {
    try { const j = JSON.parse(text) as Widget; const p = problem(j); return p ? { error: p } : { widget: { ...j, id: widget.id } }; }
    catch (e) { return { error: `This is not valid JSON. ${(e as Error).message}.` }; }
  }, [text, widget.id]);
  const error = tab === "json" ? parsed.error : problem(w);
  const set = (patch: Partial<Widget>) => { const next = { ...w, ...patch }; setW(next); setText(JSON.stringify(next, null, 2)); };
  const edit = (t: string) => {
    setText(t);
    try { const j = JSON.parse(t) as Widget; if (!problem(j)) setW({ ...j, id: widget.id }); } catch {}
  };
  const q = w.queries ?? [];
  const setQ = (i: number, patch: Partial<{ measure: string; group_by: string | undefined }>) => set({ queries: q.map((x, k) => (k === i ? { ...x, ...patch } : x)) });
  const info = TYPE_INFO[w.type];
  const unit = measure(q[0]?.measure ?? "")?.unit;
  const num = (v: string) => (v.trim() === "" || Number.isNaN(+v) ? undefined : +v);

  const what = (() => {
    switch (w.type) {
      case "timeseries":
        return (
          <>
            {q.map((x, i) => (
              <div key={i} className="flex gap-2">
                <MeasureSelect E={ctx.E} value={x.measure} onChange={(id) => setQ(i, { measure: id })} label={`Measure ${i + 1}`} />
                {q.length > 1 && <Button variant="ghost" size="sm" className="h-9" onClick={() => set({ queries: q.filter((_, k) => k !== i) })}>Remove</Button>}
              </div>
            ))}
            {q.length < 3 && !q[0]?.group_by && <div><Button variant="ghost" size="sm" onClick={() => set({ queries: [...q, { measure: "messages_in_flight" }] })}>Add a measure</Button></div>}
            {q.length === 1 && (
              <Labeled label="Group by">
                <select value={q[0].group_by ?? ""} onChange={(e) => setQ(0, { group_by: e.target.value || undefined })} className={FIELD}>
                  <option value="">Nothing, one line</option>
                  {GROUP_FIELDS.filter((f) => f.id === "queue" || !measure(q[0].measure)?.now).map((f) => <option key={f.id} value={f.id}>{f.label}</option>)}
                </select>
              </Labeled>
            )}
          </>
        );
      case "number":
        return <Labeled label="Measure"><MeasureSelect E={ctx.E} value={q[0]?.measure ?? "dlq_depth"} onChange={(id) => set({ queries: [{ measure: id }] })} /></Labeled>;
      case "toplist":
        return (
          <>
            <Labeled label="Measure"><MeasureSelect E={ctx.E} value={q[0]?.measure ?? "oldest_age"} onChange={(id) => set({ queries: [{ measure: id, group_by: measure(id)?.now ? "queue" : q[0]?.group_by ?? "queue" }] })} /></Labeled>
            <Labeled label="Rank by">
              <select value={q[0]?.group_by ?? "queue"} onChange={(e) => setQ(0, { group_by: e.target.value })} className={FIELD}>
                {GROUP_FIELDS.filter((f) => f.id === "queue" || !measure(q[0]?.measure ?? "")?.now).map((f) => <option key={f.id} value={f.id}>{f.label}</option>)}
              </select>
            </Labeled>
          </>
        );
      case "gauge":
        return (
          <div className="grid gap-1.5">
            {HEADROOM.map((id) => {
              const on = q.some((x) => x.measure === id);
              return (
                <label key={id} className="flex cursor-pointer items-center gap-2 text-[13px] text-ink">
                  <input type="checkbox" checked={on} disabled={on && q.length === 1} onChange={() => set({ queries: on ? q.filter((x) => x.measure !== id) : [...q, { measure: id }] })} className="size-3.5 accent-brand" />
                  {measure(id)!.label}
                </label>
              );
            })}
          </div>
        );
      case "heatmap":
        return <p className="text-[13px] text-ink-2">Handling time: from receive to delete or error, for each message.</p>;
      case "distribution":
        return (
          <Labeled label="Field">
            <select value={w.field ?? "receive_count"} onChange={(e) => set({ field: e.target.value })} className={FIELD}>
              {NUMBER_FIELDS.map((f) => <option key={f.id} value={f.id}>{f.label}</option>)}
            </select>
          </Labeled>
        );
      case "queue_table": {
        const cols = w.columns?.length ? w.columns : TABLE_COLUMNS;
        return (
          <div className="grid gap-1.5">
            <label className="mb-1.5 flex cursor-pointer items-center gap-2 text-[13px] text-ink">
              <input type="checkbox" checked={!!w.trend} onChange={(e) => set({ trend: e.target.checked || undefined })} className="size-3.5 accent-brand" />
              Draw a trend beside the first column
            </label>
            {MEASURES.filter((m) => m.now).map((m) => {
              const on = cols.includes(m.id);
              return (
                <label key={m.id} className="flex cursor-pointer items-center gap-2 text-[13px] text-ink">
                  <input type="checkbox" checked={on} disabled={on && cols.length === 1} onChange={() => set({ columns: on ? cols.filter((c) => c !== m.id) : [...cols, m.id] })} className="size-3.5 accent-brand" />
                  {m.label}
                </label>
              );
            })}
          </div>
        );
      }
      case "event_list":
        return (
          <div className="rounded-lg border border-line">
            <div className="max-h-[300px] overflow-y-auto px-3.5 py-1.5"><ColumnChoices columns={w.columns?.length ? w.columns : EVENT_COLUMNS} setColumns={(columns) => set({ columns })} live={!!ctx.data.live} lang={ctx.lang} /></div>
          </div>
        );
      case "note":
        return (
          <Labeled label="Text" hint="A line starting with # is a heading. [words](https://link) makes a link and `backticks` mark code.">
            <textarea value={w.text ?? ""} onChange={(e) => set({ text: e.target.value })} rows={8} className={`${FIELD} h-auto py-2 leading-relaxed`} />
          </Labeled>
        );
      default:
        return null;
    }
  })();

  return (
    <Modal
      title={`${isNew ? "New" : "Edit"} ${info.label.toLowerCase()}`}
      onClose={onClose}
      width="max-w-[1040px]"
      footer={
        <>
          {tab === "json" && (
            <Button variant="ghost" size="sm" className="mr-auto" onClick={() => { navigator.clipboard?.writeText(text); setCopied(true); setTimeout(() => setCopied(false), 1500); }}>
              <Copy size={14} /> {copied ? "Copied" : "Copy JSON"}
            </Button>
          )}
          <Button variant="secondary" size="sm" onClick={onClose}>Cancel</Button>
          <Button variant="primary" size="sm" disabled={!!error} className="disabled:opacity-50" onClick={() => onSave(tab === "json" && parsed.widget ? parsed.widget : w)}>Save widget</Button>
        </>
      }
    >
      <div className="mt-3 border-y border-line bg-canvas px-5 py-4">
        <div className="flex h-[240px] flex-col overflow-hidden rounded-card border border-line bg-surface">
          <div className="flex h-9 shrink-0 items-center px-3 text-[13px] font-semibold">{w.title}</div>
          <div className={`flex min-h-0 flex-1 flex-col ${FLUSH.has(w.type) ? "" : "px-3 pb-2.5"}`}><WidgetBody w={w} ctx={ctx} /></div>
        </div>
      </div>
      <div className="flex gap-5 border-b border-line px-5">
        {(["form", "json"] as const).map((t) => (
          <button key={t} type="button" onClick={() => { if (t === "json") setText(JSON.stringify(w, null, 2)); setTab(t); }} aria-pressed={tab === t} className={`-mb-px h-9 border-b-2 text-sm font-medium ${tab === t ? "border-brand text-ink" : "border-transparent text-ink-2 hover:text-ink"}`}>
            {t === "form" ? "Form" : "JSON"}
          </button>
        ))}
      </div>
      {tab === "form" ? (
        <div className="grid max-h-[46vh] overflow-y-auto md:grid-cols-2">
          <div className="grid content-start gap-4 px-5 pt-4 pb-5">
            <h4 className={H4}>What to show</h4>
            {what}
            {(w.type === "number" || w.type === "timeseries" || w.type === "gauge") && (
              <>
                <h4 className={`${H4} mt-1`}>When it alerts</h4>
                <div className="grid grid-cols-2 gap-2">
                  <Labeled label={`Warning ${w.rule?.below ? "below" : "above"}`}><input inputMode="decimal" value={w.rule?.warn ?? ""} onChange={(e) => set({ rule: { ...w.rule, warn: num(e.target.value) } })} className={`${FIELD} font-mono`} /></Labeled>
                  <Labeled label={`Critical ${w.rule?.below ? "below" : "above"}`}><input inputMode="decimal" value={w.rule?.crit ?? ""} onChange={(e) => set({ rule: { ...w.rule, crit: num(e.target.value) } })} className={`${FIELD} font-mono`} /></Labeled>
                </div>
                <p className="-mt-2 text-xs text-ink-3">
                  {unit === "s" ? "In seconds." : unit === "ms" ? "In milliseconds." : unit === "pct" ? "In percent." : ""}
                </p>
                <label className="flex cursor-pointer items-center gap-2 text-[13px] text-ink">
                  <input type="checkbox" checked={!!w.rule?.below} onChange={(e) => set({ rule: { ...w.rule, below: e.target.checked || undefined } })} className="size-3.5 accent-brand" />
                  Alert when the value is below the limit
                </label>
              </>
            )}
          </div>
          <div className="grid content-start gap-4 border-line px-5 pt-4 pb-5 md:border-l">
            {w.type !== "note" && (
              <>
                <h4 className={H4}>{w.type === "event_list" || w.type === "distribution" || w.type === "heatmap" ? "Which events" : "Which queues"}</h4>
                <div className="grid gap-1.5">
                  <span className="text-[13px] text-ink-2">Filter</span>
                  <QueryInput query={w.filter ?? ""} setQuery={(filter) => set({ filter })} live={!!ctx.data.live} lang={ctx.lang} bodyPaths={ctx.bodyPaths} vars={ctx.vars.map((v) => v.name)} placeholder="All queues. For example queue:billing-* or queue:$queue" label="Widget filter" />
                </div>
                <label className="flex cursor-pointer items-center gap-2 text-[13px] text-ink">
                  <input type="checkbox" checked={w.follow !== false} onChange={(e) => set({ follow: e.target.checked ? undefined : false })} className="size-3.5 accent-brand" />
                  Follow dashboard variables
                </label>
              </>
            )}
            <h4 className={`${H4} mt-1`}>How it looks</h4>
            <Labeled label="Title"><input value={w.title} onChange={(e) => set({ title: e.target.value })} className={FIELD} /></Labeled>
            {w.type === "timeseries" && (
              <>
                <div className="grid gap-1.5">
                  <span className="text-[13px] text-ink-2">Style</span>
                  <span className="inline-flex w-fit rounded-lg bg-sunken p-0.5">
                    {(["line", "stacked", "bars"] as const).map((s) => (
                      <button key={s} type="button" aria-pressed={(w.style ?? "line") === s} onClick={() => set({ style: s })} className={`rounded-md px-3 py-1 text-[13px] ${(w.style ?? "line") === s ? "bg-surface font-medium text-ink shadow-card" : "text-ink-2"}`}>
                        {s === "line" ? "Line" : s === "stacked" ? "Stacked" : "Bars"}
                      </button>
                    ))}
                  </span>
                </div>
                <div className="grid grid-cols-[110px_minmax(0,1fr)] gap-2">
                  <Labeled label="Marker at"><input inputMode="decimal" value={w.marker?.value ?? ""} onChange={(e) => { const v = num(e.target.value); set({ marker: v === undefined ? undefined : { value: v, label: w.marker?.label ?? "Limit" } }); }} className={`${FIELD} font-mono`} /></Labeled>
                  <Labeled label="Marker label"><input value={w.marker?.label ?? ""} disabled={!w.marker} onChange={(e) => set({ marker: { value: w.marker!.value, label: e.target.value } })} className={`${FIELD} disabled:opacity-50`} /></Labeled>
                </div>
              </>
            )}
            {w.type !== "note" && (
              <Labeled label="Time range" hint={w.time ? "Its own range: the widget is marked with a clock, and dragging across it doesn't zoom the dashboard." : undefined}>
                <select value={w.time ?? ""} onChange={(e) => set({ time: e.target.value || undefined })} className={FIELD}>
                  <option value="">Same as the dashboard</option>
                  {PRESETS.map((p) => <option key={p.id} value={p.id}>{p.label}</option>)}
                </select>
              </Labeled>
            )}
          </div>
        </div>
      ) : (
        <div className="px-5 pt-4 pb-5">
          <textarea value={text} onChange={(e) => edit(e.target.value)} spellCheck={false} aria-label="Widget definition as JSON" aria-invalid={!!parsed.error} className={`block h-[42vh] w-full resize-none rounded-lg border bg-canvas p-3.5 font-mono text-caption leading-relaxed text-ink !outline-none ${parsed.error ? "border-critical" : "border-line focus:border-accent"}`} />
          {parsed.error && <p className="mt-2 flex items-center gap-2 text-[13px] text-critical-text"><TriangleAlert size={14} className="shrink-0" />{parsed.error}</p>}
        </div>
      )}
      {tab === "form" && error && <p className="flex items-center gap-2 border-t border-line px-5 py-2 text-[13px] text-critical-text"><TriangleAlert size={14} className="shrink-0" />{error}</p>}
    </Modal>
  );
}
