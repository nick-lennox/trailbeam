"use client";

import { useLayoutEffect, useRef, useState } from "react";
import { Plus, X } from "lucide-react";
import { SECTION_COLORS, type Section, type SectionColor } from "./types.ts";
import { FIELD } from "./ui.tsx";
import { Tooltip } from "../design/Tooltip.tsx";

/*
  How a section can be made to look, and the panel that sets it. Every choice is
  a field on the section in the dashboard's JSON. A color is one of the chart
  palette's names or any hex value.
*/

export const PALETTE: Record<SectionColor, string> = { blue: "var(--color-series-1)", orange: "var(--color-series-2)", green: "var(--color-series-3)", yellow: "var(--cat-4)", pink: "var(--cat-5)", gray: "var(--color-sunken)" };
/* The text pair for a coloured header: the sky's ink and the sky, fixed in both themes, so a header reads the same on
   the colour a person picked whichever theme views it. */
const LIGHT = "var(--color-sky-ink)", DARK = "var(--color-sky)";
/* <input type="color"> takes only a hex, so its starting values are written out: the series blue, and the ember. */
const PICK_BG = "#2a78d6", PICK_TEXT = "#ffb65e";
const isHex = (c?: string): c is string => !!c && /^#[0-9a-f]{6}$/i.test(c);
const isName = (c?: string): c is SectionColor => !!c && (SECTION_COLORS as readonly string[]).includes(c);
const dark = (hex: string) => { const n = parseInt(hex.slice(1), 16); return (0.2126 * (n >> 16) + 0.7152 * ((n >> 8) & 255) + 0.0722 * (n & 255)) / 255 < 0.55; };

/* The styles a section's fields come to. */
export function sectionLook(s: Section) {
  const h = s.header ?? s.color;
  const headerBg = isName(h) ? PALETTE[h] : isHex(h) ? h : undefined;
  /* text that reads on the header: dark on the palette's colors, and on a custom color whichever of the two contrasts */
  const auto = isName(h) && h !== "gray" ? DARK : isHex(h) ? (dark(h) ? LIGHT : DARK) : undefined;
  const text = s.text === "light" ? LIGHT : s.text === "dark" ? DARK : isHex(s.text) ? s.text : auto;
  /* a palette color behind widgets is softened so the widgets stay the subject; a custom color is used as given */
  const bodyBg = isName(s.body) ? (s.body === "gray" ? PALETTE.gray : `color-mix(in srgb, ${PALETTE[s.body]} 16%, var(--color-canvas))`) : isHex(s.body) ? s.body : undefined;
  return { headerBg, text, bodyBg };
}

const LABEL = "text-[13px] text-ink-2";
function Seg<T extends string>({ value, options, onChange }: { value: T; options: [T, string][]; onChange: (v: T) => void }) {
  return (
    <span className="inline-flex w-fit rounded-lg bg-sunken p-0.5">
      {options.map(([id, label]) => (
        <button key={id} type="button" aria-pressed={value === id} onClick={() => onChange(id)} className={`rounded-md px-2.5 py-1 text-[13px] ${value === id ? "bg-surface font-medium text-ink shadow-card" : "text-ink-2"}`}>{label}</button>
      ))}
    </span>
  );
}
/* None, the palette, and any color. soft shows the palette as it will look behind widgets. */
function Swatches({ value, onChange, label, soft = false }: { value?: string; onChange: (c: string | undefined) => void; label: string; soft?: boolean }) {
  const ring = "ring-2 ring-ink ring-offset-2 ring-offset-surface";
  return (
    <span className="flex flex-wrap items-center gap-1.5">
      <Tooltip content="None" className="inline-flex"><button type="button" aria-pressed={!value} aria-label={`No ${label}`} onClick={() => onChange(undefined)} className={`grid size-7 place-items-center rounded-md border text-ink-3 ${!value ? "border-ink ring-1 ring-ink" : "border-line-strong"}`}><X size={13} /></button></Tooltip>
      {SECTION_COLORS.map((c) => (
        <Tooltip key={c} content={c[0].toUpperCase() + c.slice(1)} className="inline-flex"><button type="button" aria-pressed={value === c} aria-label={`${c} ${label}`} onClick={() => onChange(c)} className={`size-7 rounded-md ${value === c ? ring : ""} ${c === "gray" || soft ? "border border-line-strong" : ""}`} style={{ background: soft && c !== "gray" ? `color-mix(in srgb, ${PALETTE[c]} 16%, var(--color-canvas))` : PALETTE[c] }} /></Tooltip>
      ))}
      <Tooltip content="Any color" className="inline-flex"><label className={`relative grid size-7 cursor-pointer place-items-center overflow-hidden rounded-md border border-line-strong text-ink-3 ${isHex(value) ? ring : ""}`} style={{ background: isHex(value) ? value : undefined }}>
        {!isHex(value) && <Plus size={13} />}
        <input type="color" aria-label={`Any ${label}`} value={isHex(value) ? value : PICK_BG} onChange={(e) => onChange(e.target.value)} className="absolute inset-0 cursor-pointer opacity-0" />
      </label></Tooltip>
    </span>
  );
}

export function SectionOptions({ section: g, onChange, onClose }: { section: Section; onChange: (patch: Partial<Section>) => void; onClose: () => void }) {
  const textMode = g.text === "light" || g.text === "dark" ? g.text : isHex(g.text) ? "custom" : "auto";
  /* The panel is placed against the window, not the page, so it never makes the page longer. It opens
     under the header when it fits there, above it when it fits there, and otherwise as low as it can go. */
  const anchor = useRef<HTMLSpanElement>(null);
  const panel = useRef<HTMLDivElement>(null);
  const [at, setAt] = useState<{ top: number; right: number; maxHeight: number } | null>(null);
  useLayoutEffect(() => {
    const place = () => {
      const head = anchor.current?.parentElement?.getBoundingClientRect();
      if (!head || !panel.current) return;
      const GAP = 6, EDGE = 8, vh = window.innerHeight;
      const h = Math.min(panel.current.scrollHeight + 2, vh - 2 * EDGE);
      const below = head.bottom + GAP, above = head.top - GAP - h;
      const top = below + h <= vh - EDGE ? below : above >= EDGE ? above : Math.max(EDGE, vh - EDGE - h);
      setAt({ top, right: Math.max(EDGE, window.innerWidth - head.right), maxHeight: vh - 2 * EDGE });
    };
    place();
    window.addEventListener("resize", place);
    window.addEventListener("scroll", place, true);
    return () => { window.removeEventListener("resize", place); window.removeEventListener("scroll", place, true); };
  }, [g.size, g.description]);
  const styled = !!(g.header ?? g.color) || g.text || g.body || g.border || g.align || g.weight || g.size || g.divider === false;
  return (
    <>
      <button aria-label="Close the section options" className="fixed inset-0 z-30 cursor-default" onClick={onClose} />
      <span ref={anchor} aria-hidden className="hidden" />
      <div ref={panel} role="dialog" aria-label={`Customize ${g.title}`} className="fixed z-40 grid w-[340px] gap-3.5 overflow-y-auto rounded-card border border-line-strong bg-surface p-3.5 text-left text-ink shadow-raised" style={at ? { top: at.top, right: at.right, maxHeight: at.maxHeight } : { top: 0, right: 0, visibility: "hidden" }}>
        <label className="grid gap-1.5"><span className={LABEL}>Title</span><input value={g.title} onChange={(e) => onChange({ title: e.target.value })} className={FIELD} /></label>
        <label className="grid gap-1.5"><span className={LABEL}>Description</span><input value={g.description ?? ""} onChange={(e) => onChange({ description: e.target.value || undefined })} placeholder="Optional" className={FIELD} /></label>

        <h4 className="border-t border-line pt-3 text-[13px] font-semibold">Header</h4>
        <div className="grid grid-cols-2 gap-3">
          <div className="grid gap-1.5"><span className={LABEL}>Size</span><Seg value={g.size ?? "md"} options={[["sm", "S"], ["md", "M"], ["lg", "L"], ["xl", "XL"]]} onChange={(v) => onChange({ size: v === "md" ? undefined : v })} /></div>
          <div className="grid gap-1.5"><span className={LABEL}>Weight</span><Seg value={g.weight ?? "bold"} options={[["regular", "Regular"], ["bold", "Bold"]]} onChange={(v) => onChange({ weight: v === "bold" ? undefined : v })} /></div>
        </div>
        <div className="grid gap-1.5"><span className={LABEL}>Alignment</span><Seg value={g.align ?? "left"} options={[["left", "Left"], ["center", "Center"]]} onChange={(v) => onChange({ align: v === "left" ? undefined : v })} /></div>
        <div className="grid gap-1.5"><span className={LABEL}>Background</span><Swatches label="header background" value={g.header ?? g.color} onChange={(c) => onChange({ header: c, color: undefined })} /></div>
        <div className="grid gap-1.5">
          <span className={LABEL}>Text color</span>
          <span className="flex items-center gap-2">
            <Seg value={textMode} options={[["auto", "Auto"], ["light", "Light"], ["dark", "Dark"], ["custom", "Custom"]]} onChange={(v) => onChange({ text: v === "auto" ? undefined : v === "custom" ? (isHex(g.text) ? g.text : PICK_TEXT) : v })} />
            {textMode === "custom" && <input type="color" aria-label="Header text color" value={g.text} onChange={(e) => onChange({ text: e.target.value })} className="size-7 cursor-pointer rounded-md border border-line-strong bg-transparent p-0" />}
          </span>
        </div>
        <label className="flex cursor-pointer items-center gap-2 text-[13px]">
          <input type="checkbox" checked={g.divider !== false} onChange={(e) => onChange({ divider: e.target.checked ? undefined : false })} className="size-3.5 accent-brand" />
          Line under the header when it has no background
        </label>

        <h4 className="border-t border-line pt-3 text-[13px] font-semibold">Content</h4>
        <div className="grid gap-1.5"><span className={LABEL}>Background</span><Swatches soft label="content background" value={g.body} onChange={(c) => onChange({ body: c })} /></div>
        <label className="flex cursor-pointer items-center gap-2 text-[13px]">
          <input type="checkbox" checked={!!g.border} onChange={(e) => onChange({ border: e.target.checked || undefined })} className="size-3.5 accent-brand" />
          Border around the section
        </label>

        {styled && <button type="button" onClick={() => onChange({ header: undefined, color: undefined, text: undefined, body: undefined, border: undefined, align: undefined, weight: undefined, size: undefined, divider: undefined })} className="justify-self-start text-[13px] text-brand hover:underline">Reset to plain</button>}
      </div>
    </>
  );
}
