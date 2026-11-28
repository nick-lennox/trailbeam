"use client";

import { useRef, useState } from "react";
import { COLS, type Layout } from "./types.ts";
import { useSize } from "./ui.tsx";

/*
  The 12-column snap grid. There is no edit mode: a widget is dragged by any
  element marked data-grid-drag and resized by one marked data-grid-resize.
  While it moves, a flat block shows where it will land, the widgets it lands on
  move down, and the grid closes gaps upward.
*/

export const ROW = 72, GAP = 12;
export type Item = { id: string; layout: Layout };

const hits = (a: Layout, b: Layout) => a.x < b.x + b.w && b.x < a.x + a.w && a.y < b.y + b.h && b.y < a.y + a.h;

/* Put one item where it was dropped, push what it lands on down, then let everything else rise. */
export function settle(items: Item[], moved?: Item): Item[] {
  const order = (a: Item, b: Item) => a.layout.y - b.layout.y || a.layout.x - b.layout.x;
  const out: Item[] = moved ? [{ id: moved.id, layout: { ...moved.layout } }] : [];
  for (const it of items.filter((i) => i.id !== moved?.id).sort(order)) {
    const l = { ...it.layout };
    while (out.some((p) => hits(p.layout, l))) l.y++;
    out.push({ id: it.id, layout: l });
  }
  const sorted = [...out].sort(order);
  for (const it of sorted) {
    if (it.id === moved?.id) continue;
    while (it.layout.y > 0 && !sorted.some((p) => p !== it && hits(p.layout, { ...it.layout, y: it.layout.y - 1 }))) it.layout.y--;
  }
  return sorted;
}

/* The first free place for a new widget of this size. */
export function firstFree(items: Item[], w: number, h: number): Layout {
  for (let y = 0; ; y++) for (let x = 0; x <= COLS - w; x++) if (!items.some((i) => hits(i.layout, { x, y, w, h }))) return { x, y, w, h };
}

export function Grid({ items, locked, onChange, render }: { items: Item[]; locked: boolean; onChange: (items: Item[]) => void; render: (id: string, dragging: boolean) => React.ReactNode }) {
  const [ref, { w }] = useSize<HTMLDivElement>();
  const [live, setLive] = useState<{ id: string; items: Item[] } | null>(null);
  const start = useRef<{ id: string; mode: "move" | "size"; px: number; py: number; from: Layout } | null>(null);
  const col = (w - GAP * (COLS - 1)) / COLS;
  const shown = live?.items ?? items;
  const rows = Math.max(1, ...shown.map((i) => i.layout.y + i.layout.h));
  const box = (l: Layout) => ({ left: l.x * (col + GAP), top: l.y * (ROW + GAP), width: l.w * col + (l.w - 1) * GAP, height: l.h * ROW + (l.h - 1) * GAP });

  const down = (e: React.PointerEvent, id: string) => {
    if (locked || e.button !== 0) return;
    const t = e.target as HTMLElement;
    if (t.closest("button, a, input, select, textarea") && !t.closest("[data-grid-resize]")) return;
    const mode = t.closest("[data-grid-resize]") ? "size" : t.closest("[data-grid-drag]") ? "move" : null;
    if (!mode) return;
    e.preventDefault();
    try { (e.currentTarget as HTMLElement).setPointerCapture(e.pointerId); } catch {}
    start.current = { id, mode, px: e.clientX, py: e.clientY, from: items.find((i) => i.id === id)!.layout };
  };
  const move = (e: React.PointerEvent) => {
    const s = start.current;
    if (!s) return;
    const dx = Math.round((e.clientX - s.px) / (col + GAP)), dy = Math.round((e.clientY - s.py) / (ROW + GAP));
    const l = s.mode === "move"
      ? { ...s.from, x: Math.max(0, Math.min(COLS - s.from.w, s.from.x + dx)), y: Math.max(0, s.from.y + dy) }
      : { ...s.from, w: Math.max(2, Math.min(COLS - s.from.x, s.from.w + dx)), h: Math.max(2, s.from.h + dy) };
    setLive({ id: s.id, items: settle(items, { id: s.id, layout: l }) });
  };
  const up = () => {
    const s = start.current;
    start.current = null;
    if (!s || !live) return setLive(null);
    const done = settle(live.items);
    setLive(null);
    if (JSON.stringify(done.map((i) => [i.id, i.layout]).sort()) !== JSON.stringify(items.map((i) => [i.id, i.layout]).sort())) onChange(done);
  };

  return (
    <div ref={ref} className="relative" style={{ height: rows * ROW + (rows - 1) * GAP }}>
      {w > 0 && live && <div className="absolute rounded-card bg-sunken" style={box(shown.find((i) => i.id === live.id)!.layout)} />}
      {w > 0 && items.map((it) => {
        const l = shown.find((i) => i.id === it.id)?.layout ?? it.layout;
        const dragging = live?.id === it.id;
        return (
          <div
            key={it.id}
            className={`absolute ${dragging ? "z-20" : "transition-[left,top,width,height] duration-150"}`}
            style={box(l)}
            onPointerDown={(e) => down(e, it.id)}
            onPointerMove={move}
            onPointerUp={up}
            onPointerCancel={up}
          >
            {render(it.id, dragging)}
          </div>
        );
      })}
    </div>
  );
}
