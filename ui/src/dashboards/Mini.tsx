import type { Tone } from "./measures.ts";
import type { DashboardDef, WidgetType } from "./types.ts";

/*
  A small picture of a dashboard: its real layout, each widget drawn as what it
  is, in the colors it uses on the dashboard. A widget that is alerting has an
  outline in its status color.
*/

const S1 = "var(--color-series-1)", S2 = "var(--color-series-2)", S3 = "var(--color-series-3)", LS = "var(--color-line-strong)";
const wob = (seed: number, i: number) => Math.sin(seed * 12.9898 + i * 4.1) * 3;
const line = (col: string, seed: number, base: number, rise = 0) => (
  <path d={Array.from({ length: 12 }, (_, i) => `${i ? "L" : "M"}${4 + (i * 92) / 11},${Math.max(5, Math.min(35, base - rise * Math.max(0, i - 5) + wob(seed, i)))}`).join(" ")} fill="none" stroke={col} strokeWidth={1.5} vectorEffect="non-scaling-stroke" strokeLinejoin="round" />
);

function art(type: WidgetType, tone: string | null, k: number) {
  switch (type) {
    case "number": return <><rect x="10" y="11" width="42" height="11" rx="2" fill={tone ?? "var(--color-ink-2)"} /><rect x="10" y="27" width="26" height="3" rx="1.5" fill={LS} /></>;
    case "timeseries": return <>{line(tone ?? S1, k, 26, tone ? 3.2 : 0)}{line(S2, k + 1, 21)}{!tone && line(S3, k + 2, 30)}</>;
    case "heatmap": return <>{Array.from({ length: 48 }, (_, n) => { const i = n % 12, j = Math.floor(n / 12); return <rect key={n} x={5 + i * 7.6} y={5 + j * 8} width="6.6" height="7" fill={S3} fillOpacity={Math.max(0.08, Math.min(1, Math.exp(-((j - 2.6) ** 2) / 1.2) + (i > 7 && j === 0 ? 0.8 : 0)))} />; })}</>;
    case "distribution": return <>{[26, 14, 8, 5, 3].map((h, i) => <rect key={i} x={12 + i * 16} y={34 - h} width="7" height={h} rx="3.5" fill={i === 4 ? "var(--color-critical)" : S1} />)}</>;
    case "gauge": return <>{[0, 1, 2].map((j) => <g key={j}><rect x="8" y={9 + j * 10} width="46" height="1.8" fill={LS} /><rect x="78" y={9 + j * 10} width="14" height="1.8" fill={j === 0 && tone ? tone : "var(--color-ink-2)"} /></g>)}</>;
    case "toplist": return <>{[52, 40, 46, 30].map((wd, j) => <g key={j}><rect x="8" y={8 + j * 7.5} width={wd} height="1.6" fill={LS} /><rect x="78" y={8 + j * 7.5} width="14" height="1.6" fill="var(--color-ink-2)" /></g>)}</>;
    case "queue_table":
    case "event_list": return <>{[30, 44, 36, 40, 28].map((wd, j) => <g key={j}>{j > 0 && <rect x="6" y={6 + j * 7.5} width="3" height="3.5" fill={type === "event_list" ? "var(--color-critical)" : j === 1 && tone ? tone : S3} />}<rect x="13" y={7 + j * 7.5} width={wd} height="1.6" fill={LS} /><rect x="70" y={7 + j * 7.5} width="22" height="1.6" fill={LS} /></g>)}</>;
    case "status_grid": return <>{Array.from({ length: 30 }, (_, n) => <rect key={n} x={6 + (n % 10) * 8.8} y={6 + Math.floor(n / 10) * 10} width="7" height="8" rx="1" fill={n === 16 && tone ? tone : "var(--color-sunken)"} />)}</>;
    case "queue_flow": return <><path d="M16,12 C40,12 30,20 50,20 M16,28 C40,28 30,20 50,20" fill="none" stroke={S2} strokeWidth={1.5} vectorEffect="non-scaling-stroke" /><path d="M50,20 C70,20 64,12 86,12" fill="none" stroke={S1} strokeWidth={1.5} vectorEffect="non-scaling-stroke" /><path d="M50,20 C70,20 64,28 86,28" fill="none" stroke="var(--color-lost)" strokeWidth={1.5} strokeDasharray="3 2" vectorEffect="non-scaling-stroke" />{[[14, 12], [14, 28], [50, 20], [86, 12], [86, 28]].map(([x, y]) => <rect key={`${x}-${y}`} x={x - 4} y={y - 5} width="8" height="10" rx="2" fill="var(--color-canvas)" stroke={LS} vectorEffect="non-scaling-stroke" />)}</>;
    default: return <>{[60, 78, 70, 40].map((wd, j) => <rect key={j} x="8" y={9 + j * 7} width={wd} height="1.6" fill={LS} />)}</>;
  }
}

export function Mini({ def, tones = {}, rows: cap = 13 }: { def: DashboardDef; tones?: Record<string, Tone>; rows?: number }) {
  /* groups sit under the ungrouped widgets, in order, as they do on the dashboard */
  let offset = Math.max(0, ...def.widgets.filter((w) => !w.section).map((w) => w.layout.y + w.layout.h));
  const placed = def.widgets.filter((w) => !w.section).map((w) => ({ w, y: w.layout.y }));
  for (const g of def.sections) {
    const inside = def.widgets.filter((w) => w.section === g.id);
    for (const w of inside) placed.push({ w, y: offset + w.layout.y });
    offset += Math.max(0, ...inside.map((w) => w.layout.y + w.layout.h));
  }
  const rows = Math.min(cap, Math.max(4, offset));
  return (
    <div className="rounded-lg bg-canvas p-2.5">
      <div className="relative overflow-hidden" style={{ height: rows * 10 }}>
        {placed.filter(({ w, y }) => y < rows).map(({ w, y }, k) => {
          const tone = tones[w.id] && tones[w.id] !== "good" ? `var(--color-${tones[w.id]})` : null;
          return (
            <span
              key={w.id}
              className="absolute overflow-hidden rounded-[3px] bg-surface"
              style={{ left: `calc(${(w.layout.x / 12) * 100}% + 1.5px)`, top: y * 10 + 1.5, width: `calc(${(w.layout.w / 12) * 100}% - 3px)`, height: w.layout.h * 10 - 3, boxShadow: `inset 0 0 0 1px ${tone ?? "var(--color-line)"}` }}
            >
              <svg viewBox="0 0 100 40" preserveAspectRatio="none" aria-hidden className="block size-full">{art(w.type, tone, k)}</svg>
            </span>
          );
        })}
      </div>
    </div>
  );
}
