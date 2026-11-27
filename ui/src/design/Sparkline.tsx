export function Sparkline({ data, w = 96, h = 28, color = "var(--color-series-1)" }: { data: number[]; w?: number; h?: number; color?: string }) {
  const max = Math.max(...data);
  const min = Math.min(...data);
  const pts = data.map((v, i) => {
    const x = (i / (data.length - 1)) * (w - 4) + 2;
    const y = h - 3 - ((v - min) / (max - min || 1)) * (h - 6);
    return [x, y] as const;
  });
  const d = pts.map(([x, y], i) => `${i ? "L" : "M"}${x.toFixed(1)},${y.toFixed(1)}`).join(" ");
  const [lx, ly] = pts[pts.length - 1];
  return (
    <svg width={w} height={h} aria-hidden className="overflow-visible">
      <path d={`${d} L${lx},${h} L2,${h} Z`} fill={color} opacity=".1" />
      <path d={d} fill="none" stroke={color} strokeWidth="1.75" strokeLinejoin="round" strokeLinecap="round" />
      <circle cx={lx} cy={ly} r="2.5" fill={color} stroke="var(--color-surface)" strokeWidth="1.5" />
    </svg>
  );
}
