"use client";

import { useId, type ReactNode } from "react";
import { DOME } from "../design/Logo.tsx";

/* The gap between the dome's two pieces, in the logo's units: the slit the light leaves by. */
const SLIT = "M135.72 113.92 A27.8 142 0 0 1 143.8 214 H169.8 A53.8 142 0 0 0 156.45 120.36 A102 102 0 0 0 135.72 113.92 Z";

/*
  The picture for "nothing to show here yet": the logo's observatory with its
  beam sweeping slowly across the sky. The logo holds one message above the
  dome as an amber dot; here the dot is left out, because no message has been
  observed. The dome takes the ink of its ground and the light is the accent.

  The light starts inside the dome. The slit is lit from within, brightest at
  its mouth, and the beam leaves the mouth at the slit's full width, along the
  line from the dome's centre through the mouth. A soft halo goes with it.
  The sweep is in tokens.css (.watch-beam): a short arc either side of that
  line, so the beam always leaves by the slit. It stands still under reduced motion.
*/
export function Watching({ title, children, className = "" }: { title: ReactNode; children?: ReactNode; className?: string }) {
  const id = useId();
  const accent = { stopColor: "var(--color-accent)" };
  return (
    <div className={`flex flex-col items-center text-center ${className}`}>
      <svg viewBox="0 0 320 132" width={320} height={132} aria-hidden className="max-w-full overflow-visible text-ink-3">
        <defs>
          <linearGradient id={`${id}-beam`} gradientUnits="userSpaceOnUse" x1="169.4" y1="95" x2="169.4" y2="-40">
            <stop offset="0" style={accent} stopOpacity="0.7" />
            <stop offset="0.45" style={accent} stopOpacity="0.26" />
            <stop offset="1" style={accent} stopOpacity="0" />
          </linearGradient>
          <linearGradient id={`${id}-halo`} gradientUnits="userSpaceOnUse" x1="169.4" y1="95" x2="169.4" y2="-40">
            <stop offset="0" style={accent} stopOpacity="0.3" />
            <stop offset="1" style={accent} stopOpacity="0" />
          </linearGradient>
          <linearGradient id={`${id}-slit`} gradientUnits="userSpaceOnUse" x1="0" y1="114" x2="0" y2="214">
            <stop offset="0" style={accent} stopOpacity="0.95" />
            <stop offset="1" style={accent} stopOpacity="0.2" />
          </linearGradient>
          <radialGradient id={`${id}-mouth`} gradientUnits="userSpaceOnUse" cx="170" cy="93.4" r="15">
            <stop offset="0" style={accent} stopOpacity="0.5" />
            <stop offset="1" style={accent} stopOpacity="0" />
          </radialGradient>
          <filter id={`${id}-soft`} filterUnits="userSpaceOnUse" x="110" y="-70" width="120" height="190">
            <feGaussianBlur stdDeviation="5" />
          </filter>
          <filter id={`${id}-edge`} filterUnits="userSpaceOnUse" x="110" y="-70" width="120" height="190">
            <feGaussianBlur stdDeviation="0.5" />
          </filter>
        </defs>
        {/* the beam, drawn pointing straight up from just inside the slit's mouth; .watch-beam turns it */}
        <g className="watch-beam">
          <path d="M163.4 95.3 L141.4 -40 L197.4 -40 L175.4 95.3 Z" fill={`url(#${id}-halo)`} filter={`url(#${id}-soft)`} />
          <path d="M165.9 95.3 L153.4 -40 L185.4 -40 L172.9 95.3 Z" fill={`url(#${id}-beam)`} filter={`url(#${id}-edge)`} />
        </g>
        {/* light spilling from the mouth; the dome covers the half that falls inside it */}
        <circle cx="170" cy="93.4" r="15" fill={`url(#${id}-mouth)`} />
        {/* the dome from the logo, at a third of its size, standing on the ground line, with its slit lit */}
        <g transform="translate(121.3 54.3) scale(0.3333)">
          <path d={DOME} fill="var(--color-canvas)" />
          <path d={SLIT} fill="var(--color-canvas)" />
          <path d={SLIT} fill={`url(#${id}-slit)`} />
          <path d={DOME} fill="currentColor" />
        </g>
        <path d="M40 126.5 H280" stroke="var(--color-line-strong)" strokeWidth="1" strokeLinecap="round" />
      </svg>
      <p className="mt-4 text-sm font-medium text-ink">{title}</p>
      {children && <p className="mt-1 max-w-md text-sm leading-relaxed text-ink-2">{children}</p>}
    </div>
  );
}

/*
  A page's first-run state, while nothing has reported to the workspace (or, open source, while the server has
  never stored a span): the watching picture, one message ending in what this page will show, and the host's ways
  forward. The Overview and the Event explorer draw it in place of their empty charts and tables (QV-272, QV-777).
*/
export function NothingReporting({ to, children, actions }: { to?: string; children: ReactNode; actions?: ReactNode }) {
  return (
    <section
      aria-label="No data"
      className="rounded-card border border-line bg-canvas/60"
      style={{ backgroundImage: "radial-gradient(color-mix(in srgb, var(--color-ink-3) 26%, transparent) 1px, transparent 1px)", backgroundSize: "16px 16px", backgroundPosition: "8px 8px" }}
    >
      <div className="mx-auto flex min-h-[400px] max-w-lg flex-col items-center justify-center px-6 py-12 text-center">
        <Watching title="Waiting to observe your first message">
          No data yet. Nothing is reporting to {to ?? "this workspace"}. {children}
        </Watching>
        {actions && <div className="mt-5 flex flex-wrap justify-center gap-3">{actions}</div>}
      </div>
    </section>
  );
}
