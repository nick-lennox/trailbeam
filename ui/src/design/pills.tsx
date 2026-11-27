import type { ElementType } from "react";
import { AlertTriangle, CheckCircle2, CircleDashed, Eraser, Filter, PauseCircle, RotateCw, Send, Skull, XOctagon } from "lucide-react";
import { POSSIBLY_PURGED_TITLE } from "../messages/Messages.tsx";
import { Tooltip } from "./Tooltip.tsx";

/*
  The pills: a message's outcome or state, a queue's health, and where a timeline step came from.
  Status colors are reserved and always ship with an icon and a label, never color alone.
*/
export type Outcome = "deleted" | "in_flight" | "retrying" | "dead_lettered" | "stuck" | "filtered";
export type StepSource = "otel" | "package" | "cloudtrail" | "function_logs" | "inferred";

type Look = { label: string; icon: ElementType; cls: string };

const OUTCOME: Record<Outcome, Look> = {
  deleted: { label: "Deleted", icon: CheckCircle2, cls: "bg-good-soft text-good-text ring-good-ring" },
  in_flight: { label: "In flight", icon: CircleDashed, cls: "bg-sunken text-ink-2 ring-line" },
  retrying: { label: "Retrying", icon: RotateCw, cls: "bg-warning-soft text-warning-text ring-warning-ring" },
  stuck: { label: "Stuck", icon: PauseCircle, cls: "bg-serious-soft text-serious-text ring-serious-ring" },
  dead_lettered: { label: "Dead-lettered", icon: Skull, cls: "bg-critical-soft text-critical-text ring-critical-ring" },
  filtered: { label: "Filtered by trigger", icon: Filter, cls: "bg-serious-soft text-serious-text ring-serious-ring" },
};

export function OutcomePill({ outcome }: { outcome: Outcome }) {
  const o = OUTCOME[outcome];
  const Icon = o.icon;
  return (
    <span className={`inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium ring-1 ring-inset ${o.cls}`}>
      <Icon size={12} strokeWidth={2.25} aria-hidden />
      {o.label}
    </span>
  );
}

export function HealthPill({ health, label }: { health: "good" | "warning" | "critical"; label?: string }) {
  const map = {
    good: { label: "Healthy", icon: CheckCircle2, cls: "text-good-text" },
    warning: { label: "Backlog", icon: AlertTriangle, cls: "text-warning-text" },
    critical: { label: "DLQ growing", icon: XOctagon, cls: "text-critical-text" },
  }[health];
  const Icon = map.icon;
  return (
    <span className={`inline-flex items-center gap-1.5 text-xs font-medium ${map.cls}`}>
      <Icon size={14} strokeWidth={2.25} aria-hidden />
      {label ?? map.label}
    </span>
  );
}

/*
  Provenance tags. Every timeline step says where it came from, so users know how
  much to trust it. Recorded sources are solid; "inferred" is dashed: Trailbeam
  deduced the step (e.g. a DLQ move from the redrive policy), nothing recorded it.
*/
export const SOURCE: Record<StepSource, { label: string; title: string }> = {
  otel: { label: "OTel", title: "Recorded by OpenTelemetry instrumentation in your service" },
  package: { label: "Trailbeam package", title: "Recorded by the Trailbeam package (receive count, tenant, receipt handle, processing outcome)" },
  cloudtrail: { label: "CloudTrail", title: "Recorded by AWS CloudTrail data events, which cover code we don't instrument" },
  function_logs: { label: "Function logs", title: "Recorded by Lambda in the function's own log (a REPORT line), matched to the message by the invocation's request ID" },
  inferred: { label: "inferred", title: "Not recorded by any source. Trailbeam deduced it from your queue configuration and earlier steps" },
};

export function SourceTag({ source, night = false }: { source: StepSource; night?: boolean }) {
  const s = SOURCE[source];
  const dashed = source === "inferred";
  const cls = night ? "border-night-line text-night-ink-3" : "border-line bg-surface text-ink-3";
  return (
    <Tooltip content={s.title} tabIndex={0} className={`inline-flex items-center whitespace-nowrap rounded-md border px-1.5 py-px text-micro ${dashed ? "border-dashed italic" : ""} ${cls}`}>
      {s.label}
    </Tooltip>
  );
}

/*
  A message's state as Trailbeam reports it, for a workspace's own messages. Like OutcomePill, always an
  icon and a label, never color alone. The sample pages keep OutcomePill, whose outcomes (retrying,
  filtered) Trailbeam doesn't report.

  A recorded PurgeQueue call (contract 3.3): a message it removed is "Purged"; one whose last step was
  during the purge or within 60 s after it keeps its state, with "possibly purged" after the pill.
*/
export type PurgeOf = { status: "purged" | "possibly_purged"; start: string };
export type StateOf = {
  state: string; in_dead_letter_queue: boolean; deliveries: number; purge?: PurgeOf | null;
};
const GOOD = "bg-good-soft text-good-text ring-good-ring";
const NEUTRAL = "bg-sunken text-ink-2 ring-line";

export function stateLook(m: StateOf): Look {
  if (m.in_dead_letter_queue) return { label: "In the DLQ", icon: Skull, cls: "bg-critical-soft text-critical-text ring-critical-ring" };
  if (m.state === "deleted") return { label: "Deleted", icon: CheckCircle2, cls: GOOD };
  if (m.state === "deleted_inferred") return { label: "Deleted by Lambda", icon: CheckCircle2, cls: GOOD };
  if (m.state === "purged") return { label: "Purged", icon: Eraser, cls: NEUTRAL };
  if (m.state === "processing") return { label: "Being handled", icon: CircleDashed, cls: NEUTRAL };
  if (m.deliveries === 0) return { label: "Not received", icon: Send, cls: NEUTRAL };
  return { label: "Not deleted", icon: PauseCircle, cls: "bg-serious-soft text-serious-text ring-serious-ring" };
}

/** "12:00:09.000 UTC": a purge's start, as the lists show it. */
export const purgeClock = (iso: string) => `${iso.slice(11, 23)} UTC`;

/** What a recorded purge says about the message, as plain words for a note or a list cell; empty when none applies. */
export function purgeText(m: { purge?: PurgeOf | null }): string {
  const p = m.purge;
  if (!p) return "";
  const at = purgeClock(p.start);
  return p.status === "purged" ? `Purged by a PurgeQueue call at ${at}` : `Possibly purged: a PurgeQueue call ran at ${at}`;
}

export { POSSIBLY_PURGED_TITLE };

export function StatePill({ m }: { m: StateOf }) {
  const { label, icon: Icon, cls } = stateLook(m);
  const purge = purgeText(m);
  const look = `inline-flex items-center gap-1 whitespace-nowrap rounded-full px-2 py-0.5 text-xs font-medium ring-1 ring-inset ${cls}`;
  const inner = <><Icon size={12} strokeWidth={2.25} aria-hidden />{label}</>;
  const pill = m.purge?.status === "purged"
    ? <Tooltip content={purge} tabIndex={0} className={look}>{inner}</Tooltip>
    : <span className={look}>{inner}</span>;
  if (m.purge?.status !== "possibly_purged") return pill;
  return (
    <span className="inline-flex flex-wrap items-center gap-x-1.5">
      {pill}
      <Tooltip content={POSSIBLY_PURGED_TITLE} tabIndex={0} className="whitespace-nowrap text-xs text-ink-3" data-purge="possibly_purged">{purge}</Tooltip>
    </span>
  );
}
