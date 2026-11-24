/*
  Needs attention, shared by self-hosted Trailbeam (/stuck) and the hosted app: the stuck messages grouped by
  cause (one row per cause, like Sentry's issues), the messages with their cause, and the dead-letter queues
  holding them. The page around them (filters, counts, the duplicates) is the host's. The hosted app adds
  alerts, a copy-link button and redrive through the slots; redrive needs its AWS redrive role, so it stays there.
  Role tokens (theme.css), so the views take the host page's theme. Links come from href() and Link.
*/
import type { ComponentType, ReactNode } from "react";
import { fmtAge, shortId, stateText, type MessageSummary } from "../messages/Messages.tsx";
import { dlqSplit, OUTCOME_LABELS, tenantsText, trendOf, TREND_BARS, TREND_MS, versionsText, whenFirst, whenLast } from "./text.ts";
import type { CauseGroup, CauseMessage, CauseOutcome, DlqGroup } from "./types.ts";

type LinkProps = { href: string; className?: string; children: ReactNode; "aria-current"?: "true" };
const A = ({ children, ...p }: LinkProps) => <a {...p}>{children}</a>;

const plural = (n: number, one: string, many = `${one}s`) => `${n.toLocaleString("en-US")} ${n === 1 ? one : many}`;

const OUTCOME_CLS: Record<CauseOutcome, string> = { dead_lettered: "text-critical-text", stuck: "text-warning-text", filtered: "text-ink-2" };

/** A cause's worst outcome as a word in its colour; never colour alone. */
export const OutcomeTag = ({ outcome }: { outcome: CauseOutcome }) => (
  <span className={`shrink-0 text-xs font-medium ${OUTCOME_CLS[outcome]}`} data-outcome={outcome}>{OUTCOME_LABELS[outcome]}</span>
);

/** A cause's messages over the past 24 hours, in TREND_BARS bars. One series, so no legend. Hidden below `md`. */
export function TrendBars({ data }: { data: number[] }) {
  const max = Math.max(...data, 1);
  const hours = Math.round((TREND_MS / 3_600_000 / data.length) * 10) / 10;
  return (
    <div className="hidden h-8 items-end gap-[2px] md:flex" role="img" aria-label={`Messages over the past 24 hours, in ${data.length} parts of ${hours} h, oldest first: ${data.join(", ")}`}>
      {data.map((v, i) => (
        <span key={i} title={`${v}`} className="w-2.5 rounded-t-[3px] bg-series-1" style={{ height: `${Math.max(6, (v / max) * 100)}%`, opacity: v ? 1 : 0.18 }} />
      ))}
    </div>
  );
}

export type CauseListProps = {
  groups: CauseGroup[];
  nowMs: number;
  /** the `id` of the cause the page is narrowed to */
  picked?: string | null;
  /** where a cause's row links to: the page narrowed to it */
  href: (g: CauseGroup) => string;
  /** a message's timeline, for "Open the newest message" */
  messageHref: (messageId: string) => string;
  /** a link component, e.g. next/link; plain <a> by default */
  Link?: ComponentType<LinkProps>;
  /** the host's outcome pill; OutcomeTag by default */
  outcome?: (o: CauseOutcome) => ReactNode;
  /** beside a row, outside its link (no button inside a link): the hosted app's alert and copy-link buttons */
  actions?: (g: CauseGroup) => ReactNode;
  /** a row with a red edge: an alert rule on the cause is firing, or the host's own reason (the hosted app's regressed causes) */
  firing?: (g: CauseGroup) => boolean;
  /** after the label and basis: the hosted app's triage tag (assignee, resolved, regressed) */
  tag?: (g: CauseGroup) => ReactNode;
  /** at the end of the facts line: e.g. the hosted app's "2 notes" */
  note?: (g: CauseGroup) => ReactNode;
  /** a quieter row, its label in ink-2 and no trend: the hosted app's resolved and ignored causes */
  quiet?: (g: CauseGroup) => boolean;
};

/** One row per cause, in the API's order (worst outcome first, then the most messages). */
export function CauseList({ groups, ...rest }: CauseListProps) {
  return (
    <ul className="divide-y divide-line" data-causes>
      {groups.map((g) => <CauseRow key={g.id} g={g} {...rest} />)}
    </ul>
  );
}

/** One cause: what went wrong, where, for whom, when, how many, and its trend. Opens to say why an inferred one is
 *  thought so, what earlier deliveries did and the newest raw error text. */
function CauseRow({ g, nowMs, picked, href, messageHref, Link = A, outcome, actions, firing, tag, note, quiet }: Omit<CauseListProps, "groups"> & { g: CauseGroup }) {
  const on = picked === g.id;
  const split = dlqSplit(g);
  const trend = trendOf(g.times, nowMs - TREND_MS, nowMs, TREND_BARS);
  const extra = actions?.(g);
  const hush = quiet?.(g) ?? false;
  const noted = note?.(g);
  return (
    <li className={`${on ? "bg-canvas" : ""} ${firing?.(g) ? "border-l-2 border-l-critical" : ""}`} data-cause={g.id}>
      <div className="flex items-center hover:bg-canvas">
        <Link href={href(g)} aria-current={on ? "true" : undefined} className="grid min-w-0 flex-1 grid-cols-[1fr_auto] items-center gap-5 py-4 pl-5 pr-5 md:grid-cols-[1fr_120px_80px_auto]">
          <div className="min-w-0">
            <div className="flex items-center gap-2">
              {outcome ? outcome(g.outcome) : <OutcomeTag outcome={g.outcome} />}
              <span className={`truncate font-medium ${hush ? "text-ink-2" : "text-ink"}`} title={g.label}>{g.label}</span>
              <span className="shrink-0 rounded border border-line px-1 text-micro text-ink-3" title="What this cause rests on">{g.basis}</span>
              {tag?.(g)}
            </div>
            <div className="mt-1 text-xs text-ink-3">
              <span className="font-mono">{g.queue}</span>{g.tenants.length > 0 && <>, {tenantsText(g.tenants)}</>}. First seen {whenFirst(g.firstMs, nowMs)}, last {whenLast(g.lastMs, nowMs)}.{split && <> {split}.</>}
              {g.versions.length > 0 && <> <span title={g.versions.map((v) => `${v.version}: ${plural(v.count, "message")}, first ${whenFirst(v.firstMs, nowMs)}, last ${whenLast(v.lastMs, nowMs)}`).join("\n")}>{versionsText(g.versions).replace(/^s/, "S")}.</span></>}
              {noted && <> {noted}</>}
            </div>
          </div>
          {hush ? <span className="hidden md:block" /> : <TrendBars data={trend} />}
          <div className="hidden text-right md:block">
            <div className="font-mono text-lg font-medium tabular-nums text-ink">{g.count.toLocaleString("en-US")}</div>
            <div className="text-xs text-ink-3">{g.count === 1 ? "message" : "messages"}</div>
          </div>
          <span aria-hidden className="text-ink-3">&rarr;</span>
        </Link>
        {extra && <div className="flex shrink-0 items-center pr-2">{extra}</div>}
      </div>
      {(g.why || g.earlier.length > 0 || g.sampleError) && (
        <details className="px-5 pb-3 text-xs text-ink-2">
          <summary className="cursor-pointer text-ink-3 hover:text-ink">More about this cause</summary>
          <div className="mt-2 grid max-w-[72ch] gap-1.5 leading-relaxed">
            {g.why && <p>Why we think so: {g.why}</p>}
            {g.earlier.length > 0 && <p>Earlier deliveries: {g.earlier.map((e) => `${e.what} ×${e.n}`).join(", ")}.</p>}
            {g.sampleError && <p>Newest error text, as stored: <code className="font-mono text-[12px] text-ink [overflow-wrap:anywhere]">{g.sampleError}</code></p>}
            <p><Link href={messageHref(g.sampleMid)} className="font-medium text-brand hover:underline">Open the newest message</Link></p>
          </div>
        </details>
      )}
    </li>
  );
}

/** A stuck message's age, with "SQS clock" when it was measured from SQS's SentTimestamp. */
const Age = ({ m }: { m: MessageSummary }) => (
  <span title={m.age_across_clocks ? "Measured from SQS's SentTimestamp, so clock skew between SQS and Trailbeam is in it" : undefined}>
    {fmtAge(m.age_s)}{m.age_across_clocks && <span className="text-ink-3"> SQS clock</span>}
  </span>
);

export type CauseMessagesTableProps = {
  messages: CauseMessage[];
  /** the groups, for each message's cause label */
  groups: CauseGroup[];
  /** the `id` of the cause the page is narrowed to: its column says "Above" */
  picked?: string | null;
  /** a message's timeline */
  href: (messageId: string) => string;
  /** the page narrowed to a cause */
  causeHref: (g: CauseGroup) => string;
  Link?: ComponentType<LinkProps>;
  /** the host's state pill; stateText by default */
  state?: (m: CauseMessage) => ReactNode;
};

/** The stuck messages, in the order given (the API's: oldest first), each with its cause. Below `md`, two lines per row. */
export function CauseMessagesTable({ messages, groups, picked, href, causeHref, Link = A, state }: CauseMessagesTableProps) {
  const byId = new Map(groups.map((g) => [g.id, g]));
  const cause = (m: CauseMessage) => {
    const g = byId.get(m.cause);
    if (!g) return null;
    if (picked === g.id) return <span className="text-ink-3">Above</span>;
    return <Link href={causeHref(g)} className="hover:text-brand">{g.label}</Link>;
  };
  const queue = (m: CauseMessage) => m.current_queue || m.source_queue || "?";
  return (
    <>
      <ul className="md:hidden" data-phone-list>
        {messages.map((m) => (
          <li key={m.message_id} className="border-b border-line/60 py-3 pl-5 pr-5 leading-[1.45] last:border-0">
            <Link href={href(m.message_id)} className="block truncate font-mono text-caption text-ink hover:text-brand">{shortId(m.message_id)}</Link>
            <p className="truncate text-[15px] text-ink-2">{stateText(m)} · {queue(m)}{m.in_dead_letter_queue && " (DLQ)"} · {fmtAge(m.age_s)} old</p>
            <p className="truncate text-sm text-ink-3" title={byId.get(m.cause)?.label}>{byId.get(m.cause)?.label}</p>
          </li>
        ))}
      </ul>
      <table className="hidden w-full text-[15px] md:table">
        <thead>
          <tr className="border-b border-line text-left font-mono text-caption text-ink-3">
            <th className="py-2 pl-5 pr-3 font-medium">Message ID</th>
            <th className="px-3 py-2 font-medium">State</th>
            <th className="px-3 py-2 font-medium">Queue now</th>
            <th className="px-3 py-2 font-medium">Cause</th>
            <th className="px-3 py-2 text-right font-medium">Receives</th>
            <th className="py-2 pl-3 pr-5 text-right font-medium">Age</th>
          </tr>
        </thead>
        <tbody>
          {messages.map((m) => (
            <tr key={m.message_id} className="border-b border-line/60 align-top last:border-0">
              <td className="py-2.5 pl-5 pr-3"><Link href={href(m.message_id)} className="font-mono text-caption text-ink hover:text-brand">{shortId(m.message_id)}</Link></td>
              <td className="px-3 py-2.5 text-ink-2">{state ? state(m) : stateText(m)}</td>
              <td className="px-3 py-2.5 font-mono text-caption text-ink-2">{queue(m)}</td>
              <td className="max-w-[28rem] truncate px-3 py-2.5 text-ink-2" title={byId.get(m.cause)?.label}>{cause(m)}</td>
              <td className="px-3 py-2.5 text-right font-mono text-caption tabular-nums text-ink-2"><span className={m.deliveries > 1 ? "text-warning-text" : undefined}>{m.deliveries}</span></td>
              <td className="py-2.5 pl-3 pr-5 text-right font-mono text-caption tabular-nums text-ink-2"><Age m={m} /></td>
            </tr>
          ))}
        </tbody>
      </table>
    </>
  );
}

export type DeadLetterQueuesProps = {
  groups: DlqGroup[];
  nowMs: number;
  /** before a queue's name: the hosted app's redrive checkbox */
  lead?: (g: DlqGroup) => ReactNode;
  /** after a queue's facts: what can be done with it */
  action?: (g: DlqGroup) => ReactNode;
};

/** The dead-letter queues holding stuck messages, one row each: how many, from which queues, last seen when. */
export function DeadLetterQueues({ groups, nowMs, lead, action }: DeadLetterQueuesProps) {
  return (
    <ul className="divide-y divide-line" data-dlqs>
      {groups.map((g) => (
        <li key={g.key} className="flex flex-wrap items-center gap-x-3 gap-y-1 px-5 py-3 text-sm">
          {lead?.(g)}
          <span className="font-mono text-caption text-ink" title={[g.account, g.region].filter(Boolean).join(" ") || undefined}>{g.name}</span>
          <span className="text-ink-2">{plural(g.messageIds.length, "message")} from {g.sources.join(", ") || "?"}</span>
          {g.newest !== null && <span className="text-ink-3">last seen {whenLast(g.newest, nowMs)}</span>}
          {action && <span className="ml-auto">{action(g)}</span>}
        </li>
      ))}
    </ul>
  );
}
