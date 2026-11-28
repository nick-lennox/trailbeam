"use client";

/*
  The Redrive views (QV-590), shared by both products: "Redrive whole DLQ" on a message and on the dead-letter
  queues of Stuck (Issues in the hosted app), Cancel on a running redrive, and the redrive log. Moved from the hosted
  app's components/app/Redrive.tsx and RedriveLog.tsx; what changes anything is the host's (RedriveActions), and
  after a change the views ask the host to read the page again (`onChanged`, the Nav's refresh by default).

  A redrive moves every message in a dead-letter queue back to the queue it came from, with SQS's message move task.
  The button is off, with the reason and a link to the docs, until the host's gate opens. Pressing it reads what
  would move, asks to confirm, then starts the move; the host checks the gate again and logs each request.
*/
import { useState, useTransition, type ReactNode } from "react";
import { RotateCw } from "lucide-react";
import { Button } from "../design/Button.tsx";
import { StateMark, type StateTone } from "../design/Settings.tsx";
import { Tooltip } from "../design/Tooltip.tsx";
import { useNav } from "../design/nav.tsx";
import { DeadLetterQueues } from "../causes/NeedsAttention.tsx";
import type { DlqGroup } from "../causes/types.ts";
import { ago } from "../model/issues.ts";
import { freshWarning, moveWords, type MoveState } from "./words.ts";
import type { RedrivePreview, RedriveActions, RedriveEntry, RedriveTarget } from "./types.ts";

type Step =
  | { at: "idle" }
  | { at: "confirm"; previews: { t: RedriveTarget; preview: RedrivePreview | null; error: string | null; fresh: string | null }[] }
  | { at: "done"; results: { t: RedriveTarget; ok: boolean; text: string }[] };

const plural = (n: number, one: string, many = `${one}s`) => `${n} ${n === 1 ? one : many}`;
const keyOf = (t: RedriveTarget) => `${t.dlq.account ?? ""}/${t.dlq.region ?? ""}/${t.dlq.name}`;

/** The account and region of a dead-letter queue, values in mono. */
export function DlqWhere({ account, region }: { account: string | null; region: string | null }) {
  if (!account && !region) return null;
  return (
    <span className="text-caption text-ink-3">
      {account && <span className="font-mono">{account}</span>}
      {account && region && " in "}
      {region && <span className="font-mono">{region}</span>}
    </span>
  );
}

export type RedriveProps = {
  targets: RedriveTarget[];
  label: string;
  actions: RedriveActions;
  /** the docs page on redrive, linked under a button that is off */
  docsHref?: string;
  /** where the person sees how the move went, as a sentence: "The Redrives log on Issues says when the queue is empty." */
  logHint?: string;
  /** after a redrive started: read the page again (the Nav's refresh when not given) */
  onChanged?: () => void;
  className?: string;
};

export function Redrive({ targets, label, actions, docsHref, logHint = "The redrive log says when the queue is empty.", onChanged, className = "" }: RedriveProps) {
  const nav = useNav();
  const changed = onChanged ?? nav.refresh;
  const [step, setStep] = useState<Step>({ at: "idle" });
  const [busy, start] = useTransition();
  const on = targets.filter((t) => t.gate.ok);
  const off = targets.find((t) => !t.gate.ok)?.gate as { ok: false; reason: string } | undefined;

  const ask = () => start(async () => {
    const previews = await Promise.all(on.map(async (t) => {
      const r = await actions.preview(t.dlq);
      const fresh = r.ok && !r.preview.running ? freshWarning(t.dlq.name, t.newest, Date.now()) : null;
      return r.ok ? { t, preview: r.preview, error: null, fresh } : { t, preview: null, error: r.error, fresh: null };
    }));
    setStep({ at: "confirm", previews });
  });

  const go = () => start(async () => {
    if (step.at !== "confirm") return;
    const results = [];
    for (const p of step.previews.filter((x) => x.preview && !x.preview.running)) {
      const r = await actions.start(p.t.dlq, p.t.messageIds);
      results.push({ t: p.t, ok: r.ok, text: r.ok ? `Started moving every message in ${p.t.dlq.name} back. They arrive with new message IDs. ${logHint}` : r.error });
    }
    setStep({ at: "done", results });
    changed();
  });

  const movable = step.at === "confirm" ? step.previews.filter((x) => x.preview && !x.preview.running) : [];
  const total = movable.reduce((n, x) => n + (x.preview?.visible ?? 0), 0);

  return (
    <div className={`grid justify-items-start gap-2 ${className}`}>
      <Button size="sm" disabled={!on.length || busy || step.at === "confirm"} onClick={ask}
        aria-describedby={!on.length && off ? "redrive-off" : undefined} className="disabled:cursor-not-allowed disabled:opacity-55">
        <RotateCw size={14} aria-hidden className={busy && step.at === "idle" ? "animate-spin" : ""} /> {label}
      </Button>
      {on.length > 0 && step.at === "idle" && (
        <p className="max-w-[46ch] text-xs leading-relaxed text-ink-3">Moves every message in the dead-letter queue, not one. Moved messages get new message IDs.</p>
      )}
      {!on.length && (
        <p id="redrive-off" className="max-w-[46ch] text-xs leading-relaxed text-ink-3">
          {targets.length ? off?.reason : "Select a dead-letter queue to redrive."}
          {docsHref && <>{" "}<a href={docsHref} className="underline decoration-line-strong underline-offset-2 hover:text-ink">How redrive works</a></>}
        </p>
      )}

      {step.at === "confirm" && (
        <div role="dialog" aria-label="Confirm redrive" className="grid w-full max-w-md gap-3 rounded-lg border border-line bg-surface p-4 text-left text-sm shadow-card">
          <p className="font-medium text-ink">Move every message in {step.previews.length > 1 ? "these dead-letter queues" : "this dead-letter queue"} back to the queues they came from?</p>
          <ul className="grid gap-1.5 text-ink-2">
            {step.previews.map((p) => (
              <li key={keyOf(p.t)}>
                <span className="font-mono text-caption text-ink">{p.t.dlq.name}</span>{": "}
                {p.error ? <span className="text-critical-text">{p.error}</span>
                  : p.preview!.running ? <span className="text-warning-text">a redrive is already running ({p.preview!.running.moved ?? 0} moved so far), so it is left alone.</span>
                  : <>{p.preview!.visible === null ? "SQS gave no count" : plural(p.preview!.visible, "message")} now{p.preview!.inFlight ? `, and ${p.preview!.inFlight} in flight that SQS moves when they are visible again` : ""}.</>}
                {p.fresh && <span className="mt-0.5 block text-xs text-warning-text">{p.fresh}</span>}
              </li>
            ))}
          </ul>
          <p className="text-xs leading-relaxed text-ink-3">
            SQS moves the whole dead-letter queue, oldest first, not only the messages you picked. Each message moved gets a new message ID, so its timeline continues under that ID. SQS can report a move finished before it has moved everything, so the log says Done only once the queue is empty a minute later. This is recorded in the redrive log with your name.
          </p>
          <div className="flex gap-2">
            <Button size="sm" variant="primary" disabled={busy || !movable.length} onClick={go}>{busy ? "Starting" : total ? `Redrive ${plural(total, "message")}` : "Redrive"}</Button>
            <Button size="sm" variant="ghost" disabled={busy} onClick={() => setStep({ at: "idle" })}>Cancel</Button>
          </div>
        </div>
      )}

      {step.at === "done" && (
        <div role="status" className="grid max-w-md gap-1 text-left text-sm">
          {step.results.length === 0 && <p className="text-ink-2">Nothing was started.</p>}
          {step.results.map((r) => <p key={keyOf(r.t)} className={r.ok ? "text-good-text" : "text-critical-text"}>{r.text}</p>)}
          <button type="button" onClick={() => setStep({ at: "idle" })} className="justify-self-start text-xs text-ink-3 underline underline-offset-2 hover:text-ink">Close</button>
        </div>
      )}
    </div>
  );
}

export type RedriveGroup = DlqGroup & Pick<RedriveTarget, "gate">;

export type RedriveSelectProps = Omit<RedriveProps, "targets" | "label"> & { groups: RedriveGroup[]; nowMs: number };

/* The dead-letter queues that hold messages, as the shared list (../causes, DeadLetterQueues), with a box before each
   to pick the ones to redrive; "Redrive whole DLQ" redrives the ticked ones. nowMs: the page's, for "last seen". */
export function RedriveSelect({ groups, nowMs, ...rest }: RedriveSelectProps) {
  const [picked, setPicked] = useState<Set<string>>(new Set());
  const toggle = (k: string) => setPicked((s) => { const n = new Set(s); if (n.has(k)) n.delete(k); else n.add(k); return n; });
  const target = (g: RedriveGroup): RedriveTarget => ({ dlq: { name: g.name, account: g.account, region: g.region }, messageIds: g.messageIds, newest: g.newest, gate: g.gate });
  const chosen = groups.filter((g) => picked.has(g.key)).map(target);
  const byKey = new Map(groups.map((g) => [g.key, g]));
  return (
    <div className="grid gap-3">
      <DeadLetterQueues groups={groups} nowMs={nowMs}
        lead={(d) => { const g = byKey.get(d.key)!; return (
          <input type="checkbox" checked={picked.has(g.key)} onChange={() => toggle(g.key)} disabled={!g.gate.ok}
            aria-label={`Select ${g.name}`} className="size-4 shrink-0 rounded accent-[var(--color-brand)] disabled:opacity-40" />); }}
        action={(d) => { const g = byKey.get(d.key)!; return (
          <span className="grid justify-items-end gap-0.5 text-right">
            <DlqWhere account={g.account} region={g.region} />
            {!g.gate.ok && <span className="max-w-[48ch] text-xs text-ink-3">{g.gate.reason}</span>}
          </span>); }} />
      <div className="border-t border-line px-5 py-3">
        <Redrive {...rest} label={chosen.length > 1 ? `Redrive ${chosen.length} whole DLQs` : "Redrive whole DLQ"}
          targets={chosen.length ? chosen : groups.some((g) => g.gate.ok) ? [] : groups.slice(0, 1).map(target)} />
      </div>
    </div>
  );
}

/* Cancel on a running redrive in the log: asks SQS to cancel. SQS can keep moving after it; messages already moved stay where SQS put them. */
export function CancelRedrive({ entryId, actions, onChanged }: { entryId: string; actions: Pick<RedriveActions, "cancel">; onChanged?: () => void }) {
  const nav = useNav();
  const changed = onChanged ?? nav.refresh;
  const [sure, setSure] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [busy, start] = useTransition();
  const go = () => start(async () => {
    const r = await actions.cancel(entryId);
    setError(r.ok ? null : r.error);
    setSure(false);
    changed();
  });
  return (
    <span className="inline-flex flex-wrap items-center gap-2">
      {sure ? (
        <>
          <Button size="sm" variant="ghost" disabled={busy} onClick={() => setSure(false)}>Keep moving</Button>
          <Button size="sm" disabled={busy} className="text-critical-text" onClick={go}>Cancel redrive</Button>
        </>
      ) : <Button size="sm" variant="ghost" onClick={() => setSure(true)}>Cancel</Button>}
      {error && <span role="alert" className="text-xs text-critical-text">{error}</span>}
    </span>
  );
}

/*
  The redrive log: who asked to redrive which dead-letter queue, when, and how it went. "Done" only once the
  dead-letter queue was seen empty a minute after SQS said the move finished (words.ts, moveWords): SQS can say
  COMPLETED before it has moved everything.
*/
const TONE: Record<MoveState, StateTone> = { moving: "warning", checking: "warning", emptied: "good", left: "warning", unchecked: "idle", stopped: "idle", failed: "critical" };

/** Each entry's state word, tone and detail, as the log shows them. */
export function entryState(e: RedriveEntry, now: number): { tone: StateTone; word: string; detail: string | null } {
  const w = e.outcome === "started" ? moveWords(e, e.dlqName, now) : null;
  const [tone, word]: [StateTone, string] = w ? [TONE[w.state], w.word] : e.outcome === "refused" ? ["idle", "Not started"] : ["critical", "Could not start"];
  return { tone, word, detail: w ? w.detail ?? e.detail : e.detail };
}

export const cancellable = (e: RedriveEntry) => e.outcome === "started" && !!e.taskHandle && (!e.status || e.status === "RUNNING");

export type RedriveLogProps = { entries: RedriveEntry[]; now: number; canCancel: boolean; actions: Pick<RedriveActions, "cancel">; onChanged?: () => void; empty?: ReactNode };

export function RedriveLog({ entries, now, canCancel, actions, onChanged, empty = "No redrive has been asked for yet." }: RedriveLogProps) {
  if (!entries.length) return <p className="px-5 py-6 text-sm text-ink-2">{empty}</p>;
  return (
    <table className="w-full min-w-[48rem] text-sm">
      <thead>
        <tr className="border-b border-line bg-canvas text-left text-xs text-ink-3">
          <th className="rounded-tl-xl px-5 py-2 font-medium">Dead-letter queue</th>
          <th className="px-3 py-2 font-medium">Asked by</th>
          <th className="px-3 py-2 font-medium">When</th>
          <th className="rounded-tr-xl px-5 py-2 font-medium">Status</th>
        </tr>
      </thead>
      <tbody>
        {entries.map((e) => {
          const s = entryState(e, now);
          return (
            <tr key={e.id} className="border-b border-line align-top last:border-0">
              <td className="px-5 py-2.5"><span className="font-mono text-[12.5px] text-ink">{e.dlqName}</span><div><DlqWhere account={e.accountId} region={e.region} /></div></td>
              <td className="px-3 py-2.5 text-ink-2">{e.requestedByEmail ?? e.requestedBy}</td>
              <td className="px-3 py-2.5 text-ink-2">
                <Tooltip content={<span className="font-mono">{new Date(e.requestedAt).toISOString().replace("T", " ").slice(0, 19)} UTC</span>} tabIndex={0}>{ago(e.requestedAt, now)}</Tooltip>
              </td>
              <td className="px-5 py-2.5">
                <StateMark tone={s.tone}>{s.word}</StateMark>
                {s.detail && <div className="mt-1 max-w-[52ch] text-xs leading-relaxed text-ink-3">{s.detail}</div>}
                {canCancel && cancellable(e) && <div className="mt-1"><CancelRedrive entryId={e.id} actions={actions} onChanged={onChanged} /></div>}
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}
