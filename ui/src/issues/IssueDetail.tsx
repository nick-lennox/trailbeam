import type { ReactNode } from "react";
import { ChevronRight } from "lucide-react";
import { Card } from "../design/Card.tsx";
import { UiLink } from "../design/nav.tsx";
import { OutcomePill, StatePill } from "../design/pills.tsx";
import { CHECKS, ago, utc, type Issue } from "../model/issues.ts";
import { CopySummary } from "./CopySummary.tsx";
import { IssueFlow } from "./IssueFlow.tsx";
import { Apart, Figure, InferredTag, IssuePill, Lifespans, Panel, Steps, TimingRuler, type IssueStatus } from "./parts.tsx";
import { issueFigures, issueSummary, sharedBy, shortId } from "./rows.ts";

/*
  One issue, from the top down in the order someone on call asks: where on the flow it stops messages, how its
  messages stand and how far each receive got, what the messages have in common, what one of them went through, and
  the messages themselves. Moved from the hosted app's issue page (QV-583); the page around it (its top bar, and
  reading the issue) stays the host's.

  The host passes in, as slots, what is its own for now: the triage tag and the actions beside Copy summary (triage,
  Alert me, Its events), the triage panel, and the example message's timeline as its own message page draws it. Without
  an example, the steps most of the messages took are listed.
*/

const H2 = "font-serif text-[13px] font-semibold text-ink";
const NOTE = "mt-0.5 text-xs text-ink-3";
const TONE_CLS = { critical: "text-critical-text", serious: "text-serious-text" } as const;

export type IssueDetailProps = {
  issue: Issue;
  now: number;
  status?: IssueStatus;
  /* the Issues list, keeping the page's age and range; and the list narrowed to this issue's queue */
  listHref: string;
  queueHref: string;
  messageHref: (id: string) => string;
  /* beside the issue's pill (the triage tag), and beside Copy summary (triage, Alert me, Its events) */
  tags?: ReactNode;
  actions?: ReactNode;
  /* the example message drawn as its own page draws it, under "What one of them went through" */
  example?: ReactNode;
  /* the triage panel, above the messages */
  panel?: ReactNode;
  /* under "Messages" when the page lists fewer messages than the issue has */
  fewerNote?: string;
};

export function IssueDetail({ issue, now, status = "open", listHref, queueHref, messageHref, tags, actions, example, panel, fewerNote }: IssueDetailProps) {
  const exampleId = issue.shape?.example;
  const drawn = !!(exampleId && example);
  const shared = sharedBy(issue, drawn);
  return (
    <div className="px-6 pt-5 pb-12">
      <nav aria-label="Breadcrumb" className="mb-3 flex flex-wrap items-center gap-1 text-sm text-ink-3">
        <UiLink href={listHref} className="hover:text-ink">Issues</UiLink>
        <ChevronRight size={14} aria-hidden />
        <UiLink href={queueHref} className="font-mono text-caption hover:text-ink">{issue.queue}</UiLink>
      </nav>
      <div className="flex flex-wrap items-start gap-4">
        <div className="min-w-0">
          <h1 className="font-serif text-title-sm text-ink">{issue.title}</h1>
          <div className="mt-2 flex flex-wrap items-center gap-x-3 gap-y-1.5 text-sm text-ink-3">
            <IssuePill issue={issue} status={status} />
            {tags}
            {issue.inferred && <InferredTag />}
            <span>First seen {ago(issue.firstSeen, now)}</span>
            <span>Last seen {ago(issue.lastSeen, now)}</span>
            {issue.tenants && <span>Tenants: {issue.tenants}</span>}
          </div>
        </div>
        <div className="ml-auto flex items-center gap-2">
          <CopySummary lines={issueSummary(issue)} />
          {actions}
        </div>
      </div>
      <p className="mt-3 max-w-[78ch] text-sm leading-relaxed text-ink-2">{issue.evidence}</p>

      <div className="mt-5 grid gap-3">
        {/* the figures someone on call reads first */}
        <dl className="grid grid-cols-2 gap-px overflow-hidden rounded-card border border-line bg-line shadow-card md:grid-cols-3 xl:grid-cols-6">
          {issueFigures(issue, now).map((f) => (
            <div key={f.label} className="bg-surface px-4 py-3">
              <dt className="text-xs text-ink-3">{f.label}</dt>
              <dd className={`mt-1 text-[17px] ${f.tone ? TONE_CLS[f.tone] : "text-ink"}`}>{f.value === null ? <span className="text-sm text-ink-3">No data</span> : <Figure>{f.value}</Figure>}</dd>
              {f.note && <dd className="mt-0.5 truncate text-xs text-ink-3">{f.note}</dd>}
            </div>
          ))}
        </dl>

        {/* where on the flow */}
        <IssueFlow issue={issue} />

        {/* when, message by message, and how far one receive gets */}
        <div className="grid items-start gap-3 xl:grid-cols-[minmax(0,1.7fr)_minmax(0,1fr)]">
          <Panel title="Every message, from first seen to last seen">
            <div className="px-3 pt-1 pb-3">
              {issue.lifespans.length ? <Lifespans spans={issue.lifespans} now={now} /> : <p className="text-[13px] text-ink-2">No data. No message in this issue has a first and last seen time.</p>}
            </div>
          </Panel>
          <div className="grid content-start gap-3">
            <Panel title="One receive against the visibility timeout">
              <div className="px-3 pt-1 pb-3">
                {issue.timing ? <TimingRuler t={issue.timing} /> : (
                  <p className="text-[13px] leading-relaxed text-ink-2">
                    No data. {issue.inferred ? "This cause is inferred, and nothing recorded the moment it happened." : "No span recorded the moment a receive failed."}
                    {issue.visibilityTimeoutS !== null && ` The queue's visibility timeout is ${issue.visibilityTimeoutS} s.`}
                  </p>
                )}
              </div>
            </Panel>
            <Panel title="What to check">
              <ul>
                {CHECKS[issue.kind].map((c, i) => <li key={i} className="border-t border-line px-3 py-2 text-[13px] leading-relaxed text-ink-2">{c}</li>)}
              </ul>
            </Panel>
          </div>
        </div>

        {/* what sets the messages apart */}
        {issue.breakdowns.length > 0 && (
          <section aria-label="What sets these messages apart">
            <Apart breakdowns={issue.breakdowns} total={issue.count} />
          </section>
        )}

        {/* what one of them went through */}
        {issue.shape && (issue.shape.steps.length > 0 || drawn) && (
          <section aria-labelledby="shape-h" className="mt-3">
            <div className="flex flex-wrap items-baseline gap-x-3">
              <h2 id="shape-h" className={H2}>What one of them went through</h2>
              {drawn && <UiLink href={messageHref(exampleId!)} className="font-mono text-caption text-brand hover:underline">{shortId(exampleId!)}</UiLink>}
            </div>
            <p className={`${NOTE} mb-2.5`}>{shared}</p>
            {drawn ? example : <Card className="overflow-hidden"><Steps steps={issue.shape.steps} /></Card>}
          </section>
        )}

        {panel && <div className="mt-3">{panel}</div>}

        {/* the messages */}
        <section aria-labelledby="msgs-h" className="mt-3">
          <h2 id="msgs-h" className={H2}>Messages</h2>
          <p className={`${NOTE} mb-2.5`}>
            {issue.messages.length < issue.count
              ? fewerNote ?? `${issue.messages.length} of ${issue.count}, the latest first. Open one to see its timeline.`
              : "Latest first. Open one to see its timeline."}
          </p>
          {issue.messages.length > 0 && <IssueMessages issue={issue} messageHref={messageHref} />}
        </section>
      </div>
    </div>
  );
}

/* The issue's messages, latest first, each a link to its own page. */
export function IssueMessages({ issue, messageHref }: { issue: Pick<Issue, "messages">; messageHref: (id: string) => string }) {
  return (
    <Card className="overflow-x-auto">
      <table className="w-full min-w-[52rem] text-sm">
        <thead>
          <tr className="border-b border-line text-left text-xs text-ink-3">
            <th className="rounded-tl-xl px-5 py-1.5 font-medium">Message ID</th>
            <th className="px-3 py-1.5 font-medium">State</th>
            <th className="px-3 py-1.5 font-medium">Queue now</th>
            <th className="px-3 py-1.5 text-right font-medium">Receives</th>
            <th className="px-3 py-1.5 font-medium">First seen (UTC)</th>
            <th className="rounded-tr-xl px-5 py-1.5 font-medium">Last seen (UTC)</th>
          </tr>
        </thead>
        <tbody>
          {issue.messages.map((m) => (
            <tr key={m.id} className="group border-b border-line last:border-0 hover:bg-sunken/60">
              <td className="px-5 py-2"><UiLink href={messageHref(m.id)} className="font-mono text-[12.5px] text-ink group-hover:text-brand">{shortId(m.id)}</UiLink></td>
              <td className="px-3 py-2">{m.real ? <StatePill m={m.real} /> : m.outcome ? <OutcomePill outcome={m.outcome} /> : null}</td>
              <td className="px-3 py-2 font-mono text-[12.5px] text-ink-2">{m.queue}</td>
              <td className="px-3 py-2 text-right font-mono tabular-nums"><span className={m.receives > 1 ? "font-medium text-warning-text" : "text-ink-2"}>{m.receives}</span></td>
              <td className="px-3 py-2 font-mono text-caption text-ink-3">{utc(m.firstSeen)}</td>
              <td className="px-5 py-2 font-mono text-caption text-ink-3">{utc(m.lastSeen)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </Card>
  );
}

/*
  An issue page with no issue to draw. "none": no unfinished message older than the page's age has the cause now.
  "finished": every message with the cause has finished, and the host keeps its triage and notes (the panel).
*/
export function IssueGone({ kind, listHref, age, panel }: { kind: "none" | "finished"; listHref: string; age: string; panel?: ReactNode }) {
  return (
    <div className="grid gap-4 px-6 pt-5 pb-12">
      <nav aria-label="Breadcrumb" className="flex flex-wrap items-center gap-1 text-sm text-ink-3"><UiLink href={listHref} className="hover:text-ink">Issues</UiLink></nav>
      {kind === "none" ? (
        <p className="max-w-[78ch] text-sm text-ink-2">
          No unfinished message older than {age} has this cause now. Its messages may have finished, for example after a redrive.{" "}
          <UiLink href={listHref} className="font-medium text-brand hover:underline">See every open issue</UiLink>
        </p>
      ) : (
        <p className="max-w-[78ch] text-sm text-ink-2">Every message with this cause has finished, so there is nothing to draw. Its triage and notes are kept below.</p>
      )}
      {panel}
    </div>
  );
}
