/*
  The Body tab: one message's kept body, or the reason it has none (contract/body-view.md).

  Designed once for both products, from one API shape (./types.ts, Trailbeam's GET
  /api/messages/{id}/body). Each product passes only what differs between them:
  - access: who can read this body. Self-hosted Trailbeam has no logins, so it says anyone who can
    reach the server can; the hosted app says owners and admins.
  - hints: where that product's own body setting is, under a reason (TRAILBEAM_STORE_BODIES, or Settings).

  No client JS.
*/
import type { ReactNode } from "react";
import { NO_BODY_TEXT, noBodyNotes } from "./rules.ts";
import type { BodyReason, MessageBody } from "./types.ts";

export type BodyPanelProps = {
  view: MessageBody;
  access: ReactNode;
  hints?: Partial<Record<BodyReason, ReactNode>>;
};

const pretty = (b: string, format: string | null) => {
  if (format === "text") return b;
  try { return JSON.stringify(JSON.parse(b), null, 2); } catch { return b; }
};
const bytes = (n: number) => `${n.toLocaleString("en-US")} ${n === 1 ? "byte" : "bytes"}`;

export function BodyPanel({ view, access, hints = {} }: BodyPanelProps) {
  if (view.kind === "none") {
    const hint = hints[view.reason];
    return (
      <section className="px-5 py-5 md:px-7" data-body-reason={view.reason}>
        <h2 className="text-[1.125rem] text-sky-ink">No body for this message</h2>
        <p className="mt-2 max-w-[72ch] text-[15px] leading-[1.55] text-sky-ink-2">{NO_BODY_TEXT[view.reason]}</p>
        {noBodyNotes(view).map((n) => <p key={n} className="mt-1 max-w-[72ch] text-[15px] leading-[1.55] text-sky-ink-3">{n}</p>)}
        {hint && <p className="mt-3 max-w-[72ch] text-sm leading-[1.5] text-sky-ink-2">{hint}</p>}
        {view.size !== null && <p className="mt-3 font-mono text-caption text-sky-ink-3">Body size: {bytes(view.size)}, from the span.</p>}
        <p className="mt-3 max-w-[72ch] text-sm text-sky-ink-3">{access}</p>
      </section>
    );
  }
  const b = view.body;
  const facts = [
    b.from_kind === "send" ? `From the send by ${b.service ?? "the producer"}` : `From a receive by ${b.service ?? "a consumer"}: the producer's copy was not captured`,
    b.format === "text" ? "Text" : "JSON",
    view.size !== null ? `${bytes(view.size)} before redaction` : null,
    b.redacted_count !== null ? `${b.redacted_count} ${b.redacted_count === 1 ? "value" : "values"} redacted` : null,
    b.envelope ? `${b.envelope === "sns" ? "SNS" : b.envelope === "eventbridge" ? "EventBridge" : b.envelope} envelope` : null,
    view.tenant ? `tenant ${view.tenant}${view.tenant_source === "body_path" ? " (from the body)" : view.tenant_source === "message_attribute" ? " (from a message attribute)" : ""}` : null,
  ].filter(Boolean);
  return (
    <section data-body-from={b.from_kind}>
      <div className="flex flex-wrap items-baseline justify-between gap-x-4 gap-y-1 px-5 pt-5 pb-3 md:px-7">
        <h2 className="text-[1.125rem] text-sky-ink">Body</h2>
        <span className="text-sm text-sky-ink-3">{access}</span>
        <p className="w-full font-mono text-caption text-sky-ink-3">{facts.join(" · ")}</p>
      </div>
      <pre className="max-h-[32rem] overflow-auto border-y border-sky-line bg-sky p-4 font-mono text-caption leading-5 whitespace-pre-wrap break-words text-sky-ink-2 md:px-7">
        {pretty(b.body, b.format)}
      </pre>
      <p className="px-5 py-3 text-sm text-sky-ink-3 md:px-7">
        As stored, after the package redacted it. Redaction removes what its keys and patterns describe, and nothing else.
      </p>
    </section>
  );
}
