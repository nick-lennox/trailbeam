/*
  A message's failed Lambda outcomes, grouped by cause (../errors.ts). Drawn at the foot of the timeline,
  in both products, whenever a handler failed, on the design system's role tokens. Each failed step also shows its own error text on the
  timeline; this section counts them.
*/
import type { ErrorCause } from "../errors.ts";

const OUTCOME: Record<string, string> = {
  failed_exception: "The handler threw",
  failed_reported: "Reported in batchItemFailures",
  failed_invalid_response: "Invalid batchItemFailures response",
};

const time = (iso: string) => iso.slice(11, 23);
const plural = (n: number, one: string, many = `${one}s`) => `${n} ${n === 1 ? one : many}`;

export function ErrorCauses({ causes }: { causes: ErrorCause[] }) {
  if (!causes.length) return null;
  const total = causes.reduce((a, c) => a + c.count, 0);
  return (
    <section aria-label="Handler errors, by cause" className="border-t border-line px-4 pt-5 pb-6 md:px-6">
      <h4 className="font-serif text-[15px] leading-[1.45] font-semibold text-ink">Handler errors, by cause</h4>
      <p className="mt-1 max-w-[44rem] text-[13px] leading-[1.45] text-ink-3">
        {plural(total, "failed outcome")} in {plural(causes.length, "cause")}. The packages record error text only for queues in their
        error queue list, and redact it before it leaves the function.
      </p>
      <ul className="mt-3 grid max-w-[48rem] gap-2">
        {causes.map((c) => (
          <li key={c.steps[0]} className="grid gap-1 rounded-card border border-line bg-surface px-4 py-3 shadow-card">
            <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
              <span className={`${c.errorType ? "font-mono text-caption" : "text-[13px]"} font-medium text-critical-text`}>{c.errorType ?? OUTCOME[c.outcome] ?? c.outcome}</span>
              <span className="text-xs text-ink-3">
                {c.count === 1 ? "Once" : <><span className="font-mono tabular-nums">{c.count}</span> times</>}
                {c.deliveries.length > 0 && <>, {c.deliveries.length === 1 ? "receive" : "receives"} <span className="font-mono tabular-nums">{c.deliveries.join(", ")}</span></>}
                {", "}{c.first === c.last ? <span className="font-mono tabular-nums">{time(c.first)}</span> : <><span className="font-mono tabular-nums">{time(c.first)}</span> to <span className="font-mono tabular-nums">{time(c.last)}</span></>} UTC
                {", "}{c.steps.length === 1 ? "step" : "steps"} <span className="font-mono tabular-nums">{c.steps.join(", ")}</span>
              </span>
            </div>
            {c.message
              ? <p className="font-mono text-caption break-words text-ink-2">{c.message}</p>
              : <p className="text-[13px] text-ink-3">{c.errorType ? "No error text was recorded for this failure." : OUTCOME[c.outcome] ?? ""}</p>}
          </li>
        ))}
      </ul>
    </section>
  );
}
