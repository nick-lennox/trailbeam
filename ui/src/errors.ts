import type { LambdaOutcomeEvent, Timeline } from "./timeline/types.ts";

/**
  One cause of a message's failed Lambda outcomes: the same exception class, the same error text and the
  same outcome. outcome is failed_exception (the handler threw), failed_reported (listed in batchItemFailures)
  or failed_invalid_response. first and last are the outcomes' end times; steps are their event IDs.
*/
export type ErrorCause = {
  errorType: string | null;
  message: string | null;
  outcome: Exclude<LambdaOutcomeEvent["outcome"], "success">;
  count: number;
  deliveries: number[];
  first: string;
  last: string;
  steps: string[];
};

/**
  A message's failed Lambda outcomes, grouped by exception class, error text and outcome (contract 15.5),
  most frequent first, then earliest first. The text is redacted in the package, so the same failure about
  two customers usually reads the same and is one cause. Read from the timeline alone, so self-hosted
  Trailbeam and the hosted app group the same way.
*/
export function errorCauses(t: Timeline): ErrorCause[] {
  const groups = new Map<string, ErrorCause>();
  for (const e of t.events) {
    if (e.type !== "lambda_outcome" || e.outcome === "success") continue;
    const errorType = e.error_type ?? null;
    const message = e.error_message ?? null;
    const key = JSON.stringify([errorType, message, e.outcome]);
    const g = groups.get(key) ?? { errorType, message, outcome: e.outcome, count: 0, deliveries: [], first: e.end, last: e.end, steps: [] };
    g.count++;
    g.steps.push(e.id);
    if (typeof e.delivery === "number" && !g.deliveries.includes(e.delivery)) g.deliveries.push(e.delivery);
    if (e.end < g.first) g.first = e.end;
    if (e.end > g.last) g.last = e.end;
    groups.set(key, g);
  }
  return [...groups.values()].sort((a, b) => b.count - a.count || a.first.localeCompare(b.first));
}
