/*
  The Overview's derived parts the open-source shell still uses (shell/overview.tsx, around the shared
  OverviewFlows): pure functions over a flow of GET /api/flows (the hosted app's Flow, field for field). The rest
  went with the legacy Overview table (QV-710).
*/
import type { Flow } from "../queue/selfhosted.ts";

/* ── deletes for Lambda consumers (QV-392 design §3.4) ────────────────── */

export const LAMBDA_DELETES_HINT = "Lambda deletes a batch's messages itself after the function returns, and no span records it. The queue page counts Lambda's successes as handled.";

/** "all": every consumer is a Lambda event source mapping, so no span counts the deletes. "mixed": some are. */
export function lambdaDeletes(f: Flow): "all" | "mixed" | "none" {
  const cs = f.consumers.filter((c) => c.n > 0);
  if (!cs.length) return "none";
  const n = cs.filter((c) => c.lambda).length;
  return n === 0 ? "none" : n === cs.length ? "all" : "mixed";
}

/* ── figures ──────────────────────────────────────────────────────────── */

export function fmtBytes(n: number): string {
  if (n < 1024) return `${Math.round(n)} B`;
  if (n < 1024 ** 2) return `${(n / 1024).toFixed(1)} KB`;
  if (n < 1024 ** 3) return `${(n / 1024 ** 2).toFixed(1)} MB`;
  return `${(n / 1024 ** 3).toFixed(2)} GB`;
}
