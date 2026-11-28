/*
  When a cause's failures reached Trailbeam (QV-393 design §3.2, §8.3), for triage's "failures after the resolve"
  and "until N more messages". Pure and free of imports, so the browser-safe triage words can use it.

  Each unfinished message in a cause has one arrival: the `seq` of the span that decided its cause (the failed
  outcome, the failed delete, the receive of a delivery that timed out or wasn't deleted, the send of one never
  received). seq is the span's arrival on the server's clock (ms × 1000 + a counter: the hosted app's engine/ingest.ts nextSeq, the server's spans.seq),
  never the reporting host's.

  AWS-side evidence (a CloudTrail filter drop, a Lambda log timeout or out-of-memory) has no span. tb_aws_fact keeps
  no time of its own storing, so such a fact counts as arriving at the time AWS recorded it, in seq units (ms × 1000).
  That is AWS's clock, not a host's, and a fact can only be counted once a sync has stored it, so a timeout from
  before a resolve that a sync stores after it still doesn't count.
*/

/** One message of a cause: when what decided its cause arrived (seq units; NaN when unknown), its evidence time (ms), its consumer's version. */
export type Arrival = { mid: string; seq: number; ms: number; version: string | null };

/** How long before a resolve or ignore a span's own time may be and still count: room for hosts' clock drift (design §3.2). */
export const LATE_EXPORT_MS = 10 * 60_000;

/**
  The messages whose deciding span arrived after the watermark `sinceSeq`, oldest arrival first. A span whose own
  time is more than LATE_EXPORT_MS before `atMs` (the resolve or ignore) is a late export of an earlier failure and
  doesn't count. `except`: versions that don't count ("resolved in a new version": the versions seen at the resolve).
*/
export function newSince(arrivals: readonly Arrival[], sinceSeq: number, atMs: number, except?: ReadonlySet<string>): Arrival[] {
  const notBefore = atMs - LATE_EXPORT_MS;
  return arrivals
    .filter((a) => a.seq > sinceSeq && !(a.ms < notBefore) && !(except && a.version !== null && except.has(a.version)))
    .sort((a, b) => a.seq - b.seq);
}

/** Seq units (ms × 1000) as ms on the server's clock. */
export const seqMs = (seq: number) => Math.floor(seq / 1000);
