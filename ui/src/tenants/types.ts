/* What GET /api/tenants returns (contract/tenants.md in the repository). The hosted app returns the same shape. */
export type TenantRow = {
  tenant: string;
  sent: number;
  /** sent / total_sent, 0 when there were no sends */
  share: number;
  /** p95 of time in queue over first deliveries, in ms, within 1% of the nearest-rank p95 (contract/tenants.md); null when there were none */
  p95_in_queue_ms: number | null;
  /** how many first deliveries the p95 is over */
  first_deliveries: number;
  /** distinct messages SQS was seen moving to a dead-letter queue */
  dead_lettered: number;
};

export type TenantStats = {
  from_ms: number;
  to_ms: number;
  total_sent: number;
  untagged_sent: number;
  /** more messages had spans in the range than the page reads (2,000) */
  capped: boolean;
  rows: TenantRow[];
};
