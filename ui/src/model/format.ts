/*
  How the pages write durations, counts and sizes, what each platform a service runs on is called, and the
  Overview's ranges. Pure.
*/

/** 310ms, 4.2s, 2m 5s */
export function fmtMs(ms: number) {
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
  return `${Math.floor(ms / 60_000)}m ${Math.round((ms % 60_000) / 1000)}s`;
}

/** 4,210 */
export function fmtNum(n: number) {
  return n.toLocaleString("en-US");
}

/** A rate, always to one decimal, so rates side by side read alike: 6.3, 8.0, 1,204.5 */
export function fmtRate(n: number) {
  return (Math.round(n * 10) / 10).toLocaleString("en-US", { minimumFractionDigits: 1, maximumFractionDigits: 1 });
}

/**
 * A name split for truncating in the middle: everything but its last `tail` characters, and those. The head is the
 * part that gives way (MiddleTruncate), so "trailbeam-demo-order-worker" reads "trailbeam-de…order-worker" in a
 * narrow node where cutting the end would leave every service reading "trailbeam-d…". A name no longer than
 * twice the tail is not split.
 */
export function splitTail(s: string, tail = 10): [string, string] {
  const chars = Array.from(s);
  if (chars.length <= tail * 2) return [s, ""];
  return [chars.slice(0, -tail).join(""), chars.slice(-tail).join("")];
}

/** 512 B, 1.5 KB, 2.0 MB, 1.25 GB */
export function fmtBytes(n: number) {
  if (n < 1024) return `${Math.round(n)} B`;
  if (n < 1024 ** 2) return `${(n / 1024).toFixed(1)} KB`;
  if (n < 1024 ** 3) return `${(n / 1024 ** 2).toFixed(1)} MB`;
  return `${(n / 1024 ** 3).toFixed(2)} GB`;
}

/** What a service runs on, as the Overview's icons know it. */
export type Platform = "Lambda" | "EKS" | "ECS" | "S3" | "EC2" | "Celery";
export const PLATFORM_LABEL: Record<Platform, string> = {
  Lambda: "AWS Lambda function",
  EKS: "Amazon EKS pod",
  ECS: "Amazon ECS task",
  S3: "Amazon S3 event notification",
  EC2: "Amazon EC2 instance",
  Celery: "Celery worker",
};

/** The Overview's ranges. */
export const OVERVIEW_RANGES = [
  { id: "15m", label: "15 min", long: "the last 15 minutes", minutes: 15 },
  { id: "1h", label: "1 hour", long: "the last hour", minutes: 60 },
  { id: "24h", label: "24 hours", long: "the last 24 hours", minutes: 1440 },
] as const;
export type OverviewRangeId = (typeof OVERVIEW_RANGES)[number]["id"];
