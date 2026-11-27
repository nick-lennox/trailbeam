/*
  How old an undeleted message must be before Issues lists it as not finished (QV-395). Moved here from the hosted
  app's lib/app/stuck-age.ts (QV-583), so both products' Issues pages offer the same ages and say them the same way. Open-source Trailbeam
  takes it per request (GET /api/stuck?older_than_s=, default --stuck-after, 300 s); the hosted page takes it in its link
  as ?age=, so a link to the page opens with the same age. With none, 5 minutes.

  ?age= is a whole number with an optional unit: 90 or 90s (seconds), 15m, 2h, 1d. 0 to 10⁹ seconds, as open source allows.
*/
export const STUCK_AFTER_S = 300;
const MAX_S = 1_000_000_000;

/** The ages the page offers as one click. Any other ?age= still works. */
export const AGE_CHOICES = [60, 300, 900, 3600, 21600, 86400] as const;

const UNIT_S: Record<string, number> = { s: 1, sec: 1, m: 60, min: 60, h: 3600, d: 86400 };

/** The age a link names: { s, bad: false }; with none the default; one that can't be read gives the default and bad: true. */
export function stuckAge(raw: string | null | undefined): { s: number; bad: boolean } {
  const text = raw?.trim().toLowerCase();
  if (!text) return { s: STUCK_AFTER_S, bad: false };
  const m = /^(\d{1,10})\s*(s|sec|m|min|h|d)?$/.exec(text);
  const s = m ? Number(m[1]) * UNIT_S[m[2] ?? "s"] : NaN;
  if (!Number.isSafeInteger(s) || s < 0 || s > MAX_S) return { s: STUCK_AFTER_S, bad: true };
  return { s, bad: false };
}

/** An age as ?age= writes it: the largest whole unit (5m, 1h, 1d, 90s). */
export function ageParam(s: number): string {
  if (s > 0 && s % 86400 === 0) return `${s / 86400}d`;
  if (s > 0 && s % 3600 === 0) return `${s / 3600}h`;
  if (s > 0 && s % 60 === 0) return `${s / 60}m`;
  return `${s}s`;
}

/** An age in words: "5 minutes", "1 hour", "90 seconds". */
export function ageText(s: number): string {
  const n = (v: number, one: string) => `${v} ${v === 1 ? one : `${one}s`}`;
  if (s > 0 && s % 86400 === 0) return n(s / 86400, "day");
  if (s > 0 && s % 3600 === 0) return n(s / 3600, "hour");
  if (s > 0 && s % 60 === 0) return n(s / 60, "minute");
  return n(s, "second");
}

/** A short label for the page's choices: "5 min", "1 h", "1 day". */
export function ageLabel(s: number): string {
  if (s > 0 && s % 86400 === 0) return s === 86400 ? "1 day" : `${s / 86400} days`;
  if (s > 0 && s % 3600 === 0) return `${s / 3600} h`;
  if (s > 0 && s % 60 === 0) return `${s / 60} min`;
  return `${s} s`;
}

/** The link with ?age= set to s, or without it for the default, keeping its other parameters. */
export function withAge(href: string, s: number): string {
  const [path, query = ""] = href.split("?");
  const p = new URLSearchParams(query);
  if (s === STUCK_AFTER_S) p.delete("age"); else p.set("age", ageParam(s));
  const out = p.toString();
  return out ? `${path}?${out}` : path;
}
