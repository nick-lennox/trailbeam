import { X } from "lucide-react";
import { UiLink } from "../design/nav.tsx";
import { AGE_CHOICES, ageLabel } from "../model/stuck-age.ts";

/*
  The Issues list's own filters beside its time range, kept in the page's link (QV-583, from the hosted page): how
  old an undeleted message must be to count as not finished (?age=, ?older_than_s= on the open-source API), and the
  queue the list is narrowed to (?queue=).
*/

/* One click for the usual ages. An age from the link that isn't one of them shows as its own choice. */
export function AgeChoice({ now, href }: { now: number; href: (s: number) => string }) {
  const choices: number[] = AGE_CHOICES.includes(now as (typeof AGE_CHOICES)[number]) ? [...AGE_CHOICES] : [...AGE_CHOICES, now].sort((a, b) => a - b);
  return (
    <nav aria-label="Not finished after" className="flex items-center gap-2 text-xs text-ink-3">
      <span>First seen over</span>
      <span className="flex h-10 items-center rounded-lg border border-line-strong bg-surface p-1">
        {choices.map((s) => (
          <UiLink key={s} href={href(s)} aria-current={s === now ? "true" : undefined}
            className={`rounded-md px-2 py-1 ${s === now ? "bg-sunken font-medium text-ink" : "text-ink-2 hover:text-ink"}`}>{ageLabel(s)}</UiLink>
        ))}
      </span>
      <span>ago</span>
    </nav>
  );
}

/* The queue the list is narrowed to, as a chip whose link shows every queue again. */
export function QueueChip({ queue, href }: { queue: string; href: string }) {
  return (
    <UiLink href={href} aria-label={`Queue: ${queue}. Show every queue`}
      className="inline-flex h-10 items-center gap-1.5 rounded-lg border border-line-strong bg-surface px-3 text-xs text-ink hover:border-ink-3">
      Queue: <span className="font-mono">{queue}</span> <X size={12} aria-hidden className="text-ink-3" />
    </UiLink>
  );
}
