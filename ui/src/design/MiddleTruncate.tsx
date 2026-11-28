import { splitTail } from "../model/format.ts";

/**
 * A name that, when it doesn't fit, gives way in the middle: the head ends in an ellipsis and the last `tail`
 * characters always show, so names that share a prefix stay apart. Put the full name in a Tooltip around it.
 */
export function MiddleTruncate({ text, tail = 10, className = "" }: { text: string; tail?: number; className?: string }) {
  const [head, end] = splitTail(text, tail);
  return (
    <span className={`flex min-w-0 whitespace-nowrap ${className}`}>
      <span className="sr-only">{text}</span>
      <span className="min-w-0 truncate" aria-hidden>{head}</span>
      {end && <span className="shrink-0" aria-hidden>{end}</span>}
    </span>
  );
}
