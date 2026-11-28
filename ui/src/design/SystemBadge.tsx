import { SYSTEM_NAME, SYSTEM_NOTE, SQS, type MessagingSystem } from "../systems.ts";
import { Tooltip } from "./Tooltip.tsx";

/*
  The messaging system a message, a queue or an event is on (QV-308): its name in a neutral badge, with a note on
  the badge itself (the design system's Tooltip, never `title`). Amazon SQS is the default, so SQS pages stay as they
  were: an SQS badge is drawn only when the host asks (`always`), where two systems sit side by side.
*/
export function SystemBadge({ system, always = false, className = "" }: { system: MessagingSystem; always?: boolean; className?: string }) {
  if (system === SQS && !always) return null;
  return (
    <Tooltip content={SYSTEM_NOTE[system]} tabIndex={0} data-system={system}
      className={`inline-flex shrink-0 items-center whitespace-nowrap rounded-full bg-sunken px-2 py-0.5 font-sans text-xs font-medium text-ink-2 ring-1 ring-inset ring-line ${className}`}>
      {SYSTEM_NAME[system]}
    </Tooltip>
  );
}
