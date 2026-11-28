/*
  The message page (row 3 of the one-UI plan): one message's timeline, and its Body tab, in the same chrome in both
  products. The hosted app draws it from a server component; the open-source shell draws it from the JSON API.

  The page is drawn the way the hosted pages are: the design system's TopBar with the host's search, a breadcrumb
  (Messages, the queue, the message), the one-sentence answer as the serif heading, the state pill and what the page
  knows about the message, its notices, the tabs, and the timeline (or the body) in a card. Values are in the mono
  face, labels and words never are; hover notes are the design system's Tooltip.

  What only one product has today comes in through slots, each temporary until its feature issue makes it a shared
  view: `actions` (the hosted app's redrive), `cause` (the cause and its triage note, above the timeline). The Body
  tab is the shared BodyPanel; the host passes it in `body`, or leaves it out where the reader may not read bodies.

  It has no state of its own and no hooks, so a server component can draw it and pass functions; its links go
  through the host's Nav (UiLink) unless the host passes `Link`. Reached through @trailbeam/ui/design.
*/
import type { ComponentType, ReactNode } from "react";
import { ChevronRight, Lock } from "lucide-react";
import { TopBar, type Fresh } from "../design/TopBar.tsx";
import { UiLink, type LinkProps } from "../design/nav.tsx";
import { Tooltip } from "../design/Tooltip.tsx";
import { CopyText } from "../design/CopyText.tsx";
import { StatePill } from "../design/pills.tsx";
import { SystemBadge } from "../design/SystemBadge.tsx";
import { shownId, systemOf } from "../systems.ts";
import { Notices, TimelineView } from "../timeline/Timeline.tsx";
import { answerFor, consumersOf } from "../summary.ts";
import type { Notice, Timeline } from "../timeline/types.ts";
import type { MessageHrefs, MessageTab } from "./types.ts";

export type { MessageHrefs, MessageTab };

export type MessagePageProps = {
  t: Timeline;
  /** the headline; answerFor(t) by default */
  answer?: string;
  href: MessageHrefs;
  tab: MessageTab;
  /**
   * The Body tab's content (the shared BodyPanel, or a loading line), when this reader may read the body. Left out,
   * the page has no tabs, only the timeline.
   */
  body?: ReactNode;
  /** a note on the Body tab: who may read it ("Owners and admins only"), drawn as a lock with its Tooltip */
  bodyNote?: string;
  /** the message's tenant and FIFO message group, when the host knows them */
  tenant?: string | null;
  groupId?: string | null;
  /** notices the host adds before the timeline's own (the hosted app's "redriven" and purge notices) */
  notices?: Notice[];
  /** slot: actions on the message, at the end of the state row (the hosted app's redrive) */
  actions?: ReactNode;
  /** slot: above the timeline, on the Timeline tab (the cause and its triage note) */
  cause?: ReactNode;
  /** the host's message search, drawn in the top bar */
  search?: ReactNode;
  fresh?: Fresh;
  openSteps?: string[];
  /** under the timeline or the body: times are in UTC, the JSON link */
  footer?: ReactNode;
  Link?: ComponentType<LinkProps>;
};

const mono = "font-mono tabular-nums";
const shortId = (id: string) => (id.length > 14 ? `${id.slice(0, 8)}…` : id);

/** The breadcrumb and the heading, shared with the page's other states (loading, not found) through MessageFrame. */
function Crumbs({ href, queue, id, Link }: { href: MessageHrefs; queue?: string; id?: string; Link: ComponentType<LinkProps> }) {
  return (
    <nav aria-label="Breadcrumb" className="flex flex-wrap items-center gap-1 text-sm text-ink-3">
      <Link href={href.messages} className="hover:text-ink">Messages</Link>
      {queue && (
        <>
          <ChevronRight size={14} aria-hidden />
          <Link href={href.queue(queue)} className="font-mono text-caption hover:text-ink">{queue}</Link>
        </>
      )}
      {id && (
        <>
          <ChevronRight size={14} aria-hidden />
          <Tooltip content={id.length > 14 ? id : undefined} tabIndex={id.length > 14 ? 0 : undefined} className="font-mono text-caption text-ink-2">{shortId(id)}</Tooltip>
        </>
      )}
    </nav>
  );
}

/**
 * The message page's frame without a timeline: the top bar and the breadcrumb, and what the host says instead
 * (loading, no such message, the API failed). The open-source shell draws its states in it.
 */
export function MessageFrame({ href, id, search, children, Link = UiLink }: { href: MessageHrefs; id?: string; search?: ReactNode; children: ReactNode; Link?: ComponentType<LinkProps> }) {
  return (
    <>
      <TopBar search={search} />
      <div className="px-8 pt-6 pb-12">
        <Crumbs href={href} id={id} Link={Link} />
        <div className="mt-4">{children}</div>
      </div>
    </>
  );
}

export function MessagePage(p: MessagePageProps) {
  const { t, href, tab, Link = UiLink } = p;
  const consumers = consumersOf(t);
  const tabs = p.body !== undefined;
  const showBody = tabs && tab === "body";
  const notices = [...(p.notices ?? []), ...t.notices];
  // a RabbitMQ message's key carries its system (~rabbitmq~<ID>); the page shows the ID its application set (QV-308)
  const id = shownId(t.message_id);
  return (
    <>
      <TopBar search={p.search} fresh={p.fresh} />
      <div className="px-8 pt-6 pb-12">
        <div className="flex flex-wrap items-center gap-x-4 gap-y-2">
          <Crumbs href={href} queue={t.source_queue} id={id} Link={Link} />
          {/* the message page has no time range: its link is the message (and the tab open), the same for anyone who can see it */}
          <span className="ml-auto flex items-center gap-4 text-xs">
            <CopyText value={id} label="Copy message ID" />
            {/* a string, not a function: a server component draws this page, and CopyText is a client module */}
            <CopyText path={href.tab(showBody ? "body" : "timeline")} icon="link" label="Copy link" />
          </span>
        </div>

        <h1 className="mt-3 max-w-[48rem] font-serif text-title-sm text-ink">{p.answer ?? answerFor(t)}</h1>

        <div className="mt-3 flex flex-wrap items-center gap-x-4 gap-y-2 text-sm text-ink-2" data-message-facts="">
          <StatePill m={t} />
          <SystemBadge system={systemOf(t.message_id)} />
          <span className="text-ink-3">Message <span className={`${mono} text-caption break-all text-ink-2`}>{id}</span></span>
          {consumers.length > 0 && <span>{systemOf(t.message_id) === "rabbitmq" ? "Delivered to" : "Received by"} <span className={`${mono} text-caption text-ink`}>{consumers.join(", ")}</span></span>}
          {p.tenant && (
            p.href.tenant
              ? <Link href={p.href.tenant(p.tenant)} className="rounded-full border border-line bg-surface px-2.5 py-0.5 hover:border-line-strong">Tenant <span className={`${mono} text-caption text-ink`}>{p.tenant}</span></Link>
              : <span>Tenant <span className={`${mono} text-caption text-ink`}>{p.tenant}</span></span>
          )}
          {p.groupId && <span>Message group <span className={`${mono} text-caption text-ink`}>{p.groupId}</span></span>}
          {p.actions && <span className="ml-auto" data-slot="actions">{p.actions}</span>}
        </div>

        <Notices notices={notices} className="mt-4 max-w-[72ch]" />

        {tabs && (
          <nav className="mt-6 flex gap-1 border-b border-line text-sm" aria-label="Message views">
            {(["timeline", "body"] as const).map((k) => (
              <Link key={k} href={href.tab(k)} aria-current={tab === k ? "page" : undefined}
                className={`-mb-px inline-flex items-center gap-1.5 border-b-2 px-3 py-2 ${tab === k ? "border-brand font-medium text-ink" : "border-transparent text-ink-2 hover:text-ink"}`}>
                {k === "timeline" ? "Timeline" : "Body"}
                {k === "body" && p.bodyNote && (
                  <Tooltip content={p.bodyNote} className="inline-flex text-ink-3"><Lock size={11} aria-label={p.bodyNote} /></Tooltip>
                )}
              </Link>
            ))}
          </nav>
        )}

        {showBody ? (
          <div className="mt-6 max-w-5xl overflow-hidden rounded-card border border-line bg-surface shadow-card" data-tab="body">{p.body}</div>
        ) : (
          <>
            {p.cause && <div className="mt-6" data-slot="cause">{p.cause}</div>}
            <div className={`${p.cause ? "mt-3" : "mt-6"} overflow-hidden rounded-card border border-line bg-surface shadow-card`} data-tab="timeline">
              <TimelineView t={t} answer={p.answer ?? answerFor(t)} openSteps={p.openSteps} messageHref={href.message} inPage />
            </div>
          </>
        )}
        {p.footer && <p className="mt-3 max-w-[78ch] text-xs leading-relaxed text-ink-3">{p.footer}</p>}
      </div>
    </>
  );
}
