/*
  What each page says where its list would be when the list is empty (design QV-146, section 5): one copy while
  the store holds no span at all (`empty`: /api/health data.last_row is 0), another once spans exist.
*/
import { DUPLICATE_REASONS, SET_UP, type EmptyStateProps } from "./Messages.tsx";

const C = ({ children }: { children: string }) => <code className="font-mono text-caption">{children}</code>;

/** `href` with `drop` taken out of its query, the other filters kept ("Clear the filter"). */
export function without(path: string, params: Record<string, string>, drop: string): string {
  const q = new URLSearchParams(Object.entries(params).filter(([k, v]) => k !== drop && k !== "cursor" && v !== ""));
  const s = q.toString();
  return s ? `${path}?${s}` : path;
}

/** The Messages list (`/`). Not used for the first-run case: an unfiltered `/` on an empty store is the first-run screen. */
export function messagesEmpty(params: Record<string, string>, opts: { empty: boolean; storesBodies?: boolean }): EmptyStateProps | null {
  const { queue, tenant, body } = params;
  if (body?.trim()) {
    if (opts.storesBodies === false) return null; // the page's notice already says the search can find nothing
    return { title: <>No message body matches <C>{body.trim()}</C></> };
  }
  if (opts.empty) return { title: "No messages yet.", detail: "No service has sent spans here yet.", link: SET_UP };
  if (queue) return {
    title: <>No messages on <C>{queue}</C>.</>,
    detail: "Trailbeam has seen messages, none on this queue. Queue names match exactly, as in the queue URL.",
    link: { href: without("/", params, "queue"), label: "Clear the filter" },
  };
  if (tenant) return {
    title: <>No messages for the tenant <C>{tenant}</C>.</>,
    detail: <>A message has a tenant only when the package recorded one: <C>TRAILBEAM_TENANT_ATTRIBUTE</C>, <C>TRAILBEAM_TENANT_BODY_PATH</C> or the package&rsquo;s <C>tenant</C> option.</>,
    link: { href: without("/", params, "tenant"), label: "Clear the filter" },
  };
  return { title: "No messages." };
}

export function duplicatesEmpty(params: Record<string, string>, empty: boolean): EmptyStateProps {
  if (empty) return { title: "No messages yet.", detail: "Duplicates show up once a service sends spans.", link: SET_UP };
  if (params.reason) return {
    title: `No duplicates for "${DUPLICATE_REASONS[params.reason] ?? params.reason}".`,
    link: { href: without("/duplicates", params, "reason"), label: "Show every reason" },
  };
  return { title: "No duplicates.", detail: "No message was received more than once or handled more than once. A RabbitMQ message routed to two queues is one delivery on each, not a duplicate." };
}

export function stuckEmpty(olderThanS: number, empty: boolean): EmptyStateProps {
  if (empty) return { title: "No messages yet.", detail: `A message shows up here when it's older than ${olderThanS} s and has no final delete, or on RabbitMQ no ack.`, link: SET_UP };
  return { title: "No stuck messages.", detail: `Every message first seen more than ${olderThanS} s ago has a final delete, or on RabbitMQ an ack.` };
}

export function tenantsEmpty(): EmptyStateProps {
  return { title: "No messages yet.", detail: "Tenants show up once a service sends spans with a tenant set.", link: SET_UP };
}

/** A message ID that has no spans. */
export function lookupEmpty(id: string, empty: boolean): EmptyStateProps {
  if (empty) return { title: <>No message with the ID <C>{id}</C> has reached Trailbeam. No service has sent spans here yet.</>, link: SET_UP };
  return {
    title: <>No message with the ID <C>{id}</C> has reached Trailbeam.</>,
    detail: <>Message IDs are the <C>MessageId</C> SQS returns from <C>SendMessage</C>, not a receipt handle. A RabbitMQ message&rsquo;s ID is the <C>message_id</C> property its publisher set: a message published without one has no timeline.</>,
  };
}
