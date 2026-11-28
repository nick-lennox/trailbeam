/*
  Issues (/issues) and one issue (/issues/<cause ID>) in the shell (QV-708): the shared Issues views
  (../src/issues, the hosted app's Issues pages) on GET /api/causes. An issue is a cause group, so its ID is the one
  the Overview, the message page and cause alerts use. The list has one time control, its time range: how old an
  undeleted message must be to count is the server's --stuck-after, or ?age= in the link (90s, 15m, 2h, 1d), and the
  page says which. ?queue= narrows the list to one queue.

  Beside the hosted page, it has no triage (its Open, Assigned to me and so on, QV-593) and no "Received more than
  once" (the Duplicates page has those). Each issue has the shared "Alert me" while the scheduled alert check is on.
  Under the list are the dead-letter queues holding these messages, with redrive while it is on (redrive.tsx, QV-590),
  and the redrives started.

  /stuck, the page before this one, redirects here (StuckRedirect), keeping its queue, age, time range and cause.
*/
import { useEffect, useState, type ReactNode } from "react";
import {
  answerFor, storeIsEmpty,
  type CausePage, type DlqGroup, type RedriveGateAnswer, type Timeline,
} from "../src/index.ts";
import {
  AGE_CHOICES, ageParam, ageText, issueOf, issuesOfPage, rangeFromParams, rangeParams, stuckAge,
  type Issue, type TimeRange as ListRange,
} from "../src/model/index.ts";
import { IssueDetail, IssueGone, IssueList, QueueChip, issueRow, withQueue } from "../src/issues/index.ts";
import { AlertMe, Card, CopyText, RedriveLog, RedriveSelect, TimelineView, TopBar } from "../src/design/index.ts";
import { Json, getHealth, getJSON, messageHref } from "./chrome.tsx";
import { ShellFrame, ShellSearch } from "./frame.tsx";
import { clock } from "./lists.tsx";
import { SHELL_ALERT_HREFS, useAlerts } from "./alerts.tsx";
import { SHELL_REDRIVE, redriveGates, useRedrive } from "./redrive.tsx";

/** The most messages one read asks /api/causes to list (its limit's ceiling): the list draws these. */
export const ISSUE_MESSAGES = 1000;

/** What a link to /issues or /issues/<id> names: the age (null: the server's), the queue, and the range (null: the page's default). */
export type IssuesUrl = { age: number | null; badAge: boolean; queue: string | null; range: ListRange | null };

export function readIssuesUrl(search: string, now: number): IssuesUrl {
  const p = new URLSearchParams(search);
  const raw = p.get("age") ?? p.get("older_than_s");
  const { s, bad } = stuckAge(raw);
  const named = p.has("range") || (p.has("from") && p.has("to"));
  return {
    age: raw?.trim() && !bad ? s : null, badAge: bad, queue: p.get("queue")?.trim() || null,
    range: named ? rangeFromParams((k) => p.get(k), now) : null,
  };
}

/** /issues (or an issue's page, with `id`) with the URL's age and range, and `queue` (none for every queue). */
export function issuesHref(u: Pick<IssuesUrl, "age" | "range">, queue: string | null, id?: string): string {
  const p = new URLSearchParams();
  if (u.age !== null) p.set("age", ageParam(u.age));
  if (u.range) for (const [k, v] of rangeParams(u.range, true)) p.set(k, v);
  const path = id ? `/issues/${encodeURIComponent(id)}` : "/issues";
  const q = p.toString();
  return withQueue(q ? `${path}?${q}` : path, queue);
}

/** The GET /api/causes an Issues page reads: every message (up to ISSUE_MESSAGES), at the URL's age; one cause's for an issue's page. */
export function causesAsk(u: Pick<IssuesUrl, "age" | "queue">, cause?: string): string {
  const p = new URLSearchParams({ limit: String(ISSUE_MESSAGES) });
  if (u.age !== null) p.set("older_than_s", String(u.age));
  if (u.queue) p.set("queue", u.queue);
  if (cause) p.set("cause", cause);
  return `/api/causes?${p}`;
}

/**
 * Where a /stuck link goes now: /issues/<cause> for ?cause=, else /issues, with its queue, its age (older_than_s) and
 * its range (range, or from_ms and to_ms). A range the Issues list has no name for (24h) is read as a time span.
 */
export function stuckTarget(search: string, now: number): string {
  const p = new URLSearchParams(search);
  const raw = p.get("older_than_s");
  const { s, bad } = stuckAge(raw);
  const from = Number(p.get("from_ms") ?? NaN), to = Number(p.get("to_ms") ?? (p.has("from_ms") ? now : NaN));
  const range = p.get("range") ? rangeFromParams((k) => (k === "range" ? p.get("range") : null), now)
    : Number.isFinite(from) && Number.isFinite(to) && from < to ? rangeFromParams((k) => (k === "from" ? String(from) : k === "to" ? String(to) : null), now)
    : null;
  return issuesHref({ age: raw && !bad ? s : null, range }, p.get("queue")?.trim() || null, p.get("cause") || undefined);
}

export function StuckRedirect() {
  useEffect(() => { location.replace(stuckTarget(location.search, Date.now())); }, []);
  return <ShellFrame path="/issues"><p className="px-6 pt-6 text-sm text-ink-2">Stuck messages are on the <a className="text-brand hover:underline" href="/issues">Issues</a> page.</p></ShellFrame>;
}

/** The sentence on how old an undeleted message must be to count, and where that is set. */
export function ageNote(page: Pick<CausePage, "older_than_s">, u: Pick<IssuesUrl, "age" | "badAge">): string {
  const age = ageText(page.older_than_s);
  if (u.badAge) return `The age in the link can't be read, so this is the server's, ${age}. An age is a whole number with s, m, h or d, such as 15m.`;
  if (u.age !== null) return `A message counts once it is ${age} old without a delete, as the link's age says.`;
  return `A message counts once it is ${age} old without a delete. The server sets this with --stuck-after (TRAILBEAM_STUCK_AFTER_S); a link can name another with ?age=, such as ?age=${ageParam(AGE_CHOICES[2])}.`;
}

export type Got = { page: CausePage; at: number; asked: string; storeEmpty: boolean };
type State = { kind: "loading" } | { kind: "error"; message: string } | { kind: "ok"; got: Got };

/** GET /api/causes at `asked`, read again when `again` changes. */
function useCauses(asked: string, again: number): State {
  const [state, setState] = useState<State>({ kind: "loading" });
  useEffect(() => {
    let live = true;
    const at = Date.now();
    getJSON<CausePage>(asked)
      .then(async (page) => {
        // nothing unfinished says something different while no service has sent anything (design, section 5.1)
        const storeEmpty = page.total === 0 ? await getHealth(5000).then(storeIsEmpty, () => false) : false;
        if (live) setState({ kind: "ok", got: { page, at, asked, storeEmpty } });
      })
      .catch((e: Error) => live && setState({ kind: "error", message: e.message }));
    return () => { live = false; };
  }, [asked, again]);
  return state;
}

const H2 = ({ id, children }: { id: string; children: ReactNode }) => <h2 id={id} className="mb-1 font-serif text-sm font-semibold text-ink">{children}</h2>;
const Note = ({ children }: { children: ReactNode }) => <p className="mb-2 max-w-[72ch] text-xs leading-relaxed text-ink-3">{children}</p>;
const Waiting = ({ children }: { children: ReactNode }) => <p className="px-5 pt-6 text-sm text-ink-2" role="status">{children}</p>;

const FOOT = "This server has spans only, so causes from CloudTrail and Lambda's logs aren't here, and a queue's visibility timeout is taken to be SQS's default of 30 s. A message that a recorded PurgeQueue call removed is not listed here; one whose last step was during a purge (or within 60 s after it) is listed, marked possibly purged. A message can also leave SQS without a delete when its queue's retention period (MessageRetentionPeriod) ends; SQS reports nothing when that happens, so Trailbeam cannot see it and the message stays here until Trailbeam's own retention removes it. Ages are this server's clock minus the send's time on the sender's clock.";

const EMPTY = { title: "No open issues", body: "Every message your services reported was deleted, or is too recent to call." };
const NO_SPANS = { title: "No messages yet", body: "An issue shows up here when a message your services sent has no delete. Set up a service to send its spans to this server." };

export function IssuesPage() {
  const [u] = useState(() => readIssuesUrl(location.search, Date.now()));
  // a redrive started or cancelled reads the page and the log again
  const [changes, setChanges] = useState(0);
  const state = useCauses(causesAsk(u), changes);
  const redrive = useRedrive();
  const alerts = useAlerts();
  useEffect(() => { document.title = "Issues, Trailbeam"; }, []);
  useEffect(() => { if (changes) redrive.reload(); }, [changes]);
  // while a redrive is moving messages, as the hosted page does, read again every 5 s
  const moving = !!redrive.view?.log.some((l) => l.outcome === "started" && (!l.status || l.status === "RUNNING" || l.status === "CANCELLING"));
  useEffect(() => {
    if (!moving) return;
    const t = setInterval(() => setChanges((x) => x + 1), 5000);
    return () => clearInterval(t);
  }, [moving]);

  const got = state.kind === "ok" ? state.got : null;
  return (
    <ShellFrame path="/issues">
      <TopBar title="Issues" pad="px-5" search={<ShellSearch />} fresh={got ? <>at <time>{clock(got.at)}</time> UTC</> : undefined} />
      {state.kind === "loading" && <Waiting>Reading the issues</Waiting>}
      {state.kind === "error" && <p className="px-5 pt-6 text-sm text-ink-2" role="alert">{state.message}</p>}
      {got && <IssuesBody u={u} got={got} alerts={alerts} redrive={redrive} changed={() => setChanges((x) => x + 1)} />}
    </ShellFrame>
  );
}

/** The list and what is under it, for one read of /api/causes. */
export function IssuesBody({ u, got, alerts, redrive, changed }: { u: IssuesUrl; got: Got; alerts: ReturnType<typeof useAlerts>; redrive: ReturnType<typeof useRedrive>; changed: () => void }) {
  const { page, at } = got;
  const issues = issuesOfPage(page, at);
  const href = (id: string) => issuesHref(u, u.queue, id);
  const ad = alerts.state.kind === "ok" ? alerts.state.data : null;
  const ruleOf = (id: string) => ad?.view.rules.find((x) => x.causeId === id) ?? null;
  const firing = ad ? ad.view.rules.filter((r) => r.firingSince != null).length : 0;
  const notes = [
    ageNote(page, u),
    page.capped && `More than ${page.read_limit.toLocaleString("en-US")} messages are unfinished, so the issues are of the ${page.read_limit.toLocaleString("en-US")} oldest. Counts here can be lower than the true totals.`,
    page.listed > page.messages.length && `The chart and the rows draw the ${page.messages.length.toLocaleString("en-US")} oldest of ${page.listed.toLocaleString("en-US")} messages; each issue's count is of every one.`,
  ].filter(Boolean).join(" ");

  return (
    <>
      <IssueList
        rows={issues.map((i) => issueRow(i))} now={at} defaultRange="1d" initialRange={u.range}
        hrefs={Object.fromEntries(issues.map((i) => [i.id, href(i.id)]))}
        filterName="Open" emptyText={got.storeEmpty ? NO_SPANS : EMPTY}
        filters={u.queue ? <QueueChip queue={u.queue} href={issuesHref(u, null)} /> : undefined}
        actions={Object.fromEntries(issues.map((i) => [i.id, (
          <span key={i.id} className="flex items-center">
            {ad && <AlertMe cause={{ id: i.id, label: i.title, queue: i.queue }} rule={ruleOf(i.id)} view={ad.view} canManage={ad.canManage}
              actions={alerts.rules} hrefs={SHELL_ALERT_HREFS} changed={() => { void alerts.load(); }} />}
            <CopyText path={href(i.id)} icon="link" label="Copy a link to this issue" className="grid size-9 shrink-0 place-items-center"><span className="sr-only">Copy link</span></CopyText>
          </span>
        )]))}
        note={notes}
      />

      <div className="grid gap-6 border-t border-line px-5 pt-6 pb-12">
        {ad && ad.view.rules.length > 0 && (
          <p className="text-xs text-ink-3">
            {ad.view.rules.length} alert {ad.view.rules.length === 1 ? "rule" : "rules"} on issues{firing > 0 && <span className="text-critical-text">, {firing} firing</span>}.{" "}
            <a href="/alerts" className="font-medium text-brand hover:underline">Manage alerts</a>
          </p>
        )}
        <DeadLetters dlqs={page.dead_letter_queues} now={at} redrive={redrive} changed={changed} />
        {redrive.view && redrive.view.log.length > 0 && (
          <section aria-labelledby="redrives-h">
            <H2 id="redrives-h">Redrives</H2>
            <Note>Who asked to redrive which dead-letter queue, and how it went. Done means the queue was empty a minute after SQS said the move finished, as SQS can say so before it has moved everything.</Note>
            <Card className="overflow-x-auto">
              <RedriveLog entries={redrive.view.log} now={at} canCancel={redrive.view.canRedrive && !!redrive.view.on} actions={SHELL_REDRIVE} onChanged={changed} />
            </Card>
          </section>
        )}
        <p className="max-w-[100ch] text-xs leading-relaxed text-ink-3">Times are in UTC. {FOOT} <Json href={got.asked} /></p>
      </div>
    </>
  );
}

/* The dead-letter queues holding these messages. While redrive is on, each can be redriven as its gate allows; off,
   each says how to redrive it in SQS. */
function DeadLetters({ dlqs, now, redrive, changed }: { dlqs: DlqGroup[]; now: number; redrive: ReturnType<typeof useRedrive>; changed: () => void }) {
  const on = !!redrive.view?.on;
  const gates = useGates(on ? dlqs : []);
  if (!dlqs.length) return null;
  const off = { ok: false as const, reason: "Start a redrive in the SQS console, or with aws sqs start-message-move-task. Trailbeam can start one once an admin turns on AWS and names the dead-letter queues to redrive in Settings, under AWS." };
  return (
    <section aria-labelledby="dlq-h">
      <H2 id="dlq-h">In a dead-letter queue</H2>
      <Note>A redrive moves every message in the dead-letter queue back to the queue it came from{on ? ", with SQS's message move task and this server's AWS credentials" : ""}.</Note>
      {on && !gates ? <Waiting>Checking which queues can be redriven</Waiting> : (
        <Card>
          <RedriveSelect nowMs={now} actions={SHELL_REDRIVE} onChanged={changed} logHint="The redrive log below says when the queue is empty."
            groups={dlqs.map((g, i) => ({ ...g, account: gates?.[i]?.dlq.account ?? g.account, region: gates?.[i]?.dlq.region ?? g.region, gate: on ? gateOf(gates?.[i]) : off }))} />
        </Card>
      )}
    </section>
  );
}

const gateOf = (g: RedriveGateAnswer | undefined) => (!g ? { ok: false as const, reason: "Trailbeam couldn't check this queue." } : g.ok ? { ok: true as const } : { ok: false as const, reason: g.reason });

/** Each dead-letter queue's gate, in order, or null while they are read. */
function useGates(dlqs: DlqGroup[]): RedriveGateAnswer[] | null {
  const [got, setGot] = useState<{ key: string; gates: RedriveGateAnswer[] } | null>(null);
  const key = dlqs.map((g) => g.key).join("|");
  useEffect(() => {
    if (!dlqs.length) return;
    let live = true;
    redriveGates(dlqs.map((g) => ({ name: g.name, account: g.account, region: g.region, ...(g.messageIds[0] ? { messageId: g.messageIds[0] } : {}) })))
      .then((gates) => live && setGot({ key, gates }), () => live && setGot({ key, gates: [] }));
    return () => { live = false; };
  }, [key]);
  return got && got.key === key ? got.gates : null;
}

/** The cause ID in /issues/<id>, or null for /issues itself. */
export function issueIdOf(path: string): string | null {
  const m = /^\/issues\/([^/]+)\/?$/.exec(path);
  if (!m) return null;
  try { return decodeURIComponent(m[1]); } catch { return null; }
}

export function IssuePage() {
  const id = issueIdOf(location.pathname) ?? "";
  const [u] = useState(() => readIssuesUrl(location.search, Date.now()));
  const state = useCauses(causesAsk({ age: u.age, queue: null }, id), 0);
  const alerts = useAlerts();
  const got = state.kind === "ok" ? state.got : null;
  const group = got?.page.groups.find((g) => g.id === id) ?? null;
  const issue: Issue | null = got && group ? issueOf(group, got.page.messages.filter((m) => m.cause === id), got.at) : null;
  const example = useExample(issue?.shape?.example);
  useEffect(() => { document.title = `${issue?.title ?? "Issue"}, Trailbeam`; }, [issue?.title]);

  const list = issuesHref(u, null);
  const ad = alerts.state.kind === "ok" ? alerts.state.data : null;
  return (
    <ShellFrame path="/issues">
      <TopBar pad="px-6" search={<ShellSearch />} fresh={got ? <>at <time>{clock(got.at)}</time> UTC</> : undefined} />
      {state.kind === "loading" && <Waiting>Reading the issue</Waiting>}
      {state.kind === "error" && <p className="px-6 pt-6 text-sm text-ink-2" role="alert">{state.message}</p>}
      {got && !issue && <IssueGone kind="none" listHref={list} age={ageText(got.page.older_than_s)} />}
      {got && issue && (
        <IssueDetail issue={issue} now={got.at} listHref={list} queueHref={issuesHref(u, issue.queue)} messageHref={messageHref}
          actions={ad ? <AlertMe cause={{ id, label: issue.title, queue: issue.queue }} rule={ad.view.rules.find((r) => r.causeId === id) ?? null} view={ad.view}
            canManage={ad.canManage} actions={alerts.rules} hrefs={SHELL_ALERT_HREFS} changed={() => { void alerts.load(); }} /> : undefined}
          example={example && <Card className="overflow-hidden"><TimelineView t={example} answer={answerFor(example)} messageHref={messageHref} /></Card>}
          fewerNote={`${issue.messages.length.toLocaleString("en-US")} of ${issue.count.toLocaleString("en-US")}, the latest first. Open one to see its timeline.`} />
      )}
    </ShellFrame>
  );
}

/** The example message's timeline, as its own page draws it; null while it is read, or when it can't be. */
function useExample(id: string | undefined): Timeline | null {
  const [t, setT] = useState<{ id: string; t: Timeline } | null>(null);
  useEffect(() => {
    if (!id) return;
    let live = true;
    getJSON<Timeline>(`/api/messages/${encodeURIComponent(id)}`).then((x) => live && setT({ id, t: x }), () => {});
    return () => { live = false; };
  }, [id]);
  return t && t.id === id ? t.t : null;
}
