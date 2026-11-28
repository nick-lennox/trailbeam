/*
  Stuck (/stuck): the stuck messages grouped by why they didn't finish, the messages with their cause, and the
  dead-letter queues holding them, from GET /api/causes. The views are the shared Needs attention ones
  (../src/causes), the same as the hosted app's. ?cause=<id> narrows the messages to one cause; the URL is the
  page's state, as on the other lists. While redrive is on (redrive.tsx, QV-590: AWS on and an admin named the
  dead-letter queues to redrive), the dead-letter queues are the shared RedriveSelect, with a box before each and
  "Redrive whole DLQ", and the redrive log follows them; off, the list is as before, with how to redrive in SQS.
  Each cause has the shared "Alert me" (alerts.tsx) while the scheduled alert check is on.
*/
import { useEffect, useState, type ReactNode } from "react";
import {
  CauseList, CauseMessagesTable, DeadLetterQueues, EmptyState, causesNote, fmtThreshold, storeIsEmpty, stuckEmpty,
  RANGE_PARAMS, REFRESH_S, apiQuery, pinnedQuery, rangeText, readRange, refreshOn,
  type CauseGroup, type CausePage, type DlqGroup, type RedriveGateAnswer, type TimeRange,
} from "../src/index.ts";
import { CopyLink, Json, getHealth, getJSON, messageHref } from "./chrome.tsx";
import { ShellFrame, ShellSearch } from "./frame.tsx";
import { AlertMe, RedriveLog, RedriveSelect, TopBar } from "../src/design/index.ts";
import { Filters, clock, pick, qs, useRefresh, withQuery } from "./lists.tsx";
import { SHELL_ALERT_HREFS, useAlerts } from "./alerts.tsx";
import { SHELL_REDRIVE, redriveGates, useRedrive } from "./redrive.tsx";

type State = { kind: "loading" } | { kind: "error"; message: string } | { kind: "ok"; page: CausePage; storeEmpty: boolean; at: number; asked: string };

const H2 = ({ id, children }: { id: string; children: ReactNode }) => <h2 id={id} className="font-serif text-[1.25rem] text-ink">{children}</h2>;
const Note = ({ children }: { children: ReactNode }) => <p className="mt-1 max-w-[80ch] text-sm leading-relaxed text-ink-3">{children}</p>;
const card = "mt-3 border-y border-line md:overflow-x-auto md:rounded-xl md:border md:bg-surface";
const n = (v: number) => v.toLocaleString("en-US");

const FOOT = <>A message that a recorded PurgeQueue call removed is not listed here; one whose last step was during a purge (or within 60 s after it) is listed, marked possibly purged. A message can also leave SQS without a delete when its queue&rsquo;s retention period (<code className="font-mono text-caption">MessageRetentionPeriod</code>) ends; SQS reports nothing when that happens, so Trailbeam cannot see it and the message stays here until Trailbeam&rsquo;s own retention removes it. Age is this server&rsquo;s clock now minus the send&rsquo;s time on the sender&rsquo;s clock, so skew between the two clocks shows up in it. If Trailbeam never saw the send, it counts from SQS&rsquo;s <code className="font-mono text-caption">SentTimestamp</code>, and the age says so; with neither, from the first receive, on the consumer&rsquo;s clock.</>;

export function StuckPage() {
  const params = Object.fromEntries(new URLSearchParams(location.search));
  const own = pick(params, ["older_than_s", "limit", "queue", "cause", "refresh", "range", "from_ms", "to_ms"]);
  const read = readRange(own);
  const r: TimeRange = "error" in read ? { kind: "all" } : read;
  const problem = "error" in read ? read.error : null;
  const [state, setState] = useState<State>({ kind: "loading" });
  const [refresh, setRefresh, tick] = useRefresh(refreshOn(own));
  // a redrive started or cancelled reads the page and the log again
  const [changes, setChanges] = useState(0);
  const redrive = useRedrive();
  const key = qs(own);
  useEffect(() => { document.title = "Stuck messages, Trailbeam"; }, []);
  useEffect(() => {
    if (problem) return;
    let live = true;
    // a preset range ends now: worked out again for each ask, so a refresh moves the window along
    const now = Date.now();
    const { refresh: _, ...rest } = own;
    const q = qs(apiQuery(rest, r, now, true));
    const asked = `/api/causes${q ? `?${q}` : ""}`;
    getJSON<CausePage>(asked)
      .then(async (page) => {
        // nothing stuck says something different while no service has sent anything (design, section 5.1)
        const storeEmpty = page.total === 0 ? await getHealth(5000).then(storeIsEmpty, () => false) : false;
        if (live) setState({ kind: "ok", page, storeEmpty, at: now, asked });
      })
      .catch((e: Error) => live && setState({ kind: "error", message: e.message }));
    return () => { live = false; };
  }, [key, tick, changes, Boolean(problem)]);
  useEffect(() => { if (tick || changes) redrive.reload(); }, [tick, changes]);

  const page = state.kind === "ok" ? state.page : null;
  const threshold = page?.older_than_s ?? (/^\d+$/.test(own.older_than_s ?? "") ? Number(own.older_than_s) : undefined);
  const keep: Record<string, string> = refresh ? { refresh: String(REFRESH_S) } : {};
  const { cause: wanted, ...filters } = own;
  // a cause's link keeps the page's filters and range
  const causeHref = (g: CauseGroup) => withQuery("/stuck", { ...filters, ...keep, cause: g.id });
  const everyCause = withQuery("/stuck", { ...filters, ...keep });
  const picked = page && wanted ? page.groups.find((g) => g.id === wanted) ?? null : null;
  // "Alert me" on each cause, while the scheduled check is on (the alert routes are 404 otherwise)
  const alerts = useAlerts();
  const ad = alerts.state.kind === "ok" ? alerts.state.data : null;
  const ruleOf = (g: CauseGroup) => ad?.view.rules.find((x) => x.causeId === g.id) ?? null;
  const alertMe = ad ? (g: CauseGroup) => (
    <AlertMe cause={{ id: g.id, label: g.label, queue: g.queue ?? "" }} rule={ruleOf(g)} view={ad.view} canManage={ad.canManage}
      actions={alerts.rules} hrefs={SHELL_ALERT_HREFS} changed={() => { void alerts.load(); }} />
  ) : undefined;
  const redriveOn = Boolean(redrive.view?.on);
  const dlqs = state.kind === "ok" ? state.page.dead_letter_queues : [];
  const gates = useGates(redriveOn ? dlqs : []);
  const changed = () => setChanges((x) => x + 1);
  const values = Object.fromEntries(Object.entries(own).filter(([k]) => !(RANGE_PARAMS as readonly string[]).includes(k) && k !== "cause"));

  return (
    <ShellFrame path="/stuck">
      <TopBar search={<ShellSearch />} />
      <div className="px-8 pt-8 pb-16">
        <h1 className="font-serif text-title-sm text-ink">Stuck messages</h1>
        <p className="mt-1 max-w-[80ch] text-sm text-ink-2">
          Messages first seen more than {threshold === undefined ? "?" : fmtThreshold(threshold)} ago that no consumer has deleted yet, grouped by why they didn&rsquo;t finish. Change the age with Older than, or the server default with <code className="font-mono text-caption">--stuck-after</code> (<code className="font-mono text-caption">TRAILBEAM_STUCK_AFTER_S</code>).
        </p>
        <Filters key={qs(values)} action="/stuck" values={values} refresh={refresh} hidden={picked ? { cause: picked.id } : {}}
          range={{ r, opt: { what: "cause seen", upper: true } }}
          fields={[{ name: "older_than_s", label: "Older than (s)", kind: "number" }, { name: "queue", label: "Queue", kind: "text" }, { name: "limit", label: "Limit", kind: "number" }]} />
        <div className="mt-3 flex flex-wrap items-center gap-4 text-sm text-ink-2">
          <label className="inline-flex items-center gap-2">
            <input type="checkbox" checked={refresh} onChange={(e) => setRefresh(e.target.checked)} />
            Refresh every {REFRESH_S} s
          </label>
          {state.kind === "ok" && <>
            <span className="text-ink-3">As of {clock(state.at)} UTC</span>
            <CopyLink link={() => withQuery("/stuck", pinnedQuery(own, r, state.at, true))} />
          </>}
        </div>
        <div className="mt-5">
          {problem && <p className="text-ink-2" role="alert">{problem}</p>}
          {!problem && state.kind === "loading" && <p className="text-ink-3">Loading messages</p>}
          {!problem && state.kind === "error" && <p className="text-ink-2" role="alert">{state.message}</p>}
          {!problem && state.kind === "ok" && (page!.total === 0 ? (
            <Empty page={page!} storeEmpty={state.storeEmpty} r={r} all={withQuery("/stuck", { ...values, ...keep })} />
          ) : (
            <div className="grid gap-8">
              <p className="text-ink-2" data-counts>
                <span className="font-mono tabular-nums text-ink">{n(page!.total)}</span> not finished{page!.capped && " (at least)"},{" "}
                <span className={`font-mono tabular-nums ${page!.in_dead_letter_queue ? "text-critical-text" : "text-ink"}`}>{n(page!.in_dead_letter_queue)}</span> in a dead-letter queue
                {r.kind !== "all" && <>, with a cause seen {rangeText(r)}</>}.
              </p>

              <section aria-labelledby="causes-h">
                <H2 id="causes-h">Why they didn&rsquo;t finish</H2>
                <Note>{causesNote({ capped: page!.capped, readLimit: page!.read_limit })} This server has spans only, so causes from CloudTrail and Lambda&rsquo;s logs aren&rsquo;t here, and a queue&rsquo;s visibility timeout is taken to be SQS&rsquo;s default of 30 s.</Note>
                <div className={card}>
                  <CauseList groups={page!.groups} nowMs={state.at} picked={picked?.id} href={causeHref} messageHref={messageHref}
                    actions={alertMe} firing={ad ? (g) => ruleOf(g)?.firingSince != null : undefined} />
                </div>
              </section>

              <section aria-labelledby="stuck-h">
                <div className="flex flex-wrap items-baseline gap-x-3">
                  <H2 id="stuck-h">{picked ? `Not finished: ${picked.label}` : "Not finished"}</H2>
                  {picked && <a href={everyCause} className="text-sm text-brand hover:underline">Show every cause</a>}
                </div>
                {wanted && !picked && <Note>That cause has no stuck messages now. Showing every cause.</Note>}
                {page!.listed > page!.messages.length && <Note>The {n(page!.messages.length)} oldest of {n(page!.listed)}. Raise Limit (up to 1,000), or pick a cause or a queue.</Note>}
                <div className={card}>
                  <CauseMessagesTable messages={page!.messages} groups={page!.groups} picked={picked?.id} href={messageHref} causeHref={causeHref} />
                </div>
              </section>

              {page!.dead_letter_queues.length > 0 && (
                <section aria-labelledby="dlq-h">
                  <H2 id="dlq-h">In a dead-letter queue</H2>
                  {redriveOn && gates ? <>
                    <Note>A redrive moves every message in the dead-letter queue back to the queue it came from, with SQS&rsquo;s message move task and this server&rsquo;s AWS credentials.</Note>
                    <div className={card}>
                      <RedriveSelect groups={dlqs.map((g, i) => ({ ...g, account: gates[i]?.dlq.account ?? g.account, region: gates[i]?.dlq.region ?? g.region, gate: gateOf(gates[i]) }))} nowMs={state.at} actions={SHELL_REDRIVE} onChanged={changed}
                        logHint="The redrive log below says when the queue is empty." />
                    </div>
                  </> : <>
                    <Note>To move a dead-letter queue&rsquo;s messages back to the queue they came from, start a redrive in the SQS console, or with <code className="font-mono text-caption">aws sqs start-message-move-task</code>. Trailbeam can start one too once an admin turns on AWS and names the dead-letter queues to redrive in Settings, under AWS.</Note>
                    <div className={card}><DeadLetterQueues groups={page!.dead_letter_queues} nowMs={state.at} /></div>
                  </>}
                </section>
              )}

              {redrive.view && (redriveOn || redrive.view.log.length > 0) && (
                <section aria-labelledby="redrives-h">
                  <H2 id="redrives-h">Redrives</H2>
                  <Note>Who asked to redrive which dead-letter queue, and how it went. Done means the queue was empty a minute after SQS said the move finished, as SQS can say so before it has moved everything.</Note>
                  <div className={card}>
                    <RedriveLog entries={redrive.view.log} now={state.at} canCancel={redrive.view.canRedrive && redriveOn} actions={SHELL_REDRIVE} onChanged={changed} />
                  </div>
                </section>
              )}

              <p className="text-sm text-ink-3">Times are in UTC. {FOOT} <Json href={state.asked} /></p>
            </div>
          ))}
        </div>
      </div>
    </ShellFrame>
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

/** Nothing stuck: the shared sentence, with the range when there is one. */
function Empty({ page, storeEmpty, r, all }: { page: CausePage; storeEmpty: boolean; r: TimeRange; all: string }) {
  const e = stuckEmpty(page.older_than_s, storeEmpty);
  if (storeEmpty || r.kind === "all") return <EmptyState {...e} />;
  return <EmptyState {...e} detail={<>{e.detail} Only causes seen {rangeText(r)} are counted.</>} link={{ href: all, label: "Show any time" }} />;
}
