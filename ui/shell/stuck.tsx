/*
  Stuck (/stuck): the stuck messages grouped by why they didn't finish, the messages with their cause, and the
  dead-letter queues holding them, from GET /api/causes. The views are the shared Needs attention ones
  (../src/causes), the same as the hosted app's. ?cause=<id> narrows the messages to one cause; the URL is the
  page's state, as on the other lists. Redrive is not here: it needs AWS's redrive permissions, which this
  server doesn't have.
*/
import { useEffect, useState, type ReactNode } from "react";
import {
  CauseList, CauseMessagesTable, DeadLetterQueues, EmptyState, causesNote, fmtThreshold, storeIsEmpty, stuckEmpty,
  RANGE_PARAMS, REFRESH_S, apiQuery, pinnedQuery, rangeText, readRange, refreshOn,
  type CauseGroup, type CausePage, type TimeRange,
} from "../src/index.ts";
import { CopyLink, Frame, Json, getHealth, getJSON, messageHref } from "./chrome.tsx";
import { Filters, clock, pick, qs, useRefresh, withQuery } from "./lists.tsx";

type State = { kind: "loading" } | { kind: "error"; message: string } | { kind: "ok"; page: CausePage; storeEmpty: boolean; at: number; asked: string };

const H2 = ({ id, children }: { id: string; children: ReactNode }) => <h2 id={id} className="text-[1.25rem] text-sky-ink">{children}</h2>;
const Note = ({ children }: { children: ReactNode }) => <p className="mt-1 max-w-[80ch] text-sm leading-relaxed text-sky-ink-3">{children}</p>;
const card = "mt-3 border-y border-sky-line md:overflow-x-auto md:rounded-[16px] md:border md:bg-sky-2/55";
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
  const key = qs(own);
  useEffect(() => { document.title = "Stuck messages · Trailbeam"; }, []);
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
  }, [key, tick, Boolean(problem)]);

  const page = state.kind === "ok" ? state.page : null;
  const threshold = page?.older_than_s ?? (/^\d+$/.test(own.older_than_s ?? "") ? Number(own.older_than_s) : undefined);
  const keep: Record<string, string> = refresh ? { refresh: String(REFRESH_S) } : {};
  const { cause: wanted, ...filters } = own;
  // a cause's link keeps the page's filters and range
  const causeHref = (g: CauseGroup) => withQuery("/stuck", { ...filters, ...keep, cause: g.id });
  const everyCause = withQuery("/stuck", { ...filters, ...keep });
  const picked = page && wanted ? page.groups.find((g) => g.id === wanted) ?? null : null;
  const values = Object.fromEntries(Object.entries(own).filter(([k]) => !(RANGE_PARAMS as readonly string[]).includes(k) && k !== "cause"));

  return (
    <Frame>
      <h1 className="text-[1.75rem] text-sky-ink">Stuck messages</h1>
      <p className="mt-1 max-w-[80ch] text-sky-ink-2">
        Messages first seen more than {threshold === undefined ? "?" : fmtThreshold(threshold)} ago that no consumer has deleted yet, grouped by why they didn&rsquo;t finish. Change the age with Older than, or the server default with <code className="font-mono text-caption">--stuck-after</code> (<code className="font-mono text-caption">TRAILBEAM_STUCK_AFTER_S</code>).
      </p>
      <Filters key={qs(values)} action="/stuck" values={values} refresh={refresh} hidden={picked ? { cause: picked.id } : {}}
        range={{ r, opt: { what: "cause seen", upper: true } }}
        fields={[{ name: "older_than_s", label: "Older than (s)", kind: "number" }, { name: "queue", label: "Queue", kind: "text" }, { name: "limit", label: "Limit", kind: "number" }]} />
      <div className="mt-3 flex flex-wrap items-center gap-4 text-sm text-sky-ink-2">
        <label className="inline-flex items-center gap-2">
          <input type="checkbox" checked={refresh} onChange={(e) => setRefresh(e.target.checked)} />
          Refresh every {REFRESH_S} s
        </label>
        {state.kind === "ok" && <>
          <span className="text-sky-ink-3">As of {clock(state.at)} UTC</span>
          <CopyLink link={() => withQuery("/stuck", pinnedQuery(own, r, state.at, true))} />
        </>}
      </div>
      <div className="mt-5">
        {problem && <p className="text-sky-ink-2" role="alert">{problem}</p>}
        {!problem && state.kind === "loading" && <p className="text-sky-ink-3">Loading messages</p>}
        {!problem && state.kind === "error" && <p className="text-sky-ink-2" role="alert">{state.message}</p>}
        {!problem && state.kind === "ok" && (page!.total === 0 ? (
          <Empty page={page!} storeEmpty={state.storeEmpty} r={r} all={withQuery("/stuck", { ...values, ...keep })} />
        ) : (
          <div className="grid gap-8">
            <p className="text-sky-ink-2" data-counts>
              <span className="font-mono tabular-nums text-sky-ink">{n(page!.total)}</span> not finished{page!.capped && " (at least)"},{" "}
              <span className={`font-mono tabular-nums ${page!.in_dead_letter_queue ? "text-lost" : "text-sky-ink"}`}>{n(page!.in_dead_letter_queue)}</span> in a dead-letter queue
              {r.kind !== "all" && <>, with a cause seen {rangeText(r)}</>}.
            </p>

            <section aria-labelledby="causes-h">
              <H2 id="causes-h">Why they didn&rsquo;t finish</H2>
              <Note>{causesNote({ capped: page!.capped, readLimit: page!.read_limit })} This server has spans only, so causes from CloudTrail and Lambda&rsquo;s logs aren&rsquo;t here, and a queue&rsquo;s visibility timeout is taken to be SQS&rsquo;s default of 30 s.</Note>
              <div className={card}>
                <CauseList groups={page!.groups} nowMs={state.at} picked={picked?.id} href={causeHref} messageHref={messageHref} />
              </div>
            </section>

            <section aria-labelledby="stuck-h">
              <div className="flex flex-wrap items-baseline gap-x-3">
                <H2 id="stuck-h">{picked ? `Not finished: ${picked.label}` : "Not finished"}</H2>
                {picked && <a href={everyCause} className="text-sm text-dawn hover:underline">Show every cause</a>}
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
                <Note>To move a dead-letter queue&rsquo;s messages back to the queue they came from, start a redrive in the SQS console, or with <code className="font-mono text-caption">aws sqs start-message-move-task</code>. Trailbeam only reads, so it can&rsquo;t move them.</Note>
                <div className={card}><DeadLetterQueues groups={page!.dead_letter_queues} nowMs={state.at} /></div>
              </section>
            )}

            <p className="text-sm text-sky-ink-3">Times are in UTC. {FOOT} <Json href={state.asked} /></p>
          </div>
        ))}
      </div>
    </Frame>
  );
}

/** Nothing stuck: the shared sentence, with the range when there is one. */
function Empty({ page, storeEmpty, r, all }: { page: CausePage; storeEmpty: boolean; r: TimeRange; all: string }) {
  const e = stuckEmpty(page.older_than_s, storeEmpty);
  if (storeEmpty || r.kind === "all") return <EmptyState {...e} />;
  return <EmptyState {...e} detail={<>{e.detail} Only causes seen {rangeText(r)} are counted.</>} link={{ href: all, label: "Show any time" }} />;
}
