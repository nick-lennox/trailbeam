"use client";

import { useLayoutEffect, useRef, useState } from "react";
import { fmtBytes, fmtNum } from "../model/format.ts";
import { Tooltip } from "../design/Tooltip.tsx";

/* The queue drawn as a lane, read left to right in the order a message lives: waiting to
   be received, in flight with the consumer, then out of the queue.

   What is in the queue is drawn to scale, for a queue of any size. While the waiting and
   in-flight messages fit, each is its own mark: it selects that message in the list under
   the lane, and it keeps its identity as it moves, so a receive, an expired timeout, a
   delete and a move to the dead-letter queue are each seen as one mark changing place.

   When there are too many for that, the two amounts become bars. One segment of a bar is
   a round number of messages (2, 5, 10, 20, 50, 100 and so on), the smallest that fits
   both, so the backlog and the work in hand can be compared by eye and a queue of a
   million draws no more than a queue of a hundred. The messages worth watching are then
   drawn one by one under the bars: those received more than once.

   How fast messages leave does not set the scale: a queue that is nearly empty because
   its consumer keeps up would otherwise be drawn as specks. What was deleted is a number
   and the most recent ones as marks; what was dead-lettered is always one mark each.

   Each part is headed by its count, set large, since how many messages are at each step is
   the first thing the lane is read for. Beside the count is the size of those messages,
   when the average body size on the queue is known: the count times that average. */

/* A message as the lane draws it: where it is, how far through its visibility timeout, and, once it has left the
   queue, how. The hosted app's simulated messages (queuesim) and a backend's held messages both fit it. */
export type LaneMessage = {
  id: string;
  receiveCount: number;
  state: "visible" | "in_flight";
  sentMs: number;
  leftS: number; // in flight: seconds until its visibility timeout expires
  used: number; // in flight: share of the visibility timeout already gone, 0 to 1
  since: number; // the time (in the lane's clock, t) it entered its present state
  gone?: "deleted" | "dead_lettered";
};
type Live = LaneMessage;

const PITCH = 12; // a mark and the space after it
const STEP = 10; // a segment of an amount and the space after it
const MARK = 8;
const OPEN_W = 70; // a selected mark, wide enough for 8 characters of its message ID
const GAP = 4; // extra room on each side of an open mark, for its outline
const ROW_H = 26;
const PAD = 14;
const NICE = [1, 2, 5];
const words = (s: number) => (s >= 3600 ? "hour" : "minute");
const ago = (s: number) => (s < 90 ? `${Math.round(s)} s ago` : s < 5400 ? `${Math.round(s / 60)} min ago` : `${Math.round(s / 3600)} h ago`);

type Place = { key: string; x: number; y: number; w?: number; h?: number; bg: string; fade?: number; m?: Live; label: string };
type Bar = { state: Live["state"]; x: number; w: number; n: number };

export function QueueLane({
  live, gone, consumer, dlq, max, fifo, visible, inFlight, flow, windowS, exact, t, dead, deadNote = "Since this opened.", avgBytes, selected, onSelect, onGroup,
}: {
  live: Live[]; gone: Live[]; consumer: string; dlq: string | null; max: number | null; fifo: boolean;
  visible: number; inFlight: number;
  flow: number; // deleted in the last windowS seconds
  windowS: number; exact: boolean; t: number; dead: number;
  /* what the dead-lettered count covers */
  deadNote?: string;
  avgBytes?: number; // average message body size on this queue, in bytes, when it is known
  selected: string | null; onSelect: (id: string) => void; onGroup: (state: Live["state"]) => void;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const [w, setW] = useState(0);
  useLayoutEffect(() => {
    if (!ref.current) return;
    const ro = new ResizeObserver(([e]) => setW(e.contentRect.width));
    ro.observe(ref.current);
    return () => ro.disconnect();
  }, []);

  /* the four parts of the lane, by where each starts */
  const a = Math.round(w * 0.4), b = Math.round(w * 0.68), c = Math.round(w * 0.86);
  const cols = (from: number, to: number) => Math.max(1, Math.floor((to - from - PAD * 2) / PITCH));
  const wCols = cols(0, a), fCols = cols(a, b), dCols = cols(b, c), lCols = cols(c, w);
  /* an amount is a bar of segments, packed closer than messages are */
  const segs = (from: number, to: number) => Math.max(1, Math.floor((to - from - PAD * 2) / STEP));

  /* the scale: how many messages one mark is. One, while both amounts fit as marks. */
  let unit = 10 ** 9;
  for (let p = 0, done = false; p < 10 && !done; p++)
    for (const k of NICE) {
      const u = k * 10 ** p;
      const fits = u === 1 ? visible <= wCols * 2 && inFlight <= fCols * 2 : visible / u <= segs(0, a) && inFlight / u <= segs(a, b);
      if (fits) { unit = u; done = true; break; }
    }
  const one = unit === 1;

  const marks: Place[] = [];
  const say = (m: Live) => (m.gone === "dead_lettered" ? "dead-lettered" : m.gone === "deleted" ? "deleted" : m.state === "in_flight" ? `in flight, ${m.leftS} s left` : "visible");
  const single = (m: Live, x: number, y: number, fade?: number) => {
    const flying = m.state === "in_flight" && !m.gone;
    const last = flying && max !== null && m.receiveCount >= max;
    marks.push({
      key: m.id, m, x, y, fade, label: `Message ${m.id.slice(0, 8)}, ${say(m)}, receive count ${m.receiveCount}`,
      bg: m.gone === "dead_lettered" ? "var(--color-critical)" : m.gone === "deleted" ? "var(--color-ink-3)"
        : flying ? `linear-gradient(to top, ${last ? "var(--color-critical)" : "var(--color-ink-2)"} ${Math.round((1 - m.used) * 100)}%, var(--color-line-strong) 0)`
        : "var(--color-series-1)",
    });
  };
  /* an amount as a bar: segments of `unit` messages each, the last as wide as what is left
     over. The whole bar is one target, so it is easy to point at. */
  const bars: Bar[] = [];
  const amount = (n: number, state: Live["state"], bg: string, dir: 1 | -1, from: number) => {
    const full = Math.floor(n / unit), rest = (n % unit) / unit, count = full + (rest > 0 ? 1 : 0);
    let end = from;
    for (let i = 0; i < count; i++) {
      const width = i < full ? MARK : Math.max(4, Math.round(MARK * rest));
      const x = dir === 1 ? from + i * STEP : from - i * STEP - width;
      end = dir === 1 ? x + width : x;
      /* the segments fade out toward the far end of the bar (the back of the queue for what is visible), so
         a long bar reads as an amount trailing off; a short one is left solid */
      const fade = count <= 5 || i < count * 0.35 ? undefined : Math.max(0.1, 1 - ((i - count * 0.35) / (count * 0.65)) * 0.92);
      marks.push({ key: `${state}-${i}`, x, y: 2, w: width, h: 16, bg, fade, label: "" });
    }
    if (count) bars.push({ state, x: Math.min(from, end), w: Math.abs(end - from), n });
  };

  const waiting = live.filter((m) => m.state === "visible").sort((x, y) => x.sentMs - y.sentMs);
  const flying = live.filter((m) => m.state === "in_flight").sort((x, y) => x.leftS - y.leftS || x.sentMs - y.sentMs);
  const lost = gone.filter((m) => m.gone === "dead_lettered");
  const retriedFlying = flying.filter((m) => m.receiveCount > 1);
  if (one) {
    /* visible: the oldest is at the right, next to the consumer, and is received next */
    waiting.slice(0, wCols * 2).forEach((m, i) => single(m, a - PAD - MARK - (i % wCols) * PITCH, Math.floor(i / wCols) * ROW_H));
    /* in flight: in the order their visibility timeouts run out */
    flying.slice(0, fCols * 2).forEach((m, i) => single(m, a + PAD + (i % fCols) * PITCH, Math.floor(i / fCols) * ROW_H));
  } else {
    amount(visible, "visible", "var(--color-series-1)", -1, a - PAD);
    amount(inFlight, "in_flight", "var(--color-ink-2)", 1, a + PAD);
    retriedFlying.slice(0, fCols).forEach((m, i) => single(m, a + PAD + i * PITCH, ROW_H));
  }
  /* deleted: the most recent, newest first, fading as they age */
  const span = exact ? windowS : 9;
  gone.filter((m) => m.gone === "deleted" && t - m.since < span).reverse().slice(0, dCols * 2).forEach((m, i) => single(m, b + PAD + (i % dCols) * PITCH, Math.floor(i / dCols) * ROW_H, Math.max(0.35, 1 - (t - m.since) / span)));
  lost.slice(-lCols * 2).forEach((m, i) => single(m, c + PAD + (i % lCols) * PITCH, Math.floor(i / lCols) * ROW_H));

  /* The marks are drawn in the order of their keys, not of their place in the lane, so a
     mark's element never changes position among its siblings. When it did, React moved the
     element in the document as its message changed state, and a moved element starts its
     fade-in again and drops the slide it was in the middle of: the marks flickered. */
  marks.sort((x, y) => (x.key < y.key ? -1 : x.key > y.key ? 1 : 0));

  /* the selected message's mark, which is drawn open, and which part of the lane a mark is in */
  const open = marks.find((k) => k.m && selected === k.m.id);
  const openBack = !!open && open.x + MARK <= a;
  const part = (x: number) => (x < a ? 0 : x < b ? 1 : x < c ? 2 : 3);

  /* the head of a part: what it is, then how many messages are in it and their size */
  const stat = (left: number, width: number, label: string, n: number | null, tone = "text-ink") => (
    <div className="absolute top-0 overflow-hidden" style={{ left, width }}>
      <p className="truncate text-xs text-ink-3">{label}</p>
      {n !== null && (
        <p className="mt-0.5 flex items-baseline gap-2 whitespace-nowrap">
          <span className={`font-mono text-[22px] leading-7 font-medium tabular-nums ${tone}`}>{fmtNum(n)}</span>
          {avgBytes !== undefined && n > 0 && <Tooltip content="Message count times the average message body size on this queue" className="cursor-default font-mono text-caption tabular-nums text-ink-2">{fmtBytes(n * avgBytes)}</Tooltip>}
        </p>
      )}
    </div>
  );
  const foot = "absolute bottom-0 truncate text-xs text-ink-3";
  const idle = visible + inFlight === 0;
  const latest = idle ? [...gone].sort((x, y) => y.since - x.since)[0] : undefined;
  const scale = `One segment is ${fmtNum(unit)} messages.`;

  return (
    <div ref={ref} className="relative h-[152px]" role="group" aria-label="The queue as a lane, drawn to scale">
      {w > 0 && (
        <>
          {[a, b, c].map((x) => <i key={x} className="absolute inset-y-0 w-px bg-line" style={{ left: x }} aria-hidden />)}
          {stat(0, a - PAD, "Visible", visible)}
          {stat(a + PAD, b - a - PAD * 2, `In flight with ${consumer}`, inFlight)}
          {stat(b + PAD, c - b - PAD * 2, `Deleted in the last ${words(windowS)}`, flow)}
          {stat(c + PAD, w - c - PAD, dlq ? "Dead-lettered" : "No dead-letter queue", dlq ? dead : null, dead ? "text-critical-text" : "text-ink")}

          <div className="absolute inset-x-0 top-[60px] h-[46px]">
            {idle && <p className="absolute top-0.5 left-0 text-[13px] text-ink-2" style={{ width: a - PAD }}>The queue is empty.{latest ? ` The last message left it ${ago(t - latest.since)}.` : ""}</p>}
            {marks.map((k) => {
              const on = !!k.m && selected === k.m.id;
              /* the marks after the open one, in its row and its part of the lane, move along to make
                 room, so the open box has the same clear space around it as any other mark */
              const dx = on ? (openBack ? -GAP : GAP)
                : open && k.y === open.y && part(k.x) === part(open.x) && (openBack ? k.x < open.x : k.x > open.x) ? (openBack ? -1 : 1) * (OPEN_W - MARK + GAP * 2)
                : 0;
              /* the selected message's mark widens to carry the start of its message ID. The mark
                 itself stays as it was, at the left of the wider box, so its state still reads. */
              const paint = k.bg.startsWith("linear-gradient") ? k.bg : `linear-gradient(${k.bg}, ${k.bg})`;
              /* visible messages are packed against the line they cross next, so theirs opens away
                 from it, to the left; every other mark opens to the right */
              const back = k.x + MARK <= a;
              const style = on
                ? { transform: `translate(${Math.max(0, Math.min((back ? k.x + MARK - OPEN_W : k.x) + dx, w - OPEN_W - 3))}px, ${k.y}px)`, width: OPEN_W, height: k.h ?? 20, background: `${paint} ${back ? "right" : "left"} top / ${MARK}px 100% no-repeat var(--color-surface)` }
                : { transform: `translate(${k.x + dx}px, ${k.y}px)`, width: k.w ?? MARK, height: k.h ?? 20, background: k.bg, opacity: k.fade };
              const cls = `lane-mark absolute top-0 left-0 block rounded-[2px] outline-offset-2 ${on ? "z-[1] overflow-hidden text-left outline-2 outline-ink" : ""}`;
              /* the hover note is the app's Tooltip on the mark itself; its span is the positioned mark, so it moves
                 with it, and the button inside fills it */
              if (k.m) return (
                <Tooltip key={k.key} content={on ? undefined : <><span className="font-mono">{k.m.id.slice(0, 8)}</span> {say(k.m)}</>} className={`${cls} hover:outline-1 hover:outline-ink-3`} style={style}>
                  <button type="button" aria-pressed={on} aria-label={k.label} onClick={() => onSelect(k.m!.id)} className="absolute inset-0 block size-full cursor-pointer rounded-[2px]" />
                  {on && <span aria-hidden className={`pointer-events-none relative block font-mono text-[10.5px] leading-5 whitespace-nowrap text-ink ${back ? "pl-[5px]" : "pl-[13px]"}`}>{k.m.id.slice(0, 8)}</span>}
                </Tooltip>
              );
              return <i key={k.key} className={cls} style={style} aria-hidden />;
            })}
            {bars.map((r) => (
              <Tooltip key={r.state} content={`${fmtNum(r.n)} ${r.state === "visible" ? "visible" : "in flight"}. Select to list them.`}
                className="absolute -top-0.5 h-6 rounded-[3px] outline-offset-1 hover:outline-1 hover:outline-ink-3" style={{ left: r.x - 3, width: Math.max(18, r.w + 6) }}>
                <button type="button" onClick={() => onGroup(r.state)} aria-label={`${fmtNum(r.n)} ${r.state === "visible" ? "visible" : "in flight"}. List them`} className="absolute inset-0 block size-full cursor-pointer" />
              </Tooltip>
            ))}
          </div>

          <p className={foot} style={{ left: 0, width: a - PAD }}>
            {one
              ? idle ? "A mark here is one message." : <>Oldest on the right, received next{fifo ? ", in order within a message group" : ""}.</>
              : <>{scale}{flow > 0 && windowS === 60 && visible / flow >= 1.5 && <> About {Math.round(visible / flow)} minutes of work at this rate.</>}</>}
          </p>
          <p className={foot} style={{ left: a + PAD, width: b - a - PAD * 2 }}>
            {one ? "A mark empties as its visibility timeout runs out." : retriedFlying.length > 0 ? <>Below, one mark each: {fmtNum(retriedFlying.length)} received more than once.</> : "None listed has been received more than once."}
          </p>
          <p className={foot} style={{ left: b + PAD, width: c - b - PAD * 2 }}>{exact && windowS > 60 ? "One mark each. Select one to see it." : "The latest, one mark each."}</p>
          <p className={foot} style={{ left: c + PAD, width: w - c - PAD }}>{dlq ? deadNote : ""}</p>
        </>
      )}
    </div>
  );
}
