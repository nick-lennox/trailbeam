"use client";

/*
  The alert views on the design system (QV-592), shared by self-hosted Trailbeam and the hosted app: "Alert me" on a
  cause (a ghost pill that says the rule's state in words and an icon, never colour alone, and opens a popover with
  the rule's form; QV-358 design sections 4 and 5), the list of rules (section 6), and the alert channels with their
  form. They take the data as props (types.ts) and call back for every change: the hosted app passes its server
  actions, self-hosted Trailbeam a fetch to /api/alerts. After a change goes through, `changed` runs (the host reads
  its data again); without it, the Nav's refresh. The words are Alerts.ts's. The check's cadence comes from the host,
  so the form never claims a check more often than the server runs one.
  A client module, reached through @trailbeam/ui/design, never the main entry (test/server-import.test.ts).
*/
import { useEffect, useId, useRef, useState, useTransition, type FormEvent } from "react";
import { Bell, BellOff, BellRing } from "lucide-react";
import { Button } from "../design/Button.tsx";
import { Card } from "../design/Card.tsx";
import { CopyText } from "../design/CopyText.tsx";
import { UiLink, useNav } from "../design/nav.tsx";
import { Field, FormError, SELECT, StateMark } from "../design/Settings.tsx";
import { Tooltip } from "../design/Tooltip.tsx";
import {
  HINT, KIND, PLACEHOLDER, dashboardToneText, deliveredWhen, heldText, heldWord, hm, ruleState, sortRules, toneText, whenText,
} from "./Alerts.ts";
import type {
  AlertHrefs, AlertResult, ChannelActions, ChannelForm, ChannelKind, ChannelRow, MinTone, RuleActions, RuleForm, RuleMode, RuleView, RulesView,
} from "./types.ts";

const SMALL_SELECT = "h-8 rounded-control border border-line bg-canvas px-2 text-sm text-ink outline-none hover:border-line-strong focus:border-brand";

/** Runs a change, shows its refusal, and reads the host's data again once it went through. */
function useChange(changed?: () => void) {
  const nav = useNav();
  const [error, setError] = useState<string | null>(null);
  const [busy, start] = useTransition();
  const run = (f: () => Promise<AlertResult>, then?: () => void) => start(async () => {
    setError(null);
    let r: AlertResult;
    try { r = await f(); } catch (e) { r = { ok: false, error: e instanceof Error ? e.message : String(e) }; }
    if (!r.ok) return setError(r.error);
    then?.();
    (changed ?? nav.refresh)();
  });
  return { error, setError, busy, run };
}

export type AlertMeProps = {
  cause: { id: string; label: string; queue: string };
  rule: RuleView | null; view: RulesView;
  /** whoever may add a channel: shown the way to add one when there is none */
  canManage: boolean;
  actions: RuleActions; hrefs: Pick<AlertHrefs, "channels">;
  /** sample data: the control is off, and says why */
  sample?: boolean;
  /** who may add a channel, in words: "an admin" (self-hosted), "an owner or admin" (the hosted app) */
  managers?: string;
  changed?: () => void;
};

/** "Alert me" on a cause: the pill, and the rule's form in a popover (a bottom sheet on a phone). */
export function AlertMe({ cause, rule, view, canManage, actions, hrefs, sample = false, managers = "an admin", changed }: AlertMeProps) {
  const [open, setOpen] = useState(false);
  const button = useRef<HTMLButtonElement>(null);
  const box = useRef<HTMLDivElement>(null);
  const s = ruleState(rule, view.channels);
  const Icon = s.icon === "bell" ? Bell : s.icon === "ring" ? BellRing : BellOff;

  useEffect(() => {
    if (!open) return;
    const close = () => { setOpen(false); button.current?.focus(); };
    const key = (e: KeyboardEvent) => { if (e.key === "Escape") close(); };
    const click = (e: MouseEvent) => { if (!box.current?.contains(e.target as Node) && !button.current?.contains(e.target as Node)) close(); };
    document.addEventListener("keydown", key);
    document.addEventListener("mousedown", click);
    box.current?.querySelector<HTMLElement>("input, select, button, a")?.focus();
    return () => { document.removeEventListener("keydown", key); document.removeEventListener("mousedown", click); };
  }, [open]);

  if (sample) return (
    <Tooltip content="Sample data. Alerts work on your own messages." className="shrink-0">
      <Button size="sm" variant="ghost" disabled className="text-ink-3">
        <Bell size={14} aria-hidden /><span className="hidden md:inline">Alert me</span><span className="sr-only md:hidden">Alert me</span>
      </Button>
    </Tooltip>
  );
  return (
    <div className="relative shrink-0">
      <Button ref={button} size="sm" variant="ghost" aria-label={s.aria} aria-expanded={open} aria-haspopup="dialog" onClick={() => setOpen((o) => !o)}
        className={s.tone}>
        {s.firing && <span aria-hidden className="size-2 shrink-0 rounded-[2px] bg-critical" />}
        <Icon size={14} aria-hidden /><span className="hidden md:inline" suppressHydrationWarning>{s.word}</span>
      </Button>
      {open && (
        <div ref={box} role="dialog" aria-label={rule ? "Edit the alert on this cause" : "Alert me about this cause"}
          className="fixed inset-x-0 bottom-0 z-40 max-h-[80vh] overflow-y-auto rounded-t-card border border-line bg-surface p-4 shadow-card sm:absolute sm:inset-x-auto sm:bottom-auto sm:right-0 sm:top-full sm:mt-1 sm:w-[22rem] sm:rounded-card">
          {view.channels.length === 0
            ? <NoChannels href={hrefs.channels} canManage={canManage} managers={managers} />
            : <RuleFormView cause={cause} rule={rule} view={view} actions={actions} changed={changed} done={() => { setOpen(false); button.current?.focus(); }} />}
        </div>
      )}
    </div>
  );
}

function NoChannels({ href, canManage, managers }: { href: string; canManage: boolean; managers: string }) {
  return (
    <div className="grid gap-3 text-sm text-ink-2">
      <p>Alerts go to an alert channel, and there is none yet.</p>
      {canManage ? <Button size="sm" variant="primary" href={href} className="justify-self-start">Add a channel</Button> : <p>Ask {managers} to add one.</p>}
    </div>
  );
}

/** The rule's form: where it sends, when, and for a rule that exists, mute or snooze and delete. */
export function RuleFormView({ cause, rule, view, actions, done, changed }: {
  cause: { id: string; label: string; queue: string }; rule: RuleView | null; view: RulesView; actions: RuleActions; done: () => void; changed?: () => void;
}) {
  const id = useId();
  const [channels, setChannels] = useState<string[]>(rule ? rule.channels : view.channels.slice(0, 1).map((c) => c.id));
  const [mode, setMode] = useState<RuleMode>(rule?.mode ?? "quiet");
  const [quietS, setQuietS] = useState(rule && rule.mode === "quiet" && view.quiet.some((q) => q.s === rule.quietS) ? rule.quietS : view.defaultQuiet);
  const { error, setError, busy, run } = useChange(changed);
  const save = (e: FormEvent) => {
    e.preventDefault();
    if (!channels.length) return setError("Pick at least one channel.");
    const form: RuleForm = { channels, mode, quietS: mode === "every" ? 0 : quietS };
    run(() => (rule ? actions.edit(rule.id, form) : actions.add(cause.id, form)), done);
  };
  const muted = rule?.mutedUntil != null;

  return (
    <form onSubmit={save} className="grid gap-4 text-sm">
      <div>
        <h3 className="font-semibold text-ink">{rule ? "Alert on this cause" : "Alert me about this cause"}</h3>
        <p className="mt-0.5 text-xs text-ink-3">{cause.label}</p>
        {cause.queue && <p className="mt-0.5 font-mono text-caption text-ink-3">{cause.queue}</p>}
        {rule?.firingSince != null && <p className="mt-2 text-xs text-critical-text" suppressHydrationWarning>Firing since {hm(rule.firingSince)}.</p>}
        {rule && !rule.checked && <p className="mt-2 text-xs text-ink-3">Not checked yet. {view.cadence}</p>}
        {rule?.held && !muted && <p className="mt-2 text-xs text-ink-3" suppressHydrationWarning>{heldText(rule.held)}</p>}
        {muted && <p className="mt-2 text-xs text-ink-3" suppressHydrationWarning>{rule!.forever ? "Muted" : `Snoozed until ${hm(rule!.mutedUntil!)}`}{rule!.mutedBy ? ` by ${rule!.mutedBy}` : ""}. Nothing is sent until then; the page still shows whether it is firing.</p>}
      </div>
      {muted && <Button type="button" size="sm" variant="secondary" disabled={busy} className="justify-self-start" onClick={() => run(() => actions.mute(rule!.id, 0))}>{rule!.forever ? "Unmute" : "Wake now"}</Button>}

      <fieldset className="grid gap-1.5">
        <legend className="mb-1 text-xs font-medium text-ink-2">Send to</legend>
        {view.channels.map((c) => (
          <label key={c.id} className="flex items-center gap-2 text-ink">
            <input type="checkbox" checked={channels.includes(c.id)} onChange={(e) => setChannels((x) => (e.target.checked ? [...x, c.id] : x.filter((y) => y !== c.id)))} />
            <span>{c.name} <span className="text-ink-3">({KIND[c.kind]}{c.minTone === "critical" ? ", critical only for dashboards" : ""})</span></span>
          </label>
        ))}
      </fieldset>

      <fieldset className="grid gap-1.5">
        <legend className="mb-1 text-xs font-medium text-ink-2">When</legend>
        <label className="flex flex-wrap items-center gap-2 text-ink">
          <input type="radio" name={`${id}-mode`} checked={mode === "quiet"} onChange={() => setMode("quiet")} />
          <span>New messages hit it after it has been quiet for</span>
          <select aria-label="Quiet for" className={SMALL_SELECT} value={quietS} disabled={mode !== "quiet"} onChange={(e) => setQuietS(Number(e.target.value))}>
            {view.quiet.map((q) => <option key={q.s} value={q.s}>{q.text}</option>)}
          </select>
        </label>
        <label className="flex items-center gap-2 text-ink">
          <input type="radio" name={`${id}-mode`} checked={mode === "every"} onChange={() => setMode("every")} />
          <span>Every check that finds new messages, at most one message a check</span>
        </label>
        <p className="text-xs text-ink-3">{view.cadence}</p>
      </fieldset>

      <FormError>{error}</FormError>
      <div className="flex flex-wrap items-center gap-2">
        {rule && !muted && (
          <select aria-label="Mute or snooze" className={SMALL_SELECT} value="" disabled={busy}
            onChange={(e) => { const v = e.target.value; if (v) run(() => actions.mute(rule.id, v === "mute" ? null : Number(v))); }}>
            <option value="">Mute or snooze</option>
            <option value="mute">Mute</option>
            <option value="3600">Snooze 1 h</option>
            <option value="14400">Snooze 4 h</option>
            <option value="86400">Snooze 24 h</option>
          </select>
        )}
        {rule && <Button type="button" size="sm" variant="ghost" disabled={busy} onClick={() => { if (window.confirm("Delete this alert? It stops at once.")) run(() => actions.remove(rule.id), done); }}>Delete</Button>}
        <span className="ml-auto" />
        {!rule && <Button type="button" size="sm" variant="ghost" onClick={done}>Cancel</Button>}
        <Button type="submit" size="sm" variant="primary" disabled={busy}>{rule ? "Save" : "Turn on alert"}</Button>
      </div>
      {!rule && <p className="-mt-2 text-xs text-ink-3">Trailbeam works out whether it is firing at the next check.</p>}
    </form>
  );
}

export type AlertRulesProps = {
  view: RulesView; canManage: boolean; actions: RuleActions; hrefs: AlertHrefs; managers?: string; changed?: () => void;
};

/** The list of rules: firing first, then on, snoozed, muted; dashboard rules last, read-only, edited on their dashboard. */
export function AlertRules({ view, canManage, actions, hrefs, managers, changed }: AlertRulesProps) {
  const rules = sortRules(view.rules);
  const name = (id: string) => view.channels.find((c) => c.id === id)?.name;
  if (!rules.length && !view.dashboards.length) return (
    <p className="rounded-card border border-line bg-surface px-5 py-4 text-sm text-ink-2">
      No alert rules yet. Make one from <UiLink href={hrefs.causes.href} className="font-medium text-brand hover:underline">{hrefs.causes.label}</UiLink>, on a cause.
    </p>
  );
  return (
    <ul className="rounded-card border border-line bg-surface" aria-label="Alert rules">
      {rules.map((r) => {
        const to = r.channels.map(name).filter((n): n is string => !!n);
        const notes = [
          r.createdBy ? `Made by ${r.createdBy}.` : "",
          r.checked ? "" : "Not checked yet.",
        ].filter(Boolean).join(" ");
        return (
          <li key={r.id} data-rule={r.id} className={`flex flex-wrap items-center gap-x-4 gap-y-1 border-b border-line px-5 py-3 last:border-0 ${r.firingSince !== null ? "border-l-2 border-l-critical" : ""}`}>
            <div className="min-w-0 flex-1">
              <UiLink href={hrefs.cause(r.causeId)} className="font-medium text-ink hover:underline">{r.label}</UiLink>
              {r.queue && <span className="ml-2 font-mono text-caption text-ink-3">{r.queue}</span>}
              <p className="text-xs text-ink-3">
                {whenText(r, view.quiet)}, sent to {to.length ? to.join(", ") : <span className="text-critical-text">no channel: its channels were removed</span>}.
                {notes && ` ${notes}`}
                {r.held && r.mutedUntil === null && <span suppressHydrationWarning> {heldWord(r.held)}{r.held.by ? `, ignored by ${r.held.by}` : ""}.</span>}
              </p>
            </div>
            <AlertMe cause={{ id: r.causeId, label: r.label, queue: r.queue }} rule={r} view={view} canManage={canManage} actions={actions} hrefs={hrefs} managers={managers} changed={changed} />
          </li>
        );
      })}
      {view.dashboards.map((d) => (
        <li key={`${d.dashboardId}/${d.title}`} className="flex flex-wrap items-center gap-x-4 gap-y-1 border-b border-line px-5 py-3 last:border-0">
          <div className="min-w-0 flex-1">
            <span className="text-ink">{d.title}</span>{d.dashboardName && <span className="text-ink-3"> on the dashboard &ldquo;{d.dashboardName}&rdquo;</span>}
            <p className="text-xs text-ink-3">Dashboard rule, {dashboardToneText(d.tone)} at the last check. Sent to each channel by its Send setting.</p>
          </div>
          {hrefs.dashboard && <UiLink href={hrefs.dashboard(d.dashboardId)} className="text-xs font-medium text-brand hover:underline">Edit on the dashboard</UiLink>}
        </li>
      ))}
    </ul>
  );
}

export type AlertChannelsProps = {
  channels: ChannelRow[]; canManage: boolean; actions: ChannelActions;
  /** said when there is no channel: where alerts still show without one */
  empty?: string;
  /** said under a signing secret, when the host shows it only once */
  secretNote?: string;
  /** who may add or change a channel, in words, for everyone else */
  managers?: string;
  changed?: () => void;
};

/**
  Where alerts go. The scheduled check sends each change in an alert rule (fired, warning to critical and back,
  resolved) to every channel here that wants it.
*/
export function AlertChannels({ channels, canManage, actions, empty, secretNote, managers = "an admin", changed }: AlertChannelsProps) {
  const [kind, setKind] = useState<ChannelKind>("slack");
  const [name, setName] = useState("");
  const [target, setTarget] = useState("");
  const [minTone, setMinTone] = useState<MinTone>("warning");
  const [note, setNote] = useState<{ id: string; text: string; ok: boolean } | null>(null);
  const { error, setError, busy, run } = useChange(changed);
  const nav = useNav();
  const [testing, startTest] = useTransition();

  const add = (e: FormEvent) => {
    e.preventDefault();
    setNote(null);
    const form: ChannelForm = { kind, name, target, minTone };
    run(() => actions.add(form), () => { setName(""); setTarget(""); });
  };
  // a failed test is a note on the channel, not a refusal: the host noted it on the channel either way
  const test = (id: string) => startTest(async () => {
    setNote(null); setError(null);
    let r: AlertResult;
    try { r = await actions.test(id); } catch (e) { r = { ok: false, error: e instanceof Error ? e.message : String(e) }; }
    setNote({ id, ok: r.ok, text: r.ok ? "Test sent." : r.error });
    (changed ?? nav.refresh)();
  });
  const remove = (c: ChannelRow) => {
    if (!window.confirm(`Remove ${c.name}? Alerts stop going to it at once.`)) return;
    run(() => actions.remove(c.id));
  };
  const off = busy || testing;

  return (
    <div className="grid gap-4">
      <Card>
        {channels.length === 0 ? (
          <p className="px-5 py-4 text-sm text-ink-2">{empty ?? "No channels yet. Add one to be told when an alert rule starts firing or recovers."}</p>
        ) : (
          <ul aria-label="Alert channels">
            {channels.map((c) => (
              <li key={c.id} data-channel={c.id} className="grid gap-2 border-b border-line px-5 py-3.5 last:border-0">
                <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
                  <span className="text-sm font-medium text-ink">{c.name}</span>
                  <span className="text-xs text-ink-3">{KIND[c.kind]}, {toneText(c.minTone)}</span>
                  <span className="flex-1" />
                  {c.lastAt === null ? <StateMark tone="idle">Nothing sent yet</StateMark>
                    : <StateMark tone={c.lastOk ? "good" : "critical"}><span suppressHydrationWarning>{c.lastOk ? "Delivered" : "Failed"} {deliveredWhen(c.lastAt)}</span></StateMark>}
                  {canManage && (
                    <span className="flex gap-1">
                      <Button size="sm" variant="ghost" disabled={off} onClick={() => test(c.id)}>Send test</Button>
                      <Button size="sm" variant="ghost" disabled={off} onClick={() => remove(c)}>Remove</Button>
                    </span>
                  )}
                </div>
                <span className="truncate font-mono text-caption text-ink-2">{c.shown}</span>
                {c.lastOk === false && c.lastDetail && <span className="text-xs text-critical-text">{c.lastDetail}</span>}
                {note?.id === c.id && <span role="status" className={`text-xs ${note.ok ? "text-good-text" : "text-critical-text"}`}>{note.text}</span>}
                {c.secret && (
                  <div className="grid gap-1 text-xs text-ink-3">
                    <div className="flex flex-wrap items-center gap-2">
                      Signing secret <code className="font-mono text-caption text-ink-2">{c.secret}</code>
                      <CopyText value={c.secret} label="Copy the signing secret" />
                    </div>
                    {secretNote && <p>{secretNote}</p>}
                  </div>
                )}
              </li>
            ))}
          </ul>
        )}
      </Card>

      {canManage ? (
        <form onSubmit={add} className="flex flex-wrap items-end gap-2" aria-label="Add an alert channel">
          <label className="grid gap-1.5 self-start">
            <span className="text-xs font-medium text-ink-2">Type</span>
            <select value={kind} onChange={(e) => setKind(e.target.value as ChannelKind)} className={SELECT}>
              <option value="slack">Slack</option>
              <option value="webhook">Webhook</option>
              <option value="email">Email</option>
            </select>
          </label>
          <Field label="Name" value={name} onChange={(e) => setName(e.target.value)} placeholder="#oncall" className="w-44" maxLength={60} />
          <Field label={kind === "email" ? "Addresses" : "URL"} mono value={target} onChange={(e) => setTarget(e.target.value)} required
            placeholder={PLACEHOLDER[kind]} className="min-w-72 flex-1" hint={HINT[kind]} />
          <label className="grid gap-1.5 self-start">
            <span className="text-xs font-medium text-ink-2">Send</span>
            <select value={minTone} onChange={(e) => setMinTone(e.target.value as MinTone)} className={SELECT}>
              <option value="warning">Warning and critical</option>
              <option value="critical">Critical only</option>
            </select>
          </label>
          <Button variant="primary" size="sm" type="submit" disabled={off} className="mt-[22px] h-9 self-start">{busy ? "Adding" : "Add channel"}</Button>
          <div className="basis-full"><FormError>{error}</FormError></div>
        </form>
      ) : (
        <>
          <FormError>{error}</FormError>
          <p className="text-xs text-ink-3">Only {managers} can add or change alert channels.</p>
        </>
      )}
    </div>
  );
}
