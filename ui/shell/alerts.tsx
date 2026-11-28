/*
  Alerts on this server (QV-592): the Alerts page at /alerts, in the shell's frame (frame.tsx) on the design system,
  and the alert data the Stuck page's "Alert me" reads. Both read /api/alerts (server/src/trailbeam/server/alerts_api.py):
  the rules (GET /rules), the channels (GET /channels) and the last check (GET /state), and send each change through it:
  a rule made on a cause (POST /rules), edited (PATCH /rules/{id}), muted or snoozed (POST /rules/{id}/mute) or deleted
  (DELETE /rules/{id}); a channel added (POST /channels), tested (POST /channels/{id}/test) or removed (DELETE
  /channels/{id}). After each, the data is read again, so the page shows what the server holds. The views are the
  shared ones (@trailbeam/ui/design AlertRules, AlertChannels, AlertMe), the same as the hosted app's.
  The routes answer 404 while the scheduled check is off (TRAILBEAM_ALERT_CHECK=off): the page then says how to turn
  it on, and Stuck shows no "Alert me".
*/
import { useCallback, useEffect, useState } from "react";
import {
  channelRowsOf, checkStatus, rulesViewOf,
  type AlertHrefs, type AlertResult, type ApiChannel, type ApiRules, type ApiState, type ChannelActions, type ChannelRow, type RuleActions, type RulesView,
} from "../src/index.ts";
import { AlertChannels, AlertRules, PageHeader, Problem, SettingsSection } from "../src/design/index.ts";
import { ApiError, getJSON, getMe, sendJSON, type Me } from "./chrome.tsx";
import { ShellFrame, ShellSearch } from "./frame.tsx";

export type AlertData = {
  view: RulesView; channels: ChannelRow[]; canManage: boolean;
  checkedAt: number | null; hasChannel: boolean; everyMin: number;
};
export type AlertsState = { kind: "loading" } | { kind: "off" } | { kind: "error"; message: string } | { kind: "ok"; data: AlertData };

/** The setting that turns the scheduled check, and with it these routes, on (alerts_scheduler.ENV). */
export const ALERT_SETTING = "TRAILBEAM_ALERT_CHECK";
/** The setting that lets webhooks go to this server's own network (alerts_api.PRIVATE_WEBHOOKS_ENV, QV-764). */
export const PRIVATE_WEBHOOKS_SETTING = "TRAILBEAM_ALERT_ALLOW_PRIVATE_WEBHOOKS";

export const SHELL_ALERT_HREFS: AlertHrefs = {
  channels: "/alerts#channels",
  cause: (id) => `/issues/${encodeURIComponent(id)}`,
  causes: { href: "/issues", label: "Issues" },
};

/** Who made or muted something, as the server stores it (a person's ID, or how the request was let in), as a name. */
export const byName = (me: Me | null) => (by: string) => (me?.user && by === String(me.user.id) ? "you" : "");

/** A change through the API, as the views take it: a refusal is the server's sentence. */
const result = (send: () => Promise<unknown>) => async (): Promise<AlertResult> => {
  try { await send(); return { ok: true }; } catch (e) { return { ok: false, error: e instanceof Error ? e.message : String(e) }; }
};

/**
  The alert data and the changes the views send. `secrets`: a webhook's signing secret, which POST /channels answers
  once; it is kept for as long as the page is open.
*/
export function useAlerts() {
  const [state, setState] = useState<AlertsState>({ kind: "loading" });
  const [secrets, setSecrets] = useState<Record<string, string>>({});

  // read once /api/me is: while it says the check is off the routes would only answer 404s, each a console error
  // (QV-766); a server that doesn't say is asked
  const load = useCallback(() => getMe().then((m) => m?.features && !m.features.alerts ? setState({ kind: "off" }) : Promise.all([
    getJSON<ApiRules>("/api/alerts/rules"),
    getJSON<{ channels: ApiChannel[]; canManage: boolean }>("/api/alerts/channels"),
    getJSON<ApiState>("/api/alerts/state"),
  ]).then(([rules, ch, st]) => setState((was) => {
    const data: AlertData = {
      view: rulesViewOf(rules, ch.channels, st, byName(m)), channels: channelRowsOf(ch.channels), canManage: ch.canManage,
      checkedAt: st.checkedAt, hasChannel: st.hasChannel, everyMin: st.everyMin ?? rules.everyMin ?? 5,
    };
    return was.kind === "ok" && JSON.stringify(was.data) === JSON.stringify(data) ? was : { kind: "ok", data };
  }), (e: Error) => setState(e instanceof ApiError && e.status === 404 ? { kind: "off" } : { kind: "error", message: e.message }))), []);

  useEffect(() => { void load(); }, [load]);

  const rules: RuleActions = {
    add: (causeId, form) => result(() => sendJSON("POST", "/api/alerts/rules", { causeId, ...form }))(),
    edit: (id, form) => result(() => sendJSON("PATCH", `/api/alerts/rules/${encodeURIComponent(id)}`, form))(),
    mute: (id, forS) => result(() => sendJSON("POST", `/api/alerts/rules/${encodeURIComponent(id)}/mute`, { forS }))(),
    remove: (id) => result(() => sendJSON("DELETE", `/api/alerts/rules/${encodeURIComponent(id)}`))(),
  };
  const channels: ChannelActions = {
    add: (form) => result(async () => {
      const r = await sendJSON<{ channel: ApiChannel; secret: string | null }>("POST", "/api/alerts/channels", form);
      const secret = r.secret;
      if (secret) setSecrets((s) => ({ ...s, [r.channel.id]: secret }));
    })(),
    // a failed send is a 200 with ok false and the reason, noted on the channel
    test: async (id) => {
      try {
        const r = await sendJSON<{ ok: boolean; error?: string }>("POST", `/api/alerts/channels/${encodeURIComponent(id)}/test`, {});
        return r.ok ? { ok: true } : { ok: false, error: r.error ?? "Sending failed." };
      } catch (e) { return { ok: false, error: e instanceof Error ? e.message : String(e) }; }
    },
    remove: (id) => result(() => sendJSON("DELETE", `/api/alerts/channels/${encodeURIComponent(id)}`))(),
  };
  const withSecrets = (rows: ChannelRow[]) => rows.map((c) => ({ ...c, secret: secrets[c.id] ?? null }));
  return { state, load, rules, channels, withSecrets };
}

export const ALERTS_ABOUT = "Trailbeam checks every alert rule on a schedule and sends each change to the rule's channels: when a rule starts firing, "
  + "moves between warning and critical, or recovers. A rule that stays firing is not sent again.";
export const SECRET_NOTE = "Shown only now: copy it into the receiving end, which checks each request's signature with it. Remove the channel and add it again for a new secret.";
export const CHANNELS_ABOUT = "Where alerts go: a Slack channel, a webhook, or email through this server's mail server.";
export const PRIVATE_WEBHOOKS_TEXT = "A webhook must be https on a public host unless the server is started with";
export const PRIVATE_WEBHOOKS_AFTER = "set to 1: then it can also be an internal endpoint, such as http://alertmanager:9093/… or https://relay.internal/…. Plain http goes only to private addresses, and cloud metadata addresses stay refused.";
export const OFF_TEXT = `Alerts are off on this server. Start it with ${ALERT_SETTING} set to on (a check every 5 minutes) or to a number of minutes from 1 to 1440.`;

export function AlertsPage() {
  const { state, load, rules, channels, withSecrets } = useAlerts();
  useEffect(() => { document.title = "Alerts, Trailbeam"; }, []);
  const changed = () => { void load(); };
  const data = state.kind === "ok" ? state.data : null;
  const check = data ? checkStatus({ checkedAt: data.checkedAt, now: Date.now(), hasChannel: data.hasChannel, everyMin: data.everyMin, addChannel: "Add one below." }) : null;

  return (
    <ShellFrame path="/alerts">
      <PageHeader title="Alerts" subtitle="What Trailbeam tells you about, and where it sends it." search={<ShellSearch />} />
      <div className="grid max-w-5xl gap-12 px-8 pb-16">
        {state.kind === "loading" && <p className="text-sm text-ink-3">Loading alerts</p>}
        {state.kind === "error" && <Problem>{state.message}</Problem>}
        {state.kind === "off" && (
          <p className="max-w-2xl rounded-card border border-line bg-surface px-5 py-4 text-sm leading-relaxed text-ink-2" data-alerts-off>
            {OFF_TEXT} Alerts email through the same mail server as invites (<code className="font-mono text-caption">TRAILBEAM_SMTP_HOST</code>), and their links point to <code className="font-mono text-caption">--public-url</code>.
          </p>
        )}
        {data && check && (
          <>
            <SettingsSection id="rules" title="Rules" about={<>{ALERTS_ABOUT}
              <span role={check.late ? "alert" : undefined} className={`mt-1 block text-xs ${check.late ? "text-critical-text" : "text-ink-3"}`}>{check.text}</span></>}>
              <AlertRules view={data.view} canManage={data.canManage} actions={rules} hrefs={SHELL_ALERT_HREFS} changed={changed} />
            </SettingsSection>
            <SettingsSection id="channels" title="Channels" about={<>{CHANNELS_ABOUT}
              <span className="mt-1 block text-xs text-ink-3" data-private-webhooks>{PRIVATE_WEBHOOKS_TEXT} <code className="font-mono text-caption">{PRIVATE_WEBHOOKS_SETTING}</code> {PRIVATE_WEBHOOKS_AFTER}</span></>}>
              <AlertChannels channels={withSecrets(data.channels)} canManage={data.canManage} actions={channels} secretNote={SECRET_NOTE} changed={changed} />
            </SettingsSection>
          </>
        )}
      </div>
    </ShellFrame>
  );
}
