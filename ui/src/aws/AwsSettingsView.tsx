import { useEffect, useId, useState } from "react";
import { Button } from "../design/Button.tsx";
import { Caution, Field, FormError, INPUT, SettingsSection, StateMark } from "../design/Settings.tsx";
import { utc } from "../queue/model.ts";
import { AWS_ABOUT, AWS_ACTIONS, changeOf, changed, formOf, queueList, statusLine, type AwsForm, type AwsSettingsAnswer } from "./settings.ts";

/*
  The AWS section of the Settings page (QV-589), for admins: whether this server reads what AWS says about its queues,
  and which queues. The host reads GET /api/settings/aws and saves with PATCH; `onSave` resolves with the server's
  answer or rejects with its sentence, which the form shows. Off by default; the credentials are never typed here.
*/
export type AwsSettingsViewProps = {
  answer: AwsSettingsAnswer;
  onSave: (change: ReturnType<typeof changeOf>) => Promise<AwsSettingsAnswer>;
};

export function AwsSettingsView({ answer, onSave }: AwsSettingsViewProps) {
  const [got, setGot] = useState(answer);
  const [form, setForm] = useState<AwsForm>(() => formOf(answer.settings));
  const [busy, setBusy] = useState(false);
  const [problem, setProblem] = useState("");
  const [saved, setSaved] = useState(false);
  const queuesId = useId();
  useEffect(() => { setGot(answer); setForm(formOf(answer.settings)); }, [answer]);

  const s = got.settings;
  const line = statusLine(got.status, s);
  const dirty = changed(form, s);
  const names = queueList(form.queues);
  const set = (p: Partial<AwsForm>) => { setForm((f) => ({ ...f, ...p })); setSaved(false); };
  const save = async () => {
    setBusy(true);
    setProblem("");
    try {
      const next = await onSave(changeOf(form));
      setGot(next);
      setForm(formOf(next.settings));
      setSaved(true);
    } catch (e) {
      setProblem((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <SettingsSection id="aws" title="AWS" about={AWS_ABOUT}>
      <div className="grid max-w-2xl gap-5">
        <div className="grid gap-1.5">
          <StateMark tone={line.tone}>{line.tone === "good" ? "Reading from AWS" : line.tone === "warning" ? "Can't read from AWS" : "Not reading from AWS"}</StateMark>
          <p className="text-sm leading-relaxed text-ink-2">{line.text}</p>
          {s.updatedAt !== null && <p className="text-xs text-ink-3">Last changed at <span className="font-mono text-caption">{utc(s.updatedAt, { date: true })}</span> UTC by {s.updatedBy ?? "someone"}.</p>}
        </div>
        {got.status.on && got.status.problem && <Caution>{got.status.problem}</Caution>}

        <form className="grid gap-4" onSubmit={(e) => { e.preventDefault(); if (dirty && !busy) void save(); }}>
          <label className="flex items-center gap-2.5 text-sm text-ink">
            <input type="checkbox" checked={form.enabled} onChange={(e) => set({ enabled: e.target.checked })} className="size-4 accent-[var(--color-brand)]" />
            Read from AWS for the queues named below
          </label>
          <div className="grid gap-1.5">
            <label htmlFor={queuesId} className="text-xs font-medium text-ink-2">Queues</label>
            <textarea id={queuesId} rows={5} value={form.queues} onChange={(e) => set({ queues: e.target.value })} spellCheck={false}
              placeholder={"orders\npayments-*"} aria-describedby={`${queuesId}-hint`}
              className={`${INPUT} h-auto py-2 font-mono text-caption`} />
            <span id={`${queuesId}-hint`} className="text-xs text-ink-3">
              One queue name per line. A * matches any letters, so payments-* names every queue whose name starts with payments-.
              {names.length > 0 && ` ${names.length} ${names.length === 1 ? "name" : "names"}.`}
            </span>
          </div>
          <div className="grid gap-4 sm:grid-cols-2">
            <Field label="Region" mono value={form.region} onChange={(e) => set({ region: e.target.value })} placeholder="us-east-1"
              hint="For queues whose spans don't name a region. A queue's own URL always wins." />
            <Field label="AWS account ID" mono value={form.accountId} onChange={(e) => set({ accountId: e.target.value })} placeholder="123456789012" inputMode="numeric"
              hint="Optional. When set, a queue in another account is not read." />
          </div>
          <FormError>{problem}</FormError>
          <div className="flex items-center gap-3">
            <Button variant="primary" type="submit" disabled={!dirty || busy}>{busy ? "Saving" : "Save"}</Button>
            {saved && !dirty && <span role="status" className="text-sm text-ink-3">Saved</span>}
          </div>
        </form>

        <div className="grid gap-2 border-t border-line pt-4 text-sm leading-relaxed text-ink-2">
          <p>
            Trailbeam uses the server&rsquo;s own AWS credentials, found the usual way: environment variables, a profile, or an instance or task role.
            It never stores them. They need only these read-only permissions:
          </p>
          <ul className="grid gap-0.5 pl-1">{AWS_ACTIONS.map((a) => <li key={a} className="font-mono text-caption text-ink">{a}</li>)}</ul>
          <p>
            It never receives, deletes or changes a message. CloudWatch&rsquo;s GetMetricData is billed by AWS, so each read is kept for a minute,
            queue settings for 15 seconds and Lambda triggers for 5 minutes. Installed with pip, the server needs boto3 for this: <code className="font-mono text-caption">pip install &quot;trailbeam[aws]&quot;</code>.
          </p>
        </div>
      </div>
    </SettingsSection>
  );
}
