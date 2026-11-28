/*
  The Settings page at /settings (QV-589), for admins, in the shell's frame (frame.tsx) on the design system. Its one
  section today is AWS (the shared AwsSettingsView): whether this server reads what AWS says about its queues, and
  which queues. It reads GET /api/settings/aws and saves with PATCH; the server refuses both to anyone but an admin
  (anyone, before accounts exist), and the page says so. contract/openapi.yaml, tag Settings.
*/
import { useEffect, useState } from "react";
import type { AwsSettingsAnswer } from "../src/index.ts";
import { AwsSettingsView, PageHeader, Problem } from "../src/design/index.ts";
import { ApiError, getJSON, sendJSON } from "./chrome.tsx";
import { ShellFrame, ShellSearch } from "./frame.tsx";

const PATH = "/api/settings/aws";
type Load = { kind: "loading" } | { kind: "error"; message: string } | { kind: "denied" } | { kind: "ok"; answer: AwsSettingsAnswer };

export function SettingsPage() {
  const [state, setState] = useState<Load>({ kind: "loading" });
  useEffect(() => {
    document.title = "Settings · Trailbeam";
    getJSON<AwsSettingsAnswer>(PATH).then(
      (answer) => setState({ kind: "ok", answer }),
      (e: Error) => setState(e instanceof ApiError && e.status === 403 ? { kind: "denied" } : { kind: "error", message: e.message }),
    );
  }, []);
  return (
    <ShellFrame path="/settings">
      <PageHeader title="Settings" subtitle="How this server works with the services around it." search={<ShellSearch />} />
      <div className="grid max-w-5xl gap-12 px-8 pb-16">
        {state.kind === "loading" && <p className="text-sm text-ink-3">Loading the settings</p>}
        {state.kind === "denied" && <p className="max-w-2xl text-sm leading-relaxed text-ink-2">Only an admin can see and change this server&rsquo;s settings.</p>}
        {state.kind === "error" && <Problem>Couldn&rsquo;t read the AWS settings: {state.message}</Problem>}
        {state.kind === "ok" && <AwsSettingsView answer={state.answer} onSave={(change) => sendJSON<AwsSettingsAnswer>("PATCH", PATH, change)} />}
      </div>
    </ShellFrame>
  );
}
