/*
  The AWS settings screen's model (QV-589): what GET and PATCH /api/settings/aws answer (contract/openapi.yaml, tag
  Settings), the form's state, and what the screen says about the reads. Pure, so the host fetches and this decides.
  AWS reads are off by default: with them off the server makes no AWS call, looks for no credentials and never loads
  boto3. An admin turns them on by naming queues; the credentials are boto3's own and are never stored.
*/

/** The settings as the server keeps them. */
export type AwsReadSettings = {
  enabled: boolean;
  /** exact queue names, or patterns with * in them */
  queues: string[];
  /** the dead-letter queues Trailbeam may redrive (QV-590), names or patterns; empty: redrive off. Missing from servers before 0.5. */
  redrive?: string[];
  region: string | null;
  accountId: string | null;
  updatedAt: number | null;
  updatedBy: string | null;
};

/** What the server found when it last checked (aws_reader.Status). */
export type AwsReadsStatus = {
  /** reads are switched on and at least one queue is named */
  on: boolean;
  queues: number;
  /** boto3 is installed; null when it wasn't asked */
  boto3: boolean | null;
  /** where boto3 found credentials ("env", "shared-credentials-file", "iam-role"...), null when none or not asked */
  credentials: string | null;
  region: string | null;
  /** a sentence when reads are on but can't work */
  problem: string | null;
};

export type AwsSettingsAnswer = { settings: AwsReadSettings; status: AwsReadsStatus };

/** The form's fields, as typed. */
export type AwsForm = { enabled: boolean; queues: string; redrive: string; region: string; accountId: string };

/** The three read-only calls the reads make, for the role's policy. ReceiveMessage is never called. */
export const AWS_ACTIONS = ["sqs:GetQueueAttributes", "cloudwatch:GetMetricData", "lambda:ListEventSourceMappings"] as const;

/** What redrive needs besides the reads (QV-590, the hosted app's redrive policy): on each dead-letter queue to redrive, the move-task calls
    Trailbeam makes and what SQS's move then does with the same credentials; on the queues the messages go back to, SendMessage. */
export const REDRIVE_ON_DLQ = ["sqs:StartMessageMoveTask", "sqs:ListMessageMoveTasks", "sqs:CancelMessageMoveTask", "sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"] as const;
export const REDRIVE_ON_SOURCE = ["sqs:SendMessage"] as const;

export const AWS_ABOUT =
  "Trailbeam can read what AWS says about your queues: their depth, the oldest message's age, their settings and their Lambda triggers. " +
  "It is off until an admin names the queues to read here.";

export const formOf = (s: AwsReadSettings): AwsForm =>
  ({ enabled: s.enabled, queues: s.queues.join("\n"), redrive: (s.redrive ?? []).join("\n"), region: s.region ?? "", accountId: s.accountId ?? "" });

/** The names typed one per line (or with commas), trimmed, without empty lines or repeats. */
export function queueList(text: string): string[] {
  const out: string[] = [];
  for (const raw of text.split(/[\n,]/)) {
    const q = raw.trim();
    if (q && !out.includes(q)) out.push(q);
  }
  return out;
}

/** The PATCH body for the form: every field, so what is saved is what the form shows. */
export const changeOf = (f: AwsForm) =>
  ({ enabled: f.enabled, queues: queueList(f.queues), redrive: queueList(f.redrive), region: f.region.trim() || null, accountId: f.accountId.trim() || null });

/** Whether the form differs from what is saved. */
export const changed = (f: AwsForm, s: AwsReadSettings) => JSON.stringify(changeOf(f)) !== JSON.stringify(changeOf(formOf(s)));

const CREDENTIALS: Record<string, string> = {
  env: "environment variables",
  "shared-credentials-file": "the shared credentials file",
  "config-file": "the AWS config file",
  "assume-role": "an assumed role",
  "assume-role-with-web-identity": "a web identity role",
  "container-role": "the container's task role",
  "iam-role": "the instance's role",
  sso: "AWS IAM Identity Center",
  process: "a credential process",
  explicit: "the server's own settings",
  found: "boto3's usual places",
};
/** Where boto3 found the credentials, in words. */
export const credentialsText = (source: string) => CREDENTIALS[source] ?? source;

export type AwsStatusLine = { tone: "good" | "warning" | "idle"; text: string };

/** What the screen says about the reads now. */
export function statusLine(st: AwsReadsStatus, s: AwsReadSettings): AwsStatusLine {
  if (!st.on) {
    if (s.enabled && !s.queues.length) return { tone: "idle", text: "Off: reads are switched on, but no queue is named, so nothing is read." };
    return { tone: "idle", text: "Off. This server makes no AWS calls." };
  }
  const n = `${st.queues} ${st.queues === 1 ? "name or pattern" : "names or patterns"}`;
  if (st.problem) return { tone: "warning", text: `On for ${n}, but nothing can be read. ${st.problem}` };
  const parts = [`On for ${n}.`];
  if (st.credentials) parts.push(`Credentials from ${credentialsText(st.credentials)}.`);
  if (st.region) parts.push(`Region ${st.region} for queues whose spans don't name one.`);
  const r = s.redrive?.length ?? 0;
  parts.push(r ? `Redrive is on for ${r} ${r === 1 ? "dead-letter queue name or pattern" : "dead-letter queue names or patterns"}.` : "Redrive is off.");
  return { tone: "good", text: parts.join(" ") };
}
