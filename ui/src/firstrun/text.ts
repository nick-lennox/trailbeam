/*
  The first-run screen's code samples and the parts of them a host needs without the view (QV-146): the runtimes,
  the install commands, the example that sends one message, and the environment block. No React here, so the main
  entry can carry them; the screen itself is FirstRun.tsx, in @trailbeam/ui/design.
*/
export type Runtime = "python" | "node";
export const RUNTIMES: [Runtime, string][] = [["python", "Python"], ["node", "Node.js"]];
export const RUNTIME_KEY = "trailbeam.runtime";

export const INSTALL: Record<Runtime, string> = { python: "pip install trailbeam", node: "npm install trailbeam" };

export const EXAMPLE: Record<Runtime, { caption: string; code: string }> = {
  python: {
    caption: "send_one.py",
    code: `import boto3
import trailbeam.boto

trailbeam.boto.install()  # once per process, before or after creating clients

sqs = boto3.client("sqs")
sqs.send_message(QueueUrl="<queue URL>", MessageBody="hello from Trailbeam")`,
  },
  node: {
    caption: "send-one.js",
    code: `const { SQSClient, SendMessageCommand } = require("@aws-sdk/client-sqs");
const trailbeam = require("trailbeam");

const sqs = trailbeam.instrument(new SQSClient({}));

async function main() {
  await sqs.send(new SendMessageCommand({ QueueUrl: "<queue URL>", MessageBody: "hello from Trailbeam" }));
  await trailbeam.forceFlush(); // a script that exits right away must flush, or its spans are lost
}
main();`,
  },
};

/** The environment block for row 02. The hash key is left out on purpose: a pasted `$(openssl rand -hex 32)` makes a new key per terminal. */
export function environment(endpoint: string, auth: "off" | "token"): string {
  const lines = [`export TRAILBEAM_OTLP_TRACES_ENDPOINT=${endpoint}`, "export OTEL_SERVICE_NAME=orders-worker"];
  if (auth === "token") lines.push('export OTEL_EXPORTER_OTLP_HEADERS="Authorization=Bearer%20<token>"');
  return lines.join("\n");
}
