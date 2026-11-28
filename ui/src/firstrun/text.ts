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

/* ── RabbitMQ (QV-308): the same three steps with the pika and amqplib hooks ─────────────────────────────────── */

export const INSTALL_RABBITMQ: Record<Runtime, string> = { python: 'pip install "trailbeam[pika]"', node: "npm install trailbeam amqplib" };

/** What each runtime needs, for RabbitMQ (python/README.md, node/README.md). */
export const NEEDS_RABBITMQ: Record<Runtime, string> = {
  python: "Python 3.10 or later, with pika 1.3 or later.",
  node: "Node.js 20 or later, with amqplib 0.10.6 or later (older versions can't connect to RabbitMQ 4.1).",
};

/** The example that publishes one message. A message needs a message_id to get a timeline (contract A6). */
export const EXAMPLE_RABBITMQ: Record<Runtime, { caption: string; code: string }> = {
  python: {
    caption: "publish_one.py",
    code: `import pika
import trailbeam.pika

trailbeam.pika.install()  # once per process, before opening connections

connection = pika.BlockingConnection(pika.URLParameters("amqp://<host>"))
channel = connection.channel()
channel.basic_publish(exchange="", routing_key="<queue>", body=b"hello from Trailbeam",
                      properties=pika.BasicProperties(message_id="first-message"))
connection.close()`,
  },
  node: {
    caption: "publish-one.js",
    code: `const amqp = require("amqplib");
const trailbeam = require("trailbeam");

const tracing = trailbeam.amqplib.install(); // once per process, before opening connections

async function main() {
  const connection = await amqp.connect("amqp://<host>");
  const channel = await connection.createChannel();
  channel.sendToQueue("<queue>", Buffer.from("hello from Trailbeam"), { messageId: "first-message" });
  await channel.close();
  await connection.close();
  await tracing.forceFlush(); // a script that exits right away must flush, or its spans are lost
}
main();`,
  },
};
