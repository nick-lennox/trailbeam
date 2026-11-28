/*
  Which messaging system a message is on (QV-308), read from its key as the server writes it
  (server/src/trailbeam/server/systems.py, contract/span-contract-amqp.md A6): an SQS message's key is its message
  ID as SQS gave it; any other system's is `~<system>~<message ID>`, such as `~rabbitmq~order-17`. So an SQS
  message and a RabbitMQ message whose application set the same ID text are two messages, and every page can tell
  which system a message is on from its key alone, with nothing else from the server.

  The names are each system's own: "Amazon SQS" and "RabbitMQ". Pure: no React, so a server module can import it.
*/

export type MessagingSystem = "aws.sqs" | "rabbitmq";

export const SQS: MessagingSystem = "aws.sqs";
export const RABBITMQ: MessagingSystem = "rabbitmq";

const MARK = "~";
const NAMED: readonly MessagingSystem[] = [RABBITMQ, SQS];

/** Each system's name, as a badge and a sentence write it. */
export const SYSTEM_NAME: Record<MessagingSystem, string> = { "aws.sqs": "Amazon SQS", rabbitmq: "RabbitMQ" };

/** A system's badge note: what the badge says about where the message's steps come from. */
export const SYSTEM_NOTE: Record<MessagingSystem, string> = {
  "aws.sqs": "An Amazon SQS message. Its steps are SQS calls: SendMessage, ReceiveMessage, DeleteMessage.",
  rabbitmq: "A RabbitMQ message. Its steps are AMQP 0-9-1 methods: basic.publish, basic.deliver, basic.ack. Each queue an exchange routed it to holds its own copy.",
};

/** (system, message ID) of a message key, as systems.split reads it. A key it could not have written is an SQS ID. */
export function splitKey(key: string): [MessagingSystem, string] {
  if (key.startsWith(MARK)) {
    const rest = key.slice(1);
    const at = rest.indexOf(MARK);
    if (at > 0) {
      const system = rest.slice(0, at) as MessagingSystem;
      const id = rest.slice(at + 1);
      if (NAMED.includes(system) && id) return [system, id];
    }
  }
  return [SQS, key];
}

/** The system a message key is on. */
export const systemOf = (key: string | null | undefined): MessagingSystem => (key ? splitKey(key)[0] : SQS);

/** The message ID as its application or SQS set it: a RabbitMQ key without its system mark. */
export const shownId = (key: string) => splitKey(key)[1];

/** The system a queue is on, from its URL or flow key: a RabbitMQ queue's is amqp://<host>/<vhost>/<queue> (contract A3). */
export const systemOfQueueUrl = (url: string | null | undefined): MessagingSystem => (url && /^amqps?:\/\//i.test(url) ? RABBITMQ : SQS);

/** Whether a message (a timeline or a list row) is a RabbitMQ message: its `system`, else its key. */
export const isRabbitMq = (m: { message_id?: string | null; system?: string | null }) =>
  m.system === RABBITMQ || systemOf(m.message_id) === RABBITMQ;
