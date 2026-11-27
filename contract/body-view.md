# The Body tab and `body.*` search

How Trailbeam shows a message's captured body, and searches bodies, from the spans of span contract
revision 3.2 ([section 15](span-contract.md#15-message-contents-revision-32)). Open-source Trailbeam
(`server/src/trailbeam/server/bodies.py`) and the hosted app both follow this page. The view itself
is shared: `BodyPanel` in `@trailbeam/ui`. Both are tested against the same expected outputs in
[`fixtures/`](fixtures/README.md).

## Who can read bodies

Bodies are **off unless the server is told to keep them**, per queue:

- **Open-source Trailbeam** keeps bodies only for the queues in `TRAILBEAM_STORE_BODIES`
  (`--store-bodies`), none by default. It has no users and no roles, only an optional shared
  access token (`TRAILBEAM_TOKEN`, off by default). **Anyone the server lets in can read every
  stored body**, in the Body tab and from `/api/messages/{id}/body`: with no token that is anyone
  who can reach it. Anyone who can read the SQLite file can read them there too. Set a token, or
  run it on a trusted network.
- **The hosted app** keeps bodies only for queues an admin opted in under Settings, and shows them to
  owners and admins only.

Redaction happens in the packages, before a body leaves the process (contract 15.3). Neither
server redacts.

A queue taken off `TRAILBEAM_STORE_BODIES` stops having its bodies shown or searched at once. The
bodies already in the SQLite file stay there until retention deletes their messages (README,
Retention).

## One body per message

A message keeps at most one body. It is the copy from the **first send** that reached the server,
or, when no send carried one, the copy from the **first receive** that did. A later send replaces a
receive's copy; nothing replaces a send's (contract 15.4). Only send and receive spans carry bodies.

## `GET /api/messages/{id}/body`

`404` if no span names the message. Otherwise one of these two shapes.

With a body:

```json
{
  "message_id": "m-orders-1",
  "kind": "body",
  "body": {
    "body": "{\"customer\":{\"id\":\"acme\",\"email\":\"[REDACTED]\"},…}",
    "format": "json",
    "redacted_count": 3,
    "envelope": null,
    "capture_status": "captured",
    "from_kind": "send",
    "service": "checkout-api",
    "queue": "orders",
    "trace_id": "…",
    "span_id": "…",
    "time_unix_nano": "1763312345678901234"
  },
  "size": 135,
  "offloaded_size": null,
  "tenant": "acme",
  "tenant_source": "message_attribute"
}
```

- `body`: the body as stored, after the package redacted it. `format` is `json` or `text`,
  `envelope` is `sns`, `eventbridge` or null (contract 15.2). `from_kind`, `service`, `queue`, the
  IDs and `time_unix_nano` (the span's end) say which span it came from.
- `capture_status`: `trailbeam.body.capture_status` of that span: `captured`, or `offloaded_to_s3`
  when the body is an SQS Extended Client pointer (contract 15.4). Then `body` is the pointer, not
  the payload: the payload is in S3, and neither the packages nor Trailbeam fetch it. The view says
  so, and gives the payload's size. A pointer is not redacted (contract 15.3). Null if the span has
  no status.
- `size`: `messaging.message.body.size` from that span, the body's size **before** redaction; if
  that span has none, the message's size as below. For a pointer, this is the pointer's size.
- `offloaded_size`: `trailbeam.body.offloaded_size` from that span, else from the first span that
  has it, sends first: the size of the payload in S3. Null when the spans don't give one.
- `tenant`, `tenant_source`: `trailbeam.tenant` and `trailbeam.tenant.source` from the first span
  that has a tenant, sends first.

Without one:

```json
{
  "message_id": "m-pay-1",
  "kind": "none",
  "reason": "off",
  "size": 40,
  "sends": 1,
  "receives": 1,
  "send_status": "off",
  "receive_status": "off"
}
```

- `sends`, `receives`: how many send and receive spans the message has.
- `send_status`: `trailbeam.body.capture_status` of the first send that has one, else null.
- `receive_status`: the same, of the **last** receive that has one.
- `size`: `messaging.message.body.size` from the first span that has it, sends first.
- `reason`, the first of these that applies:

| `reason` | When | |
|---|---|---|
| `not_opted_in` | a span of the message carried a body the server did not keep: it was dropped before it was written, or its queue is no longer in the server's list | *dropped by the server's configuration* |
| `package_too_old` | there is a send, and no send has a capture status | the producer's package is from before 3.2 |
| `off`, `md5_mismatch`, `redaction_failed`, `exceeds_capture_limit` | there is a send, and that is the send's status | the package's own reason (contract 15.4) |
| `unknown` | there is a send with any other status | shown with the status |
| `producer_not_instrumented` | no send | the producer runs no Trailbeam package, and no receive's copy was kept |

For `producer_not_instrumented` the view adds what the receives say: if there are receives and none
has a status, the consumer's package is from before 3.2 too; if the last receive's status is `off`,
capture is off in the consumer's configuration too; otherwise it names that status.

The words for each reason are in `@trailbeam/ui` (`NO_BODY_TEXT`). Each product adds one hint saying
where to change the server's setting: `TRAILBEAM_STORE_BODIES`, or Settings in the hosted app.

## `body.*` search

The body part of the explorer's query language, the same in both products:

- `body:text` finds text anywhere in the body, ignoring case: the body as one line of JSON, or the
  text body as a JSON string.
- `body.<path>:<value>` compares every value at a JSON path. `body.order.sku:A-100`. Keys keep their
  case. A path through an array matches if any element does; a number picks one element
  (`body.Records.0.key`, or `body.Records[0].key`).
- A value matches text ignoring case, and `*` is a wildcard (`body.order.sku:A-*`). On a number,
  `>`, `>=`, `<`, `<=` compare, and `a..b` is a range that includes `a` and stops before `b`.
  A value that is not a number compares a number as text. `true` and `false` match booleans.
- Several terms on one field are alternatives, terms on different fields must all match, and a
  leading `-` excludes. Quotes keep spaces in a value: `body.note:"call me"`.
- Only kept bodies are searched, as stored, after redaction: a value the package redacted can't be
  found.

Open-source Trailbeam takes the terms in `GET /api/messages?body=...` (with `queue`, `limit`, a time
window and `cursor`) and on its Messages page. It lists the messages whose kept body matches, most
recently seen first. There, plain text with no field (`ord-1003`, or `"call me"` in quotes) is
`body:<text>`. A term for any other field (`queue:orders`) is a `400` that reads
``Can't read `queue:orders`. Use text, or `body.<path>:<value>`.`` There is no index of body contents, so a
search is bounded instead: it reads the kept bodies of the messages last seen in the window
(`from_ms`/`to_ms`, or `since_s`, default the last day), on `queue` if given, newest first, and stops
after `limit` matches or 5,000 messages. `next_cursor` goes on from there. A page can have fewer than
`limit` matches and still have a next page.

The hosted app runs the same terms in its event explorer, where a match is the event that carried
the body.
