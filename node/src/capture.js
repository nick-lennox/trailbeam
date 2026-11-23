'use strict';
// Message contents (contract section 15, revision 3.2): body size, tenant, FIFO fields, opt-in body
// capture with redaction, and Lambda error text. SNS fan-out (section 20, revision 3.4): the publish ID
// the publish hooks add, and the three SNS envelope fields a receive reads.
//
// Nothing here makes an SQS call. Everything is read from the input and output of calls the
// application made, or from the Lambda event. This mirrors python/src/trailbeam/_capture.py exactly;
// both are tested against contract/span-contract-vectors.json.

const crypto = require('node:crypto');

const STATUS_CAPTURED = 'captured';
const STATUS_OFF = 'off';
const STATUS_MD5_MISMATCH = 'md5_mismatch';
const STATUS_REDACTION_FAILED = 'redaction_failed';
const STATUS_EXCEEDS_LIMIT = 'exceeds_capture_limit';
const STATUS_OFFLOADED = 'offloaded_to_s3';

const REDACTED = '[REDACTED]';
const DEFAULT_MAX_BODY_BYTES = 16 * 1024;
const HARD_MAX_BODY_BYTES = 256 * 1024;
const MAX_ERROR_MESSAGE_CHARS = 1024;
const MAX_TENANT_CHARS = 256;
const MAX_JSON_DEPTH = 100;
const S3_POINTER_CLASS = 'software.amazon.payloadoffloading.PayloadS3Pointer';
// Message attributes in which the SQS Extended Client puts the offloaded payload's size, current name
// first (section 15.2). The receive hooks ask for both.
const OFFLOADED_SIZE_ATTRIBUTES = Object.freeze(['ExtendedPayloadSize', 'SQSLargePayloadSize']);
const SIZE_RE = /^[0-9]{1,15}$/;

const DEFAULT_REDACT_KEYS = Object.freeze([
  'password', 'passwd', 'secret', 'token', 'access_token', 'refresh_token', 'authorization',
  'api_key', 'apikey', 'ssn', 'card_number', 'cardnumber', 'cvv', 'cvc',
]);

// Built-in text patterns (section 15.3): ASCII classes only, so RegExp and Python's re agree.
const PATTERNS = Object.freeze({
  aws_access_key: '(?<![A-Z0-9])(?:AKIA|ASIA)[A-Z0-9]{16}(?![A-Z0-9])',
  bearer: '(?<![A-Za-z])[Bb]earer [A-Za-z0-9._~+/=-]+',
  email: '[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\\.[A-Za-z0-9-]+)*\\.[A-Za-z]{2,}',
  long_number: '(?<![0-9])[0-9](?:[ -]?[0-9]){8,}(?![0-9])',
});
const DEFAULT_PATTERNS = Object.freeze(Object.keys(PATTERNS));
const KEY_RE = /^[A-Za-z0-9_.-]+$/;

const ENV = Object.freeze({
  TENANT_ATTRIBUTE: 'TRAILBEAM_TENANT_ATTRIBUTE',
  TENANT_BODY_PATH: 'TRAILBEAM_TENANT_BODY_PATH',
  BODY_QUEUES: 'TRAILBEAM_CAPTURE_BODY_QUEUES',
  ERROR_QUEUES: 'TRAILBEAM_CAPTURE_ERROR_QUEUES',
  REDACT_KEYS: 'TRAILBEAM_REDACT_KEYS',
  REDACT_PATTERNS: 'TRAILBEAM_REDACT_PATTERNS',
  MAX_BODY_BYTES: 'TRAILBEAM_CAPTURE_MAX_BYTES',
  SNS_INJECT: 'TRAILBEAM_SNS_INJECT',
  SNS_ENVELOPE_QUEUES: 'TRAILBEAM_SNS_ENVELOPE_QUEUES',
});

const lower = (s) => s.replace(/[A-Z]/g, (c) => c.toLowerCase());
const csv = (v) => (v || '').split(',').map((p) => p.trim()).filter(Boolean);
const escapeRe = (s) => s.replace(/[.\-]/g, '\\$&');

class RedactionError extends Error {}

/** What the package reads from message contents. Off unless configured. */
class Config {
  constructor({
    tenantAttribute, tenantBodyPath, bodyQueues = [], errorQueues = [],
    redactKeys = DEFAULT_REDACT_KEYS, redactPatterns = DEFAULT_PATTERNS, maxBodyBytes = DEFAULT_MAX_BODY_BYTES,
    snsInject = true, snsEnvelopeQueues = [],
  } = {}) {
    for (const k of redactKeys) {
      if (typeof k !== 'string' || !KEY_RE.test(k)) throw new Error(`trailbeam: redact key ${JSON.stringify(k)} may contain only A-Z a-z 0-9 _ . -`);
    }
    for (const p of redactPatterns) {
      if (!Object.prototype.hasOwnProperty.call(PATTERNS, p)) throw new Error(`trailbeam: unknown redact pattern ${JSON.stringify(p)}; known: ${DEFAULT_PATTERNS.join(', ')}`);
    }
    if (!(Number.isInteger(maxBodyBytes) && maxBodyBytes > 0 && maxBodyBytes <= HARD_MAX_BODY_BYTES)) {
      throw new Error(`trailbeam: maxBodyBytes must be 1 to ${HARD_MAX_BODY_BYTES}`);
    }
    this.tenantAttribute = tenantAttribute || undefined;
    this.tenantBodyPath = tenantBodyPath || undefined;
    this.bodyQueues = new Set(bodyQueues);
    this.errorQueues = new Set(errorQueues);
    this.redactKeys = [...redactKeys];
    this.redactPatterns = [...redactPatterns];
    this.maxBodyBytes = maxBodyBytes;
    // 3.4 (section 20): add trailbeam.publish_id to Publish / PublishBatch, and the queues whose SNS
    // envelope fields are read when their body is not captured.
    this.snsInject = snsInject !== false;
    this.snsEnvelopeQueues = new Set(snsEnvelopeQueues);
    this.names = new Set(this.redactKeys.filter((k) => !k.includes('.')).map(lower));
    this.paths = new Set(this.redactKeys.filter((k) => k.includes('.')).map(lower));
    const names = this.redactKeys.filter((k) => !k.includes('.'));
    this.keyValueSource = names.length
      ? `(?<![A-Za-z0-9_])(${names.map((k) => escapeRe(lower(k))).join('|')})(["']?[ \\t]*[:=][ \\t]*)("[^"]*"|'[^']*'|[^ \\t\\r\\n,;&]+)`
      : undefined;
    Object.freeze(this);
  }

  bodyOn(queueName) {
    return Boolean(queueName) && (this.bodyQueues.has('*') || this.bodyQueues.has(queueName));
  }

  errorsOn(queueName) {
    return Boolean(queueName) && (this.errorQueues.has('*') || this.errorQueues.has(queueName));
  }

  /** 3.4: the SNS envelope's MessageId, TopicArn and Timestamp are read for a queue whose body is
   * captured, or that is listed in the envelope queue list. */
  snsEnvelopeOn(queueName) {
    return this.bodyOn(queueName) || (Boolean(queueName) && (this.snsEnvelopeQueues.has('*') || this.snsEnvelopeQueues.has(queueName)));
  }
}

/** An on/off environment variable that defaults to on: only off, false, 0 or no turn it off. */
const flagOn = (v) => !['off', 'false', '0', 'no'].includes(lower((v || '').trim()));

/**
 * Explicit options, else environment variables (sections 15.1 and 20.4). Each option group replaces its
 * environment variables as a whole.
 * `tenant`: `{ messageAttribute, bodyPath }`. `capture`: `{ bodyQueues, errorQueues, redactKeys,
 * redactPatterns, maxBodyBytes }`. `sns`: `{ inject, envelopeQueues }`.
 */
function resolveConfig(tenant, capture, sns) {
  const env = process.env;
  if (sns === undefined || sns === null) {
    sns = { inject: flagOn(env[ENV.SNS_INJECT]), envelopeQueues: csv(env[ENV.SNS_ENVELOPE_QUEUES]) };
  }
  if (tenant === undefined || tenant === null) {
    tenant = { messageAttribute: env[ENV.TENANT_ATTRIBUTE] || undefined, bodyPath: env[ENV.TENANT_BODY_PATH] || undefined };
  }
  if (capture === undefined || capture === null) {
    capture = {
      bodyQueues: csv(env[ENV.BODY_QUEUES]),
      errorQueues: csv(env[ENV.ERROR_QUEUES]),
      redactKeys: csv(env[ENV.REDACT_KEYS]),
      redactPatterns: env[ENV.REDACT_PATTERNS] !== undefined ? csv(env[ENV.REDACT_PATTERNS]) : undefined,
      maxBodyBytes: env[ENV.MAX_BODY_BYTES] ? Number(env[ENV.MAX_BODY_BYTES]) : undefined,
    };
  }
  const extra = (capture.redactKeys || []).filter((k) => !DEFAULT_REDACT_KEYS.includes(k));
  return new Config({
    tenantAttribute: tenant.messageAttribute,
    tenantBodyPath: tenant.bodyPath,
    bodyQueues: capture.bodyQueues || [],
    errorQueues: capture.errorQueues || [],
    redactKeys: [...DEFAULT_REDACT_KEYS, ...extra],
    redactPatterns: capture.redactPatterns !== undefined && capture.redactPatterns !== null ? capture.redactPatterns : DEFAULT_PATTERNS,
    maxBodyBytes: capture.maxBodyBytes || DEFAULT_MAX_BODY_BYTES,
    snsInject: sns.inject !== false,
    snsEnvelopeQueues: sns.envelopeQueues || [],
  });
}

// -- redaction (section 15.3) ----------------------------------------------------------------------

/** Key-value pairs for the configured key names, then the built-in patterns. Returns [text, count]. */
function redactText(text, config) {
  let count = 0;
  if (config.keyValueSource) {
    text = text.replace(new RegExp(config.keyValueSource, 'gi'), (_m, k, sep) => { count += 1; return k + sep + REDACTED; });
  }
  for (const p of config.redactPatterns) {
    text = text.replace(new RegExp(PATTERNS[p], 'g'), () => { count += 1; return REDACTED; });
  }
  return [text, count];
}

function encodeString(s) {
  if (/[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/.test(s)) {
    throw new RedactionError('lone surrogate'); // cannot be exported as UTF-8: fail closed
  }
  return JSON.stringify(s);
}

const STRING_RE = /"(?:[^"\\\u0000-\u001f]|\\(?:["\\/bfnrt]|u[0-9a-fA-F]{4}))*"/y;
const SCALAR_RE = /-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?|true|false|null/y;
const isWs = (c) => c === ' ' || c === '\t' || c === '\n' || c === '\r';

class NotJson extends Error {}

/**
 * A strict JSON scanner that copies the input and rewrites only the values it redacts, so the
 * output keeps the input's formatting and number text. Throws NotJson if the input is not JSON.
 */
class JsonScan {
  constructor(s, config) {
    this.s = s;
    this.config = config;
    this.out = [];
    this.count = 0;
  }

  run() {
    let i = this.ws(0, true);
    i = this.value(i, [], 0, true);
    if (this.ws(i, true) !== this.s.length) throw new NotJson('trailing data');
    return this.out.join('');
  }

  ws(i, emit) {
    const s = this.s;
    while (i < s.length && isWs(s[i])) {
      if (emit) this.out.push(s[i]);
      i += 1;
    }
    return i;
  }

  put(emit, x) {
    if (emit) this.out.push(x);
  }

  value(i, path, depth, emit) {
    if (depth > MAX_JSON_DEPTH) throw new RedactionError('JSON nested too deeply');
    const s = this.s;
    if (i >= s.length) throw new NotJson('unexpected end');
    const c = s[i];
    if (c === '{') return this.object(i, path, depth, emit);
    if (c === '[') return this.array(i, path, depth, emit);
    if (c === '"') {
      const j = this.stringEnd(i);
      if (emit) this.out.push(this.redactString(s.slice(i, j)));
      return j;
    }
    SCALAR_RE.lastIndex = i;
    const m = SCALAR_RE.exec(s);
    if (!m) throw new NotJson('bad value');
    if (emit) this.out.push(m[0]);
    return i + m[0].length;
  }

  object(i, path, depth, emit) {
    const s = this.s;
    this.put(emit, '{');
    i = this.ws(i + 1, emit);
    if (s[i] === '}') { this.put(emit, '}'); return i + 1; }
    for (;;) {
      if (s[i] !== '"') throw new NotJson('expected key');
      const j = this.stringEnd(i);
      const key = JSON.parse(s.slice(i, j));
      this.put(emit, s.slice(i, j));
      i = this.ws(j, emit);
      if (s[i] !== ':') throw new NotJson('expected :');
      this.put(emit, ':');
      i = this.ws(i + 1, emit);
      const kpath = [...path, lower(key)];
      if (emit && (this.config.names.has(kpath[kpath.length - 1]) || this.config.paths.has(kpath.join('.')))) {
        this.out.push(`"${REDACTED}"`);
        this.count += 1;
        i = this.value(i, kpath, depth + 1, false);
      } else {
        i = this.value(i, kpath, depth + 1, emit);
      }
      i = this.ws(i, emit);
      if (s[i] === ',') { this.put(emit, ','); i = this.ws(i + 1, emit); continue; }
      if (s[i] === '}') { this.put(emit, '}'); return i + 1; }
      throw new NotJson('expected , or }');
    }
  }

  array(i, path, depth, emit) {
    const s = this.s;
    this.put(emit, '[');
    i = this.ws(i + 1, emit);
    if (s[i] === ']') { this.put(emit, ']'); return i + 1; }
    for (;;) {
      i = this.value(i, path, depth + 1, emit);
      i = this.ws(i, emit);
      if (s[i] === ',') { this.put(emit, ','); i = this.ws(i + 1, emit); continue; }
      if (s[i] === ']') { this.put(emit, ']'); return i + 1; }
      throw new NotJson('expected , or ]');
    }
  }

  stringEnd(i) {
    STRING_RE.lastIndex = i;
    const m = STRING_RE.exec(this.s);
    if (!m) throw new NotJson('bad string');
    return i + m[0].length;
  }

  redactString(token) {
    const value = JSON.parse(token);
    const first = value.replace(/^[ \t\n\r]+/, '')[0];
    if (first === '{' || first === '[') {
      // A JSON document inside a string (an SNS envelope's Message, for one): redact it as JSON.
      const inner = new JsonScan(value, this.config);
      let redacted;
      try {
        redacted = inner.run();
      } catch (e) {
        if (!(e instanceof NotJson)) throw e;
      }
      if (redacted !== undefined) {
        if (!inner.count) return token;
        this.count += inner.count;
        return encodeString(redacted);
      }
    }
    const [text, n] = redactText(value, this.config);
    if (!n) return token;
    this.count += n;
    return encodeString(text);
  }
}

/** [redacted body, number of values replaced, 'json' or 'text']. */
/**
 * An Extended Client pointer of exactly the shape the clients write holds no payload, only a bucket and
 * a key, so it is not redacted (section 15.3): the patterns would rewrite uuid keys and account IDs in
 * bucket names, and the pointer would then name an object that does not exist. It is written back in
 * one compact form, so nothing in the input beyond the bucket and the key (a duplicate member, say) is
 * exported.
 */
function redactBody(body, config) {
  const pointer = s3Pointer(body);
  if (pointer) {
    return [
      `["${S3_POINTER_CLASS}",{"s3BucketName":${encodeString(pointer.bucket)},"s3Key":${encodeString(pointer.key)}}]`,
      0,
      'json',
    ];
  }
  const scan = new JsonScan(body, config);
  try {
    return [scan.run(), scan.count, 'json'];
  } catch (e) {
    if (!(e instanceof NotJson)) throw e;
  }
  const [text, n] = redactText(body, config);
  return [text, n, 'text'];
}

// -- reading the body ------------------------------------------------------------------------------

const utf8Bytes = (s) => Buffer.byteLength(s, 'utf8');
const bodySize = (body) => (typeof body === 'string' ? utf8Bytes(body) : undefined);

function parse(body) {
  try {
    return JSON.parse(body);
  } catch (e) {
    return undefined;
  }
}

const isObject = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);
const isPointerDoc = (doc) => Array.isArray(doc) && doc.length === 2 && doc[0] === S3_POINTER_CLASS;

/**
 * `{bucket, key}` if the body is exactly an Extended Client pointer: the two-item array whose second
 * item is an object with only the string members `s3BucketName` and `s3Key`. Otherwise undefined.
 */
function s3Pointer(body) {
  if (typeof body !== 'string' || !body.includes(S3_POINTER_CLASS)) return undefined;
  const doc = parse(body);
  if (!isPointerDoc(doc) || !isObject(doc[1])) return undefined;
  const keys = Object.keys(doc[1]).sort();
  if (keys.length !== 2 || keys[0] !== 's3BucketName' || keys[1] !== 's3Key') return undefined;
  const { s3BucketName: bucket, s3Key: key } = doc[1];
  return typeof bucket === 'string' && typeof key === 'string' ? { bucket, key } : undefined;
}

/** The offloaded payload's size from the Extended Client's message attribute, if it is a whole number. */
function offloadedSize(attributes, lambdaRecord = false) {
  if (!isObject(attributes)) return undefined;
  for (const name of OFFLOADED_SIZE_ATTRIBUTES) {
    const a = Object.prototype.hasOwnProperty.call(attributes, name) ? attributes[name] : undefined;
    const v = isObject(a) ? a[lambdaRecord ? 'stringValue' : 'StringValue'] : undefined;
    if (typeof v === 'string' && SIZE_RE.test(v)) return Number(v);
  }
  return undefined;
}

function envelope(doc) {
  if (isObject(doc)) {
    if (doc.Type === 'Notification' && 'TopicArn' in doc) return 'sns';
    if ('detail-type' in doc && 'detail' in doc && 'source' in doc) return 'eventbridge';
  }
  return undefined;
}

function md5Matches(body, md5) {
  if (typeof md5 !== 'string' || !md5) return true; // nothing to check against
  return crypto.createHash('md5').update(body, 'utf8').digest('hex') === md5.toLowerCase();
}

/** `{tenant, source}` from the configured message attribute, else the configured body path. */
function tenantFrom(config, attributes, body, lambdaRecord = false) {
  if (config.tenantAttribute && isObject(attributes)) {
    const a = attributes[config.tenantAttribute];
    const v = isObject(a) ? a[lambdaRecord ? 'stringValue' : 'StringValue'] : undefined;
    if (typeof v === 'string' && v && v.length <= MAX_TENANT_CHARS) return { tenant: v, source: 'message_attribute' };
  }
  if (config.tenantBodyPath && typeof body === 'string') {
    let doc = parse(body);
    for (const part of config.tenantBodyPath.split('.')) doc = isObject(doc) ? doc[part] : undefined;
    if (typeof doc === 'number' && Number.isFinite(doc)) doc = JSON.stringify(doc);
    if (typeof doc === 'string' && doc && doc.length <= MAX_TENANT_CHARS) return { tenant: doc, source: 'body_path' };
  }
  return undefined;
}

/** Section 15 attributes for one message on a send or receive span. */
function contentAttrs(config, queueName, body, attributes, md5, lambdaRecord = false) {
  const out = {};
  const size = bodySize(body);
  if (size !== undefined) out['messaging.message.body.size'] = size;
  const t = tenantFrom(config, attributes, body, lambdaRecord);
  if (t) {
    out['trailbeam.tenant'] = t.tenant;
    out['trailbeam.tenant.source'] = t.source;
  }
  const offloadedBytes = offloadedSize(attributes, lambdaRecord);
  if (offloadedBytes !== undefined && typeof body === 'string' && isPointerDoc(parse(body))) {
    out['trailbeam.body.offloaded_size'] = offloadedBytes;
  }
  if (typeof body !== 'string') return out;
  if (!config.bodyOn(queueName)) {
    out['trailbeam.body.capture_status'] = STATUS_OFF;
    return out;
  }
  if (!md5Matches(body, md5)) {
    out['trailbeam.body.capture_status'] = STATUS_MD5_MISMATCH;
    return out;
  }
  let redacted;
  let count;
  let format;
  try {
    [redacted, count, format] = redactBody(body, config);
    encodeString(redacted);
  } catch (e) {
    out['trailbeam.body.capture_status'] = STATUS_REDACTION_FAILED;
    return out;
  }
  if (utf8Bytes(redacted) > config.maxBodyBytes) {
    out['trailbeam.body.capture_status'] = STATUS_EXCEEDS_LIMIT;
    return out;
  }
  const doc = format === 'json' ? parse(body) : undefined;
  out['trailbeam.body.capture_status'] = isPointerDoc(doc) ? STATUS_OFFLOADED : STATUS_CAPTURED;
  out['trailbeam.body'] = redacted;
  out['trailbeam.body.format'] = format;
  out['trailbeam.body.redacted_count'] = count;
  const env = envelope(doc);
  if (env) out['trailbeam.body.envelope'] = env;
  return out;
}

/** The thrown value's message, redacted and cut to 1,024 characters, if error text is on for the queue. */
function errorMessage(config, queueName, err) {
  if (!config.errorsOn(queueName)) return undefined;
  let text;
  try {
    text = err instanceof Error ? err.message : (typeof err === 'string' ? err : undefined);
  } catch (e) {
    return undefined;
  }
  if (typeof text !== 'string' || !text) return undefined;
  try {
    [text] = redactText(text, config);
    const chars = Array.from(text); // code points, as Python slices strings
    if (chars.length > MAX_ERROR_MESSAGE_CHARS) text = chars.slice(0, MAX_ERROR_MESSAGE_CHARS).join('');
    encodeString(text);
  } catch (e) {
    return undefined; // fail closed: no text rather than unredacted text
  }
  return text;
}

function fifoAttrs(groupId, sequenceNumber) {
  const out = {};
  if (typeof groupId === 'string' && groupId) out['trailbeam.message_group_id'] = groupId;
  if (typeof sequenceNumber === 'string' && sequenceNumber) out['trailbeam.sequence_number'] = sequenceNumber;
  return out;
}

// -- SNS fan-out (section 20, revision 3.4) -------------------------------------------------------

const PUBLISH_ID_ATTRIBUTE = 'trailbeam.publish_id';
const SNS_MAX_ATTRIBUTES = 10; // with raw delivery on, SNS drops a message with more (section 20.2)
const SNS_MAX_MESSAGE_BYTES = 262144; // the topics' default MaximumMessageSize
const SKIP_OFF = 'off';
const SKIP_JSON_STRUCTURE = 'json_structure';
const SKIP_ATTRIBUTE_LIMIT = 'attribute_limit';
const SKIP_SIZE_LIMIT = 'size_limit';
const PUBLISH_ID_RE = /^[A-Za-z0-9-]{1,64}$/;
const SNS_MESSAGE_ID_RE = /^[A-Za-z0-9-]{1,100}$/;
const TOPIC_ARN_RE = /^arn:[a-z0-9-]+:sns:[a-z0-9-]+:[0-9]{12}:[A-Za-z0-9_-]{1,256}(?:[.]fifo)?$/;
const SNS_TIMESTAMP_RE = /^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})(?:[.]([0-9]{1,9}))?Z$/;
const INJECTED_BYTES = PUBLISH_ID_ATTRIBUTE.length + 'String'.length + 36; // name, type and a UUID: 62

function utf8Len(v) {
  if (typeof v === 'string') return utf8Bytes(v);
  if (v instanceof Uint8Array) return v.byteLength; // Buffer too
  return 0;
}

/** What one Publish (batch entry) counts toward SNS's size limit, without a trailbeam.publish_id: Message,
 * Subject, and each attribute's name, type and value, in UTF-8 bytes. */
function snsEntryBytes(entry) {
  let n = utf8Len(entry.Message) + utf8Len(entry.Subject);
  const attrs = entry.MessageAttributes;
  if (isObject(attrs)) {
    for (const [name, a] of Object.entries(attrs)) {
      if (name === PUBLISH_ID_ATTRIBUTE) continue;
      n += utf8Len(name);
      if (isObject(a)) n += utf8Len(a.DataType) + utf8Len(a.StringValue) + utf8Len(a.BinaryValue);
    }
  }
  return n;
}

/**
 * Section 20.2: add trailbeam.publish_id to the entries of one Publish (a list of one: the input) or
 * PublishBatch call. Returns `{ entries, results }`: each entry is the caller's own object if nothing was
 * added, else a copy with a new MessageAttributes; each result is `['trailbeam.sns.publish_id', id]` or
 * `['trailbeam.sns.publish_id_skipped', reason]` (or undefined for an entry that is not an object). Never
 * modifies the caller's objects.
 */
function snsInject(config, entries, newId = () => crypto.randomUUID()) {
  let reasons = entries.map((e) => {
    if (!isObject(e)) return undefined;
    const attrs = isObject(e.MessageAttributes) ? e.MessageAttributes : {};
    const others = Object.keys(attrs).filter((k) => k !== PUBLISH_ID_ATTRIBUTE);
    if (!config.snsInject) return SKIP_OFF;
    if (lower(String(e.MessageStructure || '')) === 'json') return SKIP_JSON_STRUCTURE;
    if (others.length + 1 > SNS_MAX_ATTRIBUTES) return SKIP_ATTRIBUTE_LIMIT;
    return '';
  });
  // The whole call counts toward one size limit (PublishBatch: "the sum of the individual lengths").
  let total = entries.reduce((n, e) => n + (isObject(e) ? snsEntryBytes(e) : 0), 0);
  total += INJECTED_BYTES * reasons.filter((r) => r === '').length;
  if (total > SNS_MAX_MESSAGE_BYTES) reasons = reasons.map((r) => (r === '' ? SKIP_SIZE_LIMIT : r));
  const out = [];
  const results = [];
  entries.forEach((e, i) => {
    const reason = reasons[i];
    if (reason === undefined) {
      out.push(e);
      results.push(undefined);
    } else if (reason) {
      out.push(e);
      results.push(['trailbeam.sns.publish_id_skipped', reason]);
    } else {
      const id = newId();
      // Replaces a trailbeam.publish_id that a republished message carried.
      const attrs = { ...(isObject(e.MessageAttributes) ? e.MessageAttributes : {}), [PUBLISH_ID_ATTRIBUTE]: { DataType: 'String', StringValue: id } };
      out.push({ ...e, MessageAttributes: attrs });
      results.push(['trailbeam.sns.publish_id', id]);
    }
  });
  return { entries: out, results };
}

const DAYS_IN_MONTH = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
const isLeap = (y) => (y % 4 === 0 && y % 100 !== 0) || y % 400 === 0;

/** An SNS envelope Timestamp (`2021-02-16T21:41:19.978Z`) as epoch milliseconds, or undefined. Digits after
 * the third fractional one are dropped, not rounded. */
function snsTimestampMs(value) {
  if (typeof value !== 'string') return undefined;
  const m = SNS_TIMESTAMP_RE.exec(value);
  if (!m) return undefined;
  const [y, mo, d, h, mi, s] = m.slice(1, 7).map(Number);
  if (y < 1970 || mo < 1 || mo > 12 || h > 23 || mi > 59 || s > 59) return undefined;
  const days = mo === 2 && isLeap(y) ? 29 : DAYS_IN_MONTH[mo - 1];
  if (d < 1 || d > days) return undefined;
  const ms = Number(((m[7] || '') + '000').slice(0, 3));
  return Date.UTC(y, mo - 1, d, h, mi, s, ms);
}

/** The three fields of an SNS notification envelope (section 20.3), as receive attributes, or undefined if
 * the body is not one. Nothing else in the body is read or exported. */
function snsEnvelope(body) {
  if (typeof body !== 'string' || !body.includes('"Notification"') || body.replace(/^[ \t\n\r]+/, '')[0] !== '{') return undefined;
  const doc = parse(body);
  if (!isObject(doc) || doc.Type !== 'Notification') return undefined;
  const mid = doc.MessageId;
  const arn = doc.TopicArn;
  if (!(typeof mid === 'string' && SNS_MESSAGE_ID_RE.test(mid) && typeof arn === 'string' && TOPIC_ARN_RE.test(arn))) return undefined;
  const out = { 'trailbeam.sns.message_id': mid, 'trailbeam.sns.topic_arn': arn };
  const ts = snsTimestampMs(doc.Timestamp);
  if (ts !== undefined) out['trailbeam.sns.timestamp_ms'] = ts;
  return out;
}

/** Section 20.3 attributes of one received message: the publish ID from its message attribute (raw
 * delivery), and the envelope fields if the queue's envelope is read. */
function snsReceiveAttrs(config, queueName, body, attributes, lambdaRecord = false) {
  const out = {};
  if (isObject(attributes)) {
    const a = attributes[PUBLISH_ID_ATTRIBUTE];
    const v = isObject(a) ? a[lambdaRecord ? 'stringValue' : 'StringValue'] : undefined;
    if (typeof v === 'string' && PUBLISH_ID_RE.test(v)) out['trailbeam.sns.publish_id'] = v;
  }
  if (config.snsEnvelopeOn(queueName)) Object.assign(out, snsEnvelope(body) || {});
  return out;
}

module.exports = {
  PUBLISH_ID_ATTRIBUTE,
  snsEntryBytes,
  snsInject,
  snsTimestampMs,
  snsEnvelope,
  snsReceiveAttrs,
  Config,
  resolveConfig,
  redactText,
  redactBody,
  s3Pointer,
  offloadedSize,
  OFFLOADED_SIZE_ATTRIBUTES,
  tenantFrom,
  contentAttrs,
  errorMessage,
  fifoAttrs,
  bodySize,
  ENV,
  PATTERNS,
  DEFAULT_REDACT_KEYS,
  DEFAULT_MAX_BODY_BYTES,
  HARD_MAX_BODY_BYTES,
  STATUS_CAPTURED,
  STATUS_OFF,
  STATUS_MD5_MISMATCH,
  STATUS_REDACTION_FAILED,
  STATUS_EXCEEDS_LIMIT,
  STATUS_OFFLOADED,
};
