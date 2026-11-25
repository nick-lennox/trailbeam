'use strict';
// Trailbeam hooks for AWS SDK v3 S3 clients (contract section 27, revision 3.7): one `trailbeam s3_upload` per
// PutObject, CompleteMultipartUpload or CopyObject call, the three S3 calls that raise an ObjectCreated event
// notification. Trailbeam joins the span to the SQS receives of the notification it caused (section 27.3).
//
//     const trailbeam = require('trailbeam');
//     const s3 = trailbeam.instrument(new S3Client({}));   // installs with defaults on first use
//
// The hooks read only the request's Bucket and Key and the response's ETag and VersionId: never the object's
// contents (Body), its user metadata (Metadata), CopySource, tags, checksums, encryption keys or the response's
// Location. The object key is recorded only when body capture is on for the bucket (section 15.1: the bucket's
// name, or `*`, in `capture.bodyQueues`), redacted like a body, as a receive records it only on a queue whose body
// is captured (section 23.2, the board's QV-314 decision).
//
// The span name and kind are registered here, so the SQS files stay as they are; index.js wires `install`,
// `instrument` and `uninstall` to this file.

const api = require('@opentelemetry/api');

const capture = require('./capture');
const contract = require('./contract');
const order = require('./order');
const { Emitter, nowMs, SERVICE_INSTANCE_ID } = require('./emitter');
const { errorCode } = require('./sqs');

const SPAN_S3_UPLOAD = 'trailbeam s3_upload';
contract.KINDS[SPAN_S3_UPLOAD] = ['s3.upload', 'send'];
contract.SYSTEMS[SPAN_S3_UPLOAD] = 'aws.s3';

const UPLOAD_OPS = new Set(['PutObject', 'CompleteMultipartUpload', 'CopyObject']);
const S3_CLIENT = 'S3Client';
const MIDDLEWARE_NAME = 'trailbeamS3Middleware';
// A bucket name as the request gives it, with the upper case and underscores of old us-east-1 buckets. An access
// point or Object Lambda ARN is not a bucket's name, so the call gets no span (27.1).
const BUCKET_RE = /^[A-Za-z0-9._-]{1,255}$/;
const MAX_ETAG = 1024;
const MAX_VERSION_ID = 1024;
const MAX_S3_KEY_BYTES = 1024;

function debug(what, err) {
  api.diag.debug(`trailbeam: ${what} failed`, err);
}

const isObject = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);
// Printable text of 1 to `limit` characters (code points); control characters make it not recorded.
// eslint-disable-next-line no-control-regex
const shortText = (v, limit) => (typeof v === 'string' && v.length > 0 && [...v].length <= limit && !/[\u0000-\u001f\u007f-\u009f]/.test(v) ? v : undefined);

/**
 * Section 27.1: the attributes of the one trailbeam s3_upload of a call, with `messaging.destination.name` (the
 * bucket), or undefined if the call gets no span. `output` is ignored when `callError` is set (the call failed).
 */
function uploadAttrs(config, operation, input, output, callError) {
  if (!UPLOAD_OPS.has(operation) || !isObject(input)) return undefined;
  const bucket = input.Bucket;
  if (typeof bucket !== 'string' || !BUCKET_RE.test(bucket)) return undefined;
  const a = { 'messaging.destination.name': bucket };
  if (callError !== undefined) {
    a['trailbeam.result'] = 'failed';
    a['trailbeam.error_code'] = callError;
  } else {
    a['trailbeam.result'] = 'ok';
    const r = isObject(output) ? output : {};
    // CopyObject returns the new object's ETag inside CopyObjectResult; the other two at the top level.
    const inner = operation === 'CopyObject' ? r.CopyObjectResult : r;
    const etag = shortText(isObject(inner) ? inner.ETag : undefined, MAX_ETAG);
    if (etag !== undefined) a['trailbeam.s3.etag'] = etag;
    const version = shortText(r.VersionId, MAX_VERSION_ID);
    if (version !== undefined) a['trailbeam.s3.version_id'] = version;
  }
  const key = input.Key;
  if (config.bodyOn(bucket) && typeof key === 'string' && key.length > 0 && Buffer.byteLength(key, 'utf8') <= MAX_S3_KEY_BYTES) {
    a['trailbeam.s3.object_key'] = capture.redactText(key, config)[0];
  }
  return a;
}

/** Turns S3 upload calls into Trailbeam spans. Never throws. */
class S3Hooks {
  constructor(emitter, config = new capture.Config()) {
    this.emitter = emitter;
    this.config = config;
  }

  /** `output` is the response, or undefined if the call threw `err`. `times` as in sqs.js. */
  onUpload(operation, input, output, err, parentContext, { startTime, endTime, issued, returned } = {}) {
    const callError = output === undefined ? errorCode(err) : undefined;
    const attrs = uploadAttrs(this.config, operation, input, output, callError);
    if (!attrs) return;
    const bucket = attrs['messaging.destination.name'];
    delete attrs['messaging.destination.name'];
    this.emitter.emit(SPAN_S3_UPLOAD, operation, bucket, undefined, attrs, {
      parentContext, startTime, endTime, order: { issued, returned },
    });
  }
}

/** The AWS SDK v3 middleware. Looks up the hooks on every call, so `detach` turns it off. */
function s3Middleware(getHooks) {
  return (next, context) => async (args) => {
    const hooks = getHooks();
    const cmd = context && context.commandName;
    const operation = typeof cmd === 'string' ? cmd.replace(/Command$/, '') : undefined;
    if (!hooks || context.clientName !== S3_CLIENT || !UPLOAD_OPS.has(operation) || !args || !args.input) return next(args);
    const input = args.input;
    const parentContext = api.context.active();
    const issued = order.tick(); // contract section 2a: immediately before the start
    const startTime = nowMs();
    let result;
    try {
      result = await next(args);
    } catch (err) {
      const times = { startTime, endTime: nowMs(), issued, returned: order.tick() };
      try { hooks.onUpload(operation, input, undefined, err, parentContext, times); } catch (e) { debug('s3 upload hook', e); }
      throw err;
    }
    const times = { startTime, endTime: nowMs(), issued, returned: order.tick() };
    try {
      hooks.onUpload(operation, input, (result && result.output) || {}, undefined, parentContext, times);
    } catch (e) {
      debug('s3 upload hook', e);
    }
    return result;
  };
}

let hooks = null;

/**
 * Called by index.js's `install` with the Trailbeam provider it made: the S3 hooks share its provider, resource
 * (service.instance.id) and capture options.
 */
function attach(provider, options = {}) {
  const config = capture.resolveConfig(options.tenant, options.capture, options.sns);
  const resource = provider && provider._resource; // BasicTracerProvider's resource; pinned by test/s3.test.js
  const clientId = resource && resource.attributes ? resource.attributes[SERVICE_INSTANCE_ID] : undefined;
  // S3 spans carry no receipt handle, so the hasher is never used: no key warning from here.
  hooks = new S3Hooks(new Emitter(provider, null, 'client', clientId), config);
}

function detach() {
  hooks = null;
}

const attached = () => hooks !== null;

/**
 * Add the S3 middleware to a client. It acts only on an S3Client (`context.clientName`), so index.js adds it to
 * every instrumented client. Safe to call more than once on the same client. Returns the client.
 */
function instrument(client) {
  client.middlewareStack.add(s3Middleware(() => hooks), {
    step: 'initialize',
    priority: 'high',
    name: MIDDLEWARE_NAME,
    override: true,
  });
  return client;
}

module.exports = {
  SPAN_S3_UPLOAD,
  UPLOAD_OPS,
  MIDDLEWARE_NAME,
  S3Hooks,
  s3Middleware,
  uploadAttrs,
  attach,
  detach,
  attached,
  instrument,
};
