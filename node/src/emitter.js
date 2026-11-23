'use strict';
// The Trailbeam tracer provider (contract section 2) and the code that builds and emits Trailbeam spans.

const crypto = require('node:crypto');
const api = require('@opentelemetry/api');
const { BasicTracerProvider, AlwaysOnSampler } = require('@opentelemetry/sdk-trace-base');
const { detectResources, envDetector, resourceFromAttributes } = require('@opentelemetry/resources');

const { version } = require('./version');
const order = require('./order');
const { CountingSpanProcessor } = require('./export');
const { DEFAULT_ENDPOINT, ENDPOINT_ENV, KINDS, SCHEMA_VERSION, SCOPE_NAME, SYSTEMS } = require('./contract');

const SERVICE_NAME = 'service.name';
const SERVICE_INSTANCE_ID = 'service.instance.id';

/**
 * The resource for Trailbeam spans (section 5). `service.name` and `service.instance.id` come from
 * OTEL_SERVICE_NAME / OTEL_RESOURCE_ATTRIBUTES if set, else `unknown_service` and a UUID v4.
 */
function makeResource() {
  const detected = detectResources({ detectors: [envDetector] });
  const attrs = detected.attributes || {};
  const extra = {};
  if (!attrs[SERVICE_NAME]) extra[SERVICE_NAME] = 'unknown_service';
  if (!attrs[SERVICE_INSTANCE_ID]) extra[SERVICE_INSTANCE_ID] = crypto.randomUUID();
  return detected.merge(resourceFromAttributes(extra));
}

/**
 * An always-on provider of our own, so the application's sampling never drops Trailbeam spans. It is not
 * registered globally. Exports OTLP/HTTP (protobuf) to $TRAILBEAM_OTLP_TRACES_ENDPOINT unless a span
 * exporter or span processor is given. Without a span processor, spans go through a CountingSpanProcessor: a
 * batch queue of $TRAILBEAM_MAX_QUEUE_SIZE spans (default 16,384) that counts and reports every span it
 * drops (`trailbeam.exportStats()`). Returns `{ provider, resource }`.
 */
function makeProvider({ spanExporter, spanProcessor } = {}) {
  const resource = makeResource();

  if (!spanProcessor) {
    if (!spanExporter) {
      const { OTLPTraceExporter } = require('@opentelemetry/exporter-trace-otlp-proto');
      spanExporter = new OTLPTraceExporter({ url: process.env[ENDPOINT_ENV] || DEFAULT_ENDPOINT });
    }
    spanProcessor = new CountingSpanProcessor(spanExporter);
  }
  const provider = new BasicTracerProvider({ resource, sampler: new AlwaysOnSampler(), spanProcessors: [spanProcessor] });
  return { provider, resource };
}

/**
 * Wall-clock time in epoch milliseconds (contract section 2). `Date.now()` reads the system clock on
 * every call, as Python's `time.time_ns()` does, so every process on a host reads the same clock.
 * (`performance.timeOrigin + performance.now()` has sub-millisecond precision but follows the
 * monotonic clock from process start, so it drifts away from the system clock and from other
 * processes.) Millisecond rounding is always down, which never turns "A ended before B started"
 * around; it can only make two steps look simultaneous.
 */
const nowMs = () => Date.now();

/** Emits Trailbeam spans with the attributes every Trailbeam span carries (contract section 3). */
class Emitter {
  constructor(provider, hasher, source, clientId) {
    this.provider = provider;
    this.hasher = hasher;
    this.source = source;
    this.clientId = clientId;
    this.tracer = provider.getTracer(SCOPE_NAME, version);
  }

  hashAttrs(receiptHandle) {
    return {
      'trailbeam.receipt_handle.hmac': this.hasher.hash(receiptHandle),
      'trailbeam.receipt_handle.key_id': this.hasher.keyId,
    };
  }

  /**
   * Emit one span from `startTime` to `endTime` (epoch ms; contract section 2: just before the
   * request was sent, and when the response arrived). `startObserved: false` marks a span whose
   * start is not known (a Lambda receive): it is then zero-length at `endTime`. `parentContext`
   * defaults to the active context, which inside an SDK call is the stock AWS SDK span.
   *
   * Call order (schema version 3): `order` is `{ issued, returned }`, ticks of this process's call-order
   * counter (`order.tick()`) taken at the span's start and end; `issued` is undefined when the start
   * was not observed. With an order, the span also carries `trailbeam.order.scope`.
   */
  emit(name, operation, queueName, queueUrl, attrs, { parentContext, startTime, endTime, startObserved = true, order: ticks } = {}) {
    const [kind, opType] = KINDS[name];
    const base = {
      'trailbeam.schema_version': SCHEMA_VERSION,
      'trailbeam.kind': kind,
      'trailbeam.source': this.source,
      'messaging.system.name': SYSTEMS[name] || 'aws.sqs',
      'messaging.operation.type': opType,
      'messaging.operation.name': operation,
      'process.pid': process.pid,
    };
    if (queueName) base['messaging.destination.name'] = queueName;
    if (queueUrl) base['aws.sqs.queue.url'] = queueUrl;
    if (this.clientId) base['messaging.client.id'] = String(this.clientId);
    if (!startObserved) base['trailbeam.start_observed'] = false;
    if (ticks) {
      const { issued, returned } = ticks;
      if (issued !== undefined || returned !== undefined) base['trailbeam.order.scope'] = order.scope;
      if (issued !== undefined) base['trailbeam.order.issued'] = issued;
      if (returned !== undefined) base['trailbeam.order.returned'] = returned;
    }
    Object.assign(base, attrs);
    const end = endTime !== undefined ? endTime : nowMs();
    const start = startObserved && startTime !== undefined ? Math.min(startTime, end) : end;
    const span = this.tracer.startSpan(
      name,
      { kind: api.SpanKind.INTERNAL, attributes: base, startTime: start },
      parentContext || api.context.active(),
    );
    span.end(end);
  }
}

module.exports = { makeProvider, makeResource, Emitter, nowMs, SERVICE_INSTANCE_ID };
