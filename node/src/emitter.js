'use strict';
// The qv2 tracer provider (contract section 2) and the code that builds and emits qv2 spans.

const crypto = require('node:crypto');
const api = require('@opentelemetry/api');
const { BasicTracerProvider, BatchSpanProcessor, AlwaysOnSampler } = require('@opentelemetry/sdk-trace-base');
const { detectResources, envDetector, resourceFromAttributes } = require('@opentelemetry/resources');

const { version } = require('./version');
const { DEFAULT_ENDPOINT, ENDPOINT_ENV, KINDS, SCHEMA_VERSION, SCOPE_NAME } = require('./contract');

const SERVICE_NAME = 'service.name';
const SERVICE_INSTANCE_ID = 'service.instance.id';

/**
 * The resource for qv2 spans (section 5). `service.name` and `service.instance.id` come from
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
 * An always-on provider of our own, so the application's sampling never drops qv2 spans. It is not
 * registered globally. Exports OTLP/HTTP (protobuf) to $QV2_OTLP_TRACES_ENDPOINT unless a span
 * exporter or span processor is given. Returns `{ provider, resource }`.
 */
function makeProvider({ spanExporter, spanProcessor } = {}) {
  const resource = makeResource();

  if (!spanProcessor) {
    if (!spanExporter) {
      const { OTLPTraceExporter } = require('@opentelemetry/exporter-trace-otlp-proto');
      spanExporter = new OTLPTraceExporter({ url: process.env[ENDPOINT_ENV] || DEFAULT_ENDPOINT });
    }
    spanProcessor = new BatchSpanProcessor(spanExporter);
  }
  const provider = new BasicTracerProvider({ resource, sampler: new AlwaysOnSampler(), spanProcessors: [spanProcessor] });
  return { provider, resource };
}

/** Wall-clock time in epoch milliseconds, with sub-millisecond precision. */
const nowMs = () => performance.timeOrigin + performance.now();

/** Emits qv2 spans with the attributes every qv2 span carries (contract section 3). */
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
      'qv2.receipt_handle.hmac': this.hasher.hash(receiptHandle),
      'qv2.receipt_handle.key_id': this.hasher.keyId,
    };
  }

  /**
   * Emit one span. Zero-length unless `endTime` is given. `parentContext` defaults to the active
   * context, which inside an SDK call is the stock AWS SDK instrumentation span.
   */
  emit(name, operation, queueName, queueUrl, attrs, { parentContext, startTime, endTime } = {}) {
    const [kind, opType] = KINDS[name];
    const base = {
      'qv2.schema_version': SCHEMA_VERSION,
      'qv2.kind': kind,
      'qv2.source': this.source,
      'messaging.system.name': 'aws.sqs',
      'messaging.operation.type': opType,
      'messaging.operation.name': operation,
      'process.pid': process.pid,
    };
    if (queueName) base['messaging.destination.name'] = queueName;
    if (queueUrl) base['aws.sqs.queue.url'] = queueUrl;
    if (this.clientId) base['messaging.client.id'] = String(this.clientId);
    Object.assign(base, attrs);
    const t = startTime !== undefined ? startTime : nowMs();
    const span = this.tracer.startSpan(
      name,
      { kind: api.SpanKind.INTERNAL, attributes: base, startTime: t },
      parentContext || api.context.active(),
    );
    span.end(endTime !== undefined ? endTime : t);
  }
}

module.exports = { makeProvider, makeResource, Emitter, nowMs, SERVICE_INSTANCE_ID };
