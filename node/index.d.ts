// Type declarations for trailbeam. The package itself is plain CommonJS; no build step.
import type { SpanExporter, SpanProcessor, BasicTracerProvider } from '@opentelemetry/sdk-trace-base';

export declare const version: string;
/** `trailbeam.schema_version` of the spans this package emits (contract/span-contract.md). */
export declare const SCHEMA_VERSION: 3;

export interface InstallOptions {
  /** Receipt-hash key. Takes precedence over $TRAILBEAM_RECEIPT_HASH_KEY. */
  receiptHashKey?: string;
  /** Replaces the default OTLP/HTTP exporter to $TRAILBEAM_OTLP_TRACES_ENDPOINT. */
  spanExporter?: SpanExporter;
  /** Replaces the default batch span processor (and exporter). */
  spanProcessor?: SpanProcessor;
  /**
   * Where the tenant comes from (contract section 15). Replaces $TRAILBEAM_TENANT_ATTRIBUTE and
   * $TRAILBEAM_TENANT_BODY_PATH. The message attribute is tried first.
   */
  tenant?: TenantOptions;
  /**
   * Opt-in body capture and Lambda error text, with redaction (contract section 15). Replaces the
   * $TRAILBEAM_CAPTURE_* and $TRAILBEAM_REDACT_* variables. Off unless a queue is listed.
   */
  capture?: CaptureOptions;
  /**
   * SNS fan-out (contract section 20). Replaces $TRAILBEAM_SNS_INJECT.
   */
  sns?: SnsOptions;
}

export interface SnsOptions {
  /**
   * Add a `trailbeam.publish_id` message attribute to each SNS Publish / PublishBatch to a topic, so a
   * receive with raw delivery on can be joined to its publish. Default true. Skipped for a message that
   * already has 10 attributes, would pass 262,144 bytes, or uses `MessageStructure: 'json'`.
   */
  inject?: boolean;
}

export interface TenantOptions {
  /** Name of a String or Number message attribute that holds the tenant. */
  messageAttribute?: string;
  /** Dotted path to a string or number in a JSON body, such as `customer.id`. */
  bodyPath?: string;
}

export type RedactPattern = 'aws_access_key' | 'bearer' | 'email' | 'long_number';

export interface CaptureOptions {
  /** Queue names whose bodies are captured, or `*` for every queue. */
  bodyQueues?: string[];
  /** Queue names whose Lambda exception messages are recorded, or `*`. */
  errorQueues?: string[];
  /** Keys whose values are replaced, added to the defaults. A name matches at any depth; `a.b` matches that path only. */
  redactKeys?: string[];
  /** Built-in text patterns to apply. Default: all of them. */
  redactPatterns?: RedactPattern[];
  /** Largest redacted body that is captured, in UTF-8 bytes. Default 16384, at most 262144. */
  maxBodyBytes?: number;
}

/** Anything with an AWS SDK v3 middleware stack, normally an `SQSClient` or an `SNSClient`. */
export interface SdkV3Client {
  middlewareStack: { add(middleware: unknown, options: unknown): void };
}

/**
 * Create the client hooks for this process (tracer provider, receipt-hash key, handle map).
 * Calling it again replaces the previous installation.
 */
export declare function install(options?: InstallOptions): BasicTracerProvider;
/** Add the Trailbeam hooks to an SQSClient or SNSClient. Installs with defaults first if needed. Returns the client. */
export declare function instrument<C extends SdkV3Client>(client: C): C;
/** Turn the client hooks off, then flush and shut down the Trailbeam tracer provider. */
export declare function uninstall(): Promise<void>;
/** Flush client-hook spans waiting to be exported. */
export declare function forceFlush(): Promise<void>;

export interface ExportStats {
  /** Spans exported successfully. */
  exported: number;
  /** Spans accepted whose export has not finished yet. */
  queued: number;
  /** Spans dropped before export: the sum of `droppedByReason`. */
  dropped: number;
  /**
   * `queue_full`: the queue held $TRAILBEAM_MAX_QUEUE_SIZE spans; `export_failed`: the export failed or
   * timed out; `shutdown`: ended after shutdown, or still queued when it finished.
   */
  droppedByReason: { queue_full: number; export_failed: number; shutdown: number };
}
/**
 * Trailbeam spans of this process so far, over the default export path of `install` and `wrapHandler`
 * (not a `spanProcessor` passed in). Drops are also counted on the `trailbeam.sdk.spans.dropped` metric
 * and reported with `process.emitWarning` (code `TRAILBEAM_SPANS_DROPPED`).
 */
export declare function exportStats(): ExportStats;

/** `trailbeam.receipt_handle.hmac`: lower hex of HMAC-SHA256(key, receiptHandle), first 32 characters. */
export declare function hashReceiptHandle(key: string, receiptHandle: string): string;
/** `trailbeam.receipt_handle.key_id`: lower hex of HMAC-SHA256(key, "trailbeam key id"), first 8 characters. */
export declare function keyId(key: string): string;

export interface WrapHandlerOptions extends InstallOptions {
  /** Longest wait for each flush, in milliseconds. Default 2000. */
  flushTimeoutMs?: number;
  /**
   * Whether the event source mapping has `ReportBatchItemFailures` in `FunctionResponseTypes`.
   * When false, `batchItemFailures` is ignored, as Lambda ignores it. Default true.
   */
  reportBatchItemFailures?: boolean;
}

export type LambdaHandler<E = any, C = any, R = any> =
  | ((event: E, context: C) => R | Promise<R>)
  | ((event: E, context: C, callback: (error?: unknown, result?: R) => void) => void);

export type WrappedHandler<E = any, C = any, R = any> = ((event: E, context: C) => Promise<R>) & {
  /** The Trailbeam tracer provider of this wrapper. */
  trailbeamTracerProvider: BasicTracerProvider;
};

/**
 * Wrap a Lambda handler for an SQS event source mapping: one `trailbeam receive` per record, exported
 * before the handler runs, and one `trailbeam outcome` per record after it returns or throws.
 */
export declare function wrapHandler<E = any, C = any, R = any>(
  handler: LambdaHandler<E, C, R>,
  options?: WrapHandlerOptions,
): WrappedHandler<E, C, R>;
