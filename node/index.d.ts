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
}

/** Anything with an AWS SDK v3 middleware stack, normally an `SQSClient`. */
export interface SdkV3Client {
  middlewareStack: { add(middleware: unknown, options: unknown): void };
}

/**
 * Create the client hooks for this process (tracer provider, receipt-hash key, handle map).
 * Calling it again replaces the previous installation.
 */
export declare function install(options?: InstallOptions): BasicTracerProvider;
/** Add the Trailbeam hooks to an SQSClient. Installs with defaults first if needed. Returns the client. */
export declare function instrument<C extends SdkV3Client>(client: C): C;
/** Turn the client hooks off, then flush and shut down the Trailbeam tracer provider. */
export declare function uninstall(): Promise<void>;
/** Flush client-hook spans waiting to be exported. */
export declare function forceFlush(): Promise<void>;

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
