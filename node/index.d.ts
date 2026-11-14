// Type declarations for @qv2/sdk. The package itself is plain CommonJS; no build step.
import type { SpanExporter, SpanProcessor, BasicTracerProvider } from '@opentelemetry/sdk-trace-base';

export declare const version: string;
/** `qv2.schema_version` of the spans this package emits (docs/span-contract.md). */
export declare const SCHEMA_VERSION: 1;

export interface InstallOptions {
  /** Receipt-hash key. Takes precedence over $QV2_RECEIPT_HASH_KEY. */
  receiptHashKey?: string;
  /** Replaces the default OTLP/HTTP exporter to $QV2_OTLP_TRACES_ENDPOINT. */
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
/** Add the qv2 hooks to an SQSClient. Installs with defaults first if needed. Returns the client. */
export declare function instrument<C extends SdkV3Client>(client: C): C;
/** Turn the client hooks off, then flush and shut down the qv2 tracer provider. */
export declare function uninstall(): Promise<void>;
/** Flush client-hook spans waiting to be exported. */
export declare function forceFlush(): Promise<void>;

/** `qv2.receipt_handle.hmac`: lower hex of HMAC-SHA256(key, receiptHandle), first 32 characters. */
export declare function hashReceiptHandle(key: string, receiptHandle: string): string;
/** `qv2.receipt_handle.key_id`: lower hex of HMAC-SHA256(key, "qv2 key id"), first 8 characters. */
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
  /** The qv2 tracer provider of this wrapper. */
  qv2TracerProvider: BasicTracerProvider;
};

/**
 * Wrap a Lambda handler for an SQS event source mapping: one `qv2 receive` per record, exported
 * before the handler runs, and one `qv2 outcome` per record after it returns or throws.
 */
export declare function wrapHandler<E = any, C = any, R = any>(
  handler: LambdaHandler<E, C, R>,
  options?: WrapHandlerOptions,
): WrappedHandler<E, C, R>;
