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
