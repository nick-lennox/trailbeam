using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using System.Text;
using OpenTelemetry;
using OpenTelemetry.Exporter;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace Trailbeam.Sqs
{
    /// <summary>
    /// The Trailbeam runtime: its own always-on tracer provider (contract section 2), the receipt-hash key (section
    /// 7), the consumer identity (section 5) and the code that emits every Trailbeam span with the attributes of
    /// section 3.
    ///
    /// Configuration is read from the environment: <c>TRAILBEAM_OTLP_TRACES_ENDPOINT</c> (default
    /// <c>http://localhost:4318/v1/traces</c>), <c>TRAILBEAM_RECEIPT_HASH_KEY</c>, <c>TRAILBEAM_MAX_QUEUE_SIZE</c>,
    /// <c>OTEL_SERVICE_NAME</c> and <c>OTEL_RESOURCE_ATTRIBUTES</c>, and section 15's <c>TRAILBEAM_TENANT_*</c>,
    /// <c>TRAILBEAM_CAPTURE_*</c> and <c>TRAILBEAM_REDACT_*</c>.
    /// </summary>
    internal sealed class TrailbeamRuntime
    {
        internal const int SchemaVersion = 3;
        internal const string ScopeName = "trailbeam";
        internal const string DefaultKey = "trailbeam-default-receipt-hash-key";
        internal const string DefaultEndpoint = "http://localhost:4318/v1/traces";
        internal const int MinKeyBytes = 16;
        internal const int DefaultMaxQueueSize = 16384;

        internal const string SpanSend = "trailbeam send";
        internal const string SpanReceive = "trailbeam receive";
        internal const string SpanDelete = "trailbeam delete";
        internal const string SpanChangeVisibility = "trailbeam change_visibility";
        internal const string SpanPurge = "trailbeam purge";
        internal const string SpanOutcome = "trailbeam outcome";

        internal static readonly string Version = ReadVersion();

        /// <summary>One source for the life of the process; each runtime's provider listens to it.</summary>
        internal static readonly ActivitySource Source = new ActivitySource(ScopeName, Version);

        private static readonly object Lock = new object();
        private static volatile TrailbeamRuntime? _instance;
        private static bool _exitHooked;
        private static readonly ConcurrentDictionary<string, bool> Warned = new ConcurrentDictionary<string, bool>();

        private readonly TracerProvider _provider;
        private readonly long _pid;

        private TrailbeamRuntime(Func<string, string?> env, BaseProcessor<Activity>? processor)
        {
            var resource = ResourceAttributes(env);
            ClientId = (string)resource["service.instance.id"];
            Endpoint = NonEmpty(env("TRAILBEAM_OTLP_TRACES_ENDPOINT"), DefaultEndpoint);
            if (processor == null)
            {
                // Options made here, not read from OTEL_EXPORTER_OTLP_*: the application's headers (a vendor's API key)
                // and endpoint are for its own exporter, not for Trailbeam's.
                var options = new OtlpExporterOptions
                {
                    Endpoint = new Uri(Endpoint),
                    Protocol = OtlpExportProtocol.HttpProtobuf,
                    Headers = string.Empty,
                    TimeoutMilliseconds = 10000,
                };
                processor = new BatchActivityExportProcessor(new OtlpTraceExporter(options),
                    maxQueueSize: PositiveInt(env("TRAILBEAM_MAX_QUEUE_SIZE"), DefaultMaxQueueSize));
            }
            _provider = Sdk.CreateTracerProviderBuilder()
                .AddSource(ScopeName)
                .SetSampler(new AlwaysOnSampler())
                .SetResourceBuilder(ResourceBuilder.CreateEmpty().AddTelemetrySdk().AddAttributes(resource))
                .AddProcessor(processor)
                .Build()!;
            Hasher = MakeHasher(env("TRAILBEAM_RECEIPT_HASH_KEY"));
            Capture = Sqs.Capture.Config.FromEnv(env);
            if (Capture.Invalid != null)
            {
                WarnOnce("capture", "trailbeam: " + Capture.Invalid + " (TRAILBEAM_REDACT_KEYS, TRAILBEAM_REDACT_PATTERNS or "
                    + "TRAILBEAM_CAPTURE_MAX_BYTES); no body or error text is exported until it is fixed");
            }
#if NET5_0_OR_GREATER
            _pid = Environment.ProcessId;
#else
            using (var p = Process.GetCurrentProcess())
            {
                _pid = p.Id;
            }
#endif
        }

        internal string ClientId { get; }
        internal string Endpoint { get; }
        internal ReceiptHasher Hasher { get; }
        internal HandleMap Handles { get; } = new HandleMap();

        /// <summary>Section 15: tenant and opt-in body capture, from the environment.</summary>
        internal Capture.Config Capture { get; }

        // -- the process-wide runtime ----------------------------------------------------------------------

        /// <summary>The runtime, started on first use.</summary>
        internal static TrailbeamRuntime Get()
        {
            TrailbeamRuntime? t = _instance;
            if (t != null)
            {
                return t;
            }
            lock (Lock)
            {
                if (_instance == null)
                {
                    _instance = new TrailbeamRuntime(Environment.GetEnvironmentVariable, null);
                    if (!_exitHooked)
                    {
                        _exitHooked = true;
                        AppDomain.CurrentDomain.ProcessExit += (s, e) => Shutdown(TimeSpan.FromSeconds(5));
                    }
                }
                return _instance;
            }
        }

        /// <summary>Tests: replace the runtime with one that sends its spans to <paramref name="processor"/>.</summary>
        internal static TrailbeamRuntime Install(Func<string, string?> env, BaseProcessor<Activity> processor)
        {
            lock (Lock)
            {
                TrailbeamRuntime? old = _instance;
                _instance = new TrailbeamRuntime(env, processor);
                old?._provider.Shutdown(5000);
                return _instance;
            }
        }

        internal static bool Flush(TimeSpan timeout)
        {
            TrailbeamRuntime? t = _instance;
            return t == null || t._provider.ForceFlush(Millis(timeout));
        }

        internal static void Shutdown(TimeSpan timeout)
        {
            TrailbeamRuntime? t;
            lock (Lock)
            {
                t = _instance;
                _instance = null;
            }
            if (t != null)
            {
                t._provider.Shutdown(Millis(timeout));
                t._provider.Dispose();
            }
        }

        /// <summary>A new call-order scope and a new runtime (a snapshot restore); the old one is stopped without waiting.</summary>
        internal static void AfterRestore()
        {
            CallOrder.NewScope();
            TrailbeamRuntime? t;
            lock (Lock)
            {
                t = _instance;
                _instance = null;
            }
            t?._provider.Shutdown(0);
        }

        private static int Millis(TimeSpan t) =>
            t == System.Threading.Timeout.InfiniteTimeSpan ? -1 : (int)Math.Min(int.MaxValue, Math.Max(0, t.TotalMilliseconds));

        // -- emitting --------------------------------------------------------------------------------------

        /// <summary>
        /// Emits one Trailbeam span from the client hooks with the attributes every Trailbeam span carries (section 3),
        /// <paramref name="attrs"/> on top. The span is the interval [start, end] of the SQS call (section 2), with that
        /// call's order ticks (section 2a).
        /// </summary>
        internal void Emit(string name, string operation, string? queueUrl, List<KeyValuePair<string, object?>> attrs,
            ActivityContext? parent, DateTimeOffset start, DateTimeOffset end, string? threadName,
            CallOrder.Tick? issued, CallOrder.Tick? returned)
        {
            string? queue = string.IsNullOrEmpty(queueUrl) ? null : QueueName(queueUrl!);
            Emit(name, operation, "client", ClientId, queue, queueUrl, attrs, parent, start, end, threadName, issued, returned);
        }

        /// <summary>As above, for any source: <c>client</c> (the pipeline handler) or <c>lambda</c> (the Lambda wrapper).</summary>
        internal void Emit(string name, string operation, string source, string? clientId, string? queueName, string? queueUrl,
            List<KeyValuePair<string, object?>> attrs, ActivityContext? parent, DateTimeOffset start, DateTimeOffset end,
            string? threadName, CallOrder.Tick? issued, CallOrder.Tick? returned)
        {
            var tags = new List<KeyValuePair<string, object?>>(attrs.Count + 16)
            {
                new KeyValuePair<string, object?>("trailbeam.schema_version", (long)SchemaVersion),
                new KeyValuePair<string, object?>("trailbeam.kind", Kind(name)),
                new KeyValuePair<string, object?>("trailbeam.source", source),
                new KeyValuePair<string, object?>("messaging.system.name", "aws.sqs"),
                new KeyValuePair<string, object?>("messaging.operation.type", OperationType(name)),
                new KeyValuePair<string, object?>("messaging.operation.name", operation),
                new KeyValuePair<string, object?>("messaging.client.id", string.IsNullOrEmpty(clientId) ? ClientId : clientId),
                new KeyValuePair<string, object?>("process.pid", _pid),
            };
            if (!string.IsNullOrEmpty(threadName))
            {
                tags.Add(new KeyValuePair<string, object?>("thread.name", threadName));
            }
            if (!string.IsNullOrEmpty(queueName))
            {
                tags.Add(new KeyValuePair<string, object?>("messaging.destination.name", queueName));
            }
            if (!string.IsNullOrEmpty(queueUrl))
            {
                tags.Add(new KeyValuePair<string, object?>("aws.sqs.queue.url", queueUrl));
            }
            if (returned != null)
            {
                tags.Add(new KeyValuePair<string, object?>("trailbeam.order.scope", returned.Scope));
                // A tick of another scope (a snapshot restore while the call was in flight) is never mixed in.
                if (issued != null && issued.Scope == returned.Scope)
                {
                    tags.Add(new KeyValuePair<string, object?>("trailbeam.order.issued", issued.Value));
                }
                tags.Add(new KeyValuePair<string, object?>("trailbeam.order.returned", returned.Value));
            }
            tags.AddRange(attrs);
            if (end < start)
            {
                start = end;
            }

            Activity? saved = Activity.Current;
            try
            {
                // OpenTelemetry.Instrumentation.AWS suppresses instrumentation below its pipeline handler (so that the
                // HTTP call makes no second span), and that would drop Trailbeam's spans too. Trailbeam's always-on
                // provider records them whatever the application's instrumentation does.
                using (SuppressInstrumentationScope.Begin(false))
                {
                    // No ambient parent: the parent is the one given, or none (a root span).
                    Activity.Current = null;
                    Activity? a = Source.StartActivity(name, ActivityKind.Internal, parent ?? default, tags, null, start);
                    if (a != null)
                    {
                        a.SetEndTime(end.UtcDateTime);
                        a.Stop();
                    }
                }
            }
            finally
            {
                Activity.Current = saved;
            }
        }

        internal static string Kind(string spanName)
        {
            switch (spanName)
            {
                case SpanSend: return "sqs.send";
                case SpanReceive: return "sqs.receive";
                case SpanPurge: return "sqs.purge";
                case SpanOutcome: return "sqs.outcome";
                default: return "sqs.settle";
            }
        }

        internal static string OperationType(string spanName)
        {
            switch (spanName)
            {
                case SpanSend: return "send";
                case SpanReceive: return "receive";
                case SpanPurge: return "purge"; // a custom value: the semantic conventions define none for a purge
                case SpanOutcome: return "process";
                default: return "settle";
            }
        }

        /// <summary>The queue name: the last path segment of the queue URL (section 3).</summary>
        internal static string QueueName(string url)
        {
            string u = url.TrimEnd('/');
            return u.Substring(u.LastIndexOf('/') + 1);
        }

        // -- configuration ---------------------------------------------------------------------------------

        internal static ReceiptHasher MakeHasher(string? key)
        {
            if (string.IsNullOrEmpty(key))
            {
                WarnOnce("default-key", "trailbeam: no receipt-hash key configured (TRAILBEAM_RECEIPT_HASH_KEY); using the "
                    + "published default key, which gives no protection. Set the same key on every service that uses these queues.");
                key = DefaultKey;
            }
            else if (Encoding.UTF8.GetByteCount(key) < MinKeyBytes)
            {
                WarnOnce("short-key", "trailbeam: the receipt-hash key is shorter than " + MinKeyBytes + " bytes");
            }
            return new ReceiptHasher(key!);
        }

        /// <summary>
        /// <c>OTEL_RESOURCE_ATTRIBUTES</c>, then <c>OTEL_SERVICE_NAME</c> (default <c>unknown_service</c>, section 5),
        /// and a random <c>service.instance.id</c> unless one is configured.
        /// </summary>
        internal static Dictionary<string, object> ResourceAttributes(Func<string, string?> env)
        {
            var r = new Dictionary<string, object>(StringComparer.Ordinal);
            string? raw = env("OTEL_RESOURCE_ATTRIBUTES");
            if (raw != null)
            {
                foreach (string pair in raw.Split(','))
                {
                    int eq = pair.IndexOf('=');
                    if (eq <= 0)
                    {
                        continue;
                    }
                    string k = pair.Substring(0, eq).Trim();
                    string v = Decode(pair.Substring(eq + 1).Trim());
                    if (k.Length > 0 && v.Length > 0)
                    {
                        r[k] = v;
                    }
                }
            }
            string? service = env("OTEL_SERVICE_NAME");
            if (!string.IsNullOrEmpty(service))
            {
                r["service.name"] = service!;
            }
            if (!r.TryGetValue("service.name", out object? current) || ((string)current).StartsWith("unknown_service", StringComparison.Ordinal))
            {
                r["service.name"] = "unknown_service";
            }
            if (!r.TryGetValue("service.instance.id", out object? id) || ((string)id).Length == 0)
            {
                r["service.instance.id"] = Guid.NewGuid().ToString();
            }
            return r;
        }

        private static string Decode(string v)
        {
            try
            {
                return Uri.UnescapeDataString(v);
            }
            catch (Exception)
            {
                return v;
            }
        }

        private static int PositiveInt(string? raw, int dflt) =>
            int.TryParse((raw ?? string.Empty).Trim(), System.Globalization.NumberStyles.None,
                System.Globalization.CultureInfo.InvariantCulture, out int v) && v > 0 ? v : dflt;

        private static string NonEmpty(string? v, string dflt) =>
            string.IsNullOrWhiteSpace(v) ? dflt : v!.Trim();

        internal static void WarnOnce(string tag, string message)
        {
            if (Warned.TryAdd(tag, true))
            {
                TrailbeamEventSource.Log.Warning(message);
                Console.Error.WriteLine(message);
            }
        }

        internal static void InfoOnce(string tag, string message)
        {
            if (Warned.TryAdd(tag, true))
            {
                TrailbeamEventSource.Log.Info(message);
            }
        }

        private static string ReadVersion()
        {
            string? v = typeof(TrailbeamRuntime).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;
            return string.IsNullOrEmpty(v) ? "0.0.0-dev" : v!;
        }
    }
}
