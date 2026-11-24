using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Trailbeam.Sqs
{
    /// <summary>
    /// A parent read from trace headers: the X-Ray header (on the SDK request when OpenTelemetry's
    /// AWSPropagatorPipelineHandler or the X-Ray SDK put it there; and the Lambda runtime's
    /// <c>_X_AMZN_TRACE_ID</c>) or W3C <c>traceparent</c>.
    /// </summary>
    internal static class TraceHeaders
    {
        internal static ActivityContext? Parent(IDictionary<string, string>? headers)
        {
            if (headers == null)
            {
                return null;
            }
            return FromXray(First(headers, "x-amzn-trace-id")) ?? FromTraceparent(First(headers, "traceparent"));
        }

        internal static string? First(IDictionary<string, string> headers, string name)
        {
            foreach (var e in headers)
            {
                if (e.Key != null && string.Equals(e.Key, name, StringComparison.OrdinalIgnoreCase) && !string.IsNullOrEmpty(e.Value))
                {
                    return e.Value;
                }
            }
            return null;
        }

        /// <summary><c>Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1</c>.</summary>
        internal static ActivityContext? FromXray(string? value)
        {
            if (value == null)
            {
                return null;
            }
            string? root = null;
            string? parent = null;
            foreach (string part in value.Split(';'))
            {
                int eq = part.IndexOf('=');
                if (eq < 0)
                {
                    continue;
                }
                string k = part.Substring(0, eq).Trim();
                string v = part.Substring(eq + 1).Trim();
                if (k == "Root")
                {
                    root = v;
                }
                else if (k == "Parent")
                {
                    parent = v;
                }
            }
            if (root == null || parent == null || root.Length != 35 || !root.StartsWith("1-", StringComparison.Ordinal) || root[10] != '-')
            {
                return null;
            }
            string traceId = (root.Substring(2, 8) + root.Substring(11)).ToLowerInvariant();
            return Make(traceId, parent.ToLowerInvariant());
        }

        /// <summary>W3C <c>00-&lt;trace id&gt;-&lt;span id&gt;-&lt;flags&gt;</c>.</summary>
        internal static ActivityContext? FromTraceparent(string? value)
        {
            if (value == null)
            {
                return null;
            }
            string[] p = value.Trim().Split('-');
            if (p.Length < 4 || p[1].Length != 32 || p[2].Length != 16)
            {
                return null;
            }
            return Make(p[1].ToLowerInvariant(), p[2].ToLowerInvariant());
        }

        private static ActivityContext? Make(string traceId, string spanId)
        {
            if (traceId.Length != 32 || spanId.Length != 16 || !IsHex(traceId) || !IsHex(spanId)
                || traceId == new string('0', 32) || spanId == new string('0', 16))
            {
                return null;
            }
            return new ActivityContext(ActivityTraceId.CreateFromString(traceId.AsSpan()),
                ActivitySpanId.CreateFromString(spanId.AsSpan()), ActivityTraceFlags.Recorded, null, isRemote: true);
        }

        private static bool IsHex(string s)
        {
            foreach (char c in s)
            {
                if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')))
                {
                    return false;
                }
            }
            return true;
        }
    }
}
