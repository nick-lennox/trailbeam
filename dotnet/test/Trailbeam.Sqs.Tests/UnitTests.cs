using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Xunit;

namespace Trailbeam.Sqs.Tests
{
    /// <summary>The receipt-hash vectors of contract section 7.</summary>
    public class ContractVectorsTest
    {
        [Fact]
        public void ReceiptHandleHash()
        {
            int n = 0;
            foreach (var v in Vectors.Get("vectors").EnumerateArray())
            {
                var h = new ReceiptHasher(v.Str("key")!);
                Assert.Equal(v.Str("hmac"), h.Hash(v.Str("receipt_handle")!));
                Assert.Equal(v.Str("key_id"), h.KeyId);
                n++;
            }
            Assert.True(n >= 3);
            Assert.Equal("b5e633f7", new ReceiptHasher(Vectors.Get("default_key").GetString()!).KeyId);
        }

        [Fact]
        public void HasherNeverShowsTheKey()
        {
            Assert.DoesNotContain("secret", new ReceiptHasher("a-secret-key-0123456789").ToString());
        }
    }

    /// <summary>Contract section 15 against the <c>capture</c> vectors that Python, Node.js and Java are tested against.</summary>
    public class CaptureTest
    {
        internal static Capture.Config Config(JsonElement tenant, JsonElement capture) =>
            new Capture.Config(
                tenant.Str("message_attribute"),
                tenant.Str("body_path"),
                capture.Strings("body_queues"),
                capture.Strings("error_queues"),
                capture.Strings("redact_keys"),
                capture.ValueKind == JsonValueKind.Object && capture.TryGetProperty("redact_patterns", out _) ? capture.Strings("redact_patterns") : null,
                capture.Has("max_body_bytes") ? capture.GetProperty("max_body_bytes").GetInt32() : (int?)null);

        /// <summary>Message attributes as {name: StringValue} (client: StringValue, Lambda record: stringValue).</summary>
        internal static Dictionary<string, string> Attributes(JsonElement attrs, bool lambdaRecord)
        {
            var output = new Dictionary<string, string>();
            foreach (var a in attrs.EnumerateObject())
            {
                string? v = a.Value.Str(lambdaRecord ? "stringValue" : "StringValue");
                if (v != null)
                {
                    output[a.Name] = v;
                }
            }
            return output;
        }

        private static JsonElement Cap => Vectors.Get("capture");

        private static object? Get(List<KeyValuePair<string, object>> attrs, string key) =>
            attrs.Where(a => a.Key == key).Select(a => a.Value).FirstOrDefault();

        [Fact]
        public void RedactionVectors()
        {
            int n = 0;
            foreach (var v in Cap.GetProperty("redaction").EnumerateArray())
            {
                string name = v.Str("name")!;
                var r = Capture.RedactBody(v.Str("body")!, Config(default, v.GetProperty("capture")));
                Assert.True(v.Str("redacted") == r.Text, name + ": " + r.Text);
                Assert.True(v.GetProperty("redacted_count").GetInt32() == r.Count, name);
                Assert.True(v.Str("format") == r.Format, name);
                n++;
            }
            Assert.True(n >= 17);
        }

        [Fact]
        public void TenantVectors()
        {
            foreach (var v in Cap.GetProperty("tenant").EnumerateArray())
            {
                bool lambda = v.GetProperty("lambda_record").GetBoolean();
                var got = Capture.Tenant(Config(v.GetProperty("tenant"), default),
                    Attributes(v.GetProperty("message_attributes"), lambda), v.Str("body"));
                var want = v.GetProperty("expected");
                if (want.ValueKind == JsonValueKind.Null)
                {
                    Assert.True(got == null, v.Str("name"));
                }
                else
                {
                    Assert.True(got != null, v.Str("name"));
                    Assert.Equal(want.Str("tenant"), got!.Value.Tenant);
                    Assert.Equal(want.Str("source"), got.Value.Source);
                }
            }
        }

        [Fact]
        public void OffloadedSizeVectors()
        {
            foreach (var v in Cap.GetProperty("offloaded_size").EnumerateArray())
            {
                bool lambda = v.GetProperty("lambda_record").GetBoolean();
                var got = Capture.ContentAttrs(Capture.Config.Off(), "q", v.Str("body"),
                    Attributes(v.GetProperty("message_attributes"), lambda), null);
                var want = v.GetProperty("expected");
                Assert.True(Equals(want.ValueKind == JsonValueKind.Null ? null : (object)want.GetInt64(),
                    Get(got, "trailbeam.body.offloaded_size")), v.Str("name"));
                Assert.Equal("off", Get(got, "trailbeam.body.capture_status"));
            }
        }

        [Fact]
        public void ErrorMessageVectors()
        {
            foreach (var v in Cap.GetProperty("error_message").EnumerateArray())
            {
                string? got = Capture.ErrorMessage(Config(default, v.GetProperty("capture")), v.Str("queue"), v.Str("message"));
                var want = v.GetProperty("expected");
                Assert.True((want.ValueKind == JsonValueKind.Null ? null : want.GetString()) == got, v.Str("name"));
            }
        }

        [Fact]
        public void EnvironmentAndBadValues()
        {
            var env = new Dictionary<string, string>
            {
                ["TRAILBEAM_CAPTURE_BODY_QUEUES"] = "orders, payments",
                ["TRAILBEAM_TENANT_ATTRIBUTE"] = "tenant",
                ["TRAILBEAM_CAPTURE_MAX_BYTES"] = "100",
            };
            Func<Dictionary<string, string>, Func<string, string?>> lookup = e => k => e.TryGetValue(k, out var x) ? x : null;
            var c = Capture.Config.FromEnv(lookup(env));
            Assert.True(c.BodyOn("orders") && c.BodyOn("payments") && !c.BodyOn("other"));
            Assert.Equal("tenant", c.TenantAttribute);
            Assert.Equal(100, c.MaxBodyBytes);
            Assert.Null(c.Invalid);
            Assert.Equal(4, c.RedactPatterns.Count);

            env["TRAILBEAM_REDACT_PATTERNS"] = "";
            Assert.Empty(Capture.Config.FromEnv(lookup(env)).RedactPatterns); // set but empty: no patterns

            // Python and Node.js refuse these at install time. This package fails closed, as the Java one does: a listed
            // queue's body is not exported, and says why.
            foreach (var (k, v) in new[]
            {
                ("TRAILBEAM_REDACT_KEYS", "a b"), ("TRAILBEAM_REDACT_PATTERNS", "nope"),
                ("TRAILBEAM_CAPTURE_MAX_BYTES", "262145"), ("TRAILBEAM_CAPTURE_MAX_BYTES", "lots"),
            })
            {
                var e = new Dictionary<string, string>(env);
                e.Remove("TRAILBEAM_REDACT_PATTERNS");
                e[k] = v;
                var b = Capture.Config.FromEnv(lookup(e));
                Assert.True(b.Invalid != null, k + "=" + v);
                var got = Capture.ContentAttrs(b, "orders", "password=x", null, null);
                Assert.Equal("redaction_failed", Get(got, "trailbeam.body.capture_status"));
                Assert.Null(Get(got, "trailbeam.body"));
                Assert.Null(Capture.ErrorMessage(b, "orders", "boom"));
            }
        }

        internal static string Md5(string s)
        {
            using var h = MD5.Create();
            return string.Concat(h.ComputeHash(Encoding.UTF8.GetBytes(s)).Select(b => b.ToString("x2")));
        }

        [Fact]
        public void Statuses()
        {
            var c = new Capture.Config(null, null, new[] { "q" }, Array.Empty<string>(), Array.Empty<string>(), null, 100);
            Assert.Equal("off", Get(Capture.ContentAttrs(c, "other", "x", null, null), "trailbeam.body.capture_status"));
            Assert.Equal("md5_mismatch", Get(Capture.ContentAttrs(c, "q", "x", null, Md5("y")), "trailbeam.body.capture_status"));
            Assert.Equal("captured", Get(Capture.ContentAttrs(c, "q", "x", null, Md5("x").ToUpperInvariant()), "trailbeam.body.capture_status"));
            Assert.Equal("exceeds_capture_limit", Get(Capture.ContentAttrs(c, "q", new string('y', 101), null, null), "trailbeam.body.capture_status"));
            string deep = new string('[', 200) + new string(']', 200);
            Assert.Equal("redaction_failed", Get(Capture.ContentAttrs(c, "q", deep, null, null), "trailbeam.body.capture_status"));

            var sns = Capture.ContentAttrs(c, "q", "{\"Type\":\"Notification\",\"TopicArn\":\"t\",\"Message\":\"m\"}", null, null);
            Assert.Equal("json", Get(sns, "trailbeam.body.format"));
            Assert.Equal(0L, Get(sns, "trailbeam.body.redacted_count"));
            Assert.Equal("sns", Get(sns, "trailbeam.body.envelope"));
            Assert.Equal("eventbridge", Get(Capture.ContentAttrs(c, "q", "{\"detail-type\":\"x\",\"detail\":{},\"source\":\"s\"}", null, null),
                "trailbeam.body.envelope"));
        }

        [Fact]
        public void SizesAndNumbers()
        {
            Assert.Equal(2L, Capture.Utf8Length("\u00e9"));
            Assert.Equal(4L, Capture.Utf8Length("\ud83d\ude00"));
            Assert.Equal(3L, Capture.Utf8Length("\ud800")); // a lone surrogate counts as Python's surrogatepass does
            Assert.Equal("12345678901234567890", Capture.NumberText("12345678901234567890"));
            Assert.Equal("0", Capture.NumberText("-0"));
            Assert.Equal("1.5", Capture.NumberText("1.5"));
            // Out of range: .NET Framework's double.Parse throws here; the result is Python's and Java's.
            Assert.Equal("Infinity", Capture.NumberText("1E400"));
            Assert.Equal("-Infinity", Capture.NumberText("-1E400"));
            Assert.Equal(new string('9', 256), Capture.NumberText(new string('9', 256)));
            Assert.Null(Capture.NumberText(new string('9', 257))); // longer than a tenant may be
        }

        /// <summary>QV-548: a huge integer at the tenant path must not cost BigInteger's quadratic conversion.</summary>
        [Fact]
        public void HugeNumberAtTenantPathIsCheap()
        {
            var c = new Capture.Config(null, "t", Array.Empty<string>(), Array.Empty<string>(), Array.Empty<string>(), null, null);
            string body = "{\"t\":" + new string('1', 1_048_000) + "}"; // about 1 MiB, SQS's largest body
            Capture.ContentAttrs(c, "q", "{\"t\":1}", null, null); // load and JIT the code path once
            long best = long.MaxValue;
            for (int i = 0; i < 3; i++)
            {
                var sw = System.Diagnostics.Stopwatch.StartNew();
                var attrs = Capture.ContentAttrs(c, "q", body, null, null);
                best = Math.Min(best, sw.ElapsedMilliseconds);
                Assert.Null(Get(attrs, "trailbeam.tenant")); // more than 256 characters: no tenant, as before
                Assert.Equal((long)body.Length, Get(attrs, "messaging.message.body.size"));
            }
            Assert.True(best < 50, "content attributes took " + best + " ms");
            // A long decimal still prints as the double it is, and quickly.
            var sw2 = System.Diagnostics.Stopwatch.StartNew();
            Assert.Equal("1", Get(Capture.ContentAttrs(c, "q", "{\"t\":1." + new string('0', 1_047_990) + "}", null, null), "trailbeam.tenant"));
            Assert.True(sw2.ElapsedMilliseconds < 200, "long decimal took " + sw2.ElapsedMilliseconds + " ms");
        }

        [Fact]
        public void LoneSurrogateFailsClosed()
        {
            var c = new Capture.Config(null, null, new[] { "q" }, Array.Empty<string>(), Array.Empty<string>(), null, null);
            // The escape decodes to a lone surrogate; redacting the string rewrites it, and that cannot be UTF-8.
            Assert.Equal("redaction_failed", Get(Capture.ContentAttrs(c, "q", "{\"note\": \"\\ud800 mail a@b.co\"}", null, null), "trailbeam.body.capture_status"));
            // Left as written, the escape is plain ASCII and exports fine.
            Assert.Equal("captured", Get(Capture.ContentAttrs(c, "q", "{\"note\": \"\\ud800\"}", null, null), "trailbeam.body.capture_status"));
        }

        [Fact]
        public void KeyNamesFoldAsciiOnly()
        {
            var c = new Capture.Config(null, null, new[] { "q" }, Array.Empty<string>(), new[] { "kid" }, Array.Empty<string>(), null);
            Assert.Equal("KID=[REDACTED]", Capture.RedactText("KID=7", c).Text);
            // U+212A KELVIN SIGN folds to k in .NET's IgnoreCase; the contract's rules fold ASCII only.
            Assert.Equal("\u212AID=7", Capture.RedactText("\u212AID=7", c).Text);
        }
    }

    public class HandleMapTest
    {
        [Fact]
        public void BoundedOldestFirstAndExpiring()
        {
            long now = 0;
            var m = new HandleMap(2, TimeSpan.FromSeconds(10), () => now);
            m.Put("a", "1");
            m.Put("b", "2");
            m.Put("c", "3");
            Assert.Null(m.Get("a"));
            Assert.Equal("2", m.Get("b"));
            Assert.Equal(2, m.Count);
            now += (long)(11 * System.Diagnostics.Stopwatch.Frequency);
            Assert.Null(m.Get("b"));
            m.Put("d", "4");
            Assert.Equal(1, m.Count);
            m.Remove("d");
            Assert.Equal(0, m.Count);
        }
    }

    public class TraceHeadersTest
    {
        [Fact]
        public void XrayAndTraceparent()
        {
            var x = TraceHeaders.FromXray("Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1");
            Assert.Equal("5759e988bd862e3fe1be46a994272793", x!.Value.TraceId.ToHexString());
            Assert.Equal("53995c3f42cd8ad8", x.Value.SpanId.ToHexString());
            var w = TraceHeaders.FromTraceparent("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00");
            Assert.Equal("b7ad6b7169203331", w!.Value.SpanId.ToHexString());
            Assert.Null(TraceHeaders.FromXray("Root=1-5759e988-bd862e3fe1be46a994272793")); // no Parent
            Assert.Null(TraceHeaders.FromTraceparent("00-00000000000000000000000000000000-b7ad6b7169203331-01"));
            var headers = new Dictionary<string, string> { ["X-Amzn-Trace-Id"] = "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8" };
            Assert.NotNull(TraceHeaders.Parent(headers));
        }
    }
}
