using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Amazon.SQS;
using Amazon.SQS.Model;
using OpenTelemetry;
using Xunit;

// The runtime and the SDK's customizer registry are process-wide.
[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace Trailbeam.Sqs.Tests
{
    /// <summary>The pipeline handler on a real SDK client over a canned HTTP layer, registered the way users do it.</summary>
    public class PipelineTests
    {
        internal const string Key = "test-key-0123456789abcdef";
        internal const string Q = "http://localhost:9324/000000000000/orders";

        private readonly List<Activity> _spans = new List<Activity>();
        private readonly ReceiptHasher _hasher = new ReceiptHasher(Key);

        public PipelineTests()
        {
            Install(new Dictionary<string, string>());
        }

        private void Install(Dictionary<string, string> extra)
        {
            _spans.Clear();
            var env = new Dictionary<string, string>
            {
                ["TRAILBEAM_RECEIPT_HASH_KEY"] = Key,
                ["OTEL_SERVICE_NAME"] = "orders-worker",
            };
            foreach (var kv in extra)
            {
                env[kv.Key] = kv.Value;
            }
            TrailbeamRuntime.Install(k => env.TryGetValue(k, out var v) ? v : null,
                new SimpleActivityExportProcessor(new OpenTelemetry.Exporter.InMemoryExporter<Activity>(_spans)));
            TrailbeamSqs.Register();
        }

        internal static object? Tag(Activity a, string k) => a.TagObjects.Where(t => t.Key == k).Select(t => t.Value).FirstOrDefault();

        private static string Body(StubSqs.Req r) => r.Body.GetProperty("MessageBody").GetString()!;

        /// <summary>Answers the scenario's calls on one queue.</summary>
        internal static StubSqs.Res Answer(StubSqs.Req r)
        {
            switch (r.Operation)
            {
                case "SendMessage":
                    return StubSqs.Res.Ok("{\"MessageId\":\"m1\",\"MD5OfMessageBody\":\"" + CaptureTest.Md5(Body(r)) + "\"}");
                case "SendMessageBatch":
                    return StubSqs.Res.Ok("{\"Successful\":[{\"Id\":\"e2\",\"MessageId\":\"m2\",\"MD5OfMessageBody\":\""
                        + CaptureTest.Md5("{\"n\": 2}") + "\"}],\"Failed\":[{\"Id\":\"e3\",\"Code\":\"InvalidParameterValue\",\"SenderFault\":true}]}");
                case "ReceiveMessage":
                    return StubSqs.Res.Ok("{\"Messages\":[{\"MessageId\":\"m1\",\"ReceiptHandle\":\"rh-1\",\"MD5OfBody\":\""
                        + CaptureTest.Md5("{\"n\": 1}") + "\",\"Body\":\"{\\\"n\\\": 1}\",\"Attributes\":{"
                        + "\"ApproximateReceiveCount\":\"2\",\"SentTimestamp\":\"1700000000000\",\"ApproximateFirstReceiveTimestamp\":\"1700000000100\","
                        + "\"DeadLetterQueueSourceArn\":\"arn:aws:sqs:elasticmq:000000000000:src\"}}]}");
                case "DeleteMessage":
                    return r.Body.GetProperty("ReceiptHandle").GetString() == "bad"
                        ? StubSqs.Res.Error("com.amazonaws.sqs#ReceiptHandleIsInvalid")
                        : StubSqs.Res.Ok("{}");
                case "DeleteMessageBatch":
                    return StubSqs.Res.Ok("{\"Successful\":[{\"Id\":\"d1\"}],\"Failed\":[{\"Id\":\"bogus\",\"Code\":\"ReceiptHandleIsInvalid\",\"SenderFault\":true}]}");
                case "ChangeMessageVisibility":
                    return StubSqs.Res.Ok("{}");
                case "ChangeMessageVisibilityBatch":
                    return StubSqs.Res.Ok("{\"Successful\":[{\"Id\":\"v1\"}],\"Failed\":[]}");
                default:
                    return StubSqs.Res.Ok("{}");
            }
        }

        [Fact]
        public async Task SendReceiveSettle()
        {
            var stub = new StubSqs(Answer);
            using (var sqs = stub.Client())
            {
                await sqs.SendMessageAsync(new SendMessageRequest { QueueUrl = Q, MessageBody = "{\"n\": 1}" });
                await sqs.SendMessageBatchAsync(new SendMessageBatchRequest
                {
                    QueueUrl = Q,
                    Entries = new List<SendMessageBatchRequestEntry>
                    {
                        new SendMessageBatchRequestEntry { Id = "e2", MessageBody = "{\"n\": 2}" },
                        new SendMessageBatchRequestEntry { Id = "e3", MessageBody = "{\"n\": 3}" },
                    },
                });
                var receive = new ReceiveMessageRequest { QueueUrl = Q, MessageSystemAttributeNames = new List<string> { "SenderId" } };
                var got = await sqs.ReceiveMessageAsync(receive);
                Assert.Equal("2", got.Messages[0].Attributes["ApproximateReceiveCount"]);
                Assert.Equal(new List<string> { "SenderId" }, receive.MessageSystemAttributeNames); // the caller's request is put back
                await sqs.DeleteMessageAsync(new DeleteMessageRequest { QueueUrl = Q, ReceiptHandle = "rh-1" });
                await sqs.DeleteMessageBatchAsync(new DeleteMessageBatchRequest
                {
                    QueueUrl = Q,
                    Entries = new List<DeleteMessageBatchRequestEntry>
                    {
                        new DeleteMessageBatchRequestEntry { Id = "d1", ReceiptHandle = "rh-2" },
                        new DeleteMessageBatchRequestEntry { Id = "bogus", ReceiptHandle = "not-a-handle" },
                    },
                });
                await sqs.ChangeMessageVisibilityAsync(new ChangeMessageVisibilityRequest { QueueUrl = Q, ReceiptHandle = "rh-3", VisibilityTimeout = 0 });
                await sqs.ChangeMessageVisibilityBatchAsync(new ChangeMessageVisibilityBatchRequest
                {
                    QueueUrl = Q,
                    Entries = new List<ChangeMessageVisibilityBatchRequestEntry>
                    {
                        new ChangeMessageVisibilityBatchRequestEntry { Id = "v1", ReceiptHandle = "rh-4", VisibilityTimeout = 30 },
                    },
                });
                await Assert.ThrowsAnyAsync<AmazonSQSException>(() =>
                    sqs.DeleteMessageAsync(new DeleteMessageRequest { QueueUrl = Q, ReceiptHandle = "bad" }));
            }

            // The receive asked for Trailbeam's system attributes and the Extended Client's size attributes, and kept SenderId.
            var asked = stub.Requests.First(r => r.Operation == "ReceiveMessage").Body;
            var system = asked.GetProperty("MessageSystemAttributeNames").EnumerateArray().Select(x => x.GetString()).ToList();
            Assert.Contains("SenderId", system);
            foreach (string n in TrailbeamSqsPipelineHandler.ReceiveSystemAttributes)
            {
                Assert.Contains(n, system);
            }
            Assert.Contains("ExtendedPayloadSize", asked.GetProperty("MessageAttributeNames").EnumerateArray().Select(x => x.GetString()));

            Assert.Equal(new[]
            {
                "trailbeam send", "trailbeam send", "trailbeam receive", "trailbeam delete", "trailbeam delete", "trailbeam delete",
                "trailbeam change_visibility", "trailbeam change_visibility", "trailbeam delete",
            }, _spans.Select(s => s.DisplayName));

            foreach (var s in _spans)
            {
                Assert.Equal(ActivityKind.Internal, s.Kind);
                Assert.Equal("trailbeam", s.Source.Name);
                Assert.Equal(3L, Tag(s, "trailbeam.schema_version"));
                Assert.Equal("client", Tag(s, "trailbeam.source"));
                Assert.Equal("orders", Tag(s, "messaging.destination.name"));
                Assert.Equal(Q, Tag(s, "aws.sqs.queue.url"));
                Assert.IsType<long>(Tag(s, "process.pid"));
                Assert.True((long)Tag(s, "trailbeam.order.issued")! < (long)Tag(s, "trailbeam.order.returned")!);
                Assert.Equal(default, s.ParentSpanId); // no stock span: a root
                foreach (var t in s.TagObjects)
                {
                    Assert.False(t.Value is string v && (v.Length == 0 || v.Contains("rh-") || v.Contains(Key)), t.Key);
                }
            }

            var send = _spans[0];
            Assert.Equal("m1", Tag(send, "messaging.message.id"));
            Assert.Equal(8L, Tag(send, "messaging.message.body.size"));
            Assert.Equal("off", Tag(send, "trailbeam.body.capture_status"));
            Assert.Equal("e2", Tag(_spans[1], "trailbeam.batch_entry_id")); // the failed entry e3 has no span

            var recv = _spans[2];
            Assert.Equal(_hasher.Hash("rh-1"), Tag(recv, "trailbeam.receipt_handle.hmac"));
            Assert.Equal(_hasher.KeyId, Tag(recv, "trailbeam.receipt_handle.key_id"));
            Assert.Equal(2L, Tag(recv, "trailbeam.receive_count"));
            Assert.Equal(1700000000000L, Tag(recv, "trailbeam.sent_timestamp_ms"));
            Assert.Equal(1700000000100L, Tag(recv, "trailbeam.first_receive_timestamp_ms"));
            Assert.Equal("arn:aws:sqs:elasticmq:000000000000:src", Tag(recv, "trailbeam.dead_letter_queue_source_arn"));

            var del = _spans[3];
            Assert.Equal("m1", Tag(del, "messaging.message.id")); // a handle this process received
            Assert.Equal("ok", Tag(del, "trailbeam.result"));
            Assert.Null(Tag(_spans[4], "messaging.message.id"));
            Assert.Equal("ok", Tag(_spans[4], "trailbeam.result"));
            Assert.Equal("failed", Tag(_spans[5], "trailbeam.result"));
            Assert.Equal("ReceiptHandleIsInvalid", Tag(_spans[5], "trailbeam.error_code"));
            Assert.Equal("bogus", Tag(_spans[5], "trailbeam.batch_entry_id"));
            Assert.Equal(_spans[4].StartTimeUtc, _spans[5].StartTimeUtc); // one call, one interval
            Assert.Equal(Tag(_spans[4], "trailbeam.order.issued"), Tag(_spans[5], "trailbeam.order.issued"));
            Assert.Equal(0L, Tag(_spans[6], "trailbeam.visibility_timeout"));
            Assert.Equal(30L, Tag(_spans[7], "trailbeam.visibility_timeout"));
            Assert.Equal("failed", Tag(_spans[8], "trailbeam.result"));
            Assert.Equal("ReceiptHandleIsInvalid", Tag(_spans[8], "trailbeam.error_code"));

            for (int i = 1; i < _spans.Count; i++)
            {
                if (Tag(_spans[i], "trailbeam.order.issued")!.Equals(Tag(_spans[i - 1], "trailbeam.order.issued")))
                {
                    continue;
                }
                Assert.True((long)Tag(_spans[i - 1], "trailbeam.order.returned")! < (long)Tag(_spans[i], "trailbeam.order.issued")!);
                Assert.True(_spans[i - 1].StartTimeUtc + _spans[i - 1].Duration <= _spans[i].StartTimeUtc);
            }
        }

        [Fact]
        public async Task LegacyAttributeNamesStayLegacyAndAllIsLeftAlone()
        {
            var stub = new StubSqs(Answer);
            using (var sqs = stub.Client())
            {
#pragma warning disable CS0618
                await sqs.ReceiveMessageAsync(new ReceiveMessageRequest { QueueUrl = Q, AttributeNames = new List<string> { "SenderId" } });
#pragma warning restore CS0618
                await sqs.ReceiveMessageAsync(new ReceiveMessageRequest
                {
                    QueueUrl = Q, MessageSystemAttributeNames = new List<string> { "All" }, MessageAttributeNames = new List<string> { "All" },
                });
            }
            var legacy = stub.Requests[0].Body;
            Assert.False(legacy.TryGetProperty("MessageSystemAttributeNames", out _));
            var names = legacy.GetProperty("AttributeNames").EnumerateArray().Select(x => x.GetString()).ToList();
            Assert.Contains("SenderId", names);
            Assert.Contains("ApproximateReceiveCount", names);
            var all = stub.Requests[1].Body;
            Assert.Equal(new[] { "All" }, all.GetProperty("MessageSystemAttributeNames").EnumerateArray().Select(x => x.GetString()));
            Assert.Equal(new[] { "All" }, all.GetProperty("MessageAttributeNames").EnumerateArray().Select(x => x.GetString()));
        }

        [Fact]
        public async Task ParentIsTheActiveSpan()
        {
            using var source = new ActivitySource("test-app");
            using var listener = new ActivityListener
            {
                ShouldListenTo = s => s.Name == "test-app",
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            };
            ActivitySource.AddActivityListener(listener);
            var stub = new StubSqs(Answer);
            using (var sqs = stub.Client())
            using (var app = source.StartActivity("app work"))
            {
                await sqs.SendMessageAsync(new SendMessageRequest { QueueUrl = Q, MessageBody = "x" });
                Assert.Same(app, Activity.Current); // Trailbeam put the ambient span back
                Assert.Equal(app!.TraceId, _spans[0].TraceId);
                Assert.Equal(app.SpanId, _spans[0].ParentSpanId);
            }
        }

        [Fact]
        public async Task SpansSurviveSuppressedInstrumentation()
        {
            // OpenTelemetry.Instrumentation.AWS suppresses instrumentation inside its handler.
            var stub = new StubSqs(Answer);
            using (var sqs = stub.Client())
            using (SuppressInstrumentationScope.Begin())
            {
                await sqs.SendMessageAsync(new SendMessageRequest { QueueUrl = Q, MessageBody = "x" });
            }
            Assert.Single(_spans);
        }

        [Fact]
        public async Task OtherOperationsAreIgnoredAndRegisteringTwiceEmitsOnce()
        {
            TrailbeamSqs.Register();
            var stub = new StubSqs(r => r.Operation == "CreateQueue" ? StubSqs.Res.Ok("{\"QueueUrl\":\"" + Q + "\"}") : Answer(r));
            using (var sqs = stub.Client())
            {
                await sqs.CreateQueueAsync(new CreateQueueRequest { QueueName = "orders" });
                await sqs.SendMessageAsync(new SendMessageRequest { QueueUrl = Q, MessageBody = "x" });
            }
            Assert.Equal(new[] { "trailbeam send" }, _spans.Select(s => s.DisplayName));
        }

        [Fact]
        public async Task CaptureOnForAQueue()
        {
            Install(new Dictionary<string, string> { ["TRAILBEAM_CAPTURE_BODY_QUEUES"] = "orders", ["TRAILBEAM_TENANT_ATTRIBUTE"] = "tenant" });
            var stub = new StubSqs(Answer);
            using (var sqs = stub.Client())
            {
                await sqs.SendMessageAsync(new SendMessageRequest
                {
                    QueueUrl = Q, MessageBody = "{\"password\": \"hunter2\", \"n\": 1}",
                    MessageAttributes = new Dictionary<string, MessageAttributeValue>
                    {
                        ["tenant"] = new MessageAttributeValue { DataType = "String", StringValue = "acme" },
                    },
                });
                await sqs.ReceiveMessageAsync(new ReceiveMessageRequest { QueueUrl = Q });
            }
            Assert.Equal("captured", Tag(_spans[0], "trailbeam.body.capture_status"));
            Assert.Equal("{\"password\": \"[REDACTED]\", \"n\": 1}", Tag(_spans[0], "trailbeam.body"));
            Assert.Equal(1L, Tag(_spans[0], "trailbeam.body.redacted_count"));
            Assert.Equal("acme", Tag(_spans[0], "trailbeam.tenant"));
            Assert.Equal("captured", Tag(_spans[1], "trailbeam.body.capture_status"));
            Assert.Contains("tenant", stub.Requests[1].Body.GetProperty("MessageAttributeNames").EnumerateArray().Select(x => x.GetString()));
        }

        /// <summary>
        /// QV-548: an out-of-range number at the tenant path (which .NET Framework's double.Parse refuses) must
        /// not lose the receive spans of the other messages in the same response.
        /// </summary>
        [Fact]
        public async Task OutOfRangeNumberAtTenantPathKeepsTheBatch()
        {
            Install(new Dictionary<string, string> { ["TRAILBEAM_TENANT_BODY_PATH"] = "t" });
            string Msg(string id, string body) => "{\"MessageId\":\"" + id + "\",\"ReceiptHandle\":\"rh-" + id
                + "\",\"MD5OfBody\":\"" + CaptureTest.Md5(body) + "\",\"Body\":" + JsonSerializer.Serialize(body) + "}";
            var stub = new StubSqs(r => r.Operation == "ReceiveMessage"
                ? StubSqs.Res.Ok("{\"Messages\":[" + Msg("m1", "{\"t\":1E400}") + "," + Msg("m2", "{\"t\":\"acme\"}") + "]}")
                : Answer(r));
            using (var sqs = stub.Client())
            {
                var got = await sqs.ReceiveMessageAsync(new ReceiveMessageRequest { QueueUrl = Q, MaxNumberOfMessages = 10 });
                Assert.Equal(2, got.Messages.Count);
            }
            Assert.Equal(new[] { "trailbeam receive", "trailbeam receive" }, _spans.Select(s => s.DisplayName));
            Assert.Equal("Infinity", Tag(_spans[0], "trailbeam.tenant"));
            Assert.Equal("body_path", Tag(_spans[0], "trailbeam.tenant.source"));
            Assert.Equal("m2", Tag(_spans[1], "messaging.message.id"));
            Assert.Equal("acme", Tag(_spans[1], "trailbeam.tenant"));
        }

        private sealed class EndpointConnectionError : Exception
        {
            internal EndpointConnectionError()
                : base("could not connect")
            {
            }
        }

        [Fact]
        public async Task PurgeVectors()
        {
            var purge = Vectors.Get("purge");
            foreach (var c in purge.GetProperty("cases").EnumerateArray())
            {
                Install(new Dictionary<string, string>());
                string name = c.Str("name")!;
                var response = c.GetProperty("response");
                Func<StubSqs.Req, StubSqs.Res> handler;
                if (response.Has("thrown"))
                {
                    Assert.Equal("EndpointConnectionError", response.Str("thrown"));
                    handler = r => StubSqs.Res.Throw(new EndpointConnectionError());
                }
                else if (response.Has("error_code"))
                {
                    string code = response.Str("error_code")!;
                    handler = r => StubSqs.Res.Error(code);
                }
                else
                {
                    handler = r => StubSqs.Res.Ok("{}");
                }
                var stub = new StubSqs(handler);
                using (var sqs = stub.Client())
                {
                    try
                    {
                        await sqs.PurgeQueueAsync(new PurgeQueueRequest { QueueUrl = c.Str("queue_url") });
                    }
                    catch (Exception)
                    {
                        Assert.False(response.Has("ok"), name);
                    }
                }
                Assert.True(_spans.Count == 1, name);
                var s = _spans[0];
                Assert.Equal(purge.Str("span_name"), s.DisplayName);
                foreach (var a in c.GetProperty("attributes").EnumerateObject())
                {
                    object? want = a.Value.ValueKind == JsonValueKind.Number ? (object)a.Value.GetInt64() : a.Value.GetString();
                    Assert.True(Equals(want, Tag(s, a.Name)), name + ": " + a.Name + " = " + Tag(s, a.Name));
                }
                foreach (string p in purge.Strings("present"))
                {
                    Assert.True(Tag(s, p) != null, name + ": " + p);
                }
                foreach (string p in c.Strings("absent"))
                {
                    Assert.True(Tag(s, p) == null, name + ": " + p);
                }
            }
        }
    }
}
