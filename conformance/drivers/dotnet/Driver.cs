// Conformance driver for the .NET package: runs conformance/SCENARIO.md on the AWS SDK for .NET v4 and writes the
// ground truth to $KIT_TRUTH_FILE. Trailbeam is set up the way dotnet/README.md tells users: one line at startup,
// before the first SQS client.
//
// KIT_APP_TRACING=otel: the application has its own tracing, as a real service would: a tracer provider exporting
// over OTLP (its sampler from OTEL_TRACES_SAMPLER and OTEL_TRACES_SAMPLER_ARG) with the stock AWS SDK
// instrumentation, OpenTelemetry.Instrumentation.AWS. It is set up before TrailbeamSqs.Register(), unless
// KIT_TRAILBEAM_FIRST=1, which registers Trailbeam first (the two customizers' order must not matter).
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Amazon.SQS;
using Amazon.SQS.Model;
using OpenTelemetry;
using OpenTelemetry.Trace;
using Trailbeam.Sqs;

internal static class Driver
{
    private static StreamWriter _truth = null!;
    private static AmazonSQSClient _sqs = null!;

    private static void Write(JsonObject row)
    {
        _truth.Write(row.ToJsonString() + "\n");
        _truth.Flush();
    }

    private static JsonObject Row(string op, string queueUrl) => new JsonObject { ["op"] = op, ["queue_url"] = queueUrl };

    private static string ErrorCode(AmazonSQSException e) => string.IsNullOrEmpty(e.ErrorCode) ? e.GetType().Name : e.ErrorCode;

    private static string Version(Type t) =>
        t.Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion?.Split('+')[0]
        ?? t.Assembly.GetName().Version?.ToString() ?? "?";

    private static async Task<string> Send(string q, int n)
    {
        string body = "{\"n\": " + n + "}";
        var r = await _sqs.SendMessageAsync(new SendMessageRequest { QueueUrl = q, MessageBody = body });
        var row = Row("SendMessage", q);
        row["ok"] = true;
        row["message_id"] = r.MessageId;
        row["body"] = body;
        Write(row);
        return r.MessageId;
    }

    private static async Task<List<Message>> Receive(string q, List<string> asked)
    {
        var req = new ReceiveMessageRequest { QueueUrl = q, MaxNumberOfMessages = 10, WaitTimeSeconds = 1 };
        if (asked.Count > 0)
        {
            req.MessageSystemAttributeNames = new List<string>(asked);
        }
        var msgs = (await _sqs.ReceiveMessageAsync(req)).Messages ?? new List<Message>();
        var row = Row("ReceiveMessage", q);
        row["ok"] = true;
        row["asked_system_attributes"] = new JsonArray(asked.Select(a => (JsonNode)a).ToArray());
        var ms = new JsonArray();
        foreach (var m in msgs)
        {
            var attrs = new JsonObject();
            foreach (var a in m.Attributes ?? new Dictionary<string, string>())
            {
                attrs[a.Key] = a.Value;
            }
            ms.Add(new JsonObject
            {
                ["message_id"] = m.MessageId, ["receipt_handle"] = m.ReceiptHandle, ["body"] = m.Body, ["attributes"] = attrs,
            });
        }
        row["messages"] = ms;
        Write(row);
        return msgs;
    }

    private static async Task<Dictionary<string, Message>> ReceiveUntil(string q, List<string> want, List<string> asked)
    {
        var got = new Dictionary<string, Message>();
        for (int i = 0; i < 30; i++)
        {
            foreach (var m in await Receive(q, asked))
            {
                got[m.MessageId] = m;
            }
            if (want.All(got.ContainsKey))
            {
                return got;
            }
        }
        throw new InvalidOperationException("timed out waiting for " + string.Join(",", want) + " on " + q);
    }

    private static async Task Delete(string q, string handle)
    {
        var row = Row("DeleteMessage", q);
        row["receipt_handle"] = handle;
        try
        {
            await _sqs.DeleteMessageAsync(new DeleteMessageRequest { QueueUrl = q, ReceiptHandle = handle });
            row["ok"] = true;
        }
        catch (AmazonSQSException e)
        {
            row["ok"] = false;
            row["error_code"] = ErrorCode(e);
        }
        Write(row);
    }

    private static void BatchRows(string op, string q, List<(string Id, string Handle)> entries, int? vt, List<BatchResultErrorEntry>? failedList)
    {
        var failed = (failedList ?? new List<BatchResultErrorEntry>()).ToDictionary(f => f.Id, f => f.Code);
        var row = Row(op, q);
        row["ok"] = true;
        var es = new JsonArray();
        foreach (var (id, handle) in entries)
        {
            var o = new JsonObject { ["id"] = id, ["receipt_handle"] = handle, ["ok"] = !failed.ContainsKey(id) };
            if (vt != null)
            {
                o["visibility_timeout"] = vt.Value;
            }
            if (failed.TryGetValue(id, out var code))
            {
                o["error_code"] = code;
            }
            es.Add(o);
        }
        row["entries"] = es;
        Write(row);
    }

    private static TracerProvider? AppTracing()
    {
        if (Environment.GetEnvironmentVariable("KIT_APP_TRACING") != "otel")
        {
            return null;
        }
        var b = Sdk.CreateTracerProviderBuilder().AddAWSInstrumentation().AddOtlpExporter();
        if (Environment.GetEnvironmentVariable("OTEL_TRACES_SAMPLER") == "traceidratio")
        {
            double ratio = double.Parse(Environment.GetEnvironmentVariable("OTEL_TRACES_SAMPLER_ARG") ?? "1",
                System.Globalization.CultureInfo.InvariantCulture);
            b.SetSampler(new ParentBasedSampler(new TraceIdRatioBasedSampler(ratio)));
        }
        return b.Build();
    }

    public static async Task<int> Main()
    {
        string run = Environment.GetEnvironmentVariable("KIT_RUN")!;
        _truth = new StreamWriter(Environment.GetEnvironmentVariable("KIT_TRUTH_FILE")!);

        bool trailbeamFirst = Environment.GetEnvironmentVariable("KIT_TRAILBEAM_FIRST") == "1";
        if (trailbeamFirst)
        {
            TrailbeamSqs.Register();
        }
        TracerProvider? app = AppTracing();
        if (!trailbeamFirst)
        {
            TrailbeamSqs.Register();
        }
        _sqs = new AmazonSQSClient(new AmazonSQSConfig
        {
            ServiceURL = Environment.GetEnvironmentVariable("KIT_SQS_ENDPOINT"),
            AuthenticationRegion = Environment.GetEnvironmentVariable("AWS_REGION"),
        });

        string appTracing = Environment.GetEnvironmentVariable("KIT_APP_TRACING") ?? "none";
        Write(new JsonObject
        {
            ["truth"] = 1, ["driver"] = "dotnet", ["language"] = "dotnet",
            ["sdk"] = "AWSSDK.SQS " + Version(typeof(AmazonSQSClient)) + ", AWSSDK.Core " + Version(typeof(Amazon.Runtime.AmazonServiceClient)),
            ["package"] = "Trailbeam.Sqs " + TrailbeamSqs.Version,
            ["app_tracing"] = app == null ? appTracing : appTracing + " (OpenTelemetry.Instrumentation.AWS " + Version(typeof(OpenTelemetry.Instrumentation.AWS.AWSClientInstrumentationOptions)) + (trailbeamFirst ? ", registered after Trailbeam" : "") + ")",
            ["dotnet"] = Environment.Version.ToString(),
        });

        // 1. Queues
        string dlq = (await _sqs.CreateQueueAsync(new CreateQueueRequest { QueueName = run + "-dlq" })).QueueUrl;
        string dlqArn = (await _sqs.GetQueueAttributesAsync(new GetQueueAttributesRequest
        {
            QueueUrl = dlq, AttributeNames = new List<string> { "QueueArn" },
        })).Attributes["QueueArn"];
        string q = (await _sqs.CreateQueueAsync(new CreateQueueRequest
        {
            QueueName = run,
            Attributes = new Dictionary<string, string>
            {
                ["VisibilityTimeout"] = "2",
                ["RedrivePolicy"] = "{\"deadLetterTargetArn\":\"" + dlqArn + "\",\"maxReceiveCount\":\"2\"}",
            },
        })).QueueUrl;
        string pq = (await _sqs.CreateQueueAsync(new CreateQueueRequest { QueueName = run + "-purge" })).QueueUrl;

        // 2. Send and batch send
        var ids = new Dictionary<int, string> { [1] = await Send(q, 1) };
        var bodies = new Dictionary<string, string> { ["e2"] = "{\"n\": 2}", ["e3"] = "{\"n\": 3}" };
        var sb = await _sqs.SendMessageBatchAsync(new SendMessageBatchRequest
        {
            QueueUrl = q,
            Entries = bodies.Select(kv => new SendMessageBatchRequestEntry { Id = kv.Key, MessageBody = kv.Value }).ToList(),
        });
        var br = Row("SendMessageBatch", q);
        br["ok"] = true;
        var be = new JsonArray();
        foreach (var s in sb.Successful ?? new List<SendMessageBatchResultEntry>())
        {
            be.Add(new JsonObject { ["id"] = s.Id, ["ok"] = true, ["message_id"] = s.MessageId, ["body"] = bodies[s.Id] });
            ids[int.Parse(s.Id.Substring(1))] = s.MessageId;
        }
        foreach (var f in sb.Failed ?? new List<BatchResultErrorEntry>())
        {
            be.Add(new JsonObject { ["id"] = f.Id, ["ok"] = false, ["error_code"] = f.Code });
        }
        br["entries"] = be;
        Write(br);

        // 3. Receive all three; these receives ask for one system attribute of their own
        var got = await ReceiveUntil(q, new List<string> { ids[1], ids[2], ids[3] }, new List<string> { "SenderId" });

        // 4. Delete n=1; batch delete n=2 and a bogus handle (one failed entry)
        await Delete(q, got[ids[1]].ReceiptHandle);
        var de = new List<(string, string)> { ("d2", got[ids[2]].ReceiptHandle), ("bogus", "not-a-receipt-handle") };
        var dr = await _sqs.DeleteMessageBatchAsync(new DeleteMessageBatchRequest
        {
            QueueUrl = q,
            Entries = de.Select(e => new DeleteMessageBatchRequestEntry { Id = e.Item1, ReceiptHandle = e.Item2 }).ToList(),
        });
        BatchRows("DeleteMessageBatch", q, de, null, dr.Failed);

        // 5. Visibility-timeout retry: n=3 comes back with receive count 2
        await Task.Delay(3000);
        await ReceiveUntil(q, new List<string> { ids[3] }, new List<string>());

        // 6. DLQ move: ElasticMQ moves n=3 on the next receive of the source queue
        await Task.Delay(3000);
        await Receive(q, new List<string>());
        var d = await ReceiveUntil(dlq, new List<string> { ids[3] }, new List<string>());
        await Delete(dlq, d[ids[3]].ReceiptHandle);

        // 7. ChangeMessageVisibility(0), receive again, ChangeMessageVisibilityBatch, delete
        ids[4] = await Send(q, 4);
        string h = (await ReceiveUntil(q, new List<string> { ids[4] }, new List<string>()))[ids[4]].ReceiptHandle;
        await _sqs.ChangeMessageVisibilityAsync(new ChangeMessageVisibilityRequest { QueueUrl = q, ReceiptHandle = h, VisibilityTimeout = 0 });
        var cv = Row("ChangeMessageVisibility", q);
        cv["receipt_handle"] = h;
        cv["visibility_timeout"] = 0;
        cv["ok"] = true;
        Write(cv);
        string h2 = (await ReceiveUntil(q, new List<string> { ids[4] }, new List<string>()))[ids[4]].ReceiptHandle;
        var vr = await _sqs.ChangeMessageVisibilityBatchAsync(new ChangeMessageVisibilityBatchRequest
        {
            QueueUrl = q,
            Entries = new List<ChangeMessageVisibilityBatchRequestEntry>
            {
                new ChangeMessageVisibilityBatchRequestEntry { Id = "v4", ReceiptHandle = h2, VisibilityTimeout = 30 },
            },
        });
        BatchRows("ChangeMessageVisibilityBatch", q, new List<(string, string)> { ("v4", h2) }, 30, vr.Failed);
        await Delete(q, h2);

        // 8. A delete with an invalid handle: the call fails
        await Delete(q, "definitely-not-a-valid-receipt-handle");

        // 9. Purge
        await _sqs.PurgeQueueAsync(new PurgeQueueRequest { QueueUrl = pq });
        var pr = Row("PurgeQueue", pq);
        pr["ok"] = true;
        Write(pr);

        // 10. Flush
        _sqs.Dispose();
        if (!TrailbeamSqs.Flush(TimeSpan.FromSeconds(10)))
        {
            throw new InvalidOperationException("trailbeam flush timed out");
        }
        TrailbeamSqs.Shutdown();
        app?.ForceFlush(10000);
        app?.Dispose();
        _truth.Close();
        Console.WriteLine(JsonSerializer.Serialize(new Dictionary<string, string> { ["driver"] = "dotnet", ["run"] = run }));
        return 0;
    }
}
