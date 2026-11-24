using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Runtime;
using Amazon.Runtime.Internal;
using Amazon.SQS.Model;

namespace Trailbeam.Sqs
{
    /// <summary>
    /// Trailbeam's AWS SDK for .NET pipeline handler: one span per SQS message, receipt handle or purge (contract
    /// sections 4.1 to 4.3 and 18). <see cref="TrailbeamSqs.Register"/> adds it, before the SDK's
    /// <c>Marshaller</c>, to every SQS client created afterwards.
    ///
    /// Where it sits: the SDK's own <c>MetricsHandler</c>, the outermost handler, starts the stock span that
    /// OpenTelemetry.Instrumentation.AWS records, so that span is <see cref="Activity.Current"/> here, whichever of
    /// the two customizers was registered first. Before the marshaller, so a <c>ReceiveMessage</c> can still ask for
    /// the attributes Trailbeam needs.
    ///
    /// The handler never throws into the application's call: what it adds is in try blocks, and the SDK's own
    /// exception is rethrown untouched.
    /// </summary>
    internal sealed class TrailbeamSqsPipelineHandler : PipelineHandler
    {
        /// <summary>System attributes the receive hook asks for (section 4.2).</summary>
        internal static readonly IReadOnlyList<string> ReceiveSystemAttributes = new[]
        {
            "ApproximateReceiveCount",
            "SentTimestamp",
            "ApproximateFirstReceiveTimestamp",
            "AWSTraceHeader",
            "DeadLetterQueueSourceArn", // 3.1: set only on a receive from a dead-letter queue
            "MessageGroupId", // 3.2: FIFO queues (and fair standard queues)
            "SequenceNumber", // 3.2: FIFO queues only
        };

        /// <summary>The state of one SQS call.</summary>
        private sealed class Call
        {
            internal Call(string operation, string? threadName, CallOrder.Tick issued, DateTimeOffset start, ActivityContext? parent)
            {
                Operation = operation;
                ThreadName = threadName;
                Issued = issued;
                Start = start;
                Parent = parent;
            }

            internal string Operation { get; }
            internal string? ThreadName { get; }
            internal CallOrder.Tick Issued { get; }
            internal DateTimeOffset Start { get; }
            internal ActivityContext? Parent { get; set; }
            internal Action? Restore { get; set; }
        }

        public override void InvokeSync(IExecutionContext executionContext)
        {
            Call? call = Begin(executionContext);
            try
            {
                base.InvokeSync(executionContext);
            }
            catch (Exception e)
            {
                Finish(call, executionContext, e);
                throw;
            }
            Finish(call, executionContext, null);
        }

        public override async Task<T> InvokeAsync<T>(IExecutionContext executionContext)
        {
            Call? call = Begin(executionContext);
            T result;
            try
            {
                result = await base.InvokeAsync<T>(executionContext).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                Finish(call, executionContext, e);
                throw;
            }
            Finish(call, executionContext, null);
            return result;
        }

        // -- before the call -------------------------------------------------------------------------------

        private static Call? Begin(IExecutionContext ec)
        {
            try
            {
                AmazonWebServiceRequest? request = ec.RequestContext?.OriginalRequest;
                string? op = Operation(request);
                if (op == null)
                {
                    return null;
                }
                TrailbeamRuntime tb = TrailbeamRuntime.Get();
                TrailbeamRuntime.InfoOnce("active", "Trailbeam pipeline handler active: SQS calls are traced (trailbeam "
                    + TrailbeamRuntime.Version + ", exporting to " + tb.Endpoint + ")");
                Activity? current = Activity.Current;
                ActivityContext? parent = current != null && current.TraceId != default ? current.Context : (ActivityContext?)null;
                // Section 2a: the issued tick just before the start time is read. This handler runs before the SDK
                // marshals, signs and sends the request (and before any retry), so the start is a lower bound.
                CallOrder.Tick issued = CallOrder.Next();
                var call = new Call(op, Thread.CurrentThread.Name, issued, DateTimeOffset.UtcNow, parent);
                if (request is ReceiveMessageRequest r)
                {
                    call.Restore = AddReceiveNames(r, tb.Capture.TenantAttribute);
                }
                return call;
            }
            catch (Exception e)
            {
                TrailbeamEventSource.Log.Failed("request", e);
                return null;
            }
        }

        internal static string? Operation(AmazonWebServiceRequest? request)
        {
            switch (request)
            {
                case SendMessageRequest _: return "SendMessage";
                case SendMessageBatchRequest _: return "SendMessageBatch";
                case ReceiveMessageRequest _: return "ReceiveMessage";
                case DeleteMessageRequest _: return "DeleteMessage";
                case DeleteMessageBatchRequest _: return "DeleteMessageBatch";
                case ChangeMessageVisibilityRequest _: return "ChangeMessageVisibility";
                case ChangeMessageVisibilityBatchRequest _: return "ChangeMessageVisibilityBatch";
                case PurgeQueueRequest _: return "PurgeQueue";
                default: return null;
            }
        }

        /// <summary>
        /// Asks for the system attributes Trailbeam needs (section 4.2), the configured tenant attribute and the
        /// Extended Client's size attributes (15.2), keeping everything the caller asked for. That changes what SQS
        /// returns, not the message's state. The caller's request object is put back as it was after the call (a
        /// request reused for the next receive is not changed under it); returns the action that does that.
        /// </summary>
        internal static Action? AddReceiveNames(ReceiveMessageRequest r, string? tenantAttribute)
        {
#pragma warning disable CS0618 // AttributeNames is deprecated for MessageSystemAttributeNames; callers may still use it
            List<string>? legacy = r.AttributeNames;
            List<string>? current = r.MessageSystemAttributeNames;
            List<string>? messageNames = r.MessageAttributeNames;

            var asked = new HashSet<string>(StringComparer.Ordinal);
            if (legacy != null)
            {
                asked.UnionWith(legacy);
            }
            if (current != null)
            {
                asked.UnionWith(current);
            }
            var missing = new List<string>();
            if (!asked.Contains("All"))
            {
                foreach (string n in ReceiveSystemAttributes)
                {
                    if (!asked.Contains(n))
                    {
                        missing.Add(n);
                    }
                }
            }
            bool changedSystem = missing.Count > 0;
            if (changedSystem)
            {
                // A caller that uses only the deprecated AttributeNames keeps using it.
                if (legacy != null && legacy.Count > 0 && (current == null || current.Count == 0))
                {
                    var names = new List<string>(legacy);
                    names.AddRange(missing);
                    r.AttributeNames = names;
                }
                else
                {
                    var names = current == null ? new List<string>() : new List<string>(current);
                    names.AddRange(missing);
                    r.MessageSystemAttributeNames = names;
                }
            }

            var wanted = new List<string>();
            if (tenantAttribute != null)
            {
                wanted.Add(tenantAttribute);
            }
            wanted.AddRange(Capture.OffloadedSizeAttributes);
            var messageAsked = messageNames == null ? new List<string>() : new List<string>(messageNames);
            bool changedMessage = false;
            if (!messageAsked.Contains("All") && !messageAsked.Contains(".*"))
            {
                foreach (string name in wanted)
                {
                    if (!messageAsked.Contains(name))
                    {
                        messageAsked.Add(name);
                        changedMessage = true;
                    }
                }
            }
            if (changedMessage)
            {
                r.MessageAttributeNames = messageAsked;
            }
            if (!changedSystem && !changedMessage)
            {
                return null;
            }
            return () =>
            {
                r.AttributeNames = legacy;
                r.MessageSystemAttributeNames = current;
                r.MessageAttributeNames = messageNames;
            };
#pragma warning restore CS0618
        }

        // -- after the call --------------------------------------------------------------------------------

        private static void Finish(Call? call, IExecutionContext ec, Exception? error)
        {
            if (call == null)
            {
                return;
            }
            // Section 2: the span ends when the response (or error) arrived; the returned tick just after.
            DateTimeOffset end = DateTimeOffset.UtcNow;
            CallOrder.Tick returned = CallOrder.Next();
            try
            {
                call.Restore?.Invoke();
                if (call.Parent == null)
                {
                    // No ambient span: a trace header another handler put on the request (the X-Ray SDK's, say).
                    call.Parent = TraceHeaders.Parent(ec.RequestContext?.Request?.Headers);
                }
                TrailbeamRuntime tb = TrailbeamRuntime.Get();
                var emit = new Emitter(tb, call, end, returned);
                AmazonWebServiceRequest request = ec.RequestContext!.OriginalRequest;
                AmazonWebServiceResponse? response = error == null ? ec.ResponseContext?.Response : null;
                switch (request)
                {
                    case SendMessageRequest r: OnSend(emit, r, response as SendMessageResponse); break;
                    case SendMessageBatchRequest r: OnSendBatch(emit, r, response as SendMessageBatchResponse); break;
                    case ReceiveMessageRequest r: OnReceive(emit, r, response as ReceiveMessageResponse); break;
                    case PurgeQueueRequest r: OnPurge(emit, r, response, error); break;
                    default: OnSettle(emit, request, response, error); break;
                }
            }
            catch (Exception e)
            {
                TrailbeamEventSource.Log.Failed("response", e);
            }
        }

        /// <summary>Emits the spans of one call: every span shares the call's interval and ticks.</summary>
        private sealed class Emitter
        {
            internal Emitter(TrailbeamRuntime tb, Call call, DateTimeOffset end, CallOrder.Tick returned)
            {
                Tb = tb;
                CallState = call;
                End = end;
                Returned = returned;
            }

            internal TrailbeamRuntime Tb { get; }
            internal Call CallState { get; }
            internal DateTimeOffset End { get; }
            internal CallOrder.Tick Returned { get; }

            internal void Span(string name, string? queueUrl, List<KeyValuePair<string, object?>> attrs) =>
                Tb.Emit(name, CallState.Operation, queueUrl, attrs, CallState.Parent, CallState.Start, End,
                    CallState.ThreadName, CallState.Issued, Returned);
        }

        private static void OnSend(Emitter e, SendMessageRequest req, SendMessageResponse? r)
        {
            if (r == null || string.IsNullOrEmpty(r.MessageId))
            {
                return;
            }
            var b = Attrs();
            Put(b, "messaging.message.id", r.MessageId);
            Contents(e.Tb, b, req.QueueUrl, req.MessageBody, StringValues(req.MessageAttributes), r.MD5OfMessageBody);
            Fifo(b, req.MessageGroupId, r.SequenceNumber);
            e.Span(TrailbeamRuntime.SpanSend, req.QueueUrl, b);
        }

        private static void OnSendBatch(Emitter e, SendMessageBatchRequest req, SendMessageBatchResponse? r)
        {
            if (r == null)
            {
                return;
            }
            var entries = new Dictionary<string, SendMessageBatchRequestEntry>(StringComparer.Ordinal);
            foreach (var entry in req.Entries ?? new List<SendMessageBatchRequestEntry>())
            {
                if (entry?.Id != null)
                {
                    entries[entry.Id] = entry;
                }
            }
            // Only entries SQS accepted: a failed entry has no message ID and produces no span (section 4.1).
            foreach (var ok in r.Successful ?? new List<SendMessageBatchResultEntry>())
            {
                if (ok == null || string.IsNullOrEmpty(ok.MessageId))
                {
                    continue;
                }
                var b = Attrs();
                Put(b, "messaging.message.id", ok.MessageId);
                Put(b, "trailbeam.batch_entry_id", ok.Id);
                if (ok.Id != null && entries.TryGetValue(ok.Id, out var sent))
                {
                    // the body comes from the request entry with the same Id
                    Contents(e.Tb, b, req.QueueUrl, sent.MessageBody, StringValues(sent.MessageAttributes), ok.MD5OfMessageBody);
                    Fifo(b, sent.MessageGroupId, ok.SequenceNumber);
                }
                e.Span(TrailbeamRuntime.SpanSend, req.QueueUrl, b);
            }
        }

        private static void OnReceive(Emitter e, ReceiveMessageRequest req, ReceiveMessageResponse? r)
        {
            if (r?.Messages == null)
            {
                return;
            }
            foreach (Message m in r.Messages)
            {
                if (m == null || string.IsNullOrEmpty(m.ReceiptHandle) || string.IsNullOrEmpty(m.MessageId))
                {
                    continue;
                }
                e.Tb.Handles.Put(m.ReceiptHandle, m.MessageId);
                Dictionary<string, string> a = m.Attributes ?? new Dictionary<string, string>();
                var b = Attrs();
                Put(b, "messaging.message.id", m.MessageId);
                Hash(e.Tb, b, m.ReceiptHandle);
                PutLong(b, "trailbeam.receive_count", Get(a, "ApproximateReceiveCount"));
                PutLong(b, "trailbeam.sent_timestamp_ms", Get(a, "SentTimestamp"));
                PutLong(b, "trailbeam.first_receive_timestamp_ms", Get(a, "ApproximateFirstReceiveTimestamp"));
                Put(b, "trailbeam.aws_trace_header", Get(a, "AWSTraceHeader"));
                Put(b, "trailbeam.dead_letter_queue_source_arn", Get(a, "DeadLetterQueueSourceArn"));
                Fifo(b, Get(a, "MessageGroupId"), Get(a, "SequenceNumber"));
                Contents(e.Tb, b, req.QueueUrl, m.Body, StringValues(m.MessageAttributes), m.MD5OfBody);
                e.Span(TrailbeamRuntime.SpanReceive, req.QueueUrl, b);
            }
        }

        /// <summary>3.3 (section 18): one span per PurgeQueue call, ok or failed. SQS does not say which messages went.</summary>
        private static void OnPurge(Emitter e, PurgeQueueRequest req, AmazonWebServiceResponse? response, Exception? error)
        {
            string? code = CallError(response, error);
            var b = Attrs();
            b.Add(new KeyValuePair<string, object?>("trailbeam.result", code == null ? "ok" : "failed"));
            Put(b, "trailbeam.error_code", PlainCode(code));
            e.Span(TrailbeamRuntime.SpanPurge, req.QueueUrl, b);
        }

        /// <summary>One handle (or batch entry) of a settle call.</summary>
        private struct Settle
        {
            internal string? EntryId;
            internal string? Handle;
            internal int? VisibilityTimeout;
        }

        private static void OnSettle(Emitter e, AmazonWebServiceRequest request, AmazonWebServiceResponse? response, Exception? error)
        {
            string spanName;
            string queueUrl;
            bool batch;
            var entries = new List<Settle>();
            var okIds = new HashSet<string>(StringComparer.Ordinal);
            var failed = new Dictionary<string, string>(StringComparer.Ordinal);
            switch (request)
            {
                case DeleteMessageRequest r:
                    spanName = TrailbeamRuntime.SpanDelete;
                    queueUrl = r.QueueUrl;
                    batch = false;
                    entries.Add(new Settle { Handle = r.ReceiptHandle });
                    break;
                case DeleteMessageBatchRequest r:
                    spanName = TrailbeamRuntime.SpanDelete;
                    queueUrl = r.QueueUrl;
                    batch = true;
                    foreach (var x in r.Entries ?? new List<DeleteMessageBatchRequestEntry>())
                    {
                        entries.Add(new Settle { EntryId = x?.Id, Handle = x?.ReceiptHandle });
                    }
                    if (response is DeleteMessageBatchResponse dres)
                    {
                        foreach (var ok in dres.Successful ?? new List<DeleteMessageBatchResultEntry>())
                        {
                            if (ok?.Id != null)
                            {
                                okIds.Add(ok.Id);
                            }
                        }
                        CollectFailed(dres.Failed, failed);
                    }
                    break;
                case ChangeMessageVisibilityRequest r:
                    spanName = TrailbeamRuntime.SpanChangeVisibility;
                    queueUrl = r.QueueUrl;
                    batch = false;
                    entries.Add(new Settle { Handle = r.ReceiptHandle, VisibilityTimeout = r.VisibilityTimeout });
                    break;
                case ChangeMessageVisibilityBatchRequest r:
                    spanName = TrailbeamRuntime.SpanChangeVisibility;
                    queueUrl = r.QueueUrl;
                    batch = true;
                    foreach (var x in r.Entries ?? new List<ChangeMessageVisibilityBatchRequestEntry>())
                    {
                        entries.Add(new Settle { EntryId = x?.Id, Handle = x?.ReceiptHandle, VisibilityTimeout = x?.VisibilityTimeout });
                    }
                    if (response is ChangeMessageVisibilityBatchResponse cres)
                    {
                        foreach (var ok in cres.Successful ?? new List<ChangeMessageVisibilityBatchResultEntry>())
                        {
                            if (ok?.Id != null)
                            {
                                okIds.Add(ok.Id);
                            }
                        }
                        CollectFailed(cres.Failed, failed);
                    }
                    break;
                default:
                    return;
            }

            // If the call failed as a whole (an error response, or an exception before any response), every handle
            // in it gets a failed span (section 4.3).
            string? callError = CallError(response, error);
            foreach (Settle s in entries)
            {
                if (string.IsNullOrEmpty(s.Handle))
                {
                    continue;
                }
                string? err;
                if (callError != null)
                {
                    err = callError;
                }
                else if (!batch || (s.EntryId != null && okIds.Contains(s.EntryId)))
                {
                    err = null;
                }
                else
                {
                    err = s.EntryId != null && failed.TryGetValue(s.EntryId, out string? code) ? code : "MissingFromResponse";
                }
                var b = Attrs();
                Hash(e.Tb, b, s.Handle!);
                Put(b, "messaging.message.id", e.Tb.Handles.Get(s.Handle!));
                if (batch)
                {
                    Put(b, "trailbeam.batch_entry_id", s.EntryId);
                }
                if (spanName == TrailbeamRuntime.SpanChangeVisibility && s.VisibilityTimeout != null)
                {
                    b.Add(new KeyValuePair<string, object?>("trailbeam.visibility_timeout", (long)s.VisibilityTimeout.Value)); // 0 is a real value
                }
                b.Add(new KeyValuePair<string, object?>("trailbeam.result", err == null ? "ok" : "failed"));
                Put(b, "trailbeam.error_code", err);
                e.Span(spanName, queueUrl, b);
                if (err == null && spanName == TrailbeamRuntime.SpanDelete)
                {
                    e.Tb.Handles.Remove(s.Handle!);
                }
            }
        }

        private static void CollectFailed(List<BatchResultErrorEntry>? entries, Dictionary<string, string> output)
        {
            foreach (var f in entries ?? new List<BatchResultErrorEntry>())
            {
                if (f?.Id != null)
                {
                    output[f.Id] = string.IsNullOrEmpty(f.Code) ? "Failed" : f.Code;
                }
            }
        }

        // -- attribute helpers -----------------------------------------------------------------------------

        /// <summary>The error of a whole call, or null if SQS answered with success.</summary>
        internal static string? CallError(AmazonWebServiceResponse? response, Exception? error)
        {
            if (error == null)
            {
                return response == null ? "NoResponse" : null;
            }
            // The name reported is the service's error code, else the first exception that is not a wrapper.
            Exception? t = error;
            for (int i = 0; i < 8 && t != null; i++)
            {
                if (t is AmazonServiceException se)
                {
                    return string.IsNullOrEmpty(se.ErrorCode) ? t.GetType().Name : se.ErrorCode;
                }
                bool wrapper = t is AggregateException || t.GetType() == typeof(AmazonClientException);
                if (!wrapper || t.InnerException == null)
                {
                    return t.GetType().Name;
                }
                t = t.InnerException;
            }
            return error.GetType().Name;
        }

        /// <summary>An error code without a protocol namespace (<c>com.amazonaws.sqs#QueueDoesNotExist</c>), section 18.</summary>
        internal static string? PlainCode(string? code)
        {
            if (string.IsNullOrEmpty(code))
            {
                return null;
            }
            string plain = code!.Substring(code.LastIndexOf('#') + 1);
            return plain.Length == 0 ? null : plain;
        }

        private static List<KeyValuePair<string, object?>> Attrs() => new List<KeyValuePair<string, object?>>();

        private static void Hash(TrailbeamRuntime tb, List<KeyValuePair<string, object?>> b, string handle)
        {
            b.Add(new KeyValuePair<string, object?>("trailbeam.receipt_handle.hmac", tb.Hasher.Hash(handle)));
            b.Add(new KeyValuePair<string, object?>("trailbeam.receipt_handle.key_id", tb.Hasher.KeyId));
        }

        /// <summary>3.2 (section 15): body size, tenant, and the body itself for a queue that opted in.</summary>
        private static void Contents(TrailbeamRuntime tb, List<KeyValuePair<string, object?>> b, string? queueUrl, string? body,
            Dictionary<string, string> attributes, string? md5)
        {
            string? queue = string.IsNullOrEmpty(queueUrl) ? null : TrailbeamRuntime.QueueName(queueUrl!);
            foreach (var a in Capture.ContentAttrs(tb.Capture, queue, body, attributes, md5))
            {
                b.Add(new KeyValuePair<string, object?>(a.Key, a.Value));
            }
        }

        /// <summary>Each message attribute's <c>StringValue</c>, by name (binary attributes have none).</summary>
        private static Dictionary<string, string> StringValues(Dictionary<string, MessageAttributeValue>? attributes)
        {
            var output = new Dictionary<string, string>(StringComparer.Ordinal);
            if (attributes != null)
            {
                foreach (var a in attributes)
                {
                    if (a.Value?.StringValue != null)
                    {
                        output[a.Key] = a.Value.StringValue;
                    }
                }
            }
            return output;
        }

        private static void Fifo(List<KeyValuePair<string, object?>> b, string? groupId, string? sequenceNumber)
        {
            Put(b, "trailbeam.message_group_id", groupId);
            Put(b, "trailbeam.sequence_number", sequenceNumber);
        }

        private static string? Get(Dictionary<string, string> a, string name) => a.TryGetValue(name, out string? v) ? v : null;

        private static void Put(List<KeyValuePair<string, object?>> b, string name, string? value)
        {
            if (!string.IsNullOrEmpty(value))
            {
                b.Add(new KeyValuePair<string, object?>(name, value)); // section 3: an absent value is omitted, never ""
            }
        }

        private static void PutLong(List<KeyValuePair<string, object?>> b, string name, string? value)
        {
            if (!string.IsNullOrEmpty(value)
                && long.TryParse(value!.Trim(), NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out long v))
            {
                b.Add(new KeyValuePair<string, object?>(name, v));
            }
        }
    }
}
