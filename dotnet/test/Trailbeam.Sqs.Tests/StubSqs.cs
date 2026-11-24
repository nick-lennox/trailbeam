using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Runtime;
using Amazon.SQS;

namespace Trailbeam.Sqs.Tests
{
    /// <summary>
    /// A canned SQS over the AWS SDK's own HTTP layer: a real <see cref="AmazonSQSClient"/> (marshallers, pipeline,
    /// error handling) whose HttpClient answers from <see cref="Handler"/>. The SDK speaks SQS's JSON protocol.
    /// </summary>
    internal sealed class StubSqs : HttpClientFactory
    {
        internal sealed class Req
        {
            internal Req(string operation, JsonElement body, Dictionary<string, string> headers)
            {
                Operation = operation;
                Body = body;
                Headers = headers;
            }

            internal string Operation { get; }
            internal JsonElement Body { get; }
            internal Dictionary<string, string> Headers { get; }
        }

        internal sealed class Res
        {
            internal int Status { get; private set; } = 200;
            internal string Body { get; private set; } = "{}";
            internal Exception? Thrown { get; private set; }

            internal static Res Ok(string json) => new Res { Body = json };

            internal static Res Error(string code, string? message = null) => new Res
            {
                Status = 400,
                Body = "{\"__type\":\"" + code + "\",\"message\":\"" + (message ?? "stub error") + "\"}",
            };

            internal static Res Throw(Exception e) => new Res { Thrown = e };
        }

        private readonly Func<Req, Res> _handler;

        internal StubSqs(Func<Req, Res> handler)
        {
            _handler = handler;
        }

        internal List<Req> Requests { get; } = new List<Req>();

        internal AmazonSQSClient Client() => new AmazonSQSClient(new BasicAWSCredentials("x", "x"), new AmazonSQSConfig
        {
            ServiceURL = "http://localhost:9324",
            AuthenticationRegion = "elasticmq",
            HttpClientFactory = this,
            MaxErrorRetry = 0,
        });

        public override HttpClient CreateHttpClient(IClientConfig clientConfig) => new HttpClient(new Handler(this));

        public override bool UseSDKHttpClientCaching(IClientConfig clientConfig) => false;

        public override bool DisposeHttpClientsAfterUse(IClientConfig clientConfig) => true;

        private sealed class Handler : HttpMessageHandler
        {
            private readonly StubSqs _stub;

            internal Handler(StubSqs stub)
            {
                _stub = stub;
            }

            protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
            {
                string target = request.Headers.TryGetValues("X-Amz-Target", out var t) ? t.First() : "";
                string op = target.Substring(target.IndexOf('.') + 1);
                string body = request.Content == null ? "{}" : await request.Content.ReadAsStringAsync(cancellationToken);
                var headers = request.Headers.ToDictionary(h => h.Key, h => string.Join(",", h.Value), StringComparer.OrdinalIgnoreCase);
                var req = new Req(op, JsonDocument.Parse(body.Length == 0 ? "{}" : body).RootElement.Clone(), headers);
                _stub.Requests.Add(req);
                Res res = _stub._handler(req);
                if (res.Thrown != null)
                {
                    throw res.Thrown;
                }
                var r = new HttpResponseMessage((HttpStatusCode)res.Status)
                {
                    Content = new StringContent(res.Body, Encoding.UTF8, "application/x-amz-json-1.0"),
                    RequestMessage = request,
                };
                r.Headers.Add("x-amzn-RequestId", Guid.NewGuid().ToString());
                return r;
            }
        }
    }
}
