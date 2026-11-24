using System;
using Amazon.Runtime;
using Amazon.Runtime.Internal;
using Amazon.SQS;

namespace Trailbeam.Sqs
{
    /// <summary>
    /// Trailbeam for the AWS SDK for .NET: call <see cref="Register"/> once at startup, before the first SQS client is
    /// created, and every <see cref="AmazonSQSClient"/> emits Trailbeam spans.
    ///
    /// <code>
    /// Trailbeam.Sqs.TrailbeamSqs.Register();
    /// </code>
    ///
    /// The runtime starts on the first SQS call and flushes when the process exits. <see cref="Flush"/> and
    /// <see cref="Shutdown"/> are for programs that must be sure every span has left before they go on, such as a
    /// test or a job that ends with <c>Environment.Exit</c>.
    /// </summary>
    public static class TrailbeamSqs
    {
        private static readonly object Lock = new object();
        private static bool _registered;

        /// <summary>
        /// Adds Trailbeam's pipeline handler to every SQS client created from now on, through
        /// <see cref="RuntimePipelineCustomizerRegistry"/> (the hook OpenTelemetry.Instrumentation.AWS uses too). A
        /// client created before this call is not traced. Calling it again does nothing.
        /// </summary>
        public static void Register()
        {
            lock (Lock)
            {
                if (_registered)
                {
                    return;
                }
                RuntimePipelineCustomizerRegistry.Instance.Register(new TrailbeamPipelineCustomizer());
                _registered = true;
            }
        }

        /// <summary>Exports every span emitted so far. Returns false if that did not finish within the timeout.</summary>
        public static bool Flush(TimeSpan timeout) => TrailbeamRuntime.Flush(timeout);

        /// <summary>Flushes and stops the runtime. A later SQS call starts a new one. Also run when the process exits.</summary>
        public static void Shutdown() => TrailbeamRuntime.Shutdown(TimeSpan.FromSeconds(5));

        /// <summary>
        /// After a snapshot restore (Lambda SnapStart): starts a new call-order scope and a new runtime, so that the
        /// execution environments made from one snapshot share neither a scope nor a <c>service.instance.id</c>.
        /// Spans emitted before the snapshot should have been flushed already.
        /// </summary>
        public static void AfterRestore() => TrailbeamRuntime.AfterRestore();

        /// <summary>The package version, which is also the instrumentation scope version.</summary>
        public static string Version => TrailbeamRuntime.Version;
    }

    /// <summary>Adds <see cref="TrailbeamSqsPipelineHandler"/> to SQS clients.</summary>
    internal sealed class TrailbeamPipelineCustomizer : IRuntimePipelineCustomizer
    {
        internal const string Name = "Trailbeam SQS spans";

        public string UniqueName => Name;

        public void Customize(Type type, RuntimePipeline pipeline)
        {
            try
            {
                if (!typeof(AmazonSQSClient).IsAssignableFrom(type))
                {
                    return;
                }
                foreach (IPipelineHandler h in pipeline.Handlers)
                {
                    if (h is TrailbeamSqsPipelineHandler)
                    {
                        return; // emits each span once
                    }
                }
                pipeline.AddHandlerBefore<Marshaller>(new TrailbeamSqsPipelineHandler());
            }
            catch (Exception e)
            {
                TrailbeamEventSource.Log.Failed("customize", e);
            }
        }
    }
}
