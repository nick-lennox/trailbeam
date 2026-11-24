using System;
using System.Diagnostics.Tracing;

namespace Trailbeam.Sqs
{
    /// <summary>
    /// Trailbeam's diagnostics, as an EventSource named <c>Trailbeam-Sqs</c> (<c>dotnet-trace</c>,
    /// <c>dotnet-monitor</c> or an <c>EventListener</c> read it). Warnings also go to standard error, once each.
    /// </summary>
    [EventSource(Name = "Trailbeam-Sqs")]
    internal sealed class TrailbeamEventSource : EventSource
    {
        internal static readonly TrailbeamEventSource Log = new TrailbeamEventSource();

        [Event(1, Level = EventLevel.Warning, Message = "{0}")]
        public void Warning(string message) => WriteEvent(1, message);

        [Event(2, Level = EventLevel.Informational, Message = "{0}")]
        public void Info(string message) => WriteEvent(2, message);

        [Event(3, Level = EventLevel.Verbose, Message = "trailbeam {0} failed: {1}")]
        public void HookFailed(string where, string error) => WriteEvent(3, where, error);

        [NonEvent]
        internal void Failed(string where, Exception e)
        {
            if (IsEnabled(EventLevel.Verbose, EventKeywords.All))
            {
                HookFailed(where, e.ToString());
            }
        }
    }
}
