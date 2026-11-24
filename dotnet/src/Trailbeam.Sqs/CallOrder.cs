using System;
using System.Threading;

namespace Trailbeam.Sqs
{
    /// <summary>
    /// The process's call-order counter (contract section 2a): one per process (one per AppDomain on .NET
    /// Framework). <see cref="Tick"/> is strictly increasing and safe from any thread, so "A returned before B was
    /// issued" holds across threads and async tasks, as the contract argues.
    /// </summary>
    public static class CallOrder
    {
        /// <summary>A tick together with the scope it was taken in, so ticks of two scopes are never mixed.</summary>
        public sealed class Tick
        {
            internal Tick(string scope, long value)
            {
                Scope = scope;
                Value = value;
            }

            /// <summary>The counter's scope, a random UUID v4.</summary>
            public string Scope { get; }

            /// <summary>The tick: 1, 2, 3, ...</summary>
            public long Value { get; }
        }

        private sealed class Counter
        {
            internal readonly string Scope = Guid.NewGuid().ToString();
            internal long Next;
        }

        private static volatile Counter _counter = new Counter();

        /// <summary>The next tick of the counter.</summary>
        public static Tick Next()
        {
            Counter c = _counter;
            return new Tick(c.Scope, Interlocked.Increment(ref c.Next));
        }

        /// <summary>The current scope.</summary>
        public static string Scope => _counter.Scope;

        /// <summary>
        /// Starts a new scope whose counter begins again at 1: for a snapshot restore, where one snapshot becomes
        /// many execution environments that must not share a scope (Lambda SnapStart for .NET).
        /// </summary>
        public static void NewScope() => _counter = new Counter();
    }
}
