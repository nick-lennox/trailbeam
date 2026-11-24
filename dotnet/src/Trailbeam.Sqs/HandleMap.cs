using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Trailbeam.Sqs
{
    /// <summary>
    /// Receipt handle to message ID, for the message ID on settle spans (contract section 8). Thread-safe and
    /// bounded: the oldest entries go first, and entries expire after 12 hours, the longest visibility timeout.
    /// </summary>
    internal sealed class HandleMap
    {
        internal static readonly TimeSpan MaxAge = TimeSpan.FromHours(12);
        internal const int DefaultMaxEntries = 100_000;

        private sealed class Entry
        {
            internal Entry(string handle, string messageId, long at)
            {
                Handle = handle;
                MessageId = messageId;
                At = at;
            }

            internal string Handle { get; }
            internal string MessageId { get; }
            internal long At { get; }
        }

        // Insertion order is time order, so the oldest and the expired entries are at the front of the list.
        private readonly Dictionary<string, LinkedListNode<Entry>> _map = new Dictionary<string, LinkedListNode<Entry>>(StringComparer.Ordinal);
        private readonly LinkedList<Entry> _order = new LinkedList<Entry>();
        private readonly int _maxEntries;
        private readonly long _maxAgeTicks;
        private readonly Func<long> _clock;
        private readonly object _lock = new object();

        internal HandleMap()
            : this(DefaultMaxEntries, MaxAge, () => Stopwatch.GetTimestamp())
        {
        }

        /// <param name="clock">a monotonic clock in <see cref="Stopwatch"/> ticks</param>
        internal HandleMap(int maxEntries, TimeSpan maxAge, Func<long> clock)
        {
            _maxEntries = maxEntries;
            _maxAgeTicks = (long)(maxAge.TotalSeconds * Stopwatch.Frequency);
            _clock = clock;
        }

        internal void Put(string receiptHandle, string messageId)
        {
            lock (_lock)
            {
                long now = _clock();
                RemoveLocked(receiptHandle);
                _map[receiptHandle] = _order.AddLast(new Entry(receiptHandle, messageId, now));
                int excess = _map.Count - _maxEntries;
                while (_order.First != null)
                {
                    Entry e = _order.First.Value;
                    if (excess > 0)
                    {
                        excess--;
                    }
                    else if (now - e.At <= _maxAgeTicks)
                    {
                        break;
                    }
                    _order.RemoveFirst();
                    _map.Remove(e.Handle);
                }
            }
        }

        internal string? Get(string receiptHandle)
        {
            lock (_lock)
            {
                if (!_map.TryGetValue(receiptHandle, out var node))
                {
                    return null;
                }
                if (_clock() - node.Value.At > _maxAgeTicks)
                {
                    RemoveLocked(receiptHandle);
                    return null;
                }
                return node.Value.MessageId;
            }
        }

        internal void Remove(string receiptHandle)
        {
            lock (_lock)
            {
                RemoveLocked(receiptHandle);
            }
        }

        internal int Count
        {
            get
            {
                lock (_lock)
                {
                    return _map.Count;
                }
            }
        }

        private void RemoveLocked(string receiptHandle)
        {
            if (_map.TryGetValue(receiptHandle, out var node))
            {
                _order.Remove(node);
                _map.Remove(receiptHandle);
            }
        }
    }
}
