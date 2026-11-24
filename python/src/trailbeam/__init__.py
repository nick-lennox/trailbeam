"""Trailbeam: message-level timelines for Amazon SQS (Python package, pre-release)."""

__version__ = "0.5.0"


def export_stats() -> dict:
    """Trailbeam spans of this process so far: ``exported``, ``queued`` (waiting for export), ``dropped``,
    and ``dropped_by_reason`` (``queue_full``, ``export_failed``, ``shutdown``). Counts spans of the
    default export path only, not of a ``span_processor`` passed in."""
    from ._export import stats

    return stats()
