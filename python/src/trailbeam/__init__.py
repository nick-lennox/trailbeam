"""Trailbeam: message-level timelines for Amazon SQS (Python package, pre-release)."""

__version__ = "0.4.0"

# Trailbeam itself (trailbeam.server) is another distribution, trailbeam-server, that installs into this package.
# Installed from wheels both land in the same directory; this also finds it when the two sit in different places
# on sys.path, as with editable installs from a clone (pip install -e ./python -e ./server).
__path__ = __import__("pkgutil").extend_path(__path__, __name__)


def export_stats() -> dict:
    """Trailbeam spans of this process so far: ``exported``, ``queued`` (waiting for export), ``dropped``,
    and ``dropped_by_reason`` (``queue_full``, ``export_failed``, ``shutdown``). Counts spans of the
    default export path only, not of a ``span_processor`` passed in."""
    from ._export import stats

    return stats()
