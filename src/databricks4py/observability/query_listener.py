"""Streaming query progress observer via PySpark StreamingQueryListener.

Wraps the PySpark 3.4+ ``StreamingQueryListener`` API into a simple observer
that collects progress snapshots, optionally emits them to a
:class:`~databricks4py.metrics.base.MetricsSink`, and exposes the latest
state for health checks.

Requires PySpark >= 3.4. On older versions, :meth:`QueryProgressObserver.attach`
raises ``ImportError`` with a clear message.

Example::

    observer = QueryProgressObserver(spark=spark, metrics_sink=my_sink)
    observer.attach()

    query = df.writeStream.start()
    # ... wait for progress ...

    latest = observer.latest_progress()
    if latest:
        print(f"Batch {latest.batch_id}: {latest.num_input_rows} rows")

    observer.detach()
"""

from __future__ import annotations

import json
import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from databricks4py.observability._utils import parse_duration_ms
from databricks4py.spark_session import active_fallback

if TYPE_CHECKING:
    from collections.abc import Callable

    from pyspark.sql import SparkSession

    from databricks4py.metrics.base import MetricsSink

__all__ = ["QueryProgressObserver", "QueryProgressSnapshot"]

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class QueryProgressSnapshot:
    """Immutable snapshot of a single streaming query progress event.

    Attributes:
        query_id: Unique query identifier (UUID string).
        query_name: Optional query name (set via ``.queryName()``).
        batch_id: Monotonically increasing batch counter.
        batch_duration_ms: Wall-clock time for the batch in milliseconds.
        input_rows_per_second: Rate of data arriving from the source.
        processed_rows_per_second: Rate of data being processed.
        num_input_rows: Total rows read in this batch.
        timestamp: UTC time when the progress was recorded.
        sources: Per-source offset and rate info.
        sink: Sink commit progress info.
        raw_json: Full progress JSON for custom parsing.
    """

    query_id: str
    query_name: str | None
    batch_id: int
    batch_duration_ms: int
    input_rows_per_second: float
    processed_rows_per_second: float
    num_input_rows: int
    timestamp: datetime
    sources: list[dict[str, Any]] = field(default_factory=list)
    sink: dict[str, Any] = field(default_factory=dict)
    raw_json: str = ""

    @classmethod
    def from_progress(cls, progress: Any) -> QueryProgressSnapshot:
        """Build from a PySpark ``StreamingQueryProgress`` object.

        Expects an object with a ``.json`` property returning a JSON string
        (standard ``StreamingQueryProgress`` in PySpark 3.4+).
        """
        import json as _json

        raw = progress.json
        data = _json.loads(raw)

        return cls(
            query_id=data.get("id", ""),
            query_name=data.get("name"),
            batch_id=data.get("batchId", -1),
            batch_duration_ms=parse_duration_ms(data.get("batchDuration", "0 ms")),
            input_rows_per_second=data.get("inputRowsPerSecond", 0.0),
            processed_rows_per_second=data.get("processedRowsPerSecond", 0.0),
            num_input_rows=data.get("numInputRows", 0),
            timestamp=datetime.now(tz=timezone.utc),
            sources=data.get("sources", []),
            sink=data.get("sink", {}),
            raw_json=raw,
        )


class QueryProgressObserver:
    """Collects streaming query progress events and routes them to a metrics sink.

    Attaches a ``StreamingQueryListener`` to the SparkSession. Each progress
    event is captured as a :class:`QueryProgressSnapshot`, stored in a bounded
    history, and optionally emitted as a ``MetricEvent``.

    Args:
        spark: SparkSession to attach the listener to.
        metrics_sink: Optional sink for ``query_progress`` metric events.
        on_progress: Optional callback invoked on each progress event.
        history_size: Maximum number of snapshots to retain. Oldest are evicted.
        query_name_filter: If set, only track queries with this name.
    """

    def __init__(
        self,
        *,
        spark: SparkSession | None = None,
        metrics_sink: MetricsSink | None = None,
        on_progress: Callable[[QueryProgressSnapshot], None] | None = None,
        history_size: int = 100,
        query_name_filter: str | None = None,
    ) -> None:
        self._spark = active_fallback(spark)
        self._metrics_sink = metrics_sink
        self._on_progress = on_progress
        self._history: deque[QueryProgressSnapshot] = deque(maxlen=history_size)
        self._query_name_filter = query_name_filter
        self._listener: Any = None
        self._attached = False

    def attach(self) -> None:
        """Register the listener with the SparkSession.

        Raises:
            ImportError: If PySpark < 3.4 (no Python listener support).
        """
        try:
            from pyspark.sql.streaming.listener import StreamingQueryListener
        except ImportError as exc:
            raise ImportError(
                "StreamingQueryListener requires PySpark >= 3.4. "
                "Upgrade PySpark or use polling via query.lastProgress instead."
            ) from exc

        observer = self

        class _Listener(StreamingQueryListener):
            def onQueryStarted(self, event: Any) -> None:
                logger.debug("Query started: id=%s name=%s", event.id, event.name)

            def onQueryProgress(self, event: Any) -> None:
                observer._handle_progress(event.progress)

            def onQueryTerminated(self, event: Any) -> None:
                exc_str = str(event.exception)[:500] if event.exception else None
                logger.info("Query terminated: id=%s exception=%s", event.id, exc_str)

        self._listener = _Listener()
        self._spark.streams.addListener(self._listener)
        self._attached = True
        logger.info("QueryProgressObserver attached")

    def detach(self) -> None:
        """Remove the listener from the SparkSession."""
        if self._listener is not None and self._attached:
            self._spark.streams.removeListener(self._listener)
            self._attached = False
            logger.info("QueryProgressObserver detached")

    def _handle_progress(self, progress: Any) -> None:
        try:
            snapshot = QueryProgressSnapshot.from_progress(progress)
        except (json.JSONDecodeError, KeyError, TypeError, ValueError, AttributeError):
            logger.exception("Failed to process query progress event")
            return

        if self._query_name_filter is not None and snapshot.query_name != self._query_name_filter:
            return

        self._history.append(snapshot)

        if self._on_progress is not None:
            self._on_progress(snapshot)

        if self._metrics_sink is not None:
            from databricks4py.metrics.base import MetricEvent

            self._metrics_sink.emit(
                MetricEvent(
                    job_name=snapshot.query_name or snapshot.query_id,
                    event_type="query_progress",
                    timestamp=snapshot.timestamp,
                    duration_ms=snapshot.batch_duration_ms,
                    row_count=snapshot.num_input_rows,
                    batch_id=snapshot.batch_id,
                    metadata={
                        "input_rows_per_second": snapshot.input_rows_per_second,
                        "processed_rows_per_second": snapshot.processed_rows_per_second,
                    },
                )
            )

        logger.debug(
            "Progress: query=%s batch=%d rows=%d rate=%.1f rows/s",
            snapshot.query_name or snapshot.query_id,
            snapshot.batch_id,
            snapshot.num_input_rows,
            snapshot.processed_rows_per_second,
        )

    def latest_progress(self) -> QueryProgressSnapshot | None:
        """Most recent progress snapshot, or None if no events received yet."""
        return self._history[-1] if self._history else None

    def history(self, limit: int | None = None) -> list[QueryProgressSnapshot]:
        """Recent progress snapshots, newest last.

        Args:
            limit: Maximum number of snapshots to return. Returns all if None.
        """
        items = list(self._history)
        if limit is not None:
            items = items[-limit:]
        return items

    @property
    def is_attached(self) -> bool:
        return self._attached
