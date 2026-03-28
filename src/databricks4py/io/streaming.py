"""Structured Streaming utilities."""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from enum import Enum
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from databricks4py.filters.base import Filter
from databricks4py.spark_session import active_fallback

if TYPE_CHECKING:
    from databricks4py.io.checkpoint import CheckpointManager
    from databricks4py.metrics.base import MetricsSink

__all__ = [
    "StreamingTableReader",
    "StreamingTriggerOptions",
]

logger = logging.getLogger(__name__)


class StreamingTriggerOptions(Enum):
    """Common streaming trigger configurations.

    Values are dicts suitable for passing to ``writeStream.trigger(**value)``.

    Example::

        trigger = StreamingTriggerOptions.PROCESSING_TIME_1M
        stream.writeStream.trigger(**trigger.value).start()
    """

    PROCESSING_TIME_10S = {"processingTime": "10 seconds"}
    PROCESSING_TIME_30S = {"processingTime": "30 seconds"}
    PROCESSING_TIME_1M = {"processingTime": "1 minute"}
    PROCESSING_TIME_5M = {"processingTime": "5 minutes"}
    PROCESSING_TIME_10M = {"processingTime": "10 minutes"}
    AVAILABLE_NOW = {"availableNow": True}


class StreamingTableReader(ABC):
    """Abstract base for streaming micro-batch processors.

    Subclasses implement :meth:`process_batch` to handle each micro-batch.
    The :meth:`start` method wires up ``foreachBatch`` and returns a
    ``StreamingQuery``.

    Example::

        class MyProcessor(StreamingTableReader):
            def process_batch(self, df, batch_id):
                df.write.format("delta").mode("append").saveAsTable("output")

        reader = MyProcessor(
            source_table="catalog.schema.input",
            trigger=StreamingTriggerOptions.PROCESSING_TIME_1M,
            checkpoint_location="/checkpoints/my_reader",
        )
        query = reader.start()
        query.awaitTermination()

    Args:
        source_table: Table name or path to read as a stream.
        trigger: Trigger configuration, a raw dict, or None for the default.
        checkpoint_location: Path for streaming checkpoints. Auto-generated
            when a ``checkpoint_manager`` is provided and this is None.
        source_format: Source format (default ``"delta"``).
        row_filter: Optional Filter to apply before processing each batch.
        skip_empty_batches: Skip batches with 0 rows (default True).
        read_options: Additional read options as key-value pairs.
        checkpoint_manager: Optional CheckpointManager for auto-generating
            checkpoint paths.
        metrics_sink: Optional MetricsSink for emitting batch metrics.
        dead_letter_table: Fully qualified table name to write failed batches
            to. When set and ``process_batch`` raises, the offending DataFrame
            is written here with ``_dlq_error_message``, ``_dlq_error_timestamp``,
            and ``_dlq_batch_id`` columns appended. Uses ``mergeSchema=true`` so
            the table is auto-created on first failure.
        spark: Optional SparkSession.
    """

    _DEFAULT_TRIGGER: dict[str, str] = {"processingTime": "10 seconds"}

    def __init__(
        self,
        source_table: str,
        trigger: StreamingTriggerOptions | dict | None = None,
        checkpoint_location: str | None = None,
        *,
        source_format: str = "delta",
        row_filter: Filter | None = None,
        skip_empty_batches: bool = True,
        read_options: dict[str, str] | None = None,
        checkpoint_manager: CheckpointManager | None = None,
        metrics_sink: MetricsSink | None = None,
        dead_letter_table: str | None = None,
        spark: SparkSession | None = None,
    ) -> None:
        self._spark = active_fallback(spark)
        self._source_table = source_table
        self._metrics_sink = metrics_sink
        self._dead_letter_table = dead_letter_table
        self._query: StreamingQuery | None = None

        # Resolve trigger to a plain dict
        if trigger is None:
            self._trigger_dict = self._DEFAULT_TRIGGER
        elif isinstance(trigger, StreamingTriggerOptions):
            self._trigger_dict = trigger.value
        else:
            self._trigger_dict = trigger

        # Auto-generate checkpoint path when manager is provided
        if checkpoint_location is None and checkpoint_manager is not None:
            checkpoint_location = checkpoint_manager.path_for(source_table, self.__class__.__name__)
        if checkpoint_location is None:
            raise ValueError(
                "checkpoint_location is required when no checkpoint_manager is provided"
            )

        self._checkpoint_location = checkpoint_location
        self._source_format = source_format
        self._filter = row_filter
        self._skip_empty_batches = skip_empty_batches
        self._read_options = read_options or {}

    @abstractmethod
    def process_batch(self, df: DataFrame, batch_id: int) -> None:
        """Process a single micro-batch.

        Args:
            df: The micro-batch DataFrame.
            batch_id: The batch identifier.
        """
        ...

    def _write_to_dlq(self, df: DataFrame, batch_id: int, error_msg: str) -> None:
        """Append a failed batch to the dead-letter table with error metadata."""
        assert self._dead_letter_table is not None  # only called when dlq is configured
        from datetime import datetime, timezone

        from pyspark.sql import functions as F

        error_df = (
            df.withColumn("_dlq_error_message", F.lit(error_msg))
            .withColumn(
                "_dlq_error_timestamp",
                F.lit(datetime.now(tz=timezone.utc).isoformat()).cast("timestamp"),
            )
            .withColumn("_dlq_batch_id", F.lit(batch_id))
        )
        (
            error_df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(self._dead_letter_table)
        )
        logger.warning(
            "Wrote batch %d to DLQ %s: %s",
            batch_id,
            self._dead_letter_table,
            error_msg[:200],
        )

    def _foreach_batch_wrapper(self, df: DataFrame, batch_id: int) -> None:
        """Internal wrapper handling empty batch detection, filtering, and metrics."""
        if self._skip_empty_batches and df.isEmpty():
            logger.debug("Skipping empty batch %d", batch_id)
            return

        if self._filter is not None:
            df = self._filter(df)
            if self._skip_empty_batches and df.isEmpty():
                logger.debug("Skipping batch %d (empty after filtering)", batch_id)
                return

        count = df.count()
        logger.info("Processing batch %d (%d rows)", batch_id, count)

        start = time.monotonic()
        try:
            self.process_batch(df, batch_id)
        except Exception:  # noqa: BLE001 — broad catch intentional: DLQ must capture all failures
            if self._dead_letter_table is not None:
                import traceback

                original_tb = traceback.format_exc()
                try:
                    self._write_to_dlq(df, batch_id, original_tb)
                except Exception:
                    logger.error(
                        "DLQ write failed for batch %d; original error: %s",
                        batch_id,
                        original_tb[:500],
                        exc_info=True,
                    )
                    raise
                return
            raise
        duration_ms = (time.monotonic() - start) * 1000

        if self._metrics_sink is not None:
            from datetime import datetime, timezone

            from databricks4py.metrics.base import MetricEvent

            self._metrics_sink.emit(
                MetricEvent(
                    job_name=self.__class__.__name__,
                    event_type="batch_complete",
                    timestamp=datetime.now(tz=timezone.utc),
                    duration_ms=duration_ms,
                    row_count=count,
                    batch_id=batch_id,
                    table_name=self._source_table,
                )
            )

    def _build_read_stream(self) -> DataFrame:
        """Build the readStream DataFrame."""
        reader = self._spark.readStream.format(self._source_format)
        for key, value in self._read_options.items():
            reader = reader.option(key, value)

        if self._source_format == "delta":
            return reader.table(self._source_table)
        return reader.load(self._source_table)

    def start(self) -> StreamingQuery:
        """Start the streaming query.

        Returns:
            The active StreamingQuery.
        """
        stream_df = self._build_read_stream()

        self._query = (
            stream_df.writeStream.foreachBatch(self._foreach_batch_wrapper)
            .trigger(**self._trigger_dict)
            .option("checkpointLocation", self._checkpoint_location)
            .start()
        )

        logger.info(
            "Started streaming query from %s with trigger %s",
            self._source_table,
            self._trigger_dict,
        )
        return self._query

    def stop(self, timeout_seconds: int = 30) -> None:
        """Stop the streaming query and wait for graceful termination.

        Args:
            timeout_seconds: Maximum seconds to wait after calling stop.

        Raises:
            ValueError: If the query has not been started yet.
        """
        if self._query is None:
            raise ValueError("No active query. Call start() first.")
        self._query.stop()
        self._query.awaitTermination(timeout=timeout_seconds)
        logger.info("Streaming query stopped")

    @property
    def query(self) -> StreamingQuery | None:
        """The active StreamingQuery, or None if start() has not been called."""
        return self._query

    @property
    def is_active(self) -> bool:
        """True if the streaming query is currently running."""
        return self._query is not None and self._query.isActive
