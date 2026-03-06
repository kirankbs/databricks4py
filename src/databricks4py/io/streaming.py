"""Structured Streaming utilities."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from enum import Enum

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from databricks4py.filters.base import Filter
from databricks4py.spark_session import active_fallback

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
        trigger: Trigger configuration.
        checkpoint_location: Path for streaming checkpoints.
        source_format: Source format (default ``"delta"``).
        row_filter: Optional Filter to apply before processing each batch.
        skip_empty_batches: Skip batches with 0 rows (default True).
        read_options: Additional read options as key-value pairs.
        spark: Optional SparkSession.
    """

    def __init__(
        self,
        source_table: str,
        trigger: StreamingTriggerOptions,
        checkpoint_location: str,
        *,
        source_format: str = "delta",
        row_filter: Filter | None = None,
        skip_empty_batches: bool = True,
        read_options: dict[str, str] | None = None,
        spark: SparkSession | None = None,
    ) -> None:
        self._spark = active_fallback(spark)
        self._source_table = source_table
        self._trigger = trigger
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

    def _foreach_batch_wrapper(self, df: DataFrame, batch_id: int) -> None:
        """Internal wrapper handling empty batch detection and filtering."""
        if self._skip_empty_batches and df.isEmpty():
            logger.debug("Skipping empty batch %d", batch_id)
            return

        if self._filter is not None:
            df = self._filter(df)
            if self._skip_empty_batches and df.isEmpty():
                logger.debug("Skipping batch %d (empty after filtering)", batch_id)
                return

        logger.info("Processing batch %d", batch_id)
        self.process_batch(df, batch_id)

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

        query = (
            stream_df.writeStream.foreachBatch(self._foreach_batch_wrapper)
            .trigger(**self._trigger.value)
            .option("checkpointLocation", self._checkpoint_location)
            .start()
        )

        logger.info(
            "Started streaming query from %s with trigger %s",
            self._source_table,
            self._trigger.name,
        )
        return query
