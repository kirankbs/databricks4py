"""Delta Lake metrics sink."""

from __future__ import annotations

import logging
from dataclasses import asdict
from typing import TYPE_CHECKING

from databricks4py.metrics.base import MetricEvent, MetricsSink
from databricks4py.spark_session import active_fallback

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

__all__ = ["DeltaMetricsSink"]

logger = logging.getLogger(__name__)


class DeltaMetricsSink(MetricsSink):
    """Buffers metric events and writes them to a Delta table on flush or threshold."""

    def __init__(
        self,
        table_name: str,
        *,
        spark: SparkSession | None = None,
        buffer_size: int = 100,
    ) -> None:
        self._table_name = table_name
        self._spark = active_fallback(spark)
        self._buffer_size = buffer_size
        self._buffer: list[MetricEvent] = []

    def emit(self, event: MetricEvent) -> None:
        self._buffer.append(event)
        if len(self._buffer) >= self._buffer_size:
            self.flush()

    def flush(self) -> None:
        if not self._buffer:
            return

        rows = [
            {
                k: str(v) if not isinstance(v, (int, float, str, type(None))) else v
                for k, v in asdict(event).items()
            }
            for event in self._buffer
        ]
        df = self._spark.createDataFrame(rows)
        df.write.format("delta").mode("append").saveAsTable(self._table_name)
        logger.info("Flushed %d metric events to %s", len(self._buffer), self._table_name)
        self._buffer.clear()
