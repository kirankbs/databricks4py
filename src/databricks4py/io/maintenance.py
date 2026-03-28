"""Table maintenance utilities for Delta Lake."""

from __future__ import annotations

import logging
import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyspark.sql import SparkSession

from databricks4py.io.delta import optimize_table, vacuum_table
from databricks4py.spark_session import active_fallback

if TYPE_CHECKING:
    from databricks4py.metrics.base import MetricsSink

__all__ = [
    "MaintenanceResult",
    "MaintenanceRunner",
    "analyze_table",
]

logger = logging.getLogger(__name__)


def analyze_table(
    table_name: str,
    *,
    columns: Sequence[str] | None = None,
    spark: SparkSession | None = None,
) -> None:
    """Run ANALYZE TABLE to compute statistics.

    Args:
        table_name: The table to analyze.
        columns: Optional columns to compute statistics for.
        spark: Optional SparkSession.
    """
    _spark = active_fallback(spark)

    if columns:
        col_list = ", ".join(columns)
        sql = f"ANALYZE TABLE {table_name} COMPUTE STATISTICS FOR COLUMNS {col_list}"
    else:
        sql = f"ANALYZE TABLE {table_name} COMPUTE STATISTICS"

    logger.info("Running: %s", sql)
    _spark.sql(sql)


@dataclass(frozen=True)
class MaintenanceResult:
    """Result of a maintenance run."""

    table_name: str
    optimized: bool
    vacuumed: bool
    analyzed: bool
    duration_ms: float


class MaintenanceRunner:
    """Composes optimize, vacuum, and analyze into a single maintenance run."""

    def __init__(
        self,
        table_name: str,
        *,
        optimize: bool = True,
        vacuum: bool = True,
        analyze: bool = True,
        zorder_by: str | Sequence[str] | None = None,
        retention_hours: int = 168,
        analyze_columns: Sequence[str] | None = None,
        metrics_sink: MetricsSink | None = None,
        spark: SparkSession | None = None,
    ) -> None:
        self._table_name = table_name
        self._optimize = optimize
        self._vacuum = vacuum
        self._analyze = analyze
        self._zorder_by = zorder_by
        self._retention_hours = retention_hours
        self._analyze_columns = analyze_columns
        self._metrics_sink = metrics_sink
        self._spark = active_fallback(spark)

    def run(self) -> MaintenanceResult:
        start = time.monotonic()

        if self._optimize:
            logger.info("Optimizing %s", self._table_name)
            optimize_table(
                self._table_name,
                zorder_by=self._zorder_by,
                spark=self._spark,
            )

        if self._vacuum:
            logger.info("Vacuuming %s", self._table_name)
            vacuum_table(
                self._table_name,
                retention_hours=self._retention_hours,
                spark=self._spark,
            )

        if self._analyze:
            logger.info("Analyzing %s", self._table_name)
            analyze_table(
                self._table_name,
                columns=self._analyze_columns,
                spark=self._spark,
            )

        duration_ms = (time.monotonic() - start) * 1000

        result = MaintenanceResult(
            table_name=self._table_name,
            optimized=self._optimize,
            vacuumed=self._vacuum,
            analyzed=self._analyze,
            duration_ms=duration_ms,
        )

        if self._metrics_sink:
            from datetime import datetime, timezone

            from databricks4py.metrics.base import MetricEvent

            event = MetricEvent(
                job_name="maintenance",
                event_type="maintenance_complete",
                timestamp=datetime.now(tz=timezone.utc),
                duration_ms=int(duration_ms),
                table_name=self._table_name,
                metadata={
                    "optimized": result.optimized,
                    "vacuumed": result.vacuumed,
                    "analyzed": result.analyzed,
                },
            )
            self._metrics_sink.emit(event)

        logger.info(
            "Maintenance complete for %s in %.0fms",
            self._table_name,
            duration_ms,
        )
        return result
