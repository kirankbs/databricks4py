"""Tests for table maintenance utilities."""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pyspark.sql
import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from databricks4py.io.maintenance import MaintenanceResult, MaintenanceRunner, analyze_table
from databricks4py.metrics.base import MetricEvent, MetricsSink

SCHEMA = StructType(
    [
        StructField("id", IntegerType()),
        StructField("name", StringType()),
    ]
)

TABLE_NAME = "test_maintenance_table"


def _create_table(spark: pyspark.sql.SparkSession, table_name: str = TABLE_NAME) -> None:
    df = spark.createDataFrame([(1, "a"), (2, "b")], schema=SCHEMA)
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)


def _drop_table(spark: pyspark.sql.SparkSession, table_name: str = TABLE_NAME) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")


class _CapturingSink(MetricsSink):
    def __init__(self) -> None:
        self.events: list[MetricEvent] = []

    def emit(self, event: MetricEvent) -> None:
        self.events.append(event)


@pytest.mark.no_pyspark
class TestMaintenanceResult:
    def test_creation(self) -> None:
        result = MaintenanceResult(
            table_name="db.tbl",
            optimized=True,
            vacuumed=True,
            analyzed=False,
            duration_ms=123.4,
        )
        assert result.table_name == "db.tbl"
        assert result.optimized is True
        assert result.vacuumed is True
        assert result.analyzed is False
        assert result.duration_ms == 123.4

    def test_frozen(self) -> None:
        result = MaintenanceResult(
            table_name="t",
            optimized=True,
            vacuumed=False,
            analyzed=False,
            duration_ms=0,
        )
        with pytest.raises(FrozenInstanceError):
            result.table_name = "other"  # type: ignore[misc]

    def test_repr(self) -> None:
        result = MaintenanceResult(
            table_name="catalog.schema.tbl",
            optimized=True,
            vacuumed=False,
            analyzed=True,
            duration_ms=42.0,
        )
        r = repr(result)
        assert "MaintenanceResult" in r
        assert "catalog.schema.tbl" in r


@pytest.mark.integration
class TestAnalyzeTable:
    def test_analyze_table_generates_correct_sql(self) -> None:
        """Verify the SQL string that analyze_table would execute."""
        # ANALYZE TABLE is not supported for Delta v2 tables in local Spark,
        # but works on Databricks Runtime. Validate the logic without executing.
        from unittest.mock import MagicMock

        mock_spark = MagicMock()
        analyze_table("catalog.schema.tbl", spark=mock_spark)
        mock_spark.sql.assert_called_once_with(
            "ANALYZE TABLE catalog.schema.tbl COMPUTE STATISTICS"
        )

    def test_analyze_table_with_columns_sql(self) -> None:
        from unittest.mock import MagicMock

        mock_spark = MagicMock()
        analyze_table("catalog.schema.tbl", columns=["id", "name"], spark=mock_spark)
        mock_spark.sql.assert_called_once_with(
            "ANALYZE TABLE catalog.schema.tbl COMPUTE STATISTICS FOR COLUMNS id, name"
        )


@pytest.mark.integration
class TestMaintenanceRunner:
    def test_optimize_and_vacuum(self, spark_session: pyspark.sql.SparkSession) -> None:
        """Run optimize + vacuum (analyze skipped — not supported on local Delta v2)."""
        _create_table(spark_session)
        try:
            runner = MaintenanceRunner(TABLE_NAME, analyze=False, spark=spark_session)
            result = runner.run()

            assert result.table_name == TABLE_NAME
            assert result.optimized is True
            assert result.vacuumed is True
            assert result.analyzed is False
        finally:
            _drop_table(spark_session)

    def test_optimize_only(self, spark_session: pyspark.sql.SparkSession) -> None:
        _create_table(spark_session)
        try:
            runner = MaintenanceRunner(
                TABLE_NAME,
                optimize=True,
                vacuum=False,
                analyze=False,
                spark=spark_session,
            )
            result = runner.run()

            assert result.optimized is True
            assert result.vacuumed is False
            assert result.analyzed is False
        finally:
            _drop_table(spark_session)

    def test_vacuum_only(self, spark_session: pyspark.sql.SparkSession) -> None:
        _create_table(spark_session)
        try:
            runner = MaintenanceRunner(
                TABLE_NAME,
                optimize=False,
                vacuum=True,
                analyze=False,
                spark=spark_session,
            )
            result = runner.run()

            assert result.optimized is False
            assert result.vacuumed is True
            assert result.analyzed is False
        finally:
            _drop_table(spark_session)

    def test_with_metrics_sink(self, spark_session: pyspark.sql.SparkSession) -> None:
        _create_table(spark_session)
        try:
            sink = _CapturingSink()
            runner = MaintenanceRunner(
                TABLE_NAME,
                analyze=False,
                metrics_sink=sink,
                spark=spark_session,
            )
            runner.run()

            assert len(sink.events) == 1
            event = sink.events[0]
            assert event.event_type == "maintenance_complete"
            assert event.table_name == TABLE_NAME
            assert event.metadata["optimized"] is True
            assert event.metadata["vacuumed"] is True
            assert event.metadata["analyzed"] is False
        finally:
            _drop_table(spark_session)

    def test_duration_tracked(self, spark_session: pyspark.sql.SparkSession) -> None:
        _create_table(spark_session)
        try:
            runner = MaintenanceRunner(TABLE_NAME, analyze=False, spark=spark_session)
            result = runner.run()

            assert result.duration_ms > 0
        finally:
            _drop_table(spark_session)
