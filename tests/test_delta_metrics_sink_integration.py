"""Integration tests for DeltaMetricsSink writing to real Delta tables."""

from __future__ import annotations

from datetime import datetime, timezone

import pyspark.sql
import pytest

from databricks4py.metrics.base import MetricEvent
from databricks4py.metrics.delta_sink import DeltaMetricsSink


def _event(job_name: str = "TestJob", event_type: str = "batch_complete", **kwargs) -> MetricEvent:
    # Provide all fields so PySpark can infer schema (all-None columns fail inference)
    defaults = {
        "duration_ms": 100.0,
        "row_count": 10,
        "batch_id": 0,
        "table_name": "test_table",
        "metadata": {"key": "value"},
    }
    defaults.update(kwargs)
    return MetricEvent(
        job_name=job_name,
        event_type=event_type,
        timestamp=datetime.now(tz=timezone.utc),
        **defaults,
    )


@pytest.mark.integration
class TestDeltaMetricsSinkIntegration:
    def test_flush_writes_to_delta_table(
        self, spark_session_function: pyspark.sql.SparkSession
    ) -> None:
        sink = DeltaMetricsSink("default.test_metrics_flush", spark=spark_session_function)
        sink.emit(_event())
        sink.emit(_event())
        sink.emit(_event())
        sink.flush()

        df = spark_session_function.read.table("default.test_metrics_flush")
        assert df.count() == 3

    def test_threshold_auto_flush(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        sink = DeltaMetricsSink(
            "default.test_metrics_threshold",
            spark=spark_session_function,
            buffer_size=2,
        )
        sink.emit(_event())
        sink.emit(_event())  # triggers auto-flush at buffer_size=2

        df = spark_session_function.read.table("default.test_metrics_threshold")
        assert df.count() == 2

    def test_flush_clears_buffer(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        sink = DeltaMetricsSink("default.test_metrics_clear", spark=spark_session_function)
        sink.emit(_event())
        sink.flush()
        sink.emit(_event())
        sink.emit(_event())
        sink.flush()

        df = spark_session_function.read.table("default.test_metrics_clear")
        assert df.count() == 3

    def test_empty_flush_noop(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        sink = DeltaMetricsSink("default.test_metrics_empty", spark=spark_session_function)
        sink.flush()
        assert not spark_session_function.catalog.tableExists("default.test_metrics_empty")

    def test_event_fields_roundtrip(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        sink = DeltaMetricsSink("default.test_metrics_fields", spark=spark_session_function)
        sink.emit(
            _event(
                job_name="MyPipeline",
                event_type="job_complete",
                duration_ms=1234.5,
                row_count=42,
                table_name="catalog.schema.events",
            )
        )
        sink.flush()

        row = spark_session_function.read.table("default.test_metrics_fields").first()
        assert row["job_name"] == "MyPipeline"
        assert row["event_type"] == "job_complete"
        assert row["row_count"] == 42

    def test_append_preserves_previous(
        self, spark_session_function: pyspark.sql.SparkSession
    ) -> None:
        sink = DeltaMetricsSink("default.test_metrics_append", spark=spark_session_function)
        sink.emit(_event())
        sink.flush()
        sink.emit(_event())
        sink.emit(_event())
        sink.flush()

        df = spark_session_function.read.table("default.test_metrics_append")
        assert df.count() == 3
