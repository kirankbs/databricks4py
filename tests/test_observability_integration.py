"""Integration tests for observability with real Spark streaming."""

from __future__ import annotations

import json
import logging

import pyspark.sql
import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from databricks4py.io.streaming import StreamingTableReader, StreamingTriggerOptions
from databricks4py.observability.batch_context import BatchContext, BatchLogger
from databricks4py.observability.health import HealthStatus, StreamingHealthCheck

SCHEMA = StructType(
    [
        StructField("id", IntegerType()),
        StructField("name", StringType()),
    ]
)


def _write_source(spark, table_name, location, rows):
    df = spark.createDataFrame(rows, schema=SCHEMA)
    df.write.format("delta").mode("append").option("path", location).saveAsTable(table_name)


class _InstrumentedReader(StreamingTableReader):
    """Reader that uses BatchLogger inside process_batch."""

    def __init__(self, batch_logger: BatchLogger, output_path: str, **kwargs):
        super().__init__(**kwargs)
        self._batch_logger = batch_logger
        self._output_path = output_path

    def process_batch(self, df, batch_id):
        ctx = BatchContext.create(batch_id=batch_id, source_table=self._source_table)
        self._batch_logger.batch_start(ctx)

        count = df.count()
        df.write.format("delta").mode("append").save(self._output_path)

        self._batch_logger.batch_complete(ctx, row_count=count, duration_ms=ctx.elapsed_ms())


@pytest.mark.integration
class TestBatchLoggerIntegration:
    def test_batch_logger_in_streaming(
        self, spark_session_function: pyspark.sql.SparkSession, tmp_path
    ) -> None:
        src_loc = str(tmp_path / "src")
        out_loc = str(tmp_path / "out")
        cp_loc = str(tmp_path / "cp")
        _write_source(
            spark_session_function, "default.obs_src", src_loc, [(1, "a"), (2, "b"), (3, "c")]
        )

        records: list[logging.LogRecord] = []

        class Capture(logging.Handler):
            def emit(self, record):
                records.append(record)

        logger = logging.getLogger("test.obs.batch")
        logger.setLevel(logging.DEBUG)
        logger.addHandler(Capture())

        bl = BatchLogger(logger_name="test.obs.batch")
        reader = _InstrumentedReader(
            batch_logger=bl,
            output_path=out_loc,
            source_table="default.obs_src",
            trigger=StreamingTriggerOptions.AVAILABLE_NOW,
            checkpoint_location=cp_loc,
            spark=spark_session_function,
        )
        query = reader.start()
        query.awaitTermination(timeout=30)

        # Should have batch_start + batch_complete events
        events = [json.loads(r.getMessage()) for r in records]
        event_types = [e["event"] for e in events]
        assert "batch_start" in event_types
        assert "batch_complete" in event_types

        complete = next(e for e in events if e["event"] == "batch_complete")
        assert complete["row_count"] == 3
        assert complete["duration_ms"] > 0
        assert "correlation_id" in complete

    def test_batch_logger_with_extra_fields(
        self, spark_session_function: pyspark.sql.SparkSession, tmp_path
    ) -> None:
        src_loc = str(tmp_path / "src")
        out_loc = str(tmp_path / "out")
        cp_loc = str(tmp_path / "cp")
        _write_source(spark_session_function, "default.obs_extra", src_loc, [(1, "a")])

        records: list[logging.LogRecord] = []

        class Capture(logging.Handler):
            def emit(self, record):
                records.append(record)

        logger = logging.getLogger("test.obs.extra")
        logger.setLevel(logging.DEBUG)
        logger.addHandler(Capture())

        bl = BatchLogger(
            logger_name="test.obs.extra", extra_fields={"pipeline": "events", "env": "test"}
        )
        reader = _InstrumentedReader(
            batch_logger=bl,
            output_path=out_loc,
            source_table="default.obs_extra",
            trigger=StreamingTriggerOptions.AVAILABLE_NOW,
            checkpoint_location=cp_loc,
            spark=spark_session_function,
        )
        query = reader.start()
        query.awaitTermination(timeout=30)

        events = [json.loads(r.getMessage()) for r in records]
        for event in events:
            assert event["pipeline"] == "events"
            assert event["env"] == "test"


@pytest.mark.integration
class TestStreamingHealthCheckIntegration:
    def test_health_check_on_completed_query(
        self, spark_session_function: pyspark.sql.SparkSession, tmp_path
    ) -> None:
        """After availableNow finishes, the query is inactive → UNHEALTHY."""
        src_loc = str(tmp_path / "src")
        cp_loc = str(tmp_path / "cp")
        out_loc = str(tmp_path / "out")
        _write_source(spark_session_function, "default.obs_health", src_loc, [(1, "a")])

        class Writer(StreamingTableReader):
            def process_batch(self, df, batch_id):
                df.write.format("delta").mode("append").save(out_loc)

        reader = Writer(
            source_table="default.obs_health",
            trigger=StreamingTriggerOptions.AVAILABLE_NOW,
            checkpoint_location=cp_loc,
            spark=spark_session_function,
        )
        query = reader.start()
        query.awaitTermination(timeout=30)

        check = StreamingHealthCheck(query)
        result = check.evaluate()
        assert result.status == HealthStatus.UNHEALTHY
        active_checks = [c for c in result.checks if c.name == "query_active"]
        assert active_checks[0].status == HealthStatus.UNHEALTHY

    def test_health_result_summary_readable(
        self, spark_session_function: pyspark.sql.SparkSession, tmp_path
    ) -> None:
        src_loc = str(tmp_path / "src")
        cp_loc = str(tmp_path / "cp")
        out_loc = str(tmp_path / "out")
        _write_source(spark_session_function, "default.obs_summary", src_loc, [(1, "a")])

        class Writer(StreamingTableReader):
            def process_batch(self, df, batch_id):
                df.write.format("delta").mode("append").save(out_loc)

        reader = Writer(
            source_table="default.obs_summary",
            trigger=StreamingTriggerOptions.AVAILABLE_NOW,
            checkpoint_location=cp_loc,
            spark=spark_session_function,
        )
        query = reader.start()
        query.awaitTermination(timeout=30)

        check = StreamingHealthCheck(query)
        result = check.evaluate()
        summary = result.summary()
        assert "Overall:" in summary
        assert "query_active" in summary
