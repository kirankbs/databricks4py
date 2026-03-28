"""Integration tests for StreamingTableReader with real Spark streaming."""

from __future__ import annotations

import os

import pyspark.sql
import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from databricks4py.filters.base import WhereFilter
from databricks4py.io.checkpoint import CheckpointManager
from databricks4py.io.streaming import StreamingTableReader, StreamingTriggerOptions
from databricks4py.metrics.base import MetricEvent, MetricsSink

SCHEMA = StructType(
    [
        StructField("id", IntegerType()),
        StructField("name", StringType()),
    ]
)


class _CollectingSink(MetricsSink):
    """Collects emitted events for assertion."""

    def __init__(self):
        self.events: list[MetricEvent] = []

    def emit(self, event: MetricEvent) -> None:
        self.events.append(event)


class _AppendingReader(StreamingTableReader):
    """Writes each batch to an output Delta path."""

    def __init__(self, output_path: str, **kwargs):
        super().__init__(**kwargs)
        self.output_path = output_path
        self.batch_ids: list[int] = []

    def process_batch(self, df, batch_id):
        self.batch_ids.append(batch_id)
        df.write.format("delta").mode("append").save(self.output_path)


class _TrackingReader(StreamingTableReader):
    """Tracks calls without writing."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.batch_ids: list[int] = []
        self.row_counts: list[int] = []

    def process_batch(self, df, batch_id):
        self.batch_ids.append(batch_id)
        self.row_counts.append(df.count())


class _FailingReader(StreamingTableReader):
    """Raises on every batch for DLQ testing."""

    def process_batch(self, df, batch_id):
        raise ValueError("intentional batch failure")


def _write_source(spark, table_name, location, rows):
    """Write rows to a Delta table as streaming source."""
    df = spark.createDataFrame(rows, schema=SCHEMA)
    df.write.format("delta").mode("append").option("path", location).saveAsTable(table_name)


@pytest.mark.integration
class TestStreamingTableReaderIntegration:
    def test_start_processes_source_to_output(
        self, spark_session_function: pyspark.sql.SparkSession, tmp_path
    ) -> None:
        src_loc = str(tmp_path / "src")
        out_loc = str(tmp_path / "out")
        cp_loc = str(tmp_path / "cp")
        _write_source(
            spark_session_function,
            "default.stream_src",
            src_loc,
            [(1, "a"), (2, "b"), (3, "c")],
        )

        reader = _AppendingReader(
            output_path=out_loc,
            source_table="default.stream_src",
            trigger=StreamingTriggerOptions.AVAILABLE_NOW,
            checkpoint_location=cp_loc,
            spark=spark_session_function,
        )
        query = reader.start()
        query.awaitTermination(timeout=30)

        output = spark_session_function.read.format("delta").load(out_loc)
        assert output.count() == 3

    def test_stop_terminates_query(
        self, spark_session_function: pyspark.sql.SparkSession, tmp_path
    ) -> None:
        src_loc = str(tmp_path / "src")
        cp_loc = str(tmp_path / "cp")
        _write_source(spark_session_function, "default.stream_stop", src_loc, [(1, "a")])

        reader = _TrackingReader(
            source_table="default.stream_stop",
            trigger=StreamingTriggerOptions.AVAILABLE_NOW,
            checkpoint_location=cp_loc,
            spark=spark_session_function,
        )
        reader.start()
        reader.query.awaitTermination(timeout=30)

        # Query should be done (availableNow processes all and terminates)
        assert reader.is_active is False

    def test_query_property_set_after_start(
        self, spark_session_function: pyspark.sql.SparkSession, tmp_path
    ) -> None:
        src_loc = str(tmp_path / "src")
        cp_loc = str(tmp_path / "cp")
        _write_source(spark_session_function, "default.stream_prop", src_loc, [(1, "a")])

        reader = _TrackingReader(
            source_table="default.stream_prop",
            trigger=StreamingTriggerOptions.AVAILABLE_NOW,
            checkpoint_location=cp_loc,
            spark=spark_session_function,
        )
        assert reader.query is None
        reader.start()
        assert reader.query is not None
        reader.query.awaitTermination(timeout=30)

    def test_skip_empty_batches(
        self, spark_session_function: pyspark.sql.SparkSession, tmp_path
    ) -> None:
        src_loc = str(tmp_path / "src")
        cp_loc = str(tmp_path / "cp")
        # Create empty source table
        spark_session_function.createDataFrame([], schema=SCHEMA).write.format("delta").mode(
            "overwrite"
        ).option("path", src_loc).saveAsTable("default.stream_empty")

        reader = _TrackingReader(
            source_table="default.stream_empty",
            trigger=StreamingTriggerOptions.AVAILABLE_NOW,
            checkpoint_location=cp_loc,
            spark=spark_session_function,
        )
        query = reader.start()
        query.awaitTermination(timeout=30)
        # With empty source, process_batch should not be called
        assert reader.batch_ids == []

    def test_row_filter_applied(
        self, spark_session_function: pyspark.sql.SparkSession, tmp_path
    ) -> None:
        src_loc = str(tmp_path / "src")
        out_loc = str(tmp_path / "out")
        cp_loc = str(tmp_path / "cp")
        _write_source(
            spark_session_function,
            "default.stream_filter",
            src_loc,
            [(1, "a"), (2, "b"), (3, "c"), (4, "d")],
        )

        reader = _AppendingReader(
            output_path=out_loc,
            source_table="default.stream_filter",
            trigger=StreamingTriggerOptions.AVAILABLE_NOW,
            checkpoint_location=cp_loc,
            row_filter=WhereFilter("id > 2"),
            spark=spark_session_function,
        )
        query = reader.start()
        query.awaitTermination(timeout=30)

        output = spark_session_function.read.format("delta").load(out_loc)
        assert output.count() == 2
        ids = sorted([r["id"] for r in output.collect()])
        assert ids == [3, 4]

    def test_checkpoint_directory_created(
        self, spark_session_function: pyspark.sql.SparkSession, tmp_path
    ) -> None:
        src_loc = str(tmp_path / "src")
        cp_loc = str(tmp_path / "cp")
        _write_source(spark_session_function, "default.stream_cp", src_loc, [(1, "a")])

        reader = _TrackingReader(
            source_table="default.stream_cp",
            trigger=StreamingTriggerOptions.AVAILABLE_NOW,
            checkpoint_location=cp_loc,
            spark=spark_session_function,
        )
        query = reader.start()
        query.awaitTermination(timeout=30)

        assert os.path.isdir(cp_loc)

    def test_checkpoint_manager_auto_path(
        self, spark_session_function: pyspark.sql.SparkSession, tmp_path
    ) -> None:
        src_loc = str(tmp_path / "src")
        _write_source(spark_session_function, "default.stream_mgr", src_loc, [(1, "a")])

        mgr = CheckpointManager(base_path=str(tmp_path / "checkpoints"))
        reader = _TrackingReader(
            source_table="default.stream_mgr",
            trigger=StreamingTriggerOptions.AVAILABLE_NOW,
            checkpoint_manager=mgr,
            spark=spark_session_function,
        )
        query = reader.start()
        query.awaitTermination(timeout=30)

        assert os.path.isdir(reader._checkpoint_location)
        assert "default_stream_mgr" in reader._checkpoint_location

    def test_metrics_emitted_per_batch(
        self, spark_session_function: pyspark.sql.SparkSession, tmp_path
    ) -> None:
        src_loc = str(tmp_path / "src")
        cp_loc = str(tmp_path / "cp")
        _write_source(
            spark_session_function,
            "default.stream_metrics",
            src_loc,
            [(1, "a"), (2, "b"), (3, "c")],
        )

        sink = _CollectingSink()
        reader = _TrackingReader(
            source_table="default.stream_metrics",
            trigger=StreamingTriggerOptions.AVAILABLE_NOW,
            checkpoint_location=cp_loc,
            metrics_sink=sink,
            spark=spark_session_function,
        )
        query = reader.start()
        query.awaitTermination(timeout=30)

        assert len(sink.events) >= 1
        event = sink.events[0]
        assert event.event_type == "batch_complete"
        assert event.row_count == 3


@pytest.mark.integration
class TestStreamingDLQIntegration:
    def test_dlq_captures_failed_batch(
        self, spark_session_function: pyspark.sql.SparkSession, tmp_path
    ) -> None:
        src_loc = str(tmp_path / "src")
        cp_loc = str(tmp_path / "cp")
        _write_source(
            spark_session_function,
            "default.stream_dlq_src",
            src_loc,
            [(1, "a"), (2, "b")],
        )

        reader = _FailingReader(
            source_table="default.stream_dlq_src",
            trigger=StreamingTriggerOptions.AVAILABLE_NOW,
            checkpoint_location=cp_loc,
            dead_letter_table="default.stream_dlq_target",
            spark=spark_session_function,
        )
        query = reader.start()
        query.awaitTermination(timeout=30)

        dlq = spark_session_function.read.table("default.stream_dlq_target")
        assert dlq.count() > 0
        assert "_dlq_error_message" in dlq.columns
        assert "_dlq_batch_id" in dlq.columns
        assert "_dlq_error_timestamp" in dlq.columns

    def test_dlq_error_metadata_correct(
        self, spark_session_function: pyspark.sql.SparkSession, tmp_path
    ) -> None:
        src_loc = str(tmp_path / "src")
        cp_loc = str(tmp_path / "cp")
        _write_source(
            spark_session_function,
            "default.stream_dlq_meta",
            src_loc,
            [(1, "a")],
        )

        reader = _FailingReader(
            source_table="default.stream_dlq_meta",
            trigger=StreamingTriggerOptions.AVAILABLE_NOW,
            checkpoint_location=cp_loc,
            dead_letter_table="default.stream_dlq_meta_out",
            spark=spark_session_function,
        )
        query = reader.start()
        query.awaitTermination(timeout=30)

        row = spark_session_function.read.table("default.stream_dlq_meta_out").first()
        assert "intentional batch failure" in row["_dlq_error_message"]
        assert row["_dlq_error_timestamp"] is not None
