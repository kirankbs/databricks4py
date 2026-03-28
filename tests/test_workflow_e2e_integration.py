"""End-to-end integration tests: Workflow + quality + metrics, migration → validation."""

from __future__ import annotations

import pyspark.sql
import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from databricks4py.io.delta import DeltaTable
from databricks4py.metrics.base import MetricEvent, MetricsSink
from databricks4py.metrics.delta_sink import DeltaMetricsSink
from databricks4py.migrations.runner import MigrationRunner, MigrationStep
from databricks4py.migrations.schema_diff import SchemaDiff
from databricks4py.migrations.validators import TableValidator
from databricks4py.quality.expectations import NotNull, RowCount
from databricks4py.quality.gate import QualityError, QualityGate
from databricks4py.workflow import Workflow

SCHEMA = StructType(
    [
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("score", IntegerType()),
    ]
)


class _CollectingSink(MetricsSink):
    def __init__(self):
        self.events: list[MetricEvent] = []

    def emit(self, event: MetricEvent) -> None:
        self.events.append(event)


# ---------------------------------------------------------------------------
# Workflow + Quality
# ---------------------------------------------------------------------------
@pytest.mark.integration
class TestWorkflowQualityIntegration:
    def test_quality_check_passes_emits_metric(
        self, spark_session_function: pyspark.sql.SparkSession
    ) -> None:
        sink = _CollectingSink()
        df = spark_session_function.createDataFrame(
            [(1, "a", 80), (2, "b", 90)], schema=SCHEMA
        )
        gate = QualityGate(NotNull("id"), RowCount(min_count=1))

        class MyWorkflow(Workflow):
            def run(self_wf):
                result = self_wf.quality_check(df, gate, table_name="events")
                assert result.count() == 2

        wf = MyWorkflow(spark=spark_session_function, metrics=sink)
        wf.run()

        qc_events = [e for e in sink.events if e.event_type == "quality_check"]
        assert len(qc_events) == 1
        assert qc_events[0].metadata["passed"] is True

    def test_quality_check_raise_on_bad_data(
        self, spark_session_function: pyspark.sql.SparkSession
    ) -> None:
        df = spark_session_function.createDataFrame(
            [(1, None, 80)], schema=SCHEMA
        )
        gate = QualityGate(NotNull("name"), on_fail="raise")

        class MyWorkflow(Workflow):
            def run(self_wf):
                self_wf.quality_check(df, gate)

        wf = MyWorkflow(spark=spark_session_function)
        with pytest.raises(QualityError, match="Quality check failed"):
            wf.run()

    def test_quality_quarantine_in_workflow(
        self, spark_session_function: pyspark.sql.SparkSession
    ) -> None:
        df = spark_session_function.createDataFrame(
            [(1, "a", 80), (2, None, 90), (3, "c", 70)], schema=SCHEMA
        )
        captured: list[pyspark.sql.DataFrame] = []
        gate = QualityGate(
            NotNull("name"),
            on_fail="quarantine",
            quarantine_handler=lambda bad: captured.append(bad),
        )

        class MyWorkflow(Workflow):
            def run(self_wf):
                clean = self_wf.quality_check(df, gate)
                self_wf._clean_count = clean.count()

        wf = MyWorkflow(spark=spark_session_function)
        wf.run()
        assert wf._clean_count == 2
        assert len(captured) == 1
        assert captured[0].count() == 1


# ---------------------------------------------------------------------------
# Workflow + DeltaMetricsSink
# ---------------------------------------------------------------------------
@pytest.mark.integration
class TestWorkflowMetricsSinkIntegration:
    def test_execute_writes_lifecycle_metrics(
        self, spark_session_function: pyspark.sql.SparkSession
    ) -> None:
        sink = DeltaMetricsSink(
            "default.wf_metrics", spark=spark_session_function, buffer_size=100
        )

        class MyWorkflow(Workflow):
            def run(self_wf):
                pass  # no-op

        wf = MyWorkflow(spark=spark_session_function, metrics=sink)
        wf.execute()

        metrics = spark_session_function.read.table("default.wf_metrics")
        types = {r["event_type"] for r in metrics.collect()}
        assert "job_start" in types
        assert "job_complete" in types


# ---------------------------------------------------------------------------
# Cross-module E2E
# ---------------------------------------------------------------------------
@pytest.mark.integration
class TestEndToEndFlows:
    def test_migration_then_validation(
        self, spark_session_function: pyspark.sql.SparkSession, tmp_path
    ) -> None:
        location = str(tmp_path / "target")
        base_schema = StructType(
            [StructField("id", IntegerType()), StructField("name", StringType())]
        )
        DeltaTable(
            "default.e2e_mig_val", base_schema, location=location,
            spark=spark_session_function,
        )

        def add_score(spark):
            spark.sql("ALTER TABLE default.e2e_mig_val ADD COLUMNS (score INT)")

        runner = MigrationRunner("default.e2e_mig_history", spark=spark_session_function)
        runner.register(MigrationStep(version="V001", description="Add score", up=add_score))
        runner.run()

        validator = TableValidator(
            table_name="default.e2e_mig_val",
            expected_columns=["id", "name", "score"],
            spark=spark_session_function,
        )
        result = validator.validate()
        assert result.is_valid

    def test_migration_then_schema_diff(
        self, spark_session_function: pyspark.sql.SparkSession, tmp_path
    ) -> None:
        location = str(tmp_path / "target")
        base_schema = StructType(
            [StructField("id", IntegerType()), StructField("name", StringType())]
        )
        table = DeltaTable(
            "default.e2e_mig_diff", base_schema, location=location,
            spark=spark_session_function,
        )
        # Write initial data
        df1 = spark_session_function.createDataFrame([(1, "a")], schema=base_schema)
        table.write(df1)

        # Migration adds column
        spark_session_function.sql(
            "ALTER TABLE default.e2e_mig_diff ADD COLUMNS (score INT)"
        )

        # Schema diff between old schema and current table
        new_schema = StructType(base_schema.fields + [StructField("score", IntegerType())])
        diff = SchemaDiff(current=base_schema, incoming=new_schema)
        changes = diff.changes()
        assert any(c.column == "score" and c.change_type == "added" for c in changes)

    def test_full_workflow_read_quality_write(
        self, spark_session_function: pyspark.sql.SparkSession, tmp_path
    ) -> None:
        # Set up source table
        src_loc = str(tmp_path / "src")
        out_loc = str(tmp_path / "out")
        DeltaTable("default.e2e_src", SCHEMA, location=src_loc, spark=spark_session_function)
        spark_session_function.createDataFrame(
            [(1, "a", 80), (2, None, 90), (3, "c", 70)], schema=SCHEMA
        ).write.format("delta").mode("append").save(src_loc)

        DeltaTable("default.e2e_out", SCHEMA, location=out_loc, spark=spark_session_function)

        captured_bad: list[pyspark.sql.DataFrame] = []
        gate = QualityGate(
            NotNull("name"),
            on_fail="quarantine",
            quarantine_handler=lambda bad: captured_bad.append(bad),
        )

        class PipelineWorkflow(Workflow):
            def run(self_wf):
                source = self_wf.spark.read.format("delta").load(src_loc)
                clean = gate.enforce(source)
                clean.write.format("delta").mode("append").save(out_loc)

        wf = PipelineWorkflow(spark=spark_session_function)
        wf.execute()

        output = spark_session_function.read.format("delta").load(out_loc)
        assert output.count() == 2  # only clean rows
        assert len(captured_bad) == 1
        assert captured_bad[0].count() == 1
