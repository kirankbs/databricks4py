"""Integration tests for MigrationRunner with real Delta history table."""

from __future__ import annotations

import pyspark.sql
import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from databricks4py.io.delta import DeltaTable
from databricks4py.migrations.runner import MigrationRunner, MigrationStep
from databricks4py.migrations.validators import MigrationError

SCHEMA = StructType(
    [
        StructField("id", IntegerType()),
        StructField("name", StringType()),
    ]
)


@pytest.mark.integration
class TestMigrationRunnerIntegration:
    def test_history_table_auto_created(
        self, spark_session_function: pyspark.sql.SparkSession
    ) -> None:
        MigrationRunner("default.mig_history_auto", spark=spark_session_function)
        assert spark_session_function.catalog.tableExists("default.mig_history_auto")

    def test_run_applies_step_and_records(
        self, spark_session_function: pyspark.sql.SparkSession, tmp_path
    ) -> None:
        location = str(tmp_path / "target")

        def create_table(spark):
            DeltaTable("default.mig_target", SCHEMA, location=location, spark=spark)

        runner = MigrationRunner("default.mig_history_apply", spark=spark_session_function)
        runner.register(MigrationStep(version="V001", description="Create target", up=create_table))
        result = runner.run()

        assert result.applied == ["V001"]
        assert spark_session_function.catalog.tableExists("default.mig_target")

        history = spark_session_function.read.table("default.mig_history_apply")
        rows = history.collect()
        assert len(rows) == 1
        assert rows[0]["version"] == "V001"
        assert rows[0]["success"] is True

    def test_idempotent_rerun_skips(
        self, spark_session_function: pyspark.sql.SparkSession, tmp_path
    ) -> None:
        location = str(tmp_path / "idempotent")

        def create_table(spark):
            DeltaTable("default.mig_idempotent", SCHEMA, location=location, spark=spark)

        runner = MigrationRunner("default.mig_history_idemp", spark=spark_session_function)
        runner.register(MigrationStep(version="V001", description="Create", up=create_table))
        runner.run()

        result2 = runner.run()
        assert result2.skipped == ["V001"]
        assert result2.applied == []

    def test_execution_order_by_version(
        self, spark_session_function: pyspark.sql.SparkSession
    ) -> None:
        order: list[str] = []
        runner = MigrationRunner("default.mig_history_order", spark=spark_session_function)
        runner.register(
            MigrationStep(version="V003", description="c", up=lambda s: order.append("V003")),
            MigrationStep(version="V001", description="a", up=lambda s: order.append("V001")),
            MigrationStep(version="V002", description="b", up=lambda s: order.append("V002")),
        )
        runner.run()
        assert order == ["V001", "V002", "V003"]

    def test_failure_halts_records_error(
        self, spark_session_function: pyspark.sql.SparkSession
    ) -> None:
        order: list[str] = []

        def fail(spark):
            raise RuntimeError("intentional failure")

        runner = MigrationRunner("default.mig_history_fail", spark=spark_session_function)
        runner.register(
            MigrationStep(version="V001", description="ok", up=lambda s: order.append("V001")),
            MigrationStep(version="V002", description="fail", up=fail),
            MigrationStep(version="V003", description="skip", up=lambda s: order.append("V003")),
        )
        result = runner.run()

        assert result.failed == "V002"
        assert order == ["V001"]

        history = spark_session_function.read.table("default.mig_history_fail")
        fail_rows = history.where("success = false").collect()
        assert len(fail_rows) == 1
        assert fail_rows[0]["version"] == "V002"
        assert "intentional failure" in fail_rows[0]["error_message"]

    def test_pre_validation_blocks(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        runner = MigrationRunner("default.mig_history_pre", spark=spark_session_function)
        runner.register(
            MigrationStep(
                version="V001",
                description="blocked",
                up=lambda s: None,
                pre_validate=lambda s: False,
            )
        )
        with pytest.raises(MigrationError, match="Pre-validation failed"):
            runner.run()

        history = spark_session_function.read.table("default.mig_history_pre")
        assert history.count() == 0

    def test_post_validation_failure(
        self, spark_session_function: pyspark.sql.SparkSession
    ) -> None:
        runner = MigrationRunner("default.mig_history_post", spark=spark_session_function)
        runner.register(
            MigrationStep(
                version="V001",
                description="post-fail",
                up=lambda s: None,
                post_validate=lambda s: False,
            )
        )
        result = runner.run()
        assert result.failed == "V001"

        history = spark_session_function.read.table("default.mig_history_post")
        row = history.first()
        assert row["success"] is False
        assert "Post-validation" in row["error_message"]

    def test_dry_run_no_side_effects(
        self, spark_session_function: pyspark.sql.SparkSession
    ) -> None:
        runner = MigrationRunner("default.mig_history_dry", spark=spark_session_function)
        runner.register(
            MigrationStep(
                version="V001",
                description="create table",
                up=lambda s: s.sql("CREATE TABLE default.mig_dry_target (id INT) USING DELTA"),
            )
        )
        result = runner.run(dry_run=True)

        assert result.dry_run is True
        assert result.applied == ["V001"]
        assert not spark_session_function.catalog.tableExists("default.mig_dry_target")

        history_count = spark_session_function.read.table("default.mig_history_dry").count()
        assert history_count == 0

    def test_real_ddl_alter_table(
        self, spark_session_function: pyspark.sql.SparkSession, tmp_path
    ) -> None:
        location = str(tmp_path / "ddl_target")
        DeltaTable(
            "default.mig_ddl_target", SCHEMA, location=location, spark=spark_session_function
        )

        def add_column(spark):
            spark.sql("ALTER TABLE default.mig_ddl_target ADD COLUMNS (age INT)")

        runner = MigrationRunner("default.mig_history_ddl", spark=spark_session_function)
        runner.register(MigrationStep(version="V001", description="Add age column", up=add_column))
        runner.run()

        cols = {
            row["col_name"]
            for row in spark_session_function.sql("DESCRIBE TABLE default.mig_ddl_target").collect()
            if row["col_name"] and not row["col_name"].startswith("#")
        }
        assert "age" in cols

    def test_pending_reflects_applied(
        self, spark_session_function: pyspark.sql.SparkSession
    ) -> None:
        runner = MigrationRunner("default.mig_history_pending", spark=spark_session_function)
        step_a = MigrationStep(version="V001", description="a", up=lambda s: None)
        step_b = MigrationStep(version="V002", description="b", up=lambda s: None)
        runner.register(step_a, step_b)
        runner.run()

        # Re-register and check pending
        runner2 = MigrationRunner("default.mig_history_pending", spark=spark_session_function)
        runner2.register(step_a, step_b)
        runner2.register(MigrationStep(version="V003", description="c", up=lambda s: None))
        pending = runner2.pending()
        assert [s.version for s in pending] == ["V003"]
