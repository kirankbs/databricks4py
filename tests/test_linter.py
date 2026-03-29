"""Tests for PySpark performance anti-pattern linter."""

from unittest.mock import MagicMock

import pyspark.sql
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from databricks4py.linter import (
    LintReport,
    LintWarning,
    Severity,
    check_collect_safety,
    lint,
)

SCHEMA = StructType(
    [
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("value", IntegerType()),
    ]
)


@pytest.mark.no_pyspark
class TestSeverity:
    def test_values(self) -> None:
        assert Severity.INFO.value == "info"
        assert Severity.WARNING.value == "warning"
        assert Severity.ERROR.value == "error"


@pytest.mark.no_pyspark
class TestLintWarning:
    def test_frozen(self) -> None:
        w = LintWarning(rule="TEST", severity=Severity.INFO, message="msg", suggestion="fix")
        with pytest.raises(AttributeError, match="cannot assign"):
            w.rule = "OTHER"  # type: ignore[misc]

    def test_defaults(self) -> None:
        w = LintWarning(rule="TEST", severity=Severity.INFO, message="msg", suggestion="fix")
        assert w.plan_excerpt == ""


@pytest.mark.no_pyspark
class TestLintReport:
    def test_empty_report(self) -> None:
        report = LintReport()
        assert not report.has_errors
        assert not report.has_warnings
        assert "No anti-patterns" in report.summary()

    def test_has_errors(self) -> None:
        report = LintReport(
            warnings=[
                LintWarning(
                    rule="TEST",
                    severity=Severity.ERROR,
                    message="bad",
                    suggestion="fix",
                )
            ]
        )
        assert report.has_errors
        assert report.has_warnings

    def test_has_warnings_no_errors(self) -> None:
        report = LintReport(
            warnings=[
                LintWarning(
                    rule="TEST",
                    severity=Severity.WARNING,
                    message="warn",
                    suggestion="fix",
                )
            ]
        )
        assert not report.has_errors
        assert report.has_warnings

    def test_summary_format(self) -> None:
        report = LintReport(
            warnings=[
                LintWarning(
                    rule="RULE_A",
                    severity=Severity.ERROR,
                    message="error msg",
                    suggestion="fix A",
                ),
                LintWarning(
                    rule="RULE_B",
                    severity=Severity.INFO,
                    message="info msg",
                    suggestion="fix B",
                ),
            ]
        )
        summary = report.summary()
        assert "2 issue(s)" in summary
        assert "RULE_A" in summary
        assert "RULE_B" in summary
        assert "fix A" in summary

    def test_frozen(self) -> None:
        report = LintReport()
        with pytest.raises(AttributeError, match="cannot assign"):
            report.warnings = []  # type: ignore[misc]


@pytest.mark.unit
class TestLint:
    def test_clean_plan(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        df = spark_session_function.createDataFrame([(1, "a", 10), (2, "b", 20)], schema=SCHEMA)
        report = lint(df)
        assert not report.has_errors

    def test_streaming_raises(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        mock_df = MagicMock(spec=pyspark.sql.DataFrame)
        mock_df.isStreaming = True
        with pytest.raises(TypeError, match="streaming"):
            lint(mock_df)

    def test_cartesian_product_detection(
        self, spark_session_function: pyspark.sql.SparkSession
    ) -> None:
        df1 = spark_session_function.createDataFrame([(1, "a", 10)], schema=SCHEMA)
        df2 = spark_session_function.createDataFrame([(2, "b", 20)], schema=SCHEMA).select(
            F.col("id").alias("id2"), F.col("name").alias("name2")
        )

        # crossJoin creates a CartesianProduct
        crossed = df1.crossJoin(df2)
        report = lint(crossed)
        cartesian_warnings = [w for w in report.warnings if w.rule == "CARTESIAN_PRODUCT"]
        assert len(cartesian_warnings) >= 1
        assert cartesian_warnings[0].severity == Severity.ERROR

    def test_excessive_columns_detection(
        self, spark_session_function: pyspark.sql.SparkSession
    ) -> None:
        # Create a DataFrame with > 500 columns
        data = [{f"col_{i}": i for i in range(501)}]
        df = spark_session_function.createDataFrame(data)
        report = lint(df)
        wide_warnings = [w for w in report.warnings if w.rule == "EXCESSIVE_COLUMNS"]
        assert len(wide_warnings) == 1
        assert "501" in wide_warnings[0].message

    def test_simple_filter_no_false_positive(
        self, spark_session_function: pyspark.sql.SparkSession
    ) -> None:
        df = spark_session_function.createDataFrame([(1, "a", 10), (2, "b", 20)], schema=SCHEMA)
        filtered = df.where("value > 10")
        report = lint(filtered)
        # Should not flag as full table scan since there's a filter
        cartesian = [w for w in report.warnings if w.rule == "CARTESIAN_PRODUCT"]
        assert len(cartesian) == 0


@pytest.mark.unit
class TestCheckCollectSafety:
    def test_safe_small_df(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        df = spark_session_function.createDataFrame([(1, "a", 10)], schema=SCHEMA)
        result = check_collect_safety(df)
        assert result is None  # safe

    def test_plan_based_detection(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        # For a tiny in-memory DataFrame, the plan may not have rowCount stats,
        # so this primarily tests that the function runs without error
        df = spark_session_function.createDataFrame(
            [(i, f"name_{i}", i * 10) for i in range(10)], schema=SCHEMA
        )
        result = check_collect_safety(df, max_rows=5)
        # The result depends on whether Spark exposes row stats in the plan.
        # Either None (no stats available) or a warning is valid.
        if result is not None:
            assert result.rule == "COLLECT_SAFETY"
            assert result.severity == Severity.ERROR
