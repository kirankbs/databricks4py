"""Integration tests for quality expectations and QualityGate on real DataFrames."""

from __future__ import annotations

import pyspark.sql
import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from databricks4py.quality.expectations import (
    ColumnExists,
    InRange,
    MatchesRegex,
    NotNull,
    RowCount,
    Unique,
)
from databricks4py.quality.gate import QualityError, QualityGate

SCHEMA = StructType(
    [
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("score", IntegerType()),
        StructField("email", StringType()),
    ]
)


def _build_df(spark: pyspark.sql.SparkSession, rows: list[tuple]) -> pyspark.sql.DataFrame:
    return spark.createDataFrame(rows, schema=SCHEMA)


# ---------------------------------------------------------------------------
# NotNull
# ---------------------------------------------------------------------------
@pytest.mark.integration
class TestNotNullIntegration:
    def test_passes_clean_data(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        df = _build_df(spark_session_function, [(1, "a", 80, "a@b.com"), (2, "b", 90, "b@c.com")])
        result = NotNull("id", "name").validate(df)
        assert result.passed
        assert result.failing_rows == 0

    def test_detects_nulls(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        df = _build_df(
            spark_session_function,
            [(1, None, 80, "a@b.com"), (2, None, 90, "b@c.com"), (3, "c", 70, "c@d.com")],
        )
        result = NotNull("name").validate(df)
        assert not result.passed
        assert result.failing_rows == 2
        assert result.total_rows == 3


# ---------------------------------------------------------------------------
# InRange
# ---------------------------------------------------------------------------
@pytest.mark.integration
class TestInRangeIntegration:
    def test_passes_valid(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        df = _build_df(spark_session_function, [(1, "a", 50, "x"), (2, "b", 100, "y")])
        result = InRange("score", min_val=0, max_val=100).validate(df)
        assert result.passed
        assert result.failing_rows == 0

    def test_detects_outliers(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        df = _build_df(
            spark_session_function,
            [(1, "a", -5, "x"), (2, "b", 50, "y"), (3, "c", 200, "z")],
        )
        result = InRange("score", min_val=0, max_val=100).validate(df)
        assert not result.passed
        assert result.failing_rows == 2

    def test_min_only(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        df = _build_df(spark_session_function, [(1, "a", 5, "x"), (2, "b", -1, "y")])
        result = InRange("score", min_val=0).validate(df)
        assert not result.passed
        assert result.failing_rows == 1


# ---------------------------------------------------------------------------
# Unique
# ---------------------------------------------------------------------------
@pytest.mark.integration
class TestUniqueIntegration:
    def test_passes_distinct(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        df = _build_df(spark_session_function, [(1, "a", 80, "x"), (2, "b", 90, "y")])
        result = Unique("id").validate(df)
        assert result.passed

    def test_detects_duplicates(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        df = _build_df(
            spark_session_function,
            [(1, "a", 80, "x"), (1, "b", 90, "y"), (2, "c", 70, "z")],
        )
        result = Unique("id").validate(df)
        assert not result.passed
        assert result.failing_rows == 1  # 3 total - 2 distinct = 1


# ---------------------------------------------------------------------------
# RowCount
# ---------------------------------------------------------------------------
@pytest.mark.integration
class TestRowCountIntegration:
    def test_in_bounds(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        df = _build_df(spark_session_function, [(i, "a", 50, "x") for i in range(5)])
        result = RowCount(min_count=1, max_count=10).validate(df)
        assert result.passed

    def test_below_min(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        df = _build_df(spark_session_function, [(1, "a", 50, "x"), (2, "b", 60, "y")])
        result = RowCount(min_count=5).validate(df)
        assert not result.passed


# ---------------------------------------------------------------------------
# MatchesRegex
# ---------------------------------------------------------------------------
@pytest.mark.integration
class TestMatchesRegexIntegration:
    def test_valid_pattern(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        df = _build_df(
            spark_session_function,
            [(1, "a", 80, "foo@bar.com"), (2, "b", 90, "baz@qux.org")],
        )
        result = MatchesRegex("email", r"^.+@.+\..+$").validate(df)
        assert result.passed

    def test_invalid_detected(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        df = _build_df(
            spark_session_function,
            [(1, "a", 80, "foo@bar.com"), (2, "b", 90, "not-an-email")],
        )
        result = MatchesRegex("email", r"^.+@.+\..+$").validate(df)
        assert not result.passed
        assert result.failing_rows == 1


# ---------------------------------------------------------------------------
# ColumnExists
# ---------------------------------------------------------------------------
@pytest.mark.integration
class TestColumnExistsIntegration:
    def test_present(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        df = _build_df(spark_session_function, [(1, "a", 80, "x")])
        result = ColumnExists("id", "name").validate(df)
        assert result.passed

    def test_missing(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        df = _build_df(spark_session_function, [(1, "a", 80, "x")])
        result = ColumnExists("id", "nonexistent").validate(df)
        assert not result.passed

    def test_dtype_mismatch(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        df = _build_df(spark_session_function, [(1, "a", 80, "x")])
        result = ColumnExists("id", dtype="string").validate(df)
        assert not result.passed


# ---------------------------------------------------------------------------
# QualityGate
# ---------------------------------------------------------------------------
@pytest.mark.integration
class TestQualityGateIntegration:
    def test_check_all_pass(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        df = _build_df(spark_session_function, [(1, "a", 80, "a@b.com")])
        gate = QualityGate(NotNull("id"), RowCount(min_count=1))
        report = gate.check(df)
        assert report.passed

    def test_enforce_raise(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        df = _build_df(spark_session_function, [(1, None, 80, "x")])
        gate = QualityGate(NotNull("name"), on_fail="raise")
        with pytest.raises(QualityError, match="Quality check failed"):
            gate.enforce(df)

    def test_enforce_warn_returns_original(
        self, spark_session_function: pyspark.sql.SparkSession
    ) -> None:
        df = _build_df(spark_session_function, [(1, None, 80, "x"), (2, "b", 90, "y")])
        gate = QualityGate(NotNull("name"), on_fail="warn")
        result_df = gate.enforce(df)
        assert result_df.count() == 2  # original df unchanged

    def test_enforce_quarantine_splits(
        self, spark_session_function: pyspark.sql.SparkSession
    ) -> None:
        df = _build_df(
            spark_session_function,
            [(1, None, 80, "x"), (2, "b", 90, "y"), (3, None, 70, "z")],
        )
        captured_bad: list[pyspark.sql.DataFrame] = []
        gate = QualityGate(
            NotNull("name"),
            on_fail="quarantine",
            quarantine_handler=lambda bad: captured_bad.append(bad),
        )
        clean = gate.enforce(df)

        assert len(captured_bad) == 1
        bad_count = captured_bad[0].count()
        clean_count = clean.count()
        assert bad_count == 2  # two null names
        assert clean_count == 1
        assert bad_count + clean_count == 3
