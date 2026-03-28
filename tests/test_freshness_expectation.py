"""Tests for FreshnessExpectation."""

from __future__ import annotations

from datetime import timedelta

import pytest

from databricks4py.quality.expectations import FreshnessExpectation


@pytest.mark.no_pyspark
class TestFreshnessExpectationRepr:
    def test_repr_contains_column(self) -> None:
        e = FreshnessExpectation("event_time", max_age=timedelta(hours=1))
        assert "event_time" in repr(e)

    def test_repr_contains_max_age(self) -> None:
        age = timedelta(hours=6)
        e = FreshnessExpectation("ts", max_age=age)
        assert repr(age) in repr(e)

    def test_repr_format(self) -> None:
        e = FreshnessExpectation("created_at", max_age=timedelta(minutes=30))
        assert repr(e).startswith("FreshnessExpectation(")


@pytest.mark.no_pyspark
class TestFreshnessExpectationIsExpectation:
    def test_is_expectation_subclass(self) -> None:
        from databricks4py.quality.base import Expectation

        assert issubclass(FreshnessExpectation, Expectation)

    def test_exported_from_quality_package(self) -> None:
        from databricks4py.quality import FreshnessExpectation as FE

        assert FE is FreshnessExpectation


@pytest.mark.no_pyspark
class TestFreshnessExpectationInit:
    def test_stores_column(self) -> None:
        e = FreshnessExpectation("my_ts", max_age=timedelta(days=1))
        assert e._column == "my_ts"

    def test_stores_max_age(self) -> None:
        age = timedelta(hours=2)
        e = FreshnessExpectation("ts", max_age=age)
        assert e._max_age == age


@pytest.mark.unit
class TestFreshnessExpectationValidate:
    def test_fresh_table_passes(self, spark_session) -> None:
        from datetime import datetime, timezone

        from pyspark.sql.types import StructField, StructType, TimestampType

        schema = StructType([StructField("ts", TimestampType())])
        now = datetime.now(tz=timezone.utc)
        df = spark_session.createDataFrame([(now,)], schema=schema)

        e = FreshnessExpectation("ts", max_age=timedelta(hours=1))
        result = e.validate(df)
        assert result.passed is True
        assert result.total_rows == 1

    def test_stale_table_fails(self, spark_session) -> None:
        from datetime import datetime, timezone

        from pyspark.sql.types import StructField, StructType, TimestampType

        schema = StructType([StructField("ts", TimestampType())])
        old = datetime(2020, 1, 1, tzinfo=timezone.utc)
        df = spark_session.createDataFrame([(old,)], schema=schema)

        e = FreshnessExpectation("ts", max_age=timedelta(hours=1))
        result = e.validate(df)
        assert result.passed is False
        assert result.failing_rows == 1

    def test_empty_df_fails(self, spark_session) -> None:
        from pyspark.sql.types import StructField, StructType, TimestampType

        schema = StructType([StructField("ts", TimestampType())])
        df = spark_session.createDataFrame([], schema=schema)

        e = FreshnessExpectation("ts", max_age=timedelta(hours=1))
        result = e.validate(df)
        assert result.passed is False
        assert result.total_rows == 0

    def test_mixed_rows_counts_stale(self, spark_session) -> None:
        """With one fresh row and one stale row, table passes but failing_rows=1."""
        from datetime import datetime, timezone

        from pyspark.sql.types import StructField, StructType, TimestampType

        schema = StructType([StructField("ts", TimestampType())])
        now = datetime.now(tz=timezone.utc)
        old = datetime(2020, 1, 1, tzinfo=timezone.utc)
        df = spark_session.createDataFrame([(now,), (old,)], schema=schema)

        e = FreshnessExpectation("ts", max_age=timedelta(hours=1))
        result = e.validate(df)
        # Table passes because max(ts) is fresh
        assert result.passed is True
        assert result.total_rows == 2
        # But one row is individually stale
        assert result.failing_rows == 1
