"""Tests for the quality profiler module."""

from dataclasses import FrozenInstanceError
from datetime import date

import pytest

from databricks4py.quality.profiler import ColumnProfile, DataProfile, profile


@pytest.mark.no_pyspark
class TestColumnProfile:
    def test_frozen(self) -> None:
        cp = ColumnProfile(
            name="id",
            dtype="int",
            total_count=10,
            null_count=0,
            null_pct=0.0,
            distinct_count=10,
        )
        with pytest.raises(FrozenInstanceError):
            cp.name = "other"  # type: ignore[misc]

    def test_repr(self) -> None:
        cp = ColumnProfile(
            name="id",
            dtype="int",
            total_count=10,
            null_count=2,
            null_pct=20.0,
            distinct_count=8,
        )
        r = repr(cp)
        assert "ColumnProfile" in r
        assert "id" in r

    def test_null_pct_value(self) -> None:
        cp = ColumnProfile(
            name="x",
            dtype="string",
            total_count=100,
            null_count=25,
            null_pct=25.0,
            distinct_count=75,
        )
        assert cp.null_pct == 25.0

    def test_defaults(self) -> None:
        cp = ColumnProfile(
            name="x",
            dtype="string",
            total_count=0,
            null_count=0,
            null_pct=0.0,
            distinct_count=0,
        )
        assert cp.min_value is None
        assert cp.max_value is None
        assert cp.mean is None


@pytest.mark.no_pyspark
class TestDataProfile:
    def test_summary_output(self) -> None:
        cols = [
            ColumnProfile(
                name="id",
                dtype="int",
                total_count=10,
                null_count=0,
                null_pct=0.0,
                distinct_count=10,
                min_value="1",
                max_value="10",
                mean=5.5,
            ),
            ColumnProfile(
                name="name",
                dtype="string",
                total_count=10,
                null_count=2,
                null_pct=20.0,
                distinct_count=8,
                min_value="alice",
                max_value="zara",
            ),
        ]
        dp = DataProfile(columns=cols, row_count=10, column_count=2)
        summary = dp.summary()
        assert "id" in summary
        assert "name" in summary
        assert "Rows: 10" in summary
        assert "Columns: 2" in summary

    def test_column_count_matches(self) -> None:
        cols = [
            ColumnProfile(
                name="a",
                dtype="int",
                total_count=5,
                null_count=0,
                null_pct=0.0,
                distinct_count=5,
            ),
        ]
        dp = DataProfile(columns=cols, row_count=5, column_count=1)
        assert dp.column_count == len(dp.columns)


@pytest.mark.unit
class TestProfile:
    def test_mixed_types(self, spark_session) -> None:
        data = [
            (1, "alice", date(2024, 1, 1)),
            (2, "bob", date(2024, 6, 15)),
            (3, "carol", date(2024, 12, 31)),
        ]
        df = spark_session.createDataFrame(data, ["id", "name", "dt"])
        result = profile(df)

        assert result.row_count == 3
        assert result.column_count == 3
        assert len(result.columns) == 3

        id_col = result.columns[0]
        assert id_col.name == "id"
        assert id_col.null_count == 0
        assert id_col.null_pct == 0.0

    def test_with_nulls(self, spark_session) -> None:
        data = [(1, "a"), (2, None), (3, None)]
        df = spark_session.createDataFrame(data, ["id", "val"])
        result = profile(df)

        val_col = next(c for c in result.columns if c.name == "val")
        assert val_col.null_count == 2
        assert val_col.null_pct == pytest.approx(66.666, rel=0.01)

    def test_all_nulls(self, spark_session) -> None:
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType

        schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("val", StringType()),
            ]
        )
        data = [(None, None), (None, None)]
        df = spark_session.createDataFrame(data, schema)
        result = profile(df)

        for col in result.columns:
            assert col.null_count == 2
            assert col.null_pct == 100.0
            assert col.min_value is None
            assert col.max_value is None

        id_col = next(c for c in result.columns if c.name == "id")
        assert id_col.mean is None

    def test_empty_dataframe(self, spark_session) -> None:
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType

        schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("name", StringType()),
            ]
        )
        df = spark_session.createDataFrame([], schema)
        result = profile(df)

        assert result.row_count == 0
        assert result.column_count == 2
        for col in result.columns:
            assert col.total_count == 0
            assert col.null_count == 0
            assert col.distinct_count == 0

    def test_numeric_mean(self, spark_session) -> None:
        data = [(10,), (20,), (30,)]
        df = spark_session.createDataFrame(data, ["score"])
        result = profile(df)

        score_col = result.columns[0]
        assert score_col.mean == pytest.approx(20.0)

    def test_string_no_mean(self, spark_session) -> None:
        data = [("a",), ("b",), ("c",)]
        df = spark_session.createDataFrame(data, ["label"])
        result = profile(df)

        label_col = result.columns[0]
        assert label_col.mean is None

    def test_distinct_count(self, spark_session) -> None:
        data = [(1,), (1,), (2,), (3,), (3,)]
        df = spark_session.createDataFrame(data, ["val"])
        result = profile(df)

        val_col = result.columns[0]
        # approx_count_distinct may not be exact, but for small data it should be close
        assert 2 <= val_col.distinct_count <= 4
