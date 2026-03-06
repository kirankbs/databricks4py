"""Tests for the filter pipeline."""

import pyspark.sql
import pytest

from databricks4py.filters.base import (
    ColumnFilter,
    DropDuplicates,
    FilterPipeline,
    WhereFilter,
)


@pytest.mark.integration
class TestDropDuplicates:
    def test_dedup_all_columns(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        df = spark_session_function.createDataFrame(
            [{"id": 1, "v": "a"}, {"id": 1, "v": "a"}, {"id": 2, "v": "b"}]
        )
        result = DropDuplicates().apply(df)
        assert result.count() == 2

    def test_dedup_subset(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        df = spark_session_function.createDataFrame([{"id": 1, "v": "a"}, {"id": 1, "v": "b"}])
        result = DropDuplicates(subset=["id"]).apply(df)
        assert result.count() == 1

    def test_repr(self) -> None:
        assert "subset" in repr(DropDuplicates(["id"]))


@pytest.mark.integration
class TestColumnFilter:
    def test_select_columns(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        df = spark_session_function.createDataFrame([{"a": 1, "b": 2, "c": 3}])
        result = ColumnFilter(columns=["a", "c"]).apply(df)
        assert result.columns == ["a", "c"]

    def test_empty_columns_raises(self) -> None:
        with pytest.raises(ValueError):
            ColumnFilter(columns=[])


@pytest.mark.integration
class TestWhereFilter:
    def test_condition(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        df = spark_session_function.createDataFrame([{"id": 1}, {"id": 2}, {"id": 3}])
        result = WhereFilter("id > 1").apply(df)
        assert result.count() == 2


@pytest.mark.integration
class TestFilterPipeline:
    def test_chain(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        df = spark_session_function.createDataFrame(
            [
                {"id": 1, "name": "a", "score": 10},
                {"id": 1, "name": "a", "score": 10},
                {"id": 2, "name": "b", "score": 5},
            ]
        )
        pipeline = FilterPipeline(
            [
                DropDuplicates(),
                WhereFilter("score > 5"),
                ColumnFilter(columns=["id", "name"]),
            ]
        )
        result = pipeline.apply(df)
        assert result.count() == 1
        assert result.columns == ["id", "name"]

    def test_callable(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        df = spark_session_function.createDataFrame([{"id": 1}])
        pipeline = FilterPipeline()
        result = pipeline(df)
        assert result.count() == 1

    def test_add_method(self) -> None:
        pipeline = FilterPipeline()
        pipeline.add(DropDuplicates())
        assert len(pipeline) == 1

    def test_empty_pipeline(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        df = spark_session_function.createDataFrame([{"id": 1}, {"id": 2}])
        pipeline = FilterPipeline()
        assert pipeline(df).count() == 2
