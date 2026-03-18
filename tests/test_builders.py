"""Tests for DataFrameBuilder."""

from __future__ import annotations

import pyspark.sql
import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from databricks4py.testing.builders import DataFrameBuilder


@pytest.mark.integration
class TestDataFrameBuilder:
    def test_basic_build(self, spark_session: pyspark.sql.SparkSession) -> None:
        df = (
            DataFrameBuilder(spark_session)
            .with_columns({"id": "int", "name": "string"})
            .with_rows((1, "alice"), (2, "bob"))
            .build()
        )
        assert df.count() == 2
        assert df.schema["id"].dataType == IntegerType()
        assert df.schema["name"].dataType == StringType()

    def test_with_sequential(self, spark_session: pyspark.sql.SparkSession) -> None:
        df = (
            DataFrameBuilder(spark_session)
            .with_columns({"id": "int"})
            .with_sequential("id", start=5, count=3)
            .build()
        )
        rows = [r["id"] for r in df.collect()]
        assert sorted(rows) == [5, 6, 7]

    def test_with_schema_struct_type(self, spark_session: pyspark.sql.SparkSession) -> None:
        schema = StructType([
            StructField("x", IntegerType()),
            StructField("y", StringType()),
        ])
        df = (
            DataFrameBuilder(spark_session)
            .with_schema(schema)
            .with_rows((1, "a"))
            .build()
        )
        assert df.schema == schema
        assert df.count() == 1

    def test_with_nulls(self, spark_session: pyspark.sql.SparkSession) -> None:
        df = (
            DataFrameBuilder(spark_session)
            .with_columns({"id": "int"})
            .with_sequential("id", start=1, count=100)
            .with_nulls("id", frequency=1.0, seed=42)
            .build()
        )
        null_count = df.filter(df["id"].isNull()).count()
        assert null_count == 100

    def test_empty_build_raises(self, spark_session: pyspark.sql.SparkSession) -> None:
        with pytest.raises(ValueError, match="No schema defined"):
            DataFrameBuilder(spark_session).build()
