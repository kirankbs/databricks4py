"""Tests for assertion helpers."""

from __future__ import annotations

import pyspark.sql
import pytest
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from databricks4py.testing.assertions import assert_frame_equal, assert_schema_equal


@pytest.mark.integration
class TestAssertFrameEqual:
    def test_equal_frames(self, spark_session: pyspark.sql.SparkSession) -> None:
        schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("name", StringType()),
            ]
        )
        df1 = spark_session.createDataFrame([(1, "a"), (2, "b")], schema=schema)
        df2 = spark_session.createDataFrame([(1, "a"), (2, "b")], schema=schema)
        assert_frame_equal(df1, df2)

    def test_unequal_frames_raises(self, spark_session: pyspark.sql.SparkSession) -> None:
        schema = StructType([StructField("id", IntegerType())])
        df1 = spark_session.createDataFrame([(1,)], schema=schema)
        df2 = spark_session.createDataFrame([(2,)], schema=schema)
        with pytest.raises((AssertionError, Exception)):
            assert_frame_equal(df1, df2)

    def test_schema_mismatch_raises(self, spark_session: pyspark.sql.SparkSession) -> None:
        schema1 = StructType([StructField("id", IntegerType())])
        schema2 = StructType([StructField("id", StringType())])
        df1 = spark_session.createDataFrame([(1,)], schema=schema1)
        df2 = spark_session.createDataFrame([("1",)], schema=schema2)
        with pytest.raises(AssertionError, match="type mismatch"):
            assert_frame_equal(df1, df2)

    def test_ignore_order(self, spark_session: pyspark.sql.SparkSession) -> None:
        schema = StructType([StructField("id", IntegerType())])
        df1 = spark_session.createDataFrame([(2,), (1,)], schema=schema)
        df2 = spark_session.createDataFrame([(1,), (2,)], schema=schema)
        assert_frame_equal(df1, df2, check_order=False)


@pytest.mark.integration
class TestAssertSchemaEqual:
    def test_equal_schemas(self, spark_session: pyspark.sql.SparkSession) -> None:
        s1 = StructType(
            [
                StructField("id", IntegerType()),
                StructField("name", StringType()),
            ]
        )
        s2 = StructType(
            [
                StructField("id", IntegerType()),
                StructField("name", StringType()),
            ]
        )
        assert_schema_equal(s1, s2)

    def test_different_types_raises(self, spark_session: pyspark.sql.SparkSession) -> None:
        s1 = StructType([StructField("id", IntegerType())])
        s2 = StructType([StructField("id", LongType())])
        with pytest.raises(AssertionError, match="type mismatch"):
            assert_schema_equal(s1, s2)
