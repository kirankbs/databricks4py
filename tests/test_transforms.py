"""Tests for column utilities and DataFrame transforms."""

import pyspark.sql
import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from databricks4py.transforms import (
    flatten_struct,
    prefix_columns,
    single_space,
    snake_case_columns,
    suffix_columns,
    trim_all,
)


class TestSnakeCaseColumns:
    @pytest.mark.unit
    def test_camel_case(self, spark_session: pyspark.sql.SparkSession) -> None:
        df = spark_session.createDataFrame([{"firstName": "a", "lastName": "b"}])
        result = snake_case_columns(df)
        assert result.columns == ["first_name", "last_name"]

    @pytest.mark.unit
    def test_pascal_case(self, spark_session: pyspark.sql.SparkSession) -> None:
        df = spark_session.createDataFrame([(1,)], ["OrderId"])
        result = snake_case_columns(df)
        assert result.columns == ["order_id"]

    @pytest.mark.unit
    def test_spaces_and_hyphens(self, spark_session: pyspark.sql.SparkSession) -> None:
        df = spark_session.createDataFrame([(1, 2)], ["first name", "last-name"])
        result = snake_case_columns(df)
        assert result.columns == ["first_name", "last_name"]

    @pytest.mark.unit
    def test_already_snake_case(self, spark_session: pyspark.sql.SparkSession) -> None:
        df = spark_session.createDataFrame([(1,)], ["already_snake"])
        result = snake_case_columns(df)
        assert result.columns == ["already_snake"]

    @pytest.mark.unit
    def test_consecutive_underscores(self, spark_session: pyspark.sql.SparkSession) -> None:
        df = spark_session.createDataFrame([(1,)], ["some__bad___name"])
        result = snake_case_columns(df)
        assert result.columns == ["some_bad_name"]

    @pytest.mark.unit
    def test_empty_dataframe(self, spark_session: pyspark.sql.SparkSession) -> None:
        schema = StructType([StructField("CamelCase", IntegerType())])
        df = spark_session.createDataFrame([], schema=schema)
        result = snake_case_columns(df)
        assert result.columns == ["camel_case"]
        assert result.count() == 0


class TestPrefixColumns:
    @pytest.mark.unit
    def test_basic_prefix(self, spark_session: pyspark.sql.SparkSession) -> None:
        df = spark_session.createDataFrame([(1, "a")], ["id", "name"])
        result = prefix_columns(df, "src_")
        assert result.columns == ["src_id", "src_name"]

    @pytest.mark.unit
    def test_exclude(self, spark_session: pyspark.sql.SparkSession) -> None:
        df = spark_session.createDataFrame([(1, "a", 10)], ["id", "name", "score"])
        result = prefix_columns(df, "t_", exclude=["id"])
        assert result.columns == ["id", "t_name", "t_score"]

    @pytest.mark.unit
    def test_exclude_all(self, spark_session: pyspark.sql.SparkSession) -> None:
        df = spark_session.createDataFrame([(1,)], ["id"])
        result = prefix_columns(df, "x_", exclude=["id"])
        assert result.columns == ["id"]

    @pytest.mark.unit
    def test_empty_dataframe(self, spark_session: pyspark.sql.SparkSession) -> None:
        schema = StructType([StructField("col", IntegerType())])
        df = spark_session.createDataFrame([], schema=schema)
        result = prefix_columns(df, "pre_")
        assert result.columns == ["pre_col"]
        assert result.count() == 0


class TestSuffixColumns:
    @pytest.mark.unit
    def test_basic_suffix(self, spark_session: pyspark.sql.SparkSession) -> None:
        df = spark_session.createDataFrame([(1, "a")], ["id", "name"])
        result = suffix_columns(df, "_raw")
        assert result.columns == ["id_raw", "name_raw"]

    @pytest.mark.unit
    def test_exclude(self, spark_session: pyspark.sql.SparkSession) -> None:
        df = spark_session.createDataFrame([(1, "a")], ["id", "name"])
        result = suffix_columns(df, "_v2", exclude=["id"])
        assert result.columns == ["id", "name_v2"]

    @pytest.mark.unit
    def test_no_op_when_all_excluded(self, spark_session: pyspark.sql.SparkSession) -> None:
        df = spark_session.createDataFrame([(1,)], ["x"])
        result = suffix_columns(df, "_y", exclude=["x"])
        assert result.columns == ["x"]

    @pytest.mark.unit
    def test_data_preserved(self, spark_session: pyspark.sql.SparkSession) -> None:
        df = spark_session.createDataFrame([(42,)], ["val"])
        result = suffix_columns(df, "_out")
        assert result.collect()[0]["val_out"] == 42


class TestFlattenStruct:
    @pytest.mark.unit
    def test_single_level(self, spark_session: pyspark.sql.SparkSession) -> None:
        schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField(
                    "info",
                    StructType(
                        [
                            StructField("name", StringType()),
                            StructField("age", IntegerType()),
                        ]
                    ),
                ),
            ]
        )
        df = spark_session.createDataFrame([(1, ("alice", 30))], schema=schema)
        result = flatten_struct(df)
        assert sorted(result.columns) == ["id", "info_age", "info_name"]
        row = result.collect()[0]
        assert row["info_name"] == "alice"
        assert row["info_age"] == 30

    @pytest.mark.unit
    def test_nested_structs(self, spark_session: pyspark.sql.SparkSession) -> None:
        schema = StructType(
            [
                StructField(
                    "root",
                    StructType(
                        [
                            StructField(
                                "mid",
                                StructType(
                                    [
                                        StructField("leaf", StringType()),
                                    ]
                                ),
                            ),
                        ]
                    ),
                ),
            ]
        )
        df = spark_session.createDataFrame([((("val",),),)], schema=schema)
        result = flatten_struct(df)
        assert result.columns == ["root_mid_leaf"]
        assert result.collect()[0][0] == "val"

    @pytest.mark.unit
    def test_custom_separator(self, spark_session: pyspark.sql.SparkSession) -> None:
        schema = StructType(
            [
                StructField(
                    "a",
                    StructType(
                        [
                            StructField("b", IntegerType()),
                        ]
                    ),
                ),
            ]
        )
        df = spark_session.createDataFrame([((1,),)], schema=schema)
        result = flatten_struct(df, separator="__")
        assert result.columns == ["a__b"]

    @pytest.mark.unit
    def test_no_structs(self, spark_session: pyspark.sql.SparkSession) -> None:
        df = spark_session.createDataFrame([(1, "x")], ["id", "val"])
        result = flatten_struct(df)
        assert result.columns == ["id", "val"]

    @pytest.mark.unit
    def test_empty_dataframe(self, spark_session: pyspark.sql.SparkSession) -> None:
        schema = StructType(
            [
                StructField(
                    "s",
                    StructType(
                        [
                            StructField("v", IntegerType()),
                        ]
                    ),
                ),
            ]
        )
        df = spark_session.createDataFrame([], schema=schema)
        result = flatten_struct(df)
        assert result.columns == ["s_v"]
        assert result.count() == 0


class TestSingleSpace:
    @pytest.mark.unit
    def test_multiple_spaces(self, spark_session: pyspark.sql.SparkSession) -> None:
        df = spark_session.createDataFrame([("hello   world",)], ["text"])
        result = single_space(df, "text")
        assert result.collect()[0]["text"] == "hello world"

    @pytest.mark.unit
    def test_tabs_and_mixed(self, spark_session: pyspark.sql.SparkSession) -> None:
        df = spark_session.createDataFrame([("a\t\t b",)], ["text"])
        result = single_space(df, "text")
        assert result.collect()[0]["text"] == "a b"

    @pytest.mark.unit
    def test_leading_trailing_whitespace(self, spark_session: pyspark.sql.SparkSession) -> None:
        df = spark_session.createDataFrame([("  hello  ",)], ["text"])
        result = single_space(df, "text")
        assert result.collect()[0]["text"] == "hello"

    @pytest.mark.unit
    def test_multiple_columns(self, spark_session: pyspark.sql.SparkSession) -> None:
        df = spark_session.createDataFrame([("a  b", "c   d")], ["x", "y"])
        result = single_space(df, "x", "y")
        row = result.collect()[0]
        assert row["x"] == "a b"
        assert row["y"] == "c d"

    @pytest.mark.unit
    def test_already_clean(self, spark_session: pyspark.sql.SparkSession) -> None:
        df = spark_session.createDataFrame([("no change",)], ["text"])
        result = single_space(df, "text")
        assert result.collect()[0]["text"] == "no change"


class TestTrimAll:
    @pytest.mark.unit
    def test_trims_string_columns(self, spark_session: pyspark.sql.SparkSession) -> None:
        df = spark_session.createDataFrame([("  a  ", "  b  ")], ["x", "y"])
        result = trim_all(df)
        row = result.collect()[0]
        assert row["x"] == "a"
        assert row["y"] == "b"

    @pytest.mark.unit
    def test_skips_non_string(self, spark_session: pyspark.sql.SparkSession) -> None:
        df = spark_session.createDataFrame([(1, " hello ")], ["num", "text"])
        result = trim_all(df)
        row = result.collect()[0]
        assert row["num"] == 1
        assert row["text"] == "hello"

    @pytest.mark.unit
    def test_no_string_columns(self, spark_session: pyspark.sql.SparkSession) -> None:
        df = spark_session.createDataFrame([(1, 2.0)], ["a", "b"])
        result = trim_all(df)
        row = result.collect()[0]
        assert row["a"] == 1
        assert row["b"] == 2.0

    @pytest.mark.unit
    def test_empty_dataframe(self, spark_session: pyspark.sql.SparkSession) -> None:
        schema = StructType([StructField("s", StringType())])
        df = spark_session.createDataFrame([], schema=schema)
        result = trim_all(df)
        assert result.count() == 0
