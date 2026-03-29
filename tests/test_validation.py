"""Tests for DataFrame validation module."""

import pyspark.sql
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from databricks4py.validation import (
    DataFrameMissingColumnError,
    DataFrameProhibitedColumnError,
    DataFrameSchemaError,
    validate_absence_of_columns,
    validate_input,
    validate_output,
    validate_presence_of_columns,
    validate_schema,
)

SCHEMA = StructType(
    [
        StructField("id", IntegerType(), nullable=False),
        StructField("name", StringType(), nullable=True),
        StructField("value", IntegerType(), nullable=True),
    ]
)


@pytest.mark.no_pyspark
class TestExceptions:
    def test_missing_column_error_fields(self) -> None:
        err = DataFrameMissingColumnError(["a", "b"])
        assert err.missing_columns == ["a", "b"]
        assert "a" in str(err)
        assert "b" in str(err)

    def test_prohibited_column_error_fields(self) -> None:
        err = DataFrameProhibitedColumnError(["secret"])
        assert err.prohibited_columns == ["secret"]
        assert "secret" in str(err)

    def test_schema_error_fields(self) -> None:
        err = DataFrameSchemaError("mismatch", missing_fields=["x"])
        assert err.missing_fields == ["x"]
        assert "mismatch" in str(err)

    def test_all_inherit_from_value_error(self) -> None:
        assert issubclass(DataFrameMissingColumnError, ValueError)
        assert issubclass(DataFrameProhibitedColumnError, ValueError)
        assert issubclass(DataFrameSchemaError, ValueError)


@pytest.mark.unit
class TestValidatePresenceOfColumns:
    def test_all_present(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        df = spark_session_function.createDataFrame([(1, "a", 10)], schema=SCHEMA)
        validate_presence_of_columns(df, ["id", "name"])  # no exception

    def test_missing_raises(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        df = spark_session_function.createDataFrame([(1, "a", 10)], schema=SCHEMA)
        with pytest.raises(DataFrameMissingColumnError, match="Missing columns"):
            validate_presence_of_columns(df, ["id", "nonexistent"])

    def test_missing_columns_attribute(
        self, spark_session_function: pyspark.sql.SparkSession
    ) -> None:
        df = spark_session_function.createDataFrame([(1, "a", 10)], schema=SCHEMA)
        with pytest.raises(DataFrameMissingColumnError) as exc_info:
            validate_presence_of_columns(df, ["x", "y", "id"])
        assert set(exc_info.value.missing_columns) == {"x", "y"}

    def test_empty_required(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        df = spark_session_function.createDataFrame([(1, "a", 10)], schema=SCHEMA)
        validate_presence_of_columns(df, [])  # no exception


@pytest.mark.unit
class TestValidateAbsenceOfColumns:
    def test_none_present(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        df = spark_session_function.createDataFrame([(1, "a", 10)], schema=SCHEMA)
        validate_absence_of_columns(df, ["secret", "password"])  # no exception

    def test_prohibited_present_raises(
        self, spark_session_function: pyspark.sql.SparkSession
    ) -> None:
        df = spark_session_function.createDataFrame([(1, "a", 10)], schema=SCHEMA)
        with pytest.raises(DataFrameProhibitedColumnError, match="Prohibited columns"):
            validate_absence_of_columns(df, ["name", "nonexistent"])

    def test_prohibited_columns_attribute(
        self, spark_session_function: pyspark.sql.SparkSession
    ) -> None:
        df = spark_session_function.createDataFrame([(1, "a", 10)], schema=SCHEMA)
        with pytest.raises(DataFrameProhibitedColumnError) as exc_info:
            validate_absence_of_columns(df, ["id", "name", "missing"])
        assert set(exc_info.value.prohibited_columns) == {"id", "name"}


@pytest.mark.unit
class TestValidateSchema:
    def test_exact_match(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        df = spark_session_function.createDataFrame([(1, "a", 10)], schema=SCHEMA)
        validate_schema(df, SCHEMA)  # no exception

    def test_subset_match(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        df = spark_session_function.createDataFrame([(1, "a", 10)], schema=SCHEMA)
        subset = StructType([StructField("id", IntegerType(), nullable=False)])
        validate_schema(df, subset)  # extra cols OK

    def test_missing_field_raises(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        df = spark_session_function.createDataFrame([(1, "a", 10)], schema=SCHEMA)
        expected = StructType([StructField("missing_col", IntegerType())])
        with pytest.raises(DataFrameSchemaError, match="Missing fields"):
            validate_schema(df, expected)

    def test_type_mismatch_raises(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        df = spark_session_function.createDataFrame([(1, "a", 10)], schema=SCHEMA)
        expected = StructType([StructField("id", StringType())])
        with pytest.raises(DataFrameSchemaError, match="Type mismatches"):
            validate_schema(df, expected)

    def test_nullable_mismatch_raises(
        self, spark_session_function: pyspark.sql.SparkSession
    ) -> None:
        df = spark_session_function.createDataFrame([(1, "a", 10)], schema=SCHEMA)
        # SCHEMA has id as nullable=False, so nullable=True should mismatch
        expected = StructType([StructField("id", IntegerType(), nullable=True)])
        with pytest.raises(DataFrameSchemaError, match="nullable"):
            validate_schema(df, expected)

    def test_ignore_nullable(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        df = spark_session_function.createDataFrame([(1, "a", 10)], schema=SCHEMA)
        expected = StructType([StructField("id", IntegerType(), nullable=True)])
        validate_schema(df, expected, ignore_nullable=True)  # no exception

    def test_missing_fields_attribute(
        self, spark_session_function: pyspark.sql.SparkSession
    ) -> None:
        df = spark_session_function.createDataFrame([(1, "a", 10)], schema=SCHEMA)
        expected = StructType(
            [
                StructField("x", IntegerType()),
                StructField("y", StringType()),
            ]
        )
        with pytest.raises(DataFrameSchemaError) as exc_info:
            validate_schema(df, expected)
        assert set(exc_info.value.missing_fields) == {"x", "y"}


@pytest.mark.unit
class TestValidateInput:
    def test_passes_valid_input(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        @validate_input(required_columns=["id", "name"])
        def process(df):
            return df.select("id", "name")

        df = spark_session_function.createDataFrame([(1, "a", 10)], schema=SCHEMA)
        result = process(df)
        assert result.columns == ["id", "name"]

    def test_rejects_missing_columns(
        self, spark_session_function: pyspark.sql.SparkSession
    ) -> None:
        @validate_input(required_columns=["id", "missing"])
        def process(df):
            return df

        df = spark_session_function.createDataFrame([(1, "a", 10)], schema=SCHEMA)
        with pytest.raises(DataFrameMissingColumnError, match="missing"):
            process(df)

    def test_rejects_prohibited_columns(
        self, spark_session_function: pyspark.sql.SparkSession
    ) -> None:
        @validate_input(prohibited_columns=["name"])
        def process(df):
            return df

        df = spark_session_function.createDataFrame([(1, "a", 10)], schema=SCHEMA)
        with pytest.raises(DataFrameProhibitedColumnError, match="name"):
            process(df)

    def test_schema_validation(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        expected = StructType([StructField("id", StringType())])

        @validate_input(schema=expected)
        def process(df):
            return df

        df = spark_session_function.createDataFrame([(1, "a", 10)], schema=SCHEMA)
        with pytest.raises(DataFrameSchemaError, match="Type mismatches"):
            process(df)

    def test_keyword_df_arg(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        @validate_input(required_columns=["id"], df_arg="data")
        def process(config, data=None):
            return data

        df = spark_session_function.createDataFrame([(1, "a", 10)], schema=SCHEMA)
        result = process("config", data=df)
        assert result is not None

    def test_keyword_df_arg_missing_raises(
        self, spark_session_function: pyspark.sql.SparkSession
    ) -> None:
        @validate_input(required_columns=["id"], df_arg="data")
        def process(config):
            return config

        with pytest.raises(TypeError, match="not found"):
            process("config")

    def test_positional_df_arg(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        @validate_input(required_columns=["id"], df_arg=1)
        def process(config, df):
            return df

        df = spark_session_function.createDataFrame([(1, "a", 10)], schema=SCHEMA)
        result = process("config", df)
        assert result.count() == 1


@pytest.mark.unit
class TestValidateOutput:
    def test_passes_valid_output(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        @validate_output(required_columns=["id", "score"])
        def enrich(df):
            return df.withColumn("score", F.lit(0))

        df = spark_session_function.createDataFrame([(1, "a", 10)], schema=SCHEMA)
        result = enrich(df)
        assert "score" in result.columns

    def test_rejects_missing_output_columns(
        self, spark_session_function: pyspark.sql.SparkSession
    ) -> None:
        @validate_output(required_columns=["id", "missing_col"])
        def process(df):
            return df

        df = spark_session_function.createDataFrame([(1, "a", 10)], schema=SCHEMA)
        with pytest.raises(DataFrameMissingColumnError, match="missing_col"):
            process(df)

    def test_rejects_non_dataframe_return(
        self, spark_session_function: pyspark.sql.SparkSession
    ) -> None:
        @validate_output(required_columns=["id"])
        def process(df):
            return "not a dataframe"

        df = spark_session_function.createDataFrame([(1, "a", 10)], schema=SCHEMA)
        with pytest.raises(TypeError, match="expects a DataFrame return"):
            process(df)

    def test_prohibited_output_columns(
        self, spark_session_function: pyspark.sql.SparkSession
    ) -> None:
        @validate_output(prohibited_columns=["password"])
        def process(df):
            return df.withColumn("password", F.lit("secret"))

        df = spark_session_function.createDataFrame([(1, "a", 10)], schema=SCHEMA)
        with pytest.raises(DataFrameProhibitedColumnError, match="password"):
            process(df)
