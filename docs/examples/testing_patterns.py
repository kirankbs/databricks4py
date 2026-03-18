"""Test patterns for databricks4py applications.

Runnable pytest tests demonstrating DataFrameBuilder, TempDeltaTable,
assert_frame_equal, and the testing fixtures. Run with:

    pytest docs/examples/testing_patterns.py -v -m integration --timeout=120
"""

# ruff: noqa: F811

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from databricks4py.quality import InRange, NotNull, QualityError, QualityGate
from databricks4py.testing import (
    DataFrameBuilder,
    TempDeltaTable,
    assert_frame_equal,
    assert_schema_equal,
)

# Re-export fixtures so pytest discovers them in this file
from databricks4py.testing.fixtures import (  # noqa: F401
    clear_env,
    df_builder,
    spark_session,
    spark_session_function,
    temp_delta,
)

pytestmark = pytest.mark.integration


class TestDataFrameBuilder:
    """Building test DataFrames without boilerplate."""

    def test_build_from_columns_and_rows(self, spark_session: SparkSession):
        df = (
            DataFrameBuilder(spark_session)
            .with_columns({"user_id": "int", "email": "string", "score": "double"})
            .with_rows(
                (1, "alice@example.com", 92.5),
                (2, "bob@example.com", 87.3),
                (3, "carol@example.com", 95.1),
            )
            .build()
        )
        assert df.count() == 3
        assert df.schema["score"].dataType == DoubleType()

    def test_inject_nulls_for_quality_testing(self, spark_session: SparkSession):
        """Deterministic null injection for testing null-handling logic."""
        df = (
            DataFrameBuilder(spark_session)
            .with_columns({"id": "int", "value": "string"})
            .with_rows((1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"))
            .with_nulls("value", frequency=0.4, seed=42)
            .build()
        )
        null_count = df.where(F.col("value").isNull()).count()
        assert null_count > 0, "Expected some nulls with 40% frequency"


class TestTempDeltaTable:
    """Temporary Delta tables that clean up after themselves."""

    def test_context_manager_lifecycle(self, spark_session_function: SparkSession):
        with TempDeltaTable(
            spark_session_function,
            schema={"order_id": "int", "amount": "double"},
            data=[(1, 99.99), (2, 149.50)],
        ) as table:
            assert table.dataframe().count() == 2

            # Upsert: update order 1, insert order 3
            updates = spark_session_function.createDataFrame(
                [(1, 109.99), (3, 75.00)],
                schema=table.dataframe().schema,
            )
            result = table.upsert(updates, keys=["order_id"])
            assert result.rows_updated == 1
            assert result.rows_inserted == 1

    def test_schema_assertion(self, spark_session_function: SparkSession):
        expected_schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("name", StringType()),
            ]
        )
        with TempDeltaTable(
            spark_session_function,
            schema={"id": "int", "name": "string"},
            data=[(1, "warehouse_a")],
        ) as table:
            assert_schema_equal(table.dataframe().schema, expected_schema)


class TestAssertFrameEqual:
    """DataFrame equality assertions with clear diffs."""

    def test_order_independent_comparison(self, spark_session: SparkSession):
        actual = (
            DataFrameBuilder(spark_session)
            .with_columns({"id": "int", "val": "string"})
            .with_rows((2, "b"), (1, "a"))
            .build()
        )
        expected = (
            DataFrameBuilder(spark_session)
            .with_columns({"id": "int", "val": "string"})
            .with_rows((1, "a"), (2, "b"))
            .build()
        )
        # Order doesn't matter by default
        assert_frame_equal(actual, expected)


class TestQualityGateIntegration:
    """Quality gates with test data."""

    def test_gate_rejects_bad_data(self, spark_session: SparkSession):
        df = (
            DataFrameBuilder(spark_session)
            .with_columns({"id": "int", "price": "double"})
            .with_rows((1, 10.0), (2, -5.0), (3, 0.0))
            .build()
        )
        gate = QualityGate(
            InRange("price", min_val=0.01),
            on_fail="raise",
        )
        with pytest.raises(QualityError, match="Quality check failed"):
            gate.enforce(df)

    def test_gate_passes_clean_data(self, spark_session: SparkSession):
        df = (
            DataFrameBuilder(spark_session)
            .with_columns({"id": "int", "email": "string"})
            .with_rows((1, "alice@example.com"), (2, "bob@example.com"))
            .build()
        )
        gate = QualityGate(
            NotNull("id", "email"),
            on_fail="raise",
        )
        # enforce() returns the original DataFrame when all checks pass
        result = gate.enforce(df)
        assert_frame_equal(result, df)
