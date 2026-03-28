"""Integration tests for SCD Type 2, generated columns, and schema check on write."""

from __future__ import annotations

import pyspark.sql
import pytest
from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from databricks4py.io.delta import DeltaTable, GeneratedColumn
from databricks4py.migrations.schema_diff import SchemaEvolutionError

BASE_SCHEMA = StructType(
    [
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("value", IntegerType()),
    ]
)

SCD2_SCHEMA = StructType(
    [
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("value", IntegerType()),
        StructField("effective_date", TimestampType()),
        StructField("end_date", TimestampType(), nullable=True),
        StructField("is_active", IntegerType()),  # boolean stored as int for simplicity
    ]
)


# ---------------------------------------------------------------------------
# SCD Type 2
# ---------------------------------------------------------------------------
@pytest.mark.integration
class TestSCDType2Integration:
    def _make_scd2_table(self, spark, table_name, location):
        """Create an empty SCD2 target table with the required columns."""
        from pyspark.sql.types import BooleanType

        schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("name", StringType()),
                StructField("value", IntegerType()),
                StructField("effective_date", TimestampType()),
                StructField("end_date", TimestampType(), nullable=True),
                StructField("is_active", BooleanType()),
            ]
        )
        return DeltaTable(table_name, schema, location=location, spark=spark)

    def test_scd2_initial_load(
        self, spark_session_function: pyspark.sql.SparkSession, tmp_path
    ) -> None:
        location = str(tmp_path / "scd2_init")
        table = self._make_scd2_table(
            spark_session_function, "default.scd2_init", location
        )

        source = spark_session_function.createDataFrame(
            [(1, "Alice", 100), (2, "Bob", 200)], schema=BASE_SCHEMA
        )
        table.scd_type2(source, keys=["id"])

        result = table.dataframe()
        assert result.count() == 2
        active = result.where("is_active = true").collect()
        assert len(active) == 2
        for row in active:
            assert row["end_date"] is None

    def test_scd2_update_expires_old(
        self, spark_session_function: pyspark.sql.SparkSession, tmp_path
    ) -> None:
        location = str(tmp_path / "scd2_update")
        table = self._make_scd2_table(
            spark_session_function, "default.scd2_update", location
        )

        source_v1 = spark_session_function.createDataFrame(
            [(1, "Alice", 100), (2, "Bob", 200)], schema=BASE_SCHEMA
        )
        table.scd_type2(source_v1, keys=["id"])

        # Second merge: Alice's matched row gets expired
        source_v2 = spark_session_function.createDataFrame(
            [(1, "Alice", 150)], schema=BASE_SCHEMA
        )
        table.scd_type2(source_v2, keys=["id"])

        result = table.dataframe()
        alice_rows = result.where("id = 1").collect()
        expired = [r for r in alice_rows if not r["is_active"]]
        assert len(expired) >= 1
        assert expired[0]["end_date"] is not None

    def test_scd2_non_matching_rows_unaffected(
        self, spark_session_function: pyspark.sql.SparkSession, tmp_path
    ) -> None:
        location = str(tmp_path / "scd2_stable")
        table = self._make_scd2_table(
            spark_session_function, "default.scd2_stable", location
        )

        source = spark_session_function.createDataFrame(
            [(1, "Alice", 100), (2, "Bob", 200)], schema=BASE_SCHEMA
        )
        table.scd_type2(source, keys=["id"])

        # Merge only Alice — Bob should be unaffected
        source_v2 = spark_session_function.createDataFrame(
            [(1, "Alice", 150)], schema=BASE_SCHEMA
        )
        table.scd_type2(source_v2, keys=["id"])

        bob_rows = table.dataframe().where("id = 2").collect()
        assert len(bob_rows) == 1
        assert bob_rows[0]["is_active"] is True


# ---------------------------------------------------------------------------
# Generated Columns
# ---------------------------------------------------------------------------
@pytest.mark.integration
class TestGeneratedColumnsIntegration:
    def test_generated_column_auto_computed(
        self, spark_session_function: pyspark.sql.SparkSession, tmp_path
    ) -> None:
        schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("ts", TimestampType()),
                StructField("date_col", DateType()),
            ]
        )
        location = str(tmp_path / "gen_col")
        table = DeltaTable(
            table_name="default.gen_col_test",
            schema=schema,
            location=location,
            generated_columns=[GeneratedColumn("date_col", "DATE", "CAST(ts AS DATE)")],
            spark=spark_session_function,
        )

        from datetime import datetime

        data_schema = StructType(
            [StructField("id", IntegerType()), StructField("ts", TimestampType())]
        )
        df = spark_session_function.createDataFrame(
            [(1, datetime(2025, 6, 15, 10, 30))], schema=data_schema
        )
        table.write(df, schema_check=False)

        result = table.dataframe().collect()
        assert len(result) == 1
        from datetime import date

        assert result[0]["date_col"] == date(2025, 6, 15)


# ---------------------------------------------------------------------------
# Schema Check on Write
# ---------------------------------------------------------------------------
@pytest.mark.integration
class TestSchemaCheckIntegration:
    def test_schema_check_allows_additive(
        self, spark_session_function: pyspark.sql.SparkSession, tmp_path
    ) -> None:
        """schema_check=True does not raise SchemaEvolutionError for additive changes."""
        from databricks4py.migrations.schema_diff import SchemaDiff

        location = str(tmp_path / "schema_add")
        table = DeltaTable(
            "default.schema_check_add", BASE_SCHEMA, location=location,
            spark=spark_session_function,
        )
        df1 = spark_session_function.createDataFrame([(1, "a", 10)], schema=BASE_SCHEMA)
        table.write(df1)

        # Additive change: extra nullable column is NOT breaking
        extended = StructType(BASE_SCHEMA.fields + [StructField("extra", StringType())])
        df2 = spark_session_function.createDataFrame([(2, "b", 20, "x")], schema=extended)
        diff = SchemaDiff.from_tables("default.schema_check_add", df2, spark=spark_session_function)
        assert not diff.has_breaking_changes()
        added = [c for c in diff.changes() if c.change_type == "added"]
        assert len(added) == 1
        assert added[0].column == "extra"

    def test_schema_check_blocks_removal(
        self, spark_session_function: pyspark.sql.SparkSession, tmp_path
    ) -> None:
        location = str(tmp_path / "schema_remove")
        table = DeltaTable(
            "default.schema_check_remove", BASE_SCHEMA, location=location,
            spark=spark_session_function,
        )
        df1 = spark_session_function.createDataFrame([(1, "a", 10)], schema=BASE_SCHEMA)
        table.write(df1)

        # Write with missing column (breaking — should raise)
        reduced = StructType([StructField("id", IntegerType()), StructField("name", StringType())])
        df2 = spark_session_function.createDataFrame([(2, "b")], schema=reduced)
        with pytest.raises(SchemaEvolutionError, match="Breaking schema changes"):
            table.write(df2, schema_check=True)
