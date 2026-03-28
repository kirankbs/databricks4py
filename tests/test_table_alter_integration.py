"""Integration tests for TableAlter DDL operations on real Delta tables."""

from __future__ import annotations

import pyspark.sql
import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from databricks4py.io.delta import DeltaTable
from databricks4py.migrations.alter import TableAlter

SCHEMA = StructType(
    [
        StructField("id", IntegerType()),
        StructField("name", StringType()),
    ]
)


def _column_names(spark: pyspark.sql.SparkSession, table: str) -> set[str]:
    rows = spark.sql(f"DESCRIBE TABLE {table}").collect()
    return {
        row["col_name"] for row in rows if row["col_name"] and not row["col_name"].startswith("#")
    }


@pytest.mark.integration
class TestTableAlterIntegration:
    def test_add_column(self, spark_session_function: pyspark.sql.SparkSession, tmp_path) -> None:
        loc = str(tmp_path / "add_col")
        DeltaTable("default.alter_add", SCHEMA, location=loc, spark=spark_session_function)

        TableAlter("default.alter_add", spark=spark_session_function).add_column(
            "region", "STRING"
        ).apply()

        cols = _column_names(spark_session_function, "default.alter_add")
        assert "region" in cols

    def test_add_column_with_comment(
        self, spark_session_function: pyspark.sql.SparkSession, tmp_path
    ) -> None:
        loc = str(tmp_path / "add_comment")
        DeltaTable("default.alter_comment", SCHEMA, location=loc, spark=spark_session_function)

        TableAlter("default.alter_comment", spark=spark_session_function).add_column(
            "region", "STRING", comment="ISO-3166 region"
        ).apply()

        rows = spark_session_function.sql("DESCRIBE TABLE default.alter_comment").collect()
        region_row = next(r for r in rows if r["col_name"] == "region")
        assert "ISO-3166" in (region_row["comment"] or "")

    def test_set_property(self, spark_session_function: pyspark.sql.SparkSession, tmp_path) -> None:
        loc = str(tmp_path / "set_prop")
        DeltaTable("default.alter_prop", SCHEMA, location=loc, spark=spark_session_function)

        TableAlter("default.alter_prop", spark=spark_session_function).set_property(
            "delta.enableChangeDataFeed", "true"
        ).apply()

        props = spark_session_function.sql("SHOW TBLPROPERTIES default.alter_prop").collect()
        prop_dict = {r["key"]: r["value"] for r in props}
        assert prop_dict.get("delta.enableChangeDataFeed") == "true"

    def _enable_column_mapping(self, spark, table_name):
        """Enable column mapping — upgrade protocol first, then set mode."""
        spark.sql(
            f"ALTER TABLE {table_name} SET TBLPROPERTIES ("
            "'delta.minReaderVersion' = '2', "
            "'delta.minWriterVersion' = '5')"
        )
        spark.sql(
            f"ALTER TABLE {table_name} SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')"
        )

    def test_rename_column(
        self, spark_session_function: pyspark.sql.SparkSession, tmp_path
    ) -> None:
        loc = str(tmp_path / "rename")
        DeltaTable("default.alter_rename", SCHEMA, location=loc, spark=spark_session_function)

        self._enable_column_mapping(spark_session_function, "default.alter_rename")

        TableAlter("default.alter_rename", spark=spark_session_function).rename_column(
            "name", "full_name"
        ).apply()

        cols = _column_names(spark_session_function, "default.alter_rename")
        assert "full_name" in cols
        assert "name" not in cols

    def test_drop_column(self, spark_session_function: pyspark.sql.SparkSession, tmp_path) -> None:
        loc = str(tmp_path / "drop")
        DeltaTable("default.alter_drop", SCHEMA, location=loc, spark=spark_session_function)

        self._enable_column_mapping(spark_session_function, "default.alter_drop")

        TableAlter("default.alter_drop", spark=spark_session_function).drop_column("name").apply()

        cols = _column_names(spark_session_function, "default.alter_drop")
        assert "name" not in cols
        assert "id" in cols

    def test_chained_add_and_property(
        self, spark_session_function: pyspark.sql.SparkSession, tmp_path
    ) -> None:
        loc = str(tmp_path / "chained")
        DeltaTable("default.alter_chain", SCHEMA, location=loc, spark=spark_session_function)

        (
            TableAlter("default.alter_chain", spark=spark_session_function)
            .add_column("score", "INT")
            .set_property("delta.enableChangeDataFeed", "true")
            .apply()
        )

        cols = _column_names(spark_session_function, "default.alter_chain")
        assert "score" in cols

        props = spark_session_function.sql("SHOW TBLPROPERTIES default.alter_chain").collect()
        prop_dict = {r["key"]: r["value"] for r in props}
        assert prop_dict.get("delta.enableChangeDataFeed") == "true"

    def test_apply_clears_and_reuses(
        self, spark_session_function: pyspark.sql.SparkSession, tmp_path
    ) -> None:
        loc = str(tmp_path / "reuse")
        DeltaTable("default.alter_reuse", SCHEMA, location=loc, spark=spark_session_function)

        alter = TableAlter("default.alter_reuse", spark=spark_session_function)
        alter.add_column("col_a", "STRING").apply()
        alter.add_column("col_b", "INT").apply()

        cols = _column_names(spark_session_function, "default.alter_reuse")
        assert "col_a" in cols
        assert "col_b" in cols
