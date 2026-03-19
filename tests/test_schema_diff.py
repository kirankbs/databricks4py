"""Tests for schema diff module."""

import pyspark.sql
import pytest
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from databricks4py.io.delta import DeltaTable
from databricks4py.migrations.schema_diff import (
    ColumnChange,
    SchemaDiff,
    SchemaEvolutionError,
)


@pytest.mark.no_pyspark
class TestColumnChange:
    def test_creation(self) -> None:
        change = ColumnChange(
            column="age",
            change_type="added",
            new_value="IntegerType()",
            severity="info",
        )
        assert change.column == "age"
        assert change.change_type == "added"
        assert change.new_value == "IntegerType()"
        assert change.old_value is None
        assert change.severity == "info"

    def test_frozen(self) -> None:
        change = ColumnChange(column="x", change_type="added")
        with pytest.raises(AttributeError, match="cannot assign"):
            change.column = "y"  # type: ignore[misc]


@pytest.mark.no_pyspark
class TestSchemaEvolutionError:
    def test_message(self) -> None:
        err = SchemaEvolutionError("breaking change detected")
        assert "breaking change detected" in str(err)


@pytest.mark.integration
class TestSchemaDiffIntegration:
    def test_no_changes(
        self,
        spark_session_function: pyspark.sql.SparkSession,
    ) -> None:
        schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("name", StringType()),
            ]
        )
        diff = SchemaDiff(current=schema, incoming=schema)
        assert diff.changes() == []
        assert not diff.has_breaking_changes()

    def test_column_added(
        self,
        spark_session_function: pyspark.sql.SparkSession,
    ) -> None:
        current = StructType([StructField("id", IntegerType())])
        incoming = StructType(
            [
                StructField("id", IntegerType()),
                StructField("name", StringType()),
            ]
        )
        diff = SchemaDiff(current=current, incoming=incoming)
        changes = diff.changes()
        assert len(changes) == 1
        assert changes[0].column == "name"
        assert changes[0].change_type == "added"
        assert changes[0].severity == "info"

    def test_column_removed(
        self,
        spark_session_function: pyspark.sql.SparkSession,
    ) -> None:
        current = StructType(
            [
                StructField("id", IntegerType()),
                StructField("name", StringType()),
            ]
        )
        incoming = StructType([StructField("id", IntegerType())])
        diff = SchemaDiff(current=current, incoming=incoming)
        changes = diff.changes()
        assert len(changes) == 1
        assert changes[0].column == "name"
        assert changes[0].change_type == "removed"
        assert changes[0].severity == "breaking"
        assert diff.has_breaking_changes()

    def test_type_changed(
        self,
        spark_session_function: pyspark.sql.SparkSession,
    ) -> None:
        current = StructType([StructField("id", IntegerType())])
        incoming = StructType([StructField("id", LongType())])
        diff = SchemaDiff(current=current, incoming=incoming)
        changes = diff.changes()
        assert len(changes) == 1
        assert changes[0].column == "id"
        assert changes[0].change_type == "type_changed"
        assert changes[0].severity == "breaking"
        assert changes[0].old_value == "IntegerType()"
        assert changes[0].new_value == "LongType()"

    def test_nullable_changed(
        self,
        spark_session_function: pyspark.sql.SparkSession,
    ) -> None:
        current = StructType([StructField("id", IntegerType(), nullable=True)])
        incoming = StructType([StructField("id", IntegerType(), nullable=False)])
        diff = SchemaDiff(current=current, incoming=incoming)
        changes = diff.changes()
        assert len(changes) == 1
        assert changes[0].column == "id"
        assert changes[0].change_type == "nullable_changed"
        assert changes[0].severity == "warning"

    def test_summary_returns_string(
        self,
        spark_session_function: pyspark.sql.SparkSession,
    ) -> None:
        current = StructType([StructField("id", IntegerType())])
        incoming = StructType(
            [
                StructField("id", IntegerType()),
                StructField("name", StringType()),
            ]
        )
        diff = SchemaDiff(current=current, incoming=incoming)
        summary = diff.summary()
        assert isinstance(summary, str)
        assert "name" in summary
        assert "added" in summary

    def test_from_tables(
        self,
        spark_session_function: pyspark.sql.SparkSession,
        tmp_path,
    ) -> None:
        schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("name", StringType()),
            ]
        )
        location = str(tmp_path / "diff_table")
        DeltaTable(
            table_name="default.diff_table",
            schema=schema,
            location=location,
            spark=spark_session_function,
        )

        incoming_schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("name", StringType()),
                StructField("age", IntegerType()),
            ]
        )
        incoming_df = spark_session_function.createDataFrame([], incoming_schema)

        diff = SchemaDiff.from_tables(
            "default.diff_table",
            incoming_df,
            spark=spark_session_function,
        )
        changes = diff.changes()
        assert len(changes) == 1
        assert changes[0].column == "age"
        assert changes[0].change_type == "added"
        assert changes[0].severity == "info"
