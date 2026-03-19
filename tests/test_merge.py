"""Tests for MergeBuilder and MergeResult."""

from __future__ import annotations

import pyspark.sql
import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from databricks4py.io.merge import MergeBuilder, MergeResult

SCHEMA = StructType(
    [
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("value", IntegerType()),
    ]
)


@pytest.mark.no_pyspark
class TestMergeResult:
    def test_creation(self) -> None:
        result = MergeResult(rows_inserted=10, rows_updated=5, rows_deleted=2)
        assert result.rows_inserted == 10
        assert result.rows_updated == 5
        assert result.rows_deleted == 2

    def test_frozen(self) -> None:
        result = MergeResult(rows_inserted=1, rows_updated=2, rows_deleted=3)
        with pytest.raises(AttributeError):
            result.rows_inserted = 99  # type: ignore[misc]


@pytest.mark.integration
class TestMergeBuilderIntegration:
    def test_basic_merge(
        self,
        spark_session_function: pyspark.sql.SparkSession,
        tmp_path,
    ) -> None:
        spark = spark_session_function
        table_name = "default.test_merge_basic"
        location = str(tmp_path / "merge_basic")

        # Create target table with initial data
        target_df = spark.createDataFrame(
            [{"id": 1, "name": "alice", "value": 10}, {"id": 2, "name": "bob", "value": 20}],
            schema=SCHEMA,
        )
        target_df.write.format("delta").option("path", location).saveAsTable(table_name)

        # Source has an update (id=1) and an insert (id=3)
        source_df = spark.createDataFrame(
            [
                {"id": 1, "name": "alice_updated", "value": 11},
                {"id": 3, "name": "charlie", "value": 30},
            ],
            schema=SCHEMA,
        )

        result = (
            MergeBuilder(table_name, source_df, spark)
            .on("id")
            .when_matched_update()
            .when_not_matched_insert()
            .execute()
        )

        assert result.rows_updated == 1
        assert result.rows_inserted == 1
        assert result.rows_deleted == 0

        # Verify final state
        final = spark.read.format("delta").load(location)
        assert final.count() == 3
        updated = final.filter("id = 1").collect()[0]
        assert updated["name"] == "alice_updated"

    def test_merge_with_delete(
        self,
        spark_session_function: pyspark.sql.SparkSession,
        tmp_path,
    ) -> None:
        spark = spark_session_function
        table_name = "default.test_merge_delete"
        location = str(tmp_path / "merge_delete")

        target_df = spark.createDataFrame(
            [
                {"id": 1, "name": "alice", "value": 10},
                {"id": 2, "name": "bob", "value": 20},
                {"id": 3, "name": "charlie", "value": 30},
            ],
            schema=SCHEMA,
        )
        target_df.write.format("delta").option("path", location).saveAsTable(table_name)

        # Source only has id=1, so id=2 and id=3 should be deleted
        source_df = spark.createDataFrame(
            [{"id": 1, "name": "alice_v2", "value": 11}],
            schema=SCHEMA,
        )

        result = (
            MergeBuilder(table_name, source_df, spark)
            .on("id")
            .when_matched_update()
            .when_not_matched_by_source_delete()
            .execute()
        )

        assert result.rows_updated == 1
        assert result.rows_deleted == 2
        assert result.rows_inserted == 0

        final = spark.read.format("delta").load(location)
        assert final.count() == 1
        assert final.collect()[0]["name"] == "alice_v2"

    def test_upsert_specific_columns(
        self,
        spark_session_function: pyspark.sql.SparkSession,
        tmp_path,
    ) -> None:
        spark = spark_session_function
        table_name = "default.test_merge_cols"
        location = str(tmp_path / "merge_cols")

        target_df = spark.createDataFrame(
            [{"id": 1, "name": "alice", "value": 10}],
            schema=SCHEMA,
        )
        target_df.write.format("delta").option("path", location).saveAsTable(table_name)

        # Source updates name but we only update "value" column
        source_df = spark.createDataFrame(
            [{"id": 1, "name": "alice_changed", "value": 99}],
            schema=SCHEMA,
        )

        result = (
            MergeBuilder(table_name, source_df, spark)
            .on("id")
            .when_matched_update(columns=["value"])
            .execute()
        )

        assert result.rows_updated == 1

        final = spark.read.format("delta").load(location)
        row = final.collect()[0]
        # name should remain unchanged since we only updated "value"
        assert row["name"] == "alice"
        assert row["value"] == 99
