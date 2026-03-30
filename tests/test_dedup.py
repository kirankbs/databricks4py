"""Tests for Delta table deduplication utilities."""

import pyspark.sql
import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from databricks4py.io.dedup import (
    DedupResult,
    append_without_duplicates,
    drop_duplicates_pkey,
    kill_duplicates,
)

SCHEMA = StructType(
    [
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("value", IntegerType()),
    ]
)


def _create_table(spark: pyspark.sql.SparkSession, name: str, data: list[tuple]) -> str:
    df = spark.createDataFrame(data, schema=SCHEMA)
    df.write.format("delta").mode("overwrite").saveAsTable(name)
    return name


@pytest.mark.no_pyspark
class TestDedupResult:
    def test_frozen(self) -> None:
        result = DedupResult(rows_removed=5, rows_remaining=10)
        with pytest.raises(AttributeError, match="cannot assign"):
            result.rows_removed = 3  # type: ignore[misc]

    def test_fields(self) -> None:
        result = DedupResult(rows_removed=2, rows_remaining=8)
        assert result.rows_removed == 2
        assert result.rows_remaining == 8


@pytest.mark.no_pyspark
class TestSqlInjection:
    def test_kill_duplicates_rejects_unsafe_table(self) -> None:
        with pytest.raises(ValueError, match="Unsafe characters"):
            kill_duplicates("db.table; DROP TABLE--", ["col"])

    def test_kill_duplicates_rejects_unsafe_column(self) -> None:
        with pytest.raises(ValueError, match="Unsafe characters"):
            kill_duplicates("db.table", ["col; DROP TABLE"])

    def test_drop_duplicates_pkey_rejects_unsafe(self) -> None:
        with pytest.raises(ValueError, match="Unsafe characters"):
            drop_duplicates_pkey("db.table--evil", "id", ["col"])

    def test_append_without_duplicates_rejects_unsafe(self) -> None:
        with pytest.raises(ValueError, match="Unsafe characters"):
            append_without_duplicates("db.table/**/", None, ["id; --"])


@pytest.mark.integration
class TestKillDuplicates:
    def test_removes_all_duplicate_copies(
        self, spark_session_function: pyspark.sql.SparkSession
    ) -> None:
        table = _create_table(
            spark_session_function,
            "default.kill_dup_basic",
            [
                (1, "alice", 100),
                (2, "alice", 200),  # dup on name
                (3, "bob", 300),
                (4, "carol", 400),
                (5, "carol", 500),  # dup on name
            ],
        )
        result = kill_duplicates(table, ["name"], spark=spark_session_function)
        assert result.rows_removed == 4  # alice x2 + carol x2
        assert result.rows_remaining == 1  # only bob

        remaining = spark_session_function.read.table(table)
        names = [r["name"] for r in remaining.collect()]
        assert names == ["bob"]

    def test_no_duplicates(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        table = _create_table(
            spark_session_function,
            "default.kill_dup_none",
            [(1, "alice", 100), (2, "bob", 200), (3, "carol", 300)],
        )
        result = kill_duplicates(table, ["name"], spark=spark_session_function)
        assert result.rows_removed == 0
        assert result.rows_remaining == 3

    def test_empty_columns_raises(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        _create_table(
            spark_session_function,
            "default.kill_dup_err",
            [(1, "alice", 100)],
        )
        with pytest.raises(ValueError, match="duplication_columns must not be empty"):
            kill_duplicates("default.kill_dup_err", [], spark=spark_session_function)

    def test_multi_column_dedup(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        table = _create_table(
            spark_session_function,
            "default.kill_dup_multi",
            [
                (1, "alice", 100),
                (2, "alice", 100),  # dup on (name, value)
                (3, "alice", 200),  # different value
                (4, "bob", 100),
            ],
        )
        result = kill_duplicates(table, ["name", "value"], spark=spark_session_function)
        assert result.rows_removed == 2  # both alice/100 rows
        assert result.rows_remaining == 2  # alice/200 + bob/100


@pytest.mark.integration
class TestDropDuplicatesPkey:
    def test_keeps_lowest_pkey(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        table = _create_table(
            spark_session_function,
            "default.drop_dup_basic",
            [
                (1, "alice", 100),
                (5, "alice", 200),  # dup on name, higher id
                (2, "bob", 300),
            ],
        )
        result = drop_duplicates_pkey(table, "id", ["name"], spark=spark_session_function)
        assert result.rows_removed == 1
        assert result.rows_remaining == 2

        remaining = spark_session_function.read.table(table).orderBy("id").collect()
        assert remaining[0]["id"] == 1  # kept lower id for alice
        assert remaining[1]["id"] == 2  # bob untouched

    def test_no_duplicates(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        table = _create_table(
            spark_session_function,
            "default.drop_dup_none",
            [(1, "alice", 100), (2, "bob", 200)],
        )
        result = drop_duplicates_pkey(table, "id", ["name"], spark=spark_session_function)
        assert result.rows_removed == 0
        assert result.rows_remaining == 2

    def test_empty_pkey_raises(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        _create_table(
            spark_session_function,
            "default.drop_dup_err1",
            [(1, "alice", 100)],
        )
        with pytest.raises(ValueError, match="primary_key must not be empty"):
            drop_duplicates_pkey(
                "default.drop_dup_err1", "", ["name"], spark=spark_session_function
            )

    def test_empty_duplication_columns_raises(
        self, spark_session_function: pyspark.sql.SparkSession
    ) -> None:
        _create_table(
            spark_session_function,
            "default.drop_dup_err2",
            [(1, "alice", 100)],
        )
        with pytest.raises(ValueError, match="duplication_columns must not be empty"):
            drop_duplicates_pkey("default.drop_dup_err2", "id", [], spark=spark_session_function)


@pytest.mark.integration
class TestAppendWithoutDuplicates:
    def test_inserts_new_rows_only(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        table = _create_table(
            spark_session_function,
            "default.append_nodup_basic",
            [(1, "alice", 100), (2, "bob", 200)],
        )
        new_data = spark_session_function.createDataFrame(
            [(2, "bob", 999), (3, "carol", 300)],
            schema=SCHEMA,
        )
        inserted = append_without_duplicates(table, new_data, ["id"], spark=spark_session_function)
        assert inserted == 1  # only carol

        result = spark_session_function.read.table(table)
        assert result.count() == 3
        bob_row = result.where("id = 2").collect()[0]
        assert bob_row["value"] == 200  # unchanged

    def test_all_new(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        table = _create_table(
            spark_session_function,
            "default.append_nodup_allnew",
            [(1, "alice", 100)],
        )
        new_data = spark_session_function.createDataFrame(
            [(2, "bob", 200), (3, "carol", 300)],
            schema=SCHEMA,
        )
        inserted = append_without_duplicates(table, new_data, ["id"], spark=spark_session_function)
        assert inserted == 2

    def test_all_duplicates(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        table = _create_table(
            spark_session_function,
            "default.append_nodup_alldup",
            [(1, "alice", 100), (2, "bob", 200)],
        )
        new_data = spark_session_function.createDataFrame(
            [(1, "alice", 999), (2, "bob", 999)],
            schema=SCHEMA,
        )
        inserted = append_without_duplicates(table, new_data, ["id"], spark=spark_session_function)
        assert inserted == 0
        assert spark_session_function.read.table(table).count() == 2

    def test_empty_keys_raises(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        _create_table(
            spark_session_function,
            "default.append_nodup_err",
            [(1, "alice", 100)],
        )
        new_data = spark_session_function.createDataFrame([(2, "bob", 200)], schema=SCHEMA)
        with pytest.raises(ValueError, match="keys must not be empty"):
            append_without_duplicates(
                "default.append_nodup_err", new_data, [], spark=spark_session_function
            )

    def test_multi_key_dedup(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        table = _create_table(
            spark_session_function,
            "default.append_nodup_multi",
            [(1, "alice", 100)],
        )
        new_data = spark_session_function.createDataFrame(
            [
                (1, "alice", 999),  # dup on (id, name)
                (1, "bob", 200),  # different name, new
            ],
            schema=SCHEMA,
        )
        inserted = append_without_duplicates(
            table, new_data, ["id", "name"], spark=spark_session_function
        )
        assert inserted == 1
        assert spark_session_function.read.table(table).count() == 2
