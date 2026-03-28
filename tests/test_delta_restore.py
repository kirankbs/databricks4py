"""Tests for DeltaTable history/restore helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest


def _mock_delta_table(table_name: str = "catalog.schema.tbl"):
    """Return a DeltaTable with all Spark interactions mocked out."""
    from pyspark.sql.types import StringType, StructField, StructType

    from databricks4py.io.delta import DeltaTable

    schema = StructType([StructField("id", StringType())])
    mock_spark = MagicMock()

    with (
        patch("databricks4py.io.delta.active_fallback", return_value=mock_spark),
        patch.object(DeltaTable, "_table_exists", return_value=True),
        patch.object(DeltaTable, "_create_table"),
    ):
        table = DeltaTable(table_name=table_name, schema=schema)

    # Reassign spark mock so method calls on the returned object are tracked
    table._spark = mock_spark
    return table


@pytest.mark.no_pyspark
class TestDeltaTableNameValidation:
    def test_safe_names_accepted(self) -> None:
        from databricks4py.io.delta import DeltaTable

        # These must not raise
        for name in ["catalog.schema.table", "db.tbl", "simple", "`quoted`.schema"]:
            assert DeltaTable._validated_table_name(name) == name

    def test_semicolon_rejected(self) -> None:
        from databricks4py.io.delta import DeltaTable

        with pytest.raises(ValueError, match="Unsafe"):
            DeltaTable._validated_table_name("tbl; DROP TABLE users")

    def test_sql_comment_rejected(self) -> None:
        from databricks4py.io.delta import DeltaTable

        with pytest.raises(ValueError, match="Unsafe"):
            DeltaTable._validated_table_name("tbl -- comment")

    def test_block_comment_rejected(self) -> None:
        from databricks4py.io.delta import DeltaTable

        with pytest.raises(ValueError, match="Unsafe"):
            DeltaTable._validated_table_name("tbl /* DROP */")


@pytest.mark.no_pyspark
class TestDeltaTableHistory:
    def test_history_calls_describe_history(self) -> None:
        table = _mock_delta_table("db.schema.orders")
        table.history()
        table._spark.sql.assert_called_once_with("DESCRIBE HISTORY db.schema.orders LIMIT 20")

    def test_history_custom_limit(self) -> None:
        table = _mock_delta_table("db.schema.orders")
        table.history(limit=5)
        table._spark.sql.assert_called_once_with("DESCRIBE HISTORY db.schema.orders LIMIT 5")

    def test_history_returns_dataframe(self) -> None:
        table = _mock_delta_table()
        mock_df = MagicMock()
        table._spark.sql.return_value = mock_df
        result = table.history()
        assert result is mock_df


@pytest.mark.no_pyspark
class TestDeltaTableRestore:
    def test_restore_calls_correct_sql(self) -> None:
        table = _mock_delta_table("catalog.schema.events")
        table.restore(version=42)
        table._spark.sql.assert_called_once_with(
            "RESTORE TABLE catalog.schema.events TO VERSION AS OF 42"
        )

    def test_restore_returns_dataframe(self) -> None:
        table = _mock_delta_table()
        mock_df = MagicMock()
        table._spark.sql.return_value = mock_df
        result = table.restore(version=0)
        assert result is mock_df

    def test_restore_version_zero(self) -> None:
        table = _mock_delta_table("tbl")
        table.restore(version=0)
        table._spark.sql.assert_called_once_with("RESTORE TABLE tbl TO VERSION AS OF 0")


@pytest.mark.no_pyspark
class TestDeltaTableRestoreToTimestamp:
    def test_formats_timestamp_correctly(self) -> None:
        table = _mock_delta_table("catalog.schema.events")
        ts = datetime(2024, 6, 15, 12, 30, 0, tzinfo=timezone.utc)
        table.restore_to_timestamp(ts)
        table._spark.sql.assert_called_once_with(
            "RESTORE TABLE catalog.schema.events TO TIMESTAMP AS OF '2024-06-15 12:30:00'"
        )

    def test_returns_dataframe(self) -> None:
        table = _mock_delta_table()
        mock_df = MagicMock()
        table._spark.sql.return_value = mock_df
        ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
        result = table.restore_to_timestamp(ts)
        assert result is mock_df

    def test_midnight_timestamp(self) -> None:
        table = _mock_delta_table("my.table")
        ts = datetime(2023, 12, 31, 0, 0, 0)
        table.restore_to_timestamp(ts)
        sql = table._spark.sql.call_args[0][0]
        assert "'2023-12-31 00:00:00'" in sql

    def test_sub_second_truncated(self) -> None:
        """Microseconds are stripped — strftime only goes to seconds."""
        table = _mock_delta_table("t")
        ts = datetime(2024, 3, 1, 10, 5, 30, 123456, tzinfo=timezone.utc)
        table.restore_to_timestamp(ts)
        sql = table._spark.sql.call_args[0][0]
        assert "'2024-03-01 10:05:30'" in sql
        assert "123456" not in sql
