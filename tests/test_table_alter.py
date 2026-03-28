"""Tests for TableAlter DDL builder."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from databricks4py.migrations.alter import TableAlter


def _make_alter(table="catalog.schema.events"):
    mock_spark = MagicMock()
    with patch("databricks4py.migrations.alter.active_fallback", return_value=mock_spark):
        alter = TableAlter(table)
    return alter, mock_spark


@pytest.mark.no_pyspark
class TestTableAlterQueueing:
    def test_no_ops_on_init(self) -> None:
        alter, _ = _make_alter()
        assert alter._ops == []

    def test_add_column_queued(self) -> None:
        alter, _ = _make_alter()
        alter.add_column("region", "STRING")
        assert len(alter._ops) == 1
        assert "region" in alter._ops[0]
        assert "STRING" in alter._ops[0]

    def test_add_column_with_options(self) -> None:
        alter, _ = _make_alter()
        alter.add_column("score", "DOUBLE", nullable=False, comment="ml score", after="id")
        op = alter._ops[0]
        assert "score DOUBLE NOT NULL" in op
        assert "COMMENT 'ml score'" in op
        assert "AFTER id" in op

    def test_add_column_nullable_by_default(self) -> None:
        alter, _ = _make_alter()
        alter.add_column("col", "INT")
        assert "NOT NULL" not in alter._ops[0]

    def test_rename_column_queued(self) -> None:
        alter, _ = _make_alter()
        alter.rename_column("old_name", "new_name")
        assert "RENAME COLUMN old_name TO new_name" in alter._ops[0]

    def test_drop_column_queued(self) -> None:
        alter, _ = _make_alter()
        alter.drop_column("legacy_col")
        assert "DROP COLUMN legacy_col" in alter._ops[0]

    def test_set_property_queued(self) -> None:
        alter, _ = _make_alter()
        alter.set_property("delta.enableChangeDataFeed", "true")
        assert "SET TBLPROPERTIES" in alter._ops[0]
        assert "delta.enableChangeDataFeed" in alter._ops[0]
        assert "true" in alter._ops[0]

    def test_chaining_returns_self(self) -> None:
        alter, _ = _make_alter()
        returned = alter.add_column("a", "STRING").set_property("k", "v")
        assert returned is alter
        assert len(alter._ops) == 2


@pytest.mark.no_pyspark
class TestTableAlterApply:
    def test_apply_executes_all_ops(self) -> None:
        alter, mock_spark = _make_alter("mydb.mytable")
        alter.add_column("col1", "STRING")
        alter.set_property("delta.enableChangeDataFeed", "true")

        alter.apply()

        assert mock_spark.sql.call_count == 2
        sqls = [c[0][0] for c in mock_spark.sql.call_args_list]
        assert all("ALTER TABLE mydb.mytable" in s for s in sqls)

    def test_apply_clears_queue(self) -> None:
        alter, mock_spark = _make_alter()
        alter.add_column("col1", "STRING")
        alter.apply()
        assert alter._ops == []

    def test_apply_noop_when_empty(self) -> None:
        alter, mock_spark = _make_alter()
        alter.apply()
        mock_spark.sql.assert_not_called()

    def test_second_apply_after_new_op(self) -> None:
        alter, mock_spark = _make_alter()
        alter.add_column("a", "STRING").apply()
        assert mock_spark.sql.call_count == 1

        alter.drop_column("b").apply()
        assert mock_spark.sql.call_count == 2

    def test_apply_stops_on_error(self) -> None:
        alter, mock_spark = _make_alter()
        alter.add_column("a", "STRING")
        alter.rename_column("b", "c")
        mock_spark.sql.side_effect = RuntimeError("column mapping not enabled")

        with pytest.raises(RuntimeError, match="column mapping not enabled"):
            alter.apply()

        # Only first op was attempted; queue not cleared
        assert len(alter._ops) == 2
