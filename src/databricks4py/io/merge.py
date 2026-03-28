"""Fluent MERGE INTO builder for Delta Lake tables."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from pyspark.sql import DataFrame, SparkSession

from databricks4py.spark_session import active_fallback

if TYPE_CHECKING:
    from databricks4py.metrics.base import MetricsSink

__all__ = ["MergeBuilder", "MergeResult"]

logger = logging.getLogger(__name__)

_SOURCE_ALIAS = "source"
_TARGET_ALIAS = "target"


@dataclass(frozen=True)
class MergeResult:
    """Outcome metrics from a MERGE operation."""

    rows_inserted: int
    rows_updated: int
    rows_deleted: int


class MergeBuilder:
    """Fluent builder for Delta Lake MERGE INTO operations.

    Example::

        result = (
            MergeBuilder("catalog.schema.target", source_df, spark)
            .on("id")
            .when_matched_update()
            .when_not_matched_insert()
            .execute()
        )
    """

    def __init__(
        self,
        target_table_name: str,
        source: DataFrame,
        spark: SparkSession | None = None,
        *,
        metrics_sink: MetricsSink | None = None,
    ) -> None:
        self._target_table_name = target_table_name
        self._source = source
        self._spark = active_fallback(spark)
        self._metrics_sink = metrics_sink

        self._join_keys: list[str] = []
        self._join_condition: str | None = None
        self._actions: list[dict[str, Any]] = []

    def on(self, *keys: str) -> MergeBuilder:
        """Set merge join keys (ANDed equality conditions)."""
        self._join_keys = list(keys)
        return self

    def on_condition(self, condition: str) -> MergeBuilder:
        """Set a custom merge condition expression instead of key-based equality."""
        self._join_condition = condition
        return self

    def when_matched_update(self, columns: list[str] | None = None) -> MergeBuilder:
        """Update matched rows. If columns is None, updates all columns."""
        self._actions.append({"type": "matched_update", "columns": columns})
        return self

    def when_matched_delete(self, condition: str | None = None) -> MergeBuilder:
        """Delete matched rows, optionally filtered by condition."""
        self._actions.append({"type": "matched_delete", "condition": condition})
        return self

    def when_not_matched_insert(self, columns: list[str] | None = None) -> MergeBuilder:
        """Insert non-matched source rows. If columns is None, inserts all."""
        self._actions.append({"type": "not_matched_insert", "columns": columns})
        return self

    def when_not_matched_by_source_delete(self, condition: str | None = None) -> MergeBuilder:
        """Delete target rows not present in source."""
        self._actions.append({"type": "not_matched_by_source_delete", "condition": condition})
        return self

    def when_matched_soft_delete(
        self,
        *,
        deleted_col: str = "is_deleted",
        deleted_at_col: str | None = "deleted_at",
        condition: str | None = None,
    ) -> MergeBuilder:
        """Mark matched rows as logically deleted without physically removing them.

        Sets ``deleted_col`` to ``True`` and optionally ``deleted_at_col`` to
        ``current_timestamp()``. Useful for audit-friendly delete patterns in
        regulated environments.

        Args:
            deleted_col: Boolean column to set ``True`` on the matched row.
            deleted_at_col: Timestamp column to set to ``current_timestamp()``.
                Pass ``None`` to skip updating a timestamp column.
            condition: Optional SQL condition restricting which matched rows to affect.

        Returns:
            This builder for chaining.

        Raises:
            ValueError: If ``deleted_col`` or ``deleted_at_col`` is an empty string.
        """
        if not deleted_col:
            raise ValueError("deleted_col must be a non-empty column name")
        if deleted_at_col is not None and not deleted_at_col:
            raise ValueError("deleted_at_col must be a non-empty column name or None")
        self._actions.append(
            {
                "type": "matched_soft_delete",
                "deleted_col": deleted_col,
                "deleted_at_col": deleted_at_col,
                "condition": condition,
            }
        )
        return self

    def _build_condition(self) -> str:
        if self._join_condition:
            return self._join_condition
        parts = [f"{_TARGET_ALIAS}.{k} = {_SOURCE_ALIAS}.{k}" for k in self._join_keys]
        return " AND ".join(parts)

    def execute(self) -> MergeResult:
        """Execute the merge and return metrics."""
        from delta.tables import DeltaTable

        target_dt = DeltaTable.forName(self._spark, self._target_table_name)
        condition = self._build_condition()

        merger = target_dt.alias(_TARGET_ALIAS).merge(self._source.alias(_SOURCE_ALIAS), condition)

        for action in self._actions:
            merger = self._apply_action(merger, action)

        merger.execute()

        result = self._read_metrics()
        if self._metrics_sink:
            self._emit_metrics(result)
        return result

    def _apply_action(self, merger: Any, action: dict[str, Any]) -> Any:
        action_type = action["type"]

        if action_type == "matched_update":
            columns = action["columns"]
            if columns:
                update_map = {col: f"{_SOURCE_ALIAS}.{col}" for col in columns}
                return merger.whenMatchedUpdate(set=update_map)
            return merger.whenMatchedUpdateAll()

        if action_type == "matched_delete":
            cond = action.get("condition")
            return merger.whenMatchedDelete(condition=cond) if cond else merger.whenMatchedDelete()

        if action_type == "not_matched_insert":
            columns = action["columns"]
            if columns:
                insert_map = {col: f"{_SOURCE_ALIAS}.{col}" for col in columns}
                return merger.whenNotMatchedInsert(values=insert_map)
            return merger.whenNotMatchedInsertAll()

        if action_type == "not_matched_by_source_delete":
            cond = action.get("condition")
            if cond:
                return merger.whenNotMatchedBySourceDelete(condition=cond)
            return merger.whenNotMatchedBySourceDelete()

        if action_type == "matched_soft_delete":
            deleted_col = action["deleted_col"]
            deleted_at_col = action.get("deleted_at_col")
            cond = action.get("condition")

            update_map: dict[str, str] = {deleted_col: "true"}
            if deleted_at_col:
                update_map[deleted_at_col] = "current_timestamp()"

            if cond:
                return merger.whenMatchedUpdate(condition=cond, set=update_map)
            return merger.whenMatchedUpdate(set=update_map)

        msg = f"Unknown merge action: {action_type}"
        raise ValueError(msg)

    def _read_metrics(self) -> MergeResult:
        history = self._spark.sql(f"DESCRIBE HISTORY {self._target_table_name} LIMIT 1")
        row = history.collect()[0]
        metrics: dict[str, str] = row["operationMetrics"] or {}

        return MergeResult(
            rows_inserted=int(metrics.get("numTargetRowsInserted", 0)),
            rows_updated=int(metrics.get("numTargetRowsUpdated", 0)),
            rows_deleted=int(metrics.get("numTargetRowsDeleted", 0)),
        )

    def _emit_metrics(self, result: MergeResult) -> None:
        from databricks4py.metrics.base import MetricEvent

        event = MetricEvent(
            job_name="merge",
            event_type="merge_complete",
            timestamp=datetime.now(tz=timezone.utc),
            row_count=result.rows_inserted + result.rows_updated,
            table_name=self._target_table_name,
            metadata={
                "rows_inserted": result.rows_inserted,
                "rows_updated": result.rows_updated,
                "rows_deleted": result.rows_deleted,
            },
        )
        self._metrics_sink.emit(event)  # type: ignore[union-attr]
