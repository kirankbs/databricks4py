"""Delta table deduplication utilities.

UC-compatible dedup operations that work via SQL and Delta merge —
no path-based writes, so they work with Unity Catalog managed tables.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, SparkSession

from databricks4py.spark_session import active_fallback

if TYPE_CHECKING:
    from databricks4py.metrics.base import MetricsSink

__all__ = [
    "DedupResult",
    "append_without_duplicates",
    "drop_duplicates_pkey",
    "kill_duplicates",
]

logger = logging.getLogger(__name__)

_UNSAFE_PATTERN = re.compile(r"--|;|/\*|\*/")


def _validate_identifier(name: str, label: str = "identifier") -> str:
    """Reject SQL injection characters in table/column names."""
    if _UNSAFE_PATTERN.search(name):
        raise ValueError(f"Unsafe characters in {label}: {name!r}")
    return name


@dataclass(frozen=True)
class DedupResult:
    """Immutable result from a dedup operation.

    Attributes:
        rows_removed: Number of duplicate rows deleted.
        rows_remaining: Total rows remaining after dedup.
    """

    rows_removed: int
    rows_remaining: int


def kill_duplicates(
    table_name: str,
    duplication_columns: Sequence[str],
    *,
    spark: SparkSession | None = None,
    metrics_sink: MetricsSink | None = None,
) -> DedupResult:
    """Delete ALL copies of rows that have duplicates on the given columns.

    Unlike ``drop_duplicates_pkey`` which keeps one copy, this removes every
    row that participates in a duplicate group. Useful for poisoned-data
    cleanup where you can't trust any copy.

    Args:
        table_name: Fully qualified Delta table name.
        duplication_columns: Columns to check for duplicates.
        spark: Optional SparkSession.
        metrics_sink: Optional sink for dedup metrics.

    Returns:
        DedupResult with counts of removed and remaining rows.

    Raises:
        ValueError: If duplication_columns is empty.
    """
    if not duplication_columns:
        raise ValueError("duplication_columns must not be empty")

    _validate_identifier(table_name, "table_name")
    for c in duplication_columns:
        _validate_identifier(c, "column")

    _spark = active_fallback(spark)
    cols = list(duplication_columns)
    col_list = ", ".join(cols)

    count_before = _spark.read.table(table_name).count()

    # Find groups with more than one row, then delete all rows matching those groups
    dup_df = _spark.read.table(table_name).groupBy(cols).count().where("count > 1").drop("count")

    if dup_df.count() == 0:
        logger.info("No duplicates found in %s on columns [%s]", table_name, col_list)
        return DedupResult(rows_removed=0, rows_remaining=count_before)

    from delta.tables import DeltaTable

    dt = DeltaTable.forName(_spark, table_name)
    condition = " AND ".join(f"target.{c} = source.{c}" for c in cols)

    (dt.alias("target").merge(dup_df.alias("source"), condition).whenMatchedDelete().execute())

    count_after = _spark.read.table(table_name).count()
    removed = count_before - count_after

    logger.info(
        "kill_duplicates on %s: removed %d rows, %d remaining",
        table_name,
        removed,
        count_after,
    )

    result = DedupResult(rows_removed=removed, rows_remaining=count_after)
    _emit_metric(metrics_sink, "kill_duplicates", table_name, result)
    return result


def drop_duplicates_pkey(
    table_name: str,
    primary_key: str | Sequence[str],
    duplication_columns: Sequence[str],
    *,
    spark: SparkSession | None = None,
    metrics_sink: MetricsSink | None = None,
) -> DedupResult:
    """Keep one row per primary key, delete extra duplicates.

    For each group of rows sharing the same ``duplication_columns`` values,
    the row with the lowest ``primary_key`` value is kept and the rest are
    deleted via a self-merge.

    Args:
        table_name: Fully qualified Delta table name.
        primary_key: Column(s) that uniquely identify the row to keep
            (lowest value wins).
        duplication_columns: Columns to check for duplicates.
        spark: Optional SparkSession.
        metrics_sink: Optional sink for dedup metrics.

    Returns:
        DedupResult with counts of removed and remaining rows.

    Raises:
        ValueError: If primary_key or duplication_columns is empty.
    """
    if not primary_key:
        raise ValueError("primary_key must not be empty")
    if not duplication_columns:
        raise ValueError("duplication_columns must not be empty")

    _validate_identifier(table_name, "table_name")
    pkeys = [primary_key] if isinstance(primary_key, str) else list(primary_key)
    dup_cols = list(duplication_columns)
    for c in pkeys + dup_cols:
        _validate_identifier(c, "column")

    _spark = active_fallback(spark)

    count_before = _spark.read.table(table_name).count()

    from pyspark.sql import Window
    from pyspark.sql import functions as F

    df = _spark.read.table(table_name)
    window = Window.partitionBy(dup_cols).orderBy([F.col(k).asc() for k in pkeys])
    ranked = df.withColumn("_dedup_rank", F.row_number().over(window))

    duplicates_to_remove = ranked.where("_dedup_rank > 1").select(pkeys)

    if duplicates_to_remove.count() == 0:
        logger.info("No duplicates found in %s", table_name)
        return DedupResult(rows_removed=0, rows_remaining=count_before)

    # Merge-based delete: match duplicate rows by primary key and remove them.
    # This is concurrent-write safe (unlike a full overwrite).
    from delta.tables import DeltaTable

    dt = DeltaTable.forName(_spark, table_name)
    pkey_condition = " AND ".join(f"target.{k} = source.{k}" for k in pkeys)

    (
        dt.alias("target")
        .merge(duplicates_to_remove.alias("source"), pkey_condition)
        .whenMatchedDelete()
        .execute()
    )

    count_after = _spark.read.table(table_name).count()
    removed = count_before - count_after

    logger.info(
        "drop_duplicates_pkey on %s: removed %d rows, %d remaining",
        table_name,
        removed,
        count_after,
    )

    result = DedupResult(rows_removed=removed, rows_remaining=count_after)
    _emit_metric(metrics_sink, "drop_duplicates_pkey", table_name, result)
    return result


def append_without_duplicates(
    table_name: str,
    df: DataFrame,
    keys: Sequence[str],
    *,
    spark: SparkSession | None = None,
    metrics_sink: MetricsSink | None = None,
) -> int:
    """Append rows to a Delta table, skipping rows whose keys already exist.

    Uses a MERGE with ``whenNotMatchedInsertAll`` — only rows whose key
    values don't already exist in the target are inserted. Existing rows
    are left untouched.

    Args:
        table_name: Fully qualified Delta table name.
        df: DataFrame of rows to potentially append.
        keys: Columns to match on (rows with existing keys are skipped).
        spark: Optional SparkSession.
        metrics_sink: Optional sink for merge metrics.

    Returns:
        Number of rows inserted.

    Raises:
        ValueError: If keys is empty.
    """
    if not keys:
        raise ValueError("keys must not be empty")

    _validate_identifier(table_name, "table_name")
    for k in keys:
        _validate_identifier(k, "column")

    _spark = active_fallback(spark)
    key_list = list(keys)

    from delta.tables import DeltaTable

    dt = DeltaTable.forName(_spark, table_name)
    condition = " AND ".join(f"target.{k} = source.{k}" for k in key_list)

    (dt.alias("target").merge(df.alias("source"), condition).whenNotMatchedInsertAll().execute())

    # Read merge metrics from history
    history = _spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1")
    rows = history.collect()
    inserted = 0
    if rows:
        metrics = rows[0]["operationMetrics"] or {}
        inserted = int(metrics.get("numTargetRowsInserted", 0))

    logger.info("append_without_duplicates on %s: inserted %d rows", table_name, inserted)

    if metrics_sink:
        _emit_metric(
            metrics_sink,
            "append_without_duplicates",
            table_name,
            DedupResult(rows_removed=0, rows_remaining=inserted),
        )

    return inserted


def _emit_metric(
    sink: MetricsSink | None,
    operation: str,
    table_name: str,
    result: DedupResult,
) -> None:
    if sink is None:
        return

    from datetime import datetime, timezone

    from databricks4py.metrics.base import MetricEvent

    sink.emit(
        MetricEvent(
            job_name=operation,
            event_type="dedup_complete",
            timestamp=datetime.now(tz=timezone.utc),
            table_name=table_name,
            metadata={
                "rows_removed": result.rows_removed,
                "rows_remaining": result.rows_remaining,
            },
        )
    )
