"""Fluent DDL builder for Delta table schema changes."""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession

from databricks4py.spark_session import active_fallback

__all__ = ["TableAlter"]

logger = logging.getLogger(__name__)


def _sql_str(value: str) -> str:
    """Escape single quotes for embedding in SQL string literals."""
    return value.replace("'", "''")


class TableAlter:
    """Fluent interface for batching Delta table DDL operations.

    Operations are queued and executed one-by-one via ``ALTER TABLE`` when
    :meth:`apply` is called. Each operation is logged at INFO level.

    Note:
        ``rename_column`` and ``drop_column`` require Delta column mapping to be
        enabled on the target table
        (``delta.columnMapping.mode = 'name'``). Use :meth:`set_property` to
        enable it before renaming or dropping.

    Example::

        TableAlter("catalog.schema.events", spark=spark) \\
            .add_column("region", "STRING", comment="ISO-3166 region code") \\
            .set_property("delta.enableChangeDataFeed", "true") \\
            .apply()

    Args:
        table_name: Fully qualified table name.
        spark: Optional SparkSession.
    """

    def __init__(self, table_name: str, *, spark: SparkSession | None = None) -> None:
        self._table_name = table_name
        self._spark = active_fallback(spark)
        self._ops: list[str] = []

    def add_column(
        self,
        name: str,
        data_type: str,
        *,
        after: str | None = None,
        nullable: bool = True,
        comment: str | None = None,
    ) -> TableAlter:
        """Queue an ADD COLUMN operation.

        Args:
            name: Column name.
            data_type: Spark SQL type string (e.g. ``"STRING"``, ``"DOUBLE"``).
            after: If set, place the new column after this existing column.
            nullable: Whether the column allows nulls (default True).
            comment: Optional column comment.
        """
        not_null = "" if nullable else " NOT NULL"
        comment_clause = f" COMMENT '{_sql_str(comment)}'" if comment else ""
        after_clause = f" AFTER {after}" if after else ""
        self._ops.append(
            f"ADD COLUMN ({name} {data_type}{not_null}{comment_clause}{after_clause})"
        )
        return self

    def rename_column(self, old_name: str, new_name: str) -> TableAlter:
        """Queue a RENAME COLUMN operation.

        Requires ``delta.columnMapping.mode = 'name'`` on the target table.
        """
        self._ops.append(f"RENAME COLUMN {old_name} TO {new_name}")
        return self

    def drop_column(self, name: str) -> TableAlter:
        """Queue a DROP COLUMN operation.

        Requires ``delta.columnMapping.mode = 'name'`` on the target table.
        """
        self._ops.append(f"DROP COLUMN {name}")
        return self

    def set_property(self, key: str, value: str) -> TableAlter:
        """Queue a SET TBLPROPERTIES operation.

        Args:
            key: Property key (e.g. ``"delta.enableChangeDataFeed"``).
            value: Property value string.
        """
        self._ops.append(f"SET TBLPROPERTIES ('{_sql_str(key)}' = '{_sql_str(value)}')")
        return self

    def apply(self) -> None:
        """Execute all queued DDL operations against the target table.

        Each operation runs as a separate ``ALTER TABLE`` statement. The queue
        is cleared after a successful apply. If any statement raises, the
        remaining operations are not run and the queue is not cleared — a
        subsequent ``apply()`` call will retry from the beginning.
        """
        if not self._ops:
            return
        for op in self._ops:
            sql = f"ALTER TABLE {self._table_name} {op}"
            logger.info("Executing: %s", sql)
            self._spark.sql(sql)
        self._ops.clear()
