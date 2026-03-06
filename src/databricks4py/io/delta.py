"""Delta Lake table management utilities."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from databricks4py.spark_session import active_fallback

__all__ = [
    "DeltaTable",
    "DeltaTableAppender",
    "DeltaTableOverwriter",
    "GeneratedColumn",
    "optimize_table",
    "vacuum_table",
]

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class GeneratedColumn:
    """Definition of a Delta Lake generated column.

    Generated columns are computed from expressions over other columns
    and are automatically maintained by Delta Lake on write.

    Args:
        name: Column name.
        data_type: Spark SQL data type string (e.g. ``"DATE"``, ``"STRING"``).
        expression: SQL expression to generate the column value.
        comment: Optional column comment.
    """

    name: str
    data_type: str
    expression: str
    comment: str | None = None


class DeltaTable:
    """Managed Delta Lake table with structured creation and access.

    Wraps the delta-spark API to provide:
    - Automatic table creation with schema, partitioning, and generated columns
    - Convenient read/write/metadata operations
    - Atomic table replacement for migrations

    Example::

        from pyspark.sql.types import StructType, StructField, StringType, IntegerType

        schema = StructType([
            StructField("id", IntegerType()),
            StructField("name", StringType()),
        ])

        table = DeltaTable(
            table_name="catalog.schema.users",
            schema=schema,
            location="/data/users",
            partition_by=["id"],
        )

        df = table.dataframe()
        table.write(df, mode="append")

    Args:
        table_name: Fully qualified table name.
        schema: PySpark StructType for the table schema.
        location: Optional physical storage location.
        partition_by: Optional column(s) to partition by.
        generated_columns: Optional generated column definitions.
        spark: Optional SparkSession.
    """

    def __init__(
        self,
        table_name: str,
        schema: StructType,
        *,
        location: str | None = None,
        partition_by: str | Sequence[str] | None = None,
        generated_columns: Sequence[GeneratedColumn] | None = None,
        spark: SparkSession | None = None,
    ) -> None:
        self._spark = active_fallback(spark)
        self._table_name = table_name
        self._schema = schema
        self._location = location
        self._generated_columns = list(generated_columns or [])

        if isinstance(partition_by, str):
            self._partition_by = [partition_by]
        else:
            self._partition_by = list(partition_by or [])

        self._ensure_table_exists()

    @property
    def table_name(self) -> str:
        """The fully qualified table name."""
        return self._table_name

    def _ensure_table_exists(self) -> None:
        """Create the table if it doesn't exist."""
        from delta.tables import DeltaTable as _DeltaTable

        try:
            if self._location:
                _DeltaTable.forPath(self._spark, self._location)
            else:
                _DeltaTable.forName(self._spark, self._table_name)
            logger.debug("Table %s already exists", self._table_name)
        except Exception:
            self._create_table()

    def _create_table(self) -> None:
        """Create the Delta table using the builder API."""
        from delta.tables import DeltaTable as _DeltaTable

        builder = _DeltaTable.createIfNotExists(self._spark).tableName(self._table_name)

        if self._location:
            builder = builder.location(self._location)

        # Add columns from schema
        for field in self._schema.fields:
            gen_col = self._find_generated_column(field.name)
            if gen_col:
                builder = builder.addColumn(
                    field.name,
                    gen_col.data_type,
                    generatedAlwaysAs=gen_col.expression,
                    comment=gen_col.comment,
                )
            else:
                builder = builder.addColumn(
                    field.name,
                    field.dataType,
                    comment=field.metadata.get("comment") if field.metadata else None,
                )

        if self._partition_by:
            builder = builder.partitionedBy(*self._partition_by)

        builder.execute()
        logger.info("Created table %s", self._table_name)

    def _find_generated_column(self, name: str) -> GeneratedColumn | None:
        """Find a generated column definition by name."""
        for gc in self._generated_columns:
            if gc.name == name:
                return gc
        return None

    def dataframe(self) -> DataFrame:
        """Read the table as a DataFrame.

        Returns:
            The table contents as a PySpark DataFrame.
        """
        if self._location:
            return self._spark.read.format("delta").load(self._location)
        return self._spark.read.table(self._table_name)

    def write(self, df: DataFrame, mode: str = "append") -> None:
        """Write a DataFrame to the table.

        Args:
            df: The DataFrame to write.
            mode: Write mode (``"append"`` or ``"overwrite"``).
        """
        writer = df.write.format("delta").mode(mode)

        if self._partition_by:
            writer = writer.partitionBy(*self._partition_by)

        if self._location:
            writer.save(self._location)
        else:
            writer.saveAsTable(self._table_name)

        logger.info("Wrote to %s (mode=%s)", self._table_name, mode)

    def detail(self) -> DataFrame:
        """Get Delta table metadata.

        Returns:
            A DataFrame with table detail (location, partitions, size, etc.).
        """
        from delta.tables import DeltaTable as _DeltaTable

        if self._location:
            dt = _DeltaTable.forPath(self._spark, self._location)
        else:
            dt = _DeltaTable.forName(self._spark, self._table_name)
        return dt.detail()

    def location(self) -> str:
        """Get the physical storage location of the table."""
        row = self.detail().select("location").first()
        return row["location"] if row else ""

    def size_in_bytes(self) -> int:
        """Get the table size in bytes."""
        row = self.detail().select("sizeInBytes").first()
        return row["sizeInBytes"] if row else 0

    def partition_columns(self) -> list[str]:
        """Get the partition columns of the table."""
        row = self.detail().select("partitionColumns").first()
        return list(row["partitionColumns"]) if row else []

    def replace_data(
        self,
        replacement_table_name: str,
        recovery_table_name: str,
    ) -> None:
        """Replace this table's data with another table via atomic rename.

        Performs a two-step swap:
        1. Rename current table to recovery name (backup)
        2. Rename replacement table to current name

        Args:
            replacement_table_name: The table containing new data.
            recovery_table_name: Name for the backup of current data.
        """
        logger.info(
            "Replacing %s with %s (recovery: %s)",
            self._table_name,
            replacement_table_name,
            recovery_table_name,
        )
        self._spark.sql(
            f"ALTER TABLE {self._table_name} RENAME TO {recovery_table_name}"
        )
        self._spark.sql(
            f"ALTER TABLE {replacement_table_name} RENAME TO {self._table_name}"
        )
        logger.info("Table replacement complete")

    @classmethod
    def from_parquet(
        cls,
        *paths: str,
        table_name: str,
        schema: StructType,
        location: str | None = None,
        partition_by: str | Sequence[str] | None = None,
        generated_columns: Sequence[GeneratedColumn] | None = None,
        spark: SparkSession | None = None,
    ) -> DeltaTable:
        """Create a DeltaTable by loading data from Parquet files.

        Args:
            *paths: One or more Parquet file/directory paths.
            table_name: Target table name.
            schema: Table schema.
            location: Optional storage location.
            partition_by: Optional partition columns.
            generated_columns: Optional generated columns.
            spark: Optional SparkSession.

        Returns:
            A DeltaTable with the loaded data.
        """
        _spark = active_fallback(spark)
        df = _spark.read.schema(schema).parquet(*paths)

        table = cls(
            table_name=table_name,
            schema=schema,
            location=location,
            partition_by=partition_by,
            generated_columns=generated_columns,
            spark=_spark,
        )
        table.write(df, mode="overwrite")
        return table

    @classmethod
    def from_data(
        cls,
        data: list[dict[str, Any]] | list[tuple],
        *,
        table_name: str,
        schema: StructType,
        location: str | None = None,
        partition_by: str | Sequence[str] | None = None,
        generated_columns: Sequence[GeneratedColumn] | None = None,
        spark: SparkSession | None = None,
    ) -> DeltaTable:
        """Create a DeltaTable from in-memory data.

        Args:
            data: List of dicts or tuples.
            table_name: Target table name.
            schema: Table schema.
            location: Optional storage location.
            partition_by: Optional partition columns.
            generated_columns: Optional generated columns.
            spark: Optional SparkSession.

        Returns:
            A DeltaTable with the loaded data.
        """
        _spark = active_fallback(spark)
        df = _spark.createDataFrame(data, schema=schema)

        table = cls(
            table_name=table_name,
            schema=schema,
            location=location,
            partition_by=partition_by,
            generated_columns=generated_columns,
            spark=_spark,
        )
        table.write(df, mode="overwrite")
        return table

    def __repr__(self) -> str:
        return f"DeltaTable({self._table_name!r})"


class DeltaTableAppender(DeltaTable):
    """DeltaTable that provides a convenient ``append()`` method."""

    def append(self, df: DataFrame) -> None:
        """Append data to the table.

        Args:
            df: The DataFrame to append.
        """
        self.write(df, mode="append")


class DeltaTableOverwriter(DeltaTable):
    """DeltaTable that provides a convenient ``overwrite()`` method."""

    def overwrite(self, df: DataFrame) -> None:
        """Overwrite the table with new data.

        Args:
            df: The DataFrame to write.
        """
        self.write(df, mode="overwrite")


def optimize_table(
    table_name: str,
    *,
    zorder_by: str | Sequence[str] | None = None,
    spark: SparkSession | None = None,
) -> None:
    """Run OPTIMIZE on a Delta table.

    Args:
        table_name: The table to optimize.
        zorder_by: Optional column(s) for Z-ordering.
        spark: Optional SparkSession.
    """
    _spark = active_fallback(spark)
    sql = f"OPTIMIZE {table_name}"

    if zorder_by:
        cols = [zorder_by] if isinstance(zorder_by, str) else list(zorder_by)
        sql += f" ZORDER BY ({', '.join(cols)})"

    logger.info("Running: %s", sql)
    _spark.sql(sql)


def vacuum_table(
    table_name: str,
    *,
    retention_hours: int = 168,
    spark: SparkSession | None = None,
) -> None:
    """Run VACUUM on a Delta table.

    Args:
        table_name: The table to vacuum.
        retention_hours: Hours of history to retain (default 168 = 7 days).
        spark: Optional SparkSession.
    """
    _spark = active_fallback(spark)
    sql = f"VACUUM {table_name} RETAIN {retention_hours} HOURS"
    logger.info("Running: %s", sql)
    _spark.sql(sql)
