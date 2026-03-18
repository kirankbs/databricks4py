"""Delta Lake table management utilities."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.utils import AnalysisException

from databricks4py.spark_session import active_fallback

if TYPE_CHECKING:
    from databricks4py.io.merge import MergeBuilder, MergeResult
    from databricks4py.metrics.base import MetricsSink

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
        schema: StructType | dict[str, str],
        *,
        location: str | None = None,
        partition_by: str | Sequence[str] | None = None,
        generated_columns: Sequence[GeneratedColumn] | None = None,
        spark: SparkSession | None = None,
    ) -> None:
        self._spark = active_fallback(spark)
        self._table_name = table_name
        self._schema = self._resolve_schema(schema)
        self._location = location
        self._generated_columns = list(generated_columns or [])

        if isinstance(partition_by, str):
            self._partition_by = [partition_by]
        else:
            self._partition_by = list(partition_by or [])

        self._ensure_table_exists()

    @staticmethod
    def _resolve_schema(schema: StructType | dict[str, str]) -> StructType:
        if isinstance(schema, dict):
            from pyspark.sql import types as T

            fields = []
            for name, type_str in schema.items():
                spark_type = T._parse_datatype_string(type_str)
                fields.append(T.StructField(name, spark_type, nullable=True))
            return T.StructType(fields)
        return schema

    @property
    def table_name(self) -> str:
        """The fully qualified table name."""
        return self._table_name

    def _table_exists(self) -> bool:
        from delta.tables import DeltaTable as _DeltaTable

        try:
            if self._location:
                _DeltaTable.forPath(self._spark, self._location)
            else:
                _DeltaTable.forName(self._spark, self._table_name)
            return True
        except AnalysisException:
            return False

    def _ensure_table_exists(self) -> None:
        """Create the table if it doesn't exist."""
        if self._table_exists():
            logger.debug("Table %s already exists", self._table_name)
        else:
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

    def write(self, df: DataFrame, mode: str = "append", *, schema_check: bool = True) -> None:
        """Write a DataFrame to the table.

        Args:
            df: The DataFrame to write.
            mode: Write mode (``"append"`` or ``"overwrite"``).
            schema_check: If True, validates schema compatibility before writing.
        """
        if schema_check and self._table_exists():
            from databricks4py.migrations.schema_diff import SchemaDiff, SchemaEvolutionError

            diff = SchemaDiff.from_tables(self._table_name, df, spark=self._spark)
            if diff.has_breaking_changes():
                raise SchemaEvolutionError(
                    f"Breaking schema changes detected for {self._table_name}:\n{diff.summary()}"
                )

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

    def merge(
        self,
        source: DataFrame,
        *,
        metrics_sink: MetricsSink | None = None,
    ) -> MergeBuilder:
        """Start a fluent MERGE INTO operation against this table.

        Args:
            source: Source DataFrame to merge from.
            metrics_sink: Optional sink for merge metrics.

        Returns:
            A MergeBuilder for chaining merge conditions.
        """
        from databricks4py.io.merge import MergeBuilder as _MergeBuilder

        self._ensure_table_exists()
        return _MergeBuilder(self._table_name, source, self._spark, metrics_sink=metrics_sink)

    def upsert(
        self,
        source: DataFrame,
        keys: list[str],
        *,
        update_columns: list[str] | None = None,
        metrics_sink: MetricsSink | None = None,
    ) -> MergeResult:
        """Upsert (update existing, insert new) rows by key columns.

        Args:
            source: Source DataFrame.
            keys: Columns to match on.
            update_columns: Specific columns to update on match. If None, updates all.
            metrics_sink: Optional sink for merge metrics.

        Returns:
            MergeResult with insert/update/delete counts.
        """
        return (
            self.merge(source, metrics_sink=metrics_sink)
            .on(*keys)
            .when_matched_update(update_columns)
            .when_not_matched_insert()
            .execute()
        )

    def scd_type2(
        self,
        source: DataFrame,
        keys: list[str],
        *,
        effective_date_col: str = "effective_date",
        end_date_col: str = "end_date",
        active_col: str = "is_active",
        metrics_sink: MetricsSink | None = None,
    ) -> MergeResult:
        """Apply SCD Type 2 logic: expire changed records and insert new versions.

        Matches on keys where active=True. For changed records, sets end_date
        to current_timestamp and active to False. All incoming source rows are
        inserted as new active records with the current effective_date.

        Args:
            source: Incoming DataFrame (without SCD metadata columns).
            keys: Business key columns.
            effective_date_col: Column name for the effective date.
            end_date_col: Column name for the end date.
            active_col: Column name for the active flag.
            metrics_sink: Optional sink for merge metrics.

        Returns:
            MergeResult with insert/update/delete counts.
        """
        from delta.tables import DeltaTable as _DeltaTable
        from pyspark.sql import functions as F

        from databricks4py.io.merge import MergeResult as _MergeResult

        self._ensure_table_exists()

        staged = (
            source.withColumn(effective_date_col, F.current_timestamp())
            .withColumn(end_date_col, F.lit(None).cast("timestamp"))
            .withColumn(active_col, F.lit(True))
        )

        key_conds = [f"target.{k} = source.{k}" for k in keys]
        key_conds.append(f"target.{active_col} = true")
        condition = " AND ".join(key_conds)

        target_dt = _DeltaTable.forName(self._spark, self._table_name)
        (
            target_dt.alias("target")
            .merge(staged.alias("source"), condition)
            .whenMatchedUpdate(
                set={
                    end_date_col: "current_timestamp()",
                    active_col: "false",
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        history = self._spark.sql(f"DESCRIBE HISTORY {self._table_name} LIMIT 1")
        row = history.collect()[0]
        metrics: dict[str, str] = row["operationMetrics"] or {}

        result = _MergeResult(
            rows_inserted=int(metrics.get("numTargetRowsInserted", 0)),
            rows_updated=int(metrics.get("numTargetRowsUpdated", 0)),
            rows_deleted=int(metrics.get("numTargetRowsDeleted", 0)),
        )

        if metrics_sink:
            from datetime import datetime, timezone

            from databricks4py.metrics.base import MetricEvent

            event = MetricEvent(
                job_name="scd_type2",
                event_type="merge_complete",
                timestamp=datetime.now(tz=timezone.utc),
                row_count=result.rows_inserted + result.rows_updated,
                table_name=self._table_name,
                metadata={
                    "rows_inserted": result.rows_inserted,
                    "rows_updated": result.rows_updated,
                    "rows_deleted": result.rows_deleted,
                },
            )
            metrics_sink.emit(event)

        return result

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
        self._spark.sql(f"ALTER TABLE {self._table_name} RENAME TO {recovery_table_name}")
        self._spark.sql(f"ALTER TABLE {replacement_table_name} RENAME TO {self._table_name}")
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
