"""Table structure validation for migrations."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass, field

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

from databricks4py.io.delta import GeneratedColumn
from databricks4py.spark_session import active_fallback

__all__ = ["MigrationError", "TableValidator", "ValidationResult"]

logger = logging.getLogger(__name__)


class MigrationError(Exception):
    """Raised when table validation fails during migration.

    Attributes:
        table_name: The table that failed validation.
        errors: List of validation error messages.
    """

    def __init__(self, table_name: str, errors: list[str]) -> None:
        self.table_name = table_name
        self.errors = errors
        message = f"Migration validation failed for '{table_name}':\n" + "\n".join(
            f"  - {e}" for e in errors
        )
        super().__init__(message)


@dataclass
class ValidationResult:
    """Result of a table validation check.

    Attributes:
        is_valid: Whether all checks passed.
        errors: List of validation errors.
        warnings: List of non-fatal warnings.
    """

    is_valid: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def raise_if_invalid(self, table_name: str) -> None:
        """Raise MigrationError if validation failed.

        Args:
            table_name: Table name for the error message.

        Raises:
            MigrationError: If ``is_valid`` is False.
        """
        if not self.is_valid:
            raise MigrationError(table_name, self.errors)


class TableValidator:
    """Validates Delta table structure against expected configuration.

    Used in migration workflows to verify that a table matches
    expected schema, partitioning, and structure before and after
    migration steps.

    Example::

        validator = TableValidator(
            table_name="catalog.schema.events",
            expected_columns=["id", "name", "event_date"],
            expected_partition_columns=["event_date"],
        )
        result = validator.validate()
        result.raise_if_invalid("catalog.schema.events")

    Args:
        table_name: Fully qualified table name.
        expected_columns: Columns that must exist in the table.
        expected_partition_columns: Expected partition column order.
        expected_generated_columns: Expected generated column definitions.
        expected_location_contains: Substring that must appear in table location.
        spark: Optional SparkSession.
    """

    def __init__(
        self,
        table_name: str,
        *,
        expected_columns: Sequence[str] | None = None,
        expected_partition_columns: Sequence[str] | None = None,
        expected_generated_columns: Sequence[GeneratedColumn] | None = None,
        expected_location_contains: str | None = None,
        spark: SparkSession | None = None,
    ) -> None:
        self._spark = active_fallback(spark)
        self._table_name = table_name
        self._expected_columns = list(expected_columns or [])
        self._expected_partition_columns = list(expected_partition_columns or [])
        self._expected_generated_columns = list(expected_generated_columns or [])
        self._expected_location_contains = expected_location_contains

    def _table_exists(self) -> bool:
        """Check if the table exists in the catalog."""
        try:
            self._spark.sql(f"DESCRIBE TABLE {self._table_name}")
            return True
        except AnalysisException:
            return False

    def _get_actual_columns(self) -> set[str]:
        """Get column names from the table."""
        rows = self._spark.sql(f"DESCRIBE TABLE {self._table_name}").collect()
        columns: set[str] = set()
        for row in rows:
            col_name = row["col_name"]
            if col_name is None or col_name == "" or col_name.startswith("#"):
                break
            columns.add(col_name)
        return columns

    def _get_actual_partitions(self) -> list[str]:
        """Get partition columns from Delta DETAIL."""
        from delta.tables import DeltaTable

        dt = DeltaTable.forName(self._spark, self._table_name)
        row = dt.detail().select("partitionColumns").first()
        return list(row["partitionColumns"]) if row else []

    def _get_actual_location(self) -> str:
        """Get the table's physical location."""
        from delta.tables import DeltaTable

        dt = DeltaTable.forName(self._spark, self._table_name)
        row = dt.detail().select("location").first()
        return row["location"] if row else ""

    def validate(self) -> ValidationResult:
        """Run all configured validations.

        Returns:
            ValidationResult with any errors and warnings.
        """
        errors: list[str] = []
        warnings: list[str] = []

        if not self._table_exists():
            errors.append(f"Table '{self._table_name}' does not exist")
            return ValidationResult(is_valid=False, errors=errors)

        logger.info("Validating table %s", self._table_name)

        if self._expected_columns:
            actual = self._get_actual_columns()
            missing = set(self._expected_columns) - actual
            if missing:
                errors.append(f"Missing required columns: {sorted(missing)}")

        if self._expected_partition_columns:
            actual_partitions = self._get_actual_partitions()
            if sorted(actual_partitions) != sorted(self._expected_partition_columns):
                errors.append(
                    f"Partition mismatch: expected {self._expected_partition_columns}, "
                    f"got {actual_partitions}"
                )

        if self._expected_location_contains:
            actual_location = self._get_actual_location()
            if self._expected_location_contains not in actual_location:
                errors.append(
                    f"Location '{actual_location}' does not contain "
                    f"'{self._expected_location_contains}'"
                )

        if self._expected_generated_columns:
            actual_cols = self._get_actual_columns()
            for gc in self._expected_generated_columns:
                if gc.name not in actual_cols:
                    errors.append(f"Missing generated column: '{gc.name}'")

        is_valid = len(errors) == 0
        if is_valid:
            logger.info("Table %s validation passed", self._table_name)
        else:
            logger.warning("Table %s validation failed: %s", self._table_name, errors)

        return ValidationResult(is_valid=is_valid, errors=errors, warnings=warnings)
