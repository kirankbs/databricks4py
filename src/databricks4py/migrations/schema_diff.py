"""Schema diff detection for Delta Lake table evolution."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from databricks4py.spark_session import active_fallback

__all__ = ["ColumnChange", "SchemaDiff", "SchemaEvolutionError"]


class SchemaEvolutionError(Exception):
    """Raised on breaking schema changes."""


@dataclass(frozen=True)
class ColumnChange:
    column: str
    change_type: Literal["added", "removed", "type_changed", "nullable_changed"]
    old_value: str | None = None
    new_value: str | None = None
    severity: Literal["info", "warning", "breaking"] = "info"


class SchemaDiff:
    """Compares two StructType schemas and reports column-level changes."""

    def __init__(self, current: StructType, incoming: StructType) -> None:
        self._current = current
        self._incoming = incoming
        self._changes: list[ColumnChange] | None = None

    @classmethod
    def from_tables(
        cls,
        table_name: str,
        incoming_df: DataFrame,
        *,
        spark: SparkSession | None = None,
    ) -> SchemaDiff:
        spark = active_fallback(spark)
        current_schema = spark.read.table(table_name).schema
        return cls(current=current_schema, incoming=incoming_df.schema)

    def changes(self) -> list[ColumnChange]:
        if self._changes is not None:
            return self._changes

        result: list[ColumnChange] = []
        current_fields = {f.name: f for f in self._current.fields}
        incoming_fields = {f.name: f for f in self._incoming.fields}

        for name in sorted(incoming_fields.keys() - current_fields.keys()):
            field = incoming_fields[name]
            result.append(
                ColumnChange(
                    column=name,
                    change_type="added",
                    new_value=str(field.dataType),
                    severity="info",
                )
            )

        for name in sorted(current_fields.keys() - incoming_fields.keys()):
            field = current_fields[name]
            result.append(
                ColumnChange(
                    column=name,
                    change_type="removed",
                    old_value=str(field.dataType),
                    severity="breaking",
                )
            )

        for name in sorted(current_fields.keys() & incoming_fields.keys()):
            cur = current_fields[name]
            inc = incoming_fields[name]

            if cur.dataType != inc.dataType:
                result.append(
                    ColumnChange(
                        column=name,
                        change_type="type_changed",
                        old_value=str(cur.dataType),
                        new_value=str(inc.dataType),
                        severity="breaking",
                    )
                )
            elif cur.nullable != inc.nullable:
                result.append(
                    ColumnChange(
                        column=name,
                        change_type="nullable_changed",
                        old_value=str(cur.nullable),
                        new_value=str(inc.nullable),
                        severity="warning",
                    )
                )

        self._changes = result
        return result

    def has_breaking_changes(self) -> bool:
        return any(c.severity == "breaking" for c in self.changes())

    def summary(self) -> str:
        changes = self.changes()
        if not changes:
            return "No schema changes detected."

        lines = [f"{'Column':<30} {'Change':<20} {'Severity':<10} {'Details'}"]
        lines.append("-" * 80)
        for c in changes:
            details = ""
            if c.old_value and c.new_value:
                details = f"{c.old_value} -> {c.new_value}"
            elif c.new_value:
                details = c.new_value
            elif c.old_value:
                details = c.old_value
            lines.append(f"{c.column:<30} {c.change_type:<20} {c.severity:<10} {details}")
        return "\n".join(lines)
