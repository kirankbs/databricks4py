"""Migration framework for Delta Lake table evolution."""

from databricks4py.migrations.alter import TableAlter
from databricks4py.migrations.runner import MigrationRunner, MigrationRunResult, MigrationStep
from databricks4py.migrations.schema_diff import (
    ColumnChange,
    SchemaDiff,
    SchemaEvolutionError,
)
from databricks4py.migrations.validators import (
    MigrationError,
    TableValidator,
    ValidationResult,
)

__all__ = [
    "ColumnChange",
    "MigrationError",
    "MigrationRunResult",
    "MigrationRunner",
    "MigrationStep",
    "SchemaDiff",
    "SchemaEvolutionError",
    "TableAlter",
    "TableValidator",
    "ValidationResult",
]
