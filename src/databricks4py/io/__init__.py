"""I/O utilities for Delta Lake, DBFS, and streaming."""

from databricks4py.io.checkpoint import CheckpointInfo, CheckpointManager
from databricks4py.io.checkpoint_compat import (
    CheckpointDiagnostic,
    CheckpointHealth,
    CompatibilityResult,
    check_compatibility,
    diagnose_checkpoint,
)
from databricks4py.io.dbfs import copy_from_remote, inject_dbutils_module, ls, mkdirs, mv, rm
from databricks4py.io.dedup import (
    DedupResult,
    append_without_duplicates,
    drop_duplicates_pkey,
    kill_duplicates,
)
from databricks4py.io.delta import (
    DeltaTable,
    DeltaTableAppender,
    DeltaTableOverwriter,
    GeneratedColumn,
    optimize_table,
    vacuum_table,
)
from databricks4py.io.maintenance import MaintenanceResult, MaintenanceRunner, analyze_table
from databricks4py.io.merge import MergeBuilder, MergeResult
from databricks4py.io.streaming import (
    CircuitBreakerError,
    StreamingTableReader,
    StreamingTriggerOptions,
)

__all__ = [
    # Checkpoint
    "CheckpointInfo",
    "CheckpointManager",
    # Checkpoint Compatibility
    "CheckpointDiagnostic",
    "CheckpointHealth",
    "CompatibilityResult",
    "check_compatibility",
    "diagnose_checkpoint",
    # Dedup
    "DedupResult",
    "append_without_duplicates",
    "drop_duplicates_pkey",
    "kill_duplicates",
    # DBFS
    "copy_from_remote",
    "inject_dbutils_module",
    "ls",
    "mkdirs",
    "mv",
    "rm",
    # Delta
    "DeltaTable",
    "DeltaTableAppender",
    "DeltaTableOverwriter",
    "GeneratedColumn",
    "optimize_table",
    "vacuum_table",
    # Maintenance
    "MaintenanceResult",
    "MaintenanceRunner",
    "analyze_table",
    # Merge
    "MergeBuilder",
    "MergeResult",
    # Streaming
    "CircuitBreakerError",
    "StreamingTableReader",
    "StreamingTriggerOptions",
]
