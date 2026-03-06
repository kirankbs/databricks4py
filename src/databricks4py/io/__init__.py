"""I/O utilities for Delta Lake, DBFS, and streaming."""

from databricks4py.io.dbfs import copy_from_remote, inject_dbutils_module
from databricks4py.io.delta import (
    DeltaTable,
    DeltaTableAppender,
    DeltaTableOverwriter,
    GeneratedColumn,
    optimize_table,
    vacuum_table,
)
from databricks4py.io.streaming import StreamingTableReader, StreamingTriggerOptions

__all__ = [
    # DBFS
    "copy_from_remote",
    "inject_dbutils_module",
    # Delta
    "DeltaTable",
    "DeltaTableAppender",
    "DeltaTableOverwriter",
    "GeneratedColumn",
    "optimize_table",
    "vacuum_table",
    # Streaming
    "StreamingTableReader",
    "StreamingTriggerOptions",
]
