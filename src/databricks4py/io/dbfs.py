"""DBFS file operations via dbutils."""

from __future__ import annotations

import logging
from typing import Any

__all__ = [
    "copy_from_remote",
    "inject_dbutils_module",
    "ls",
    "mkdirs",
    "mv",
    "rm",
    "_set_dbutils_module",
]

logger = logging.getLogger(__name__)

_dbutils_module: Any = None


def _set_dbutils_module(dbutils_module: Any) -> None:
    """Internal: set the dbutils module. Use top-level inject_dbutils() instead."""
    global _dbutils_module
    _dbutils_module = dbutils_module
    logger.debug("Injected dbutils module for DBFS: %s", dbutils_module)


inject_dbutils_module = _set_dbutils_module


def _get_dbutils() -> Any:
    """Get a DBUtils instance from the injected module."""
    if _dbutils_module is None:
        raise RuntimeError(
            "dbutils module not injected. Call inject_dbutils_module() first."
        )

    import pyspark.sql

    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    return _dbutils_module.DBUtils(spark)


def copy_from_remote(
    remote_path: str,
    local_path: str,
    recurse: bool = False,
) -> bool:
    """Copy a file from a remote path to a local path via dbutils.fs.

    Args:
        remote_path: The source path (e.g. ``abfss://...`` or ``dbfs://...``).
        local_path: The destination local path.
        recurse: Whether to copy recursively.

    Returns:
        True if the copy succeeded.

    Raises:
        RuntimeError: If dbutils has not been injected.
    """
    dbutils = _get_dbutils()
    logger.info("Copying %s -> %s (recurse=%s)", remote_path, local_path, recurse)
    return dbutils.fs.cp(remote_path, local_path, recurse=recurse)


def ls(path: str) -> list:
    """List files at the given path."""
    dbutils = _get_dbutils()
    return dbutils.fs.ls(path)


def mv(source: str, dest: str, *, recurse: bool = False) -> None:
    """Move a file or directory."""
    dbutils = _get_dbutils()
    logger.info("Moving %s → %s (recurse=%s)", source, dest, recurse)
    dbutils.fs.mv(source, dest, recurse)


def rm(path: str, *, recurse: bool = False) -> None:
    """Remove a file or directory."""
    dbutils = _get_dbutils()
    logger.info("Removing %s (recurse=%s)", path, recurse)
    dbutils.fs.rm(path, recurse)


def mkdirs(path: str) -> None:
    """Create a directory (and parents)."""
    dbutils = _get_dbutils()
    logger.info("Creating directory %s", path)
    dbutils.fs.mkdirs(path)
