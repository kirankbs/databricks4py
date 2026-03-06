"""DBFS file operations via dbutils."""

from __future__ import annotations

import logging
from typing import Any

__all__ = ["copy_from_remote", "inject_dbutils_module"]

logger = logging.getLogger(__name__)

_dbutils_module: Any = None


def inject_dbutils_module(dbutils_module: Any) -> None:
    """Inject the dbutils module for remote file operations.

    Args:
        dbutils_module: The ``pyspark.dbutils`` module.
    """
    global _dbutils_module
    _dbutils_module = dbutils_module
    logger.debug("Injected dbutils module for DBFS: %s", dbutils_module)


def _get_dbutils() -> Any:
    """Get a DBUtils instance from the injected module."""
    if _dbutils_module is None:
        raise RuntimeError("dbutils module not injected. Call inject_dbutils_module() first.")

    from databricks4py.spark_session import get_active

    spark = get_active()
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
