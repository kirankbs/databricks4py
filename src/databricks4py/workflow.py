"""Workflow base class for Databricks job entry points."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

from pyspark.sql import SparkSession

from databricks4py.io import dbfs
from databricks4py.logging import configure_logging, get_logger
from databricks4py.secrets import inject_dbutils
from databricks4py.spark_session import active_fallback

__all__ = ["Workflow"]

logger = get_logger(__name__)


class Workflow(ABC):
    """Abstract base class for Databricks workflow entry points.

    Provides a structured pattern for job scripts that auto-initializes:

    - SparkSession (via :func:`~databricks4py.spark_session.active_fallback`)
    - Logging configuration
    - dbutils injection for both secrets and DBFS (optional)

    Subclasses implement :meth:`run` with business logic.

    Example::

        class MyETL(Workflow):
            def run(self) -> None:
                df = self.spark.read.table("source")
                df.write.format("delta").saveAsTable("target")

        # As a CLI entry point (pyspark.dbutils only on Databricks Runtime):
        def main():
            import pyspark.dbutils
            MyETL(dbutils=pyspark.dbutils).execute()

    Args:
        spark: Optional SparkSession. Defaults to active session.
        dbutils: Optional dbutils module for secret/file operations.
            Only available on Databricks Runtime (``pyspark.dbutils``).
        log_level: Logging level (default INFO).
    """

    def __init__(
        self,
        *,
        spark: SparkSession | None = None,
        dbutils: Any = None,
        log_level: int = logging.INFO,
    ) -> None:
        configure_logging(level=log_level)
        self._spark = active_fallback(spark)
        self._dbutils: Any = None
        self._execution_time: datetime = datetime.now()

        if dbutils is not None:
            try:
                inject_dbutils(dbutils)
                dbfs.inject_dbutils_module(dbutils)
                self._dbutils = dbutils
            except (ImportError, AttributeError, TypeError) as exc:
                logger.warning("dbutils injection failed: %s (running outside Databricks?)", exc)

    @property
    def spark(self) -> SparkSession:
        """The SparkSession for this workflow."""
        return self._spark

    @property
    def dbutils(self) -> Any:
        """The dbutils module, or None if not in Databricks."""
        return self._dbutils

    @property
    def execution_time(self) -> datetime:
        """The logical execution time (set by run_at_time, or defaults to init time)."""
        return self._execution_time

    @abstractmethod
    def run(self) -> None:
        """Execute the workflow business logic.

        Subclasses must implement this method.
        """
        ...

    def run_at_time(self, execution_time: datetime | None = None) -> None:
        """Execute the workflow with an explicit execution timestamp.

        Useful for backfill scenarios where the logical execution
        time differs from wall-clock time.

        Args:
            execution_time: The logical execution time. Defaults to now.
        """
        self._execution_time = execution_time or datetime.now()
        logger.info(
            "Running %s at execution_time=%s",
            self.__class__.__name__,
            self._execution_time.isoformat(),
        )
        self.run()

    def execute(self) -> None:
        """Standard entry point with logging and error handling.

        Call this from ``if __name__ == "__main__"``.
        """
        workflow_name = self.__class__.__name__
        logger.info("Starting workflow: %s", workflow_name)
        try:
            self.run_at_time()
            logger.info("Workflow %s completed successfully", workflow_name)
        except Exception:
            logger.exception("Workflow %s failed", workflow_name)
            raise
