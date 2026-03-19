"""Workflow base class for Databricks job entry points."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import TYPE_CHECKING, Any

from pyspark.sql import SparkSession

from databricks4py.logging import configure_logging, get_logger
from databricks4py.spark_session import active_fallback

if TYPE_CHECKING:
    from databricks4py.config import JobConfig
    from databricks4py.metrics import MetricsSink
    from databricks4py.quality.gate import QualityGate
    from databricks4py.retry import RetryConfig

__all__ = ["Workflow"]

logger = get_logger(__name__)


class Workflow(ABC):
    """Abstract base class for Databricks workflow entry points.

    Provides a structured pattern for job scripts that auto-initializes:

    - SparkSession (via :func:`~databricks4py.spark_session.active_fallback`)
    - Logging configuration
    - dbutils injection (optional)
    - Config, metrics, and retry integration (optional, v0.2+)

    Subclasses implement :meth:`run` with business logic.

    Example::

        class MyETL(Workflow):
            def run(self) -> None:
                df = self.spark.read.table("source")
                df.write.format("delta").saveAsTable("target")

        # As a CLI entry point:
        def main():
            import pyspark.dbutils
            MyETL(dbutils=pyspark.dbutils).execute()

    Args:
        spark: Optional SparkSession. Defaults to active session.
        dbutils: Optional dbutils module for secret/file operations.
        log_level: Logging level (default INFO).
        config: Optional JobConfig for table lookups and spark configs.
        metrics: Optional MetricsSink for lifecycle and custom metrics.
        retry_config: Optional RetryConfig for retrying run_at_time on failure.
    """

    def __init__(
        self,
        *,
        spark: SparkSession | None = None,
        dbutils: Any | None = None,
        log_level: int = logging.INFO,
        config: JobConfig | None = None,
        metrics: MetricsSink | None = None,
        retry_config: RetryConfig | None = None,
    ) -> None:
        configure_logging(level=log_level)
        self._spark = active_fallback(spark)
        self._dbutils: Any | None = None
        self._execution_time: datetime | None = None
        self._config = config
        self._metrics = metrics
        self._retry_config = retry_config

        if dbutils is not None:
            try:
                self._inject_dbutils(dbutils)
                self._dbutils = dbutils
            except Exception:
                logger.info("dbutils injection failed (running outside Databricks)")

    @staticmethod
    def _inject_dbutils(dbutils_module: Any) -> None:
        from databricks4py.secrets import SecretFetcher

        try:
            from databricks4py.io.dbfs import _set_dbutils_module

            SecretFetcher.dbutils = dbutils_module
            _set_dbutils_module(dbutils_module)
        except ImportError:
            SecretFetcher.dbutils = dbutils_module

    @property
    def spark(self) -> SparkSession:
        """The SparkSession for this workflow."""
        return self._spark

    @property
    def dbutils(self) -> Any | None:
        """The dbutils module, or None if not in Databricks."""
        return self._dbutils

    @property
    def execution_time(self) -> datetime:
        """The logical execution time (set by run_at_time, or now)."""
        return self._execution_time or datetime.now()

    @property
    def config(self) -> JobConfig | None:
        """The JobConfig for this workflow, or None."""
        return self._config

    @property
    def metrics(self) -> MetricsSink | None:
        """The MetricsSink for this workflow, or None."""
        return self._metrics

    @abstractmethod
    def run(self) -> None:
        """Execute the workflow business logic.

        Subclasses must implement this method.
        """
        ...

    def emit_metric(self, event_type: str, **kwargs: Any) -> None:
        """Emit a metric event. No-op if no metrics sink is configured."""
        if self._metrics is None:
            return
        from databricks4py.metrics.base import MetricEvent

        self._metrics.emit(
            MetricEvent(
                job_name=self.__class__.__name__,
                event_type=event_type,
                timestamp=datetime.now(),
                **kwargs,
            )
        )

    def quality_check(
        self,
        df: Any,
        gate: QualityGate,
        *,
        table_name: str | None = None,
    ) -> Any:
        """Run a quality gate on a DataFrame and enforce its policy.

        Emits a quality_check metric if a metrics sink is configured.
        Returns the original DataFrame if checks pass, or the enforced
        result (filtered/raised/warned) if they don't.
        """
        report = gate.check(df)
        self.emit_metric(
            "quality_check",
            table_name=table_name,
            metadata={"passed": report.passed, "checks": len(report.results)},
        )
        if not report.passed:
            return gate.enforce(df)
        return df

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
        """Standard entry point with logging, metrics, and error handling.

        Call this from ``if __name__ == "__main__"``.
        """
        workflow_name = self.__class__.__name__

        if self._config and self._config.spark_configs:
            for k, v in self._config.spark_configs.items():
                self._spark.conf.set(k, v)

        self.emit_metric("job_start")
        logger.info("Starting workflow: %s", workflow_name)
        start = datetime.now()

        try:
            if self._retry_config:
                from databricks4py.retry import retry

                retryable_run = retry(self._retry_config)(self.run_at_time)
                retryable_run()
            else:
                self.run_at_time()

            duration_ms = (datetime.now() - start).total_seconds() * 1000
            self.emit_metric("job_complete", duration_ms=duration_ms)
            logger.info("Workflow %s completed successfully", workflow_name)
        except Exception:
            duration_ms = (datetime.now() - start).total_seconds() * 1000
            self.emit_metric("job_failed", duration_ms=duration_ms)
            logger.exception("Workflow %s failed", workflow_name)
            raise
        finally:
            if self._metrics:
                self._metrics.flush()
