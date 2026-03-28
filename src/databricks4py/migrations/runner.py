"""Ordered, idempotent migration runner for Delta Lake tables."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType, StringType, StructField, StructType, TimestampType

from databricks4py.spark_session import active_fallback

__all__ = ["MigrationRunResult", "MigrationRunner", "MigrationStep"]

logger = logging.getLogger(__name__)

_HISTORY_SCHEMA = StructType(
    [
        StructField("version", StringType(), False),
        StructField("description", StringType(), True),
        StructField("applied_at", TimestampType(), False),
        StructField("success", BooleanType(), False),
        StructField("error_message", StringType(), True),
    ]
)


@dataclass(frozen=True)
class MigrationStep:
    """A single versioned migration step.

    Steps are sorted by ``version`` before execution, so lexicographic ordering
    determines the run order. Use a fixed-width prefix (``"V001"``, ``"V002"``, etc.)
    to keep ordering stable as the number of steps grows.

    Args:
        version: Unique version string. Determines execution order.
        description: Human-readable description stored in the history table.
        up: Callable that receives a SparkSession and applies the migration.
            May execute any Spark SQL, Delta, or Python logic.
        pre_validate: Optional guard — called before ``up``. Return ``False``
            to abort the step with a ``MigrationError``.
        post_validate: Optional check — called after ``up``. Return ``False``
            to mark the step as failed and halt the run.
    """

    version: str
    description: str
    up: Callable[[SparkSession], None]
    pre_validate: Callable[[SparkSession], bool] | None = None
    post_validate: Callable[[SparkSession], bool] | None = None


@dataclass
class MigrationRunResult:
    """Summary of a :meth:`MigrationRunner.run` execution.

    Attributes:
        applied: Versions applied in this run, in execution order.
        skipped: Versions already recorded as applied (idempotent skips).
        failed: The version that caused a failure, if any.
        dry_run: Whether this was a dry-run (no changes written).
    """

    applied: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    failed: str | None = None
    dry_run: bool = False


class MigrationRunner:
    """Ordered, idempotent migration runner for Delta Lake.

    Tracks applied versions in a Delta history table so each step runs exactly
    once across all environments. Steps execute in lexicographic version order.

    Example::

        def add_audit_columns(spark: SparkSession) -> None:
            spark.sql(
                "ALTER TABLE catalog.schema.events "
                "ADD COLUMNS (created_at TIMESTAMP, updated_at TIMESTAMP)"
            )

        runner = MigrationRunner(
            history_table="catalog.schema._migration_history",
        )
        runner.register(
            MigrationStep(
                version="V001",
                description="Add audit columns to events",
                up=add_audit_columns,
            )
        )
        result = runner.run()

    Args:
        history_table: Fully qualified Delta table name used to record applied steps.
            Created automatically on first use.
        spark: Optional SparkSession.
    """

    def __init__(
        self,
        history_table: str,
        *,
        spark: SparkSession | None = None,
    ) -> None:
        self._spark = active_fallback(spark)
        self._history_table = history_table
        self._steps: list[MigrationStep] = []
        self._ensure_history_table()

    def _ensure_history_table(self) -> None:
        self._spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self._history_table} (
                version     STRING    NOT NULL,
                description STRING,
                applied_at  TIMESTAMP NOT NULL,
                success     BOOLEAN   NOT NULL,
                error_message STRING
            )
            USING DELTA
            """
        )

    def _applied_versions(self) -> set[str]:
        rows = self._spark.sql(
            f"SELECT version FROM {self._history_table} WHERE success = true"
        ).collect()
        return {row["version"] for row in rows}

    def _record(
        self,
        step: MigrationStep,
        *,
        success: bool,
        error_message: str | None = None,
    ) -> None:
        from pyspark.sql import Row

        row = Row(
            version=step.version,
            description=step.description,
            applied_at=datetime.now(tz=timezone.utc),
            success=success,
            error_message=error_message,
        )
        (
            self._spark.createDataFrame([row], schema=_HISTORY_SCHEMA)
            .write.format("delta")
            .mode("append")
            .saveAsTable(self._history_table)
        )

    def register(self, *steps: MigrationStep) -> MigrationRunner:
        """Add one or more migration steps. Returns self for chaining.

        Steps can be registered in any call order — execution order is always
        determined by ``version``, not registration order.
        """
        self._steps.extend(steps)
        return self

    def pending(self) -> list[MigrationStep]:
        """Return steps not yet applied, sorted by version."""
        applied = self._applied_versions()
        return sorted(
            (s for s in self._steps if s.version not in applied),
            key=lambda s: s.version,
        )

    def applied(self) -> list[str]:
        """Return successfully applied versions in sorted order."""
        return sorted(self._applied_versions())

    def run(self, *, dry_run: bool = False) -> MigrationRunResult:
        """Run all pending migration steps in version order.

        Steps already in the history table are skipped (idempotent). Execution
        halts on the first failure — the failed version is recorded with
        ``success=False`` and returned in :attr:`MigrationRunResult.failed`.

        Args:
            dry_run: Log what would run without executing or writing history.

        Returns:
            MigrationRunResult summarising applied, skipped, and failed steps.

        Raises:
            MigrationError: If a step's pre-validation returns False.
        """
        from databricks4py.migrations.validators import MigrationError

        result = MigrationRunResult(dry_run=dry_run)
        applied_set = self._applied_versions()
        all_sorted = sorted(self._steps, key=lambda s: s.version)

        for step in all_sorted:
            if step.version in applied_set:
                result.skipped.append(step.version)
                logger.debug("Skipping already-applied step %s", step.version)
                continue

            logger.info("Running migration %s: %s", step.version, step.description)

            if step.pre_validate is not None and not step.pre_validate(self._spark):
                raise MigrationError(
                    step.version,
                    [f"Pre-validation failed for step {step.version}: {step.description}"],
                )

            if dry_run:
                logger.info("[dry-run] Would apply %s: %s", step.version, step.description)
                result.applied.append(step.version)
                continue

            try:
                step.up(self._spark)
            except Exception as exc:
                error_msg = str(exc)
                logger.error("Migration %s failed: %s", step.version, error_msg)
                self._record(step, success=False, error_message=error_msg)
                result.failed = step.version
                return result

            if step.post_validate is not None and not step.post_validate(self._spark):
                error_msg = f"Post-validation failed for step {step.version}"
                logger.error(error_msg)
                self._record(step, success=False, error_message=error_msg)
                result.failed = step.version
                return result

            self._record(step, success=True)
            result.applied.append(step.version)
            logger.info("Applied migration %s", step.version)

        return result
