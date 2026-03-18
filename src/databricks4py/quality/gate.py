"""Quality gate: orchestrate expectations and enforce data quality."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Literal

from databricks4py.quality.base import Expectation, QualityReport

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

__all__ = ["QualityError", "QualityGate"]

logger = logging.getLogger(__name__)


class QualityError(Exception):
    """Raised when a quality gate fails in 'raise' mode."""

    def __init__(self, report: QualityReport) -> None:
        self.report = report
        super().__init__(f"Quality check failed:\n{report.summary()}")


class QualityGate:
    """Runs expectations against a DataFrame and enforces quality policy.

    Args:
        *expectations: Expectation instances to evaluate.
        on_fail: Action on failure: "raise", "warn", or "quarantine".
        quarantine_handler: Callable receiving the bad-rows DataFrame.
            Required when on_fail="quarantine".
    """

    def __init__(
        self,
        *expectations: Expectation,
        on_fail: Literal["raise", "warn", "quarantine"] = "raise",
        quarantine_handler: Callable[[DataFrame], None] | None = None,
    ) -> None:
        if on_fail == "quarantine" and quarantine_handler is None:
            raise ValueError(
                "quarantine_handler is required when on_fail='quarantine'"
            )
        self._expectations = expectations
        self._on_fail = on_fail
        self._quarantine_handler = quarantine_handler

    def check(self, df: DataFrame) -> QualityReport:
        """Run all expectations and return a report."""
        results = [exp.validate(df) for exp in self._expectations]
        passed = all(r.passed for r in results)
        return QualityReport(results=results, passed=passed)

    def enforce(self, df: DataFrame) -> DataFrame:
        """Run expectations and enforce the configured failure policy.

        Returns the clean DataFrame (original if all pass, or filtered
        if quarantine mode splits out bad rows).
        """
        report = self.check(df)
        if report.passed:
            return df

        if self._on_fail == "raise":
            raise QualityError(report)

        if self._on_fail == "warn":
            logger.warning("Quality check failed:\n%s", report.summary())
            return df

        # quarantine: split bad rows using failing_condition()
        from functools import reduce

        conditions = []
        for exp in self._expectations:
            cond = exp.failing_condition()
            if cond is not None:
                conditions.append(cond)

        if conditions:
            bad_condition = reduce(lambda a, b: a | b, conditions)
            bad_rows = df.where(bad_condition)
            clean_rows = df.where(~bad_condition)
            self._quarantine_handler(bad_rows)  # type: ignore[misc]
            return clean_rows

        # no row-level conditions available — quarantine nothing
        logger.warning(
            "Quarantine requested but no row-level failing conditions available"
        )
        return df
