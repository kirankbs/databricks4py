"""Core quality abstractions: expectations, results, and reports."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, Row

__all__ = ["Expectation", "ExpectationResult", "QualityReport"]


@dataclass(frozen=True)
class ExpectationResult:
    """Result of a single expectation check."""

    expectation: str
    passed: bool
    total_rows: int
    failing_rows: int = 0
    sample: list[Row] | None = None


@dataclass(frozen=True)
class QualityReport:
    """Aggregated results from running multiple expectations."""

    results: list[ExpectationResult]
    passed: bool

    def summary(self) -> str:
        lines = []
        for r in self.results:
            status = "PASS" if r.passed else "FAIL"
            lines.append(f"[{status}] {r.expectation} ({r.failing_rows}/{r.total_rows} failing)")
        overall = "PASSED" if self.passed else "FAILED"
        lines.append(f"Overall: {overall}")
        return "\n".join(lines)


class Expectation(ABC):
    """Abstract base for DataFrame quality expectations."""

    @abstractmethod
    def validate(self, df: DataFrame) -> ExpectationResult: ...

    def failing_condition(self) -> Column | None:
        """Return a Column expression that is True for failing rows.

        Returns None for aggregate checks that can't filter individual rows.
        """
        return None
