"""Built-in expectation implementations."""

from __future__ import annotations

from datetime import timedelta
from functools import reduce
from typing import TYPE_CHECKING

from databricks4py.quality.base import Expectation, ExpectationResult

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

__all__ = [
    "ColumnExists",
    "FreshnessExpectation",
    "InRange",
    "MatchesRegex",
    "NotNull",
    "RowCount",
    "Unique",
]


class NotNull(Expectation):
    """Validates that specified columns contain no null values."""

    def __init__(self, *columns: str) -> None:
        self._columns = columns

    def validate(self, df: DataFrame) -> ExpectationResult:
        total = df.count()
        condition = self.failing_condition()
        failing = df.where(condition).count() if condition is not None else 0
        return ExpectationResult(
            expectation=repr(self),
            passed=failing == 0,
            total_rows=total,
            failing_rows=failing,
        )

    def failing_condition(self) -> Column | None:
        from pyspark.sql import functions as F

        conditions = [F.col(c).isNull() for c in self._columns]
        return reduce(lambda a, b: a | b, conditions)

    def __repr__(self) -> str:
        cols = ", ".join(repr(c) for c in self._columns)
        return f"NotNull({cols})"


class InRange(Expectation):
    """Validates that a column's values fall within bounds."""

    def __init__(
        self, column: str, *, min_val: float | None = None, max_val: float | None = None
    ) -> None:
        self._column = column
        self._min_val = min_val
        self._max_val = max_val

    def validate(self, df: DataFrame) -> ExpectationResult:
        total = df.count()
        condition = self.failing_condition()
        failing = df.where(condition).count() if condition is not None else 0
        return ExpectationResult(
            expectation=repr(self),
            passed=failing == 0,
            total_rows=total,
            failing_rows=failing,
        )

    def failing_condition(self) -> Column | None:
        from pyspark.sql import functions as F

        conditions = []
        col = F.col(self._column)
        if self._min_val is not None:
            conditions.append(col < self._min_val)
        if self._max_val is not None:
            conditions.append(col > self._max_val)
        if not conditions:
            return None
        return reduce(lambda a, b: a | b, conditions)

    def __repr__(self) -> str:
        return f"InRange({self._column!r}, min_val={self._min_val!r}, max_val={self._max_val!r})"


class Unique(Expectation):
    """Validates no duplicate rows exist for the specified columns."""

    def __init__(self, *columns: str) -> None:
        self._columns = columns

    def validate(self, df: DataFrame) -> ExpectationResult:
        total = df.count()
        distinct = df.select(*self._columns).distinct().count()
        failing = total - distinct
        return ExpectationResult(
            expectation=repr(self),
            passed=failing == 0,
            total_rows=total,
            failing_rows=failing,
        )

    def __repr__(self) -> str:
        cols = ", ".join(repr(c) for c in self._columns)
        return f"Unique({cols})"


class RowCount(Expectation):
    """Validates that the DataFrame row count falls within bounds."""

    def __init__(self, *, min_count: int | None = None, max_count: int | None = None) -> None:
        self._min_count = min_count
        self._max_count = max_count

    def validate(self, df: DataFrame) -> ExpectationResult:
        total = df.count()
        passed = True
        if self._min_count is not None and total < self._min_count:
            passed = False
        if self._max_count is not None and total > self._max_count:
            passed = False
        return ExpectationResult(
            expectation=repr(self),
            passed=passed,
            total_rows=total,
        )

    def __repr__(self) -> str:
        return f"RowCount(min_count={self._min_count!r}, max_count={self._max_count!r})"


class MatchesRegex(Expectation):
    """Validates that a column's values match a regex pattern."""

    def __init__(self, column: str, pattern: str) -> None:
        self._column = column
        self._pattern = pattern

    def validate(self, df: DataFrame) -> ExpectationResult:
        total = df.count()
        condition = self.failing_condition()
        failing = df.where(condition).count() if condition is not None else 0
        return ExpectationResult(
            expectation=repr(self),
            passed=failing == 0,
            total_rows=total,
            failing_rows=failing,
        )

    def failing_condition(self) -> Column | None:
        from pyspark.sql import functions as F

        return ~F.col(self._column).rlike(self._pattern)

    def __repr__(self) -> str:
        return f"MatchesRegex({self._column!r}, {self._pattern!r})"


class ColumnExists(Expectation):
    """Validates that specified columns exist in the DataFrame schema."""

    def __init__(self, *columns: str, dtype: str | None = None) -> None:
        self._columns = columns
        self._dtype = dtype

    def validate(self, df: DataFrame) -> ExpectationResult:
        total = df.count()
        actual_cols = set(df.columns)
        missing = [c for c in self._columns if c not in actual_cols]

        if not missing and self._dtype:
            schema_fields = {f.name: f.dataType.simpleString() for f in df.schema.fields}
            for c in self._columns:
                if schema_fields.get(c) != self._dtype:
                    missing.append(c)

        return ExpectationResult(
            expectation=repr(self),
            passed=len(missing) == 0,
            total_rows=total,
            failing_rows=len(missing),
        )

    def __repr__(self) -> str:
        cols = ", ".join(repr(c) for c in self._columns)
        if self._dtype:
            return f"ColumnExists({cols}, dtype={self._dtype!r})"
        return f"ColumnExists({cols})"


class FreshnessExpectation(Expectation):
    """Validates that a timestamp column contains sufficiently recent data.

    The table passes when ``max(column) >= now - max_age``. The failing condition
    identifies individual rows older than the cutoff, which is useful for quarantine
    routing via :class:`~databricks4py.quality.gate.QualityGate`.

    Args:
        column: Timestamp column to check.
        max_age: Maximum allowed age of the most-recent row. A ``timedelta(hours=1)``
            means the newest row must be within the last hour.

    Example::

        from datetime import timedelta
        from databricks4py.quality.expectations import FreshnessExpectation

        expectation = FreshnessExpectation("event_time", max_age=timedelta(hours=6))
        result = expectation.validate(df)
        if not result.passed:
            print(result)
    """

    def __init__(self, column: str, max_age: timedelta) -> None:
        self._column = column
        self._max_age = max_age

    def validate(self, df: DataFrame) -> ExpectationResult:
        from datetime import datetime, timezone

        from pyspark.sql import functions as F

        total = df.count()
        if total == 0:
            return ExpectationResult(
                expectation=repr(self),
                passed=False,
                total_rows=0,
                failing_rows=0,
            )

        cutoff = datetime.now(tz=timezone.utc) - self._max_age

        max_row = df.agg(F.max(F.col(self._column)).alias("max_ts")).first()
        max_ts = max_row["max_ts"] if max_row else None

        if max_ts is None:
            return ExpectationResult(
                expectation=repr(self),
                passed=False,
                total_rows=total,
                failing_rows=0,
            )

        max_ts_utc = max_ts.replace(tzinfo=timezone.utc) if max_ts.tzinfo is None else max_ts
        passed = max_ts_utc >= cutoff

        # Use the same cutoff computed above so row-count and table-level verdict are consistent
        stale_condition = F.col(self._column) < F.lit(cutoff)
        failing = df.where(stale_condition).count()

        return ExpectationResult(
            expectation=repr(self),
            passed=passed,
            total_rows=total,
            failing_rows=failing,
        )

    def failing_condition(self) -> Column | None:
        from datetime import datetime, timezone

        from pyspark.sql import functions as F

        cutoff = datetime.now(tz=timezone.utc) - self._max_age
        return F.col(self._column) < F.lit(cutoff)

    def __repr__(self) -> str:
        return f"FreshnessExpectation({self._column!r}, max_age={self._max_age!r})"
