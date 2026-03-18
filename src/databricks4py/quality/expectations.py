"""Built-in expectation implementations."""

from __future__ import annotations

from functools import reduce
from typing import TYPE_CHECKING

from databricks4py.quality.base import Expectation, ExpectationResult

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

__all__ = ["ColumnExists", "InRange", "MatchesRegex", "NotNull", "RowCount", "Unique"]


class NotNull(Expectation):
    """Validates that specified columns contain no null values."""

    def __init__(self, *columns: str) -> None:
        self._columns = columns

    def validate(self, df: DataFrame) -> ExpectationResult:
        total = df.count()
        condition = self.failing_condition()
        failing = df.where(condition).count() if condition else 0
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
        failing = df.where(condition).count() if condition else 0
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

    def __init__(
        self, *, min_count: int | None = None, max_count: int | None = None
    ) -> None:
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
        return (
            f"RowCount(min_count={self._min_count!r}, max_count={self._max_count!r})"
        )


class MatchesRegex(Expectation):
    """Validates that a column's values match a regex pattern."""

    def __init__(self, column: str, pattern: str) -> None:
        self._column = column
        self._pattern = pattern

    def validate(self, df: DataFrame) -> ExpectationResult:
        total = df.count()
        condition = self.failing_condition()
        failing = df.where(condition).count() if condition else 0
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
