"""DataFrame filter abstractions."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence

from pyspark.sql import DataFrame

__all__ = [
    "ColumnFilter",
    "DropDuplicates",
    "Filter",
    "FilterPipeline",
    "WhereFilter",
]


class Filter(ABC):
    """Abstract base class for DataFrame filters.

    Subclasses implement :meth:`apply` to transform a DataFrame.
    Filters are callable — ``filter(df)`` is equivalent to ``filter.apply(df)``.

    Example::

        class ActiveOnly(Filter):
            def apply(self, df: DataFrame) -> DataFrame:
                return df.where("is_active = true")

        df = ActiveOnly()(raw_df)
    """

    @abstractmethod
    def apply(self, df: DataFrame) -> DataFrame:
        """Apply the filter to a DataFrame.

        Args:
            df: Input DataFrame.

        Returns:
            Filtered DataFrame (schema should be unchanged or a subset).
        """
        ...

    def __call__(self, df: DataFrame) -> DataFrame:
        return self.apply(df)


class FilterPipeline(Filter):
    """Chain of filters applied sequentially.

    Example::

        pipeline = FilterPipeline([
            DropDuplicates(),
            WhereFilter("score > 50"),
            ColumnFilter(["id", "name"]),
        ])
        result = pipeline(df)
    """

    def __init__(self, filters: Sequence[Filter] | None = None) -> None:
        self._filters: list[Filter] = list(filters or [])

    def add(self, f: Filter) -> None:
        """Add a filter to the pipeline."""
        self._filters.append(f)

    def apply(self, df: DataFrame) -> DataFrame:
        for f in self._filters:
            df = f(df)
        return df

    def __len__(self) -> int:
        return len(self._filters)

    def __repr__(self) -> str:
        return f"FilterPipeline({self._filters!r})"


class DropDuplicates(Filter):
    """Remove duplicate rows.

    Args:
        subset: Optional column names to consider for deduplication.
            If None, all columns are used.
    """

    def __init__(self, subset: Sequence[str] | None = None) -> None:
        self._subset = list(subset) if subset else None

    def apply(self, df: DataFrame) -> DataFrame:
        if self._subset:
            return df.dropDuplicates(self._subset)
        return df.dropDuplicates()

    def __repr__(self) -> str:
        return f"DropDuplicates(subset={self._subset!r})"


class ColumnFilter(Filter):
    """Select specific columns from a DataFrame.

    Args:
        columns: Column names to keep.

    Raises:
        ValueError: If columns is empty.
    """

    def __init__(self, columns: Sequence[str]) -> None:
        if not columns:
            raise ValueError("ColumnFilter requires at least one column")
        self._columns = list(columns)

    def apply(self, df: DataFrame) -> DataFrame:
        return df.select(*self._columns)

    def __repr__(self) -> str:
        return f"ColumnFilter(columns={self._columns!r})"


class WhereFilter(Filter):
    """Apply a SQL WHERE condition.

    Args:
        condition: SQL condition string (e.g. ``"age > 18"``).
    """

    def __init__(self, condition: str) -> None:
        self._condition = condition

    def apply(self, df: DataFrame) -> DataFrame:
        return df.where(self._condition)

    def __repr__(self) -> str:
        return f"WhereFilter({self._condition!r})"
