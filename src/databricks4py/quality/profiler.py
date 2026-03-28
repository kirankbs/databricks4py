"""Lightweight column-level data profiler for Spark DataFrames."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

__all__ = ["ColumnProfile", "DataProfile", "profile"]

_NUMERIC_TYPES = frozenset({"byte", "short", "int", "bigint", "float", "double", "decimal"})


@dataclass(frozen=True)
class ColumnProfile:
    """Per-column statistics."""

    name: str
    dtype: str
    total_count: int
    null_count: int
    null_pct: float
    distinct_count: int
    min_value: str | None = None
    max_value: str | None = None
    mean: float | None = None


@dataclass(frozen=True)
class DataProfile:
    """Collection of column profiles for a DataFrame."""

    columns: list[ColumnProfile]
    row_count: int
    column_count: int

    def summary(self) -> str:
        header = f"{'Column':<25} {'Type':<12} {'Nulls':>8} {'Null%':>7} {'Distinct':>10}"
        header += f" {'Min':>15} {'Max':>15} {'Mean':>12}"
        lines = [header, "-" * len(header)]
        for c in self.columns:
            min_v = str(c.min_value) if c.min_value is not None else ""
            max_v = str(c.max_value) if c.max_value is not None else ""
            mean_v = f"{c.mean:.4f}" if c.mean is not None else ""
            lines.append(
                f"{c.name:<25} {c.dtype:<12} {c.null_count:>8} {c.null_pct:>6.1f}%"
                f" {c.distinct_count:>10} {min_v:>15} {max_v:>15} {mean_v:>12}"
            )
        lines.append(f"\nRows: {self.row_count}  Columns: {self.column_count}")
        return "\n".join(lines)


def _is_numeric(dtype_str: str) -> bool:
    base = dtype_str.split("(")[0].lower()
    return base in _NUMERIC_TYPES


def profile(df: DataFrame) -> DataProfile:
    """Profile all columns in a DataFrame with a single aggregation pass."""
    from pyspark.sql import functions as F

    dtypes = dict(df.dtypes)
    col_names = df.columns
    row_count = df.count()

    if row_count == 0:
        profiles = [
            ColumnProfile(
                name=col,
                dtype=dtypes[col],
                total_count=0,
                null_count=0,
                null_pct=0.0,
                distinct_count=0,
            )
            for col in col_names
        ]
        return DataProfile(columns=profiles, row_count=0, column_count=len(col_names))

    agg_exprs = []
    for col in col_names:
        agg_exprs.extend(
            [
                F.count(F.when(F.col(col).isNull(), 1)).alias(f"{col}__null_count"),
                F.approx_count_distinct(F.col(col)).alias(f"{col}__distinct"),
                F.min(F.col(col).cast("string")).alias(f"{col}__min"),
                F.max(F.col(col).cast("string")).alias(f"{col}__max"),
            ]
        )
        if _is_numeric(dtypes[col]):
            agg_exprs.append(F.avg(F.col(col).cast("double")).alias(f"{col}__mean"))

    result = df.agg(*agg_exprs).first()

    profiles = []
    for col in col_names:
        null_count = result[f"{col}__null_count"]
        null_pct = (null_count / row_count) * 100.0
        distinct_count = result[f"{col}__distinct"]

        min_val = result[f"{col}__min"]
        max_val = result[f"{col}__max"]

        mean = None
        if _is_numeric(dtypes[col]):
            mean = result[f"{col}__mean"]

        profiles.append(
            ColumnProfile(
                name=col,
                dtype=dtypes[col],
                total_count=row_count,
                null_count=null_count,
                null_pct=null_pct,
                distinct_count=distinct_count,
                min_value=min_val,
                max_value=max_val,
                mean=mean,
            )
        )

    return DataProfile(columns=profiles, row_count=row_count, column_count=len(col_names))
