"""Column utilities and DataFrame transforms."""

from __future__ import annotations

import re
from collections.abc import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType

__all__ = [
    "flatten_struct",
    "prefix_columns",
    "single_space",
    "snake_case_columns",
    "suffix_columns",
    "trim_all",
]

_CAMEL_BOUNDARY = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_NON_ALNUM = re.compile(r"[^a-z0-9]+")
_MULTI_UNDERSCORE = re.compile(r"_{2,}")


def _to_snake(name: str) -> str:
    # camelCase / PascalCase boundaries
    result = _CAMEL_BOUNDARY.sub("_", name)
    # replace non-alphanumeric runs (spaces, hyphens, dots, etc.) with underscore
    result = _NON_ALNUM.sub("_", result.lower())
    # collapse consecutive underscores and strip leading/trailing
    result = _MULTI_UNDERSCORE.sub("_", result).strip("_")
    return result


def snake_case_columns(df: DataFrame) -> DataFrame:
    """Convert all column names to snake_case."""
    for col_name in df.columns:
        converted = _to_snake(col_name)
        if converted != col_name:
            df = df.withColumnRenamed(col_name, converted)
    return df


def prefix_columns(
    df: DataFrame,
    prefix: str,
    *,
    exclude: Sequence[str] | None = None,
) -> DataFrame:
    """Add a prefix to all column names, optionally excluding some."""
    excluded = set(exclude or [])
    for col_name in df.columns:
        if col_name not in excluded:
            df = df.withColumnRenamed(col_name, f"{prefix}{col_name}")
    return df


def suffix_columns(
    df: DataFrame,
    suffix: str,
    *,
    exclude: Sequence[str] | None = None,
) -> DataFrame:
    """Add a suffix to all column names, optionally excluding some."""
    excluded = set(exclude or [])
    for col_name in df.columns:
        if col_name not in excluded:
            df = df.withColumnRenamed(col_name, f"{col_name}{suffix}")
    return df


def _flatten_fields(
    schema: StructType,
    col_prefix: str,
    alias_prefix: str,
    separator: str,
) -> list[tuple[str, str]]:
    """Return (dotted_path, alias) pairs for every leaf field in the schema."""
    result: list[tuple[str, str]] = []
    for field in schema.fields:
        col_path = f"{col_prefix}.{field.name}" if col_prefix else field.name
        alias = f"{alias_prefix}{separator}{field.name}" if alias_prefix else field.name
        if isinstance(field.dataType, StructType):
            result.extend(_flatten_fields(field.dataType, col_path, alias, separator))
        else:
            result.append((col_path, alias))
    return result


def flatten_struct(df: DataFrame, *, separator: str = "_") -> DataFrame:
    """Recursively flatten nested struct columns into top-level columns."""
    cols = _flatten_fields(df.schema, "", "", separator)
    return df.select([F.col(path).alias(alias) for path, alias in cols])


def single_space(df: DataFrame, *columns: str) -> DataFrame:
    """Collapse runs of whitespace into a single space and trim the result."""
    for col_name in columns:
        df = df.withColumn(col_name, F.trim(F.regexp_replace(F.col(col_name), r"\s+", " ")))
    return df


def trim_all(df: DataFrame) -> DataFrame:
    """Trim leading/trailing whitespace from all string columns."""
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, F.trim(F.col(field.name)))
    return df
