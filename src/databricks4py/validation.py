"""DataFrame schema and structure validation.

Provides column-presence, column-absence, and schema validation with
custom exceptions. Includes decorators for validating function inputs
and outputs.
"""

from __future__ import annotations

import functools
from collections.abc import Callable, Sequence
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

__all__ = [
    "DataFrameMissingColumnError",
    "DataFrameProhibitedColumnError",
    "DataFrameSchemaError",
    "validate_absence_of_columns",
    "validate_input",
    "validate_output",
    "validate_presence_of_columns",
    "validate_schema",
]


class DataFrameValidationError(ValueError):
    """Base class for DataFrame validation errors."""


class DataFrameMissingColumnError(DataFrameValidationError):
    """Raised when required columns are missing from a DataFrame."""

    def __init__(self, missing: list[str]) -> None:
        self.missing_columns = missing
        super().__init__(f"Missing columns: {missing}")


class DataFrameProhibitedColumnError(DataFrameValidationError):
    """Raised when prohibited columns are present in a DataFrame."""

    def __init__(self, present: list[str]) -> None:
        self.prohibited_columns = present
        super().__init__(f"Prohibited columns found: {present}")


class DataFrameSchemaError(DataFrameValidationError):
    """Raised when a DataFrame's schema doesn't match expectations."""

    def __init__(self, message: str, missing_fields: list[str] | None = None) -> None:
        self.missing_fields = missing_fields or []
        super().__init__(message)


def validate_presence_of_columns(df: DataFrame, required: Sequence[str]) -> None:
    """Validate that all required columns exist in the DataFrame.

    Args:
        df: DataFrame to check.
        required: Column names that must be present.

    Raises:
        DataFrameMissingColumnError: If any required columns are missing.
    """
    actual = set(df.columns)
    missing = [c for c in required if c not in actual]
    if missing:
        raise DataFrameMissingColumnError(missing)


def validate_absence_of_columns(df: DataFrame, prohibited: Sequence[str]) -> None:
    """Validate that no prohibited columns exist in the DataFrame.

    Args:
        df: DataFrame to check.
        prohibited: Column names that must NOT be present.

    Raises:
        DataFrameProhibitedColumnError: If any prohibited columns are found.
    """
    actual = set(df.columns)
    present = [c for c in prohibited if c in actual]
    if present:
        raise DataFrameProhibitedColumnError(present)


def validate_schema(
    df: DataFrame,
    expected: StructType,
    *,
    ignore_nullable: bool = False,
) -> None:
    """Validate that a DataFrame's schema contains all expected fields.

    Checks that every field in ``expected`` exists in the DataFrame with
    the correct data type. Extra columns in the DataFrame are allowed.

    Args:
        df: DataFrame to check.
        expected: Expected schema (all fields must be present).
        ignore_nullable: If True, skip nullable comparison.

    Raises:
        DataFrameSchemaError: If the schema doesn't match.
    """
    actual_fields = {f.name: f for f in df.schema.fields}
    missing = []
    type_mismatches = []

    for field in expected.fields:
        if field.name not in actual_fields:
            missing.append(field.name)
            continue

        actual_field = actual_fields[field.name]
        if actual_field.dataType != field.dataType:
            type_mismatches.append(
                f"{field.name}: expected {field.dataType.simpleString()}, "
                f"got {actual_field.dataType.simpleString()}"
            )
        elif not ignore_nullable and actual_field.nullable != field.nullable:
            type_mismatches.append(
                f"{field.name}: expected nullable={field.nullable}, "
                f"got nullable={actual_field.nullable}"
            )

    errors = []
    if missing:
        errors.append(f"Missing fields: {missing}")
    if type_mismatches:
        errors.append("Type mismatches: " + "; ".join(type_mismatches))

    if errors:
        raise DataFrameSchemaError(". ".join(errors), missing_fields=missing)


def validate_input(
    *,
    required_columns: Sequence[str] | None = None,
    prohibited_columns: Sequence[str] | None = None,
    schema: StructType | None = None,
    ignore_nullable: bool = False,
    df_arg: str | int = 0,
) -> Callable:
    """Decorator that validates the input DataFrame of a function.

    Applies validation before the function runs. The DataFrame argument
    is identified by ``df_arg`` (positional index or keyword name).

    Args:
        required_columns: Columns that must be present.
        prohibited_columns: Columns that must NOT be present.
        schema: Expected StructType (partial match — extra cols OK).
        ignore_nullable: If True, skip nullable check in schema validation.
        df_arg: Positional index (int) or keyword name (str) of the DataFrame argument.

    Example::

        @validate_input(required_columns=["id", "name"])
        def process(df: DataFrame) -> DataFrame:
            return df.select("id", "name")
    """

    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            df = _extract_df(args, kwargs, df_arg)
            if required_columns:
                validate_presence_of_columns(df, required_columns)
            if prohibited_columns:
                validate_absence_of_columns(df, prohibited_columns)
            if schema:
                validate_schema(df, schema, ignore_nullable=ignore_nullable)
            return fn(*args, **kwargs)

        return wrapper

    return decorator


def validate_output(
    *,
    required_columns: Sequence[str] | None = None,
    prohibited_columns: Sequence[str] | None = None,
    schema: StructType | None = None,
    ignore_nullable: bool = False,
) -> Callable:
    """Decorator that validates the output DataFrame of a function.

    Applies validation after the function returns. The return value
    must be a DataFrame.

    Args:
        required_columns: Columns that must be present in the output.
        prohibited_columns: Columns that must NOT be present in the output.
        schema: Expected StructType for the output.
        ignore_nullable: If True, skip nullable check in schema validation.

    Example::

        @validate_output(required_columns=["id", "name", "score"])
        def enrich(df: DataFrame) -> DataFrame:
            return df.withColumn("score", lit(0))
    """

    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            result = fn(*args, **kwargs)
            if not isinstance(result, DataFrame):
                raise TypeError(
                    f"validate_output expects a DataFrame return, got {type(result).__name__}"
                )
            if required_columns:
                validate_presence_of_columns(result, required_columns)
            if prohibited_columns:
                validate_absence_of_columns(result, prohibited_columns)
            if schema:
                validate_schema(result, schema, ignore_nullable=ignore_nullable)
            return result

        return wrapper

    return decorator


def _extract_df(args: tuple, kwargs: dict, df_arg: str | int) -> DataFrame:
    """Extract the DataFrame from function arguments."""
    if isinstance(df_arg, str):
        if df_arg in kwargs:
            return kwargs[df_arg]
        raise TypeError(f"Expected keyword argument '{df_arg}' not found")

    if isinstance(df_arg, int):
        if df_arg < len(args):
            return args[df_arg]
        raise TypeError(f"Expected positional argument at index {df_arg} not found")

    raise TypeError(f"df_arg must be str or int, got {type(df_arg).__name__}")
