"""DataFrame and schema assertion helpers for testing."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

__all__ = ["assert_frame_equal", "assert_schema_equal"]


def assert_schema_equal(
    actual: StructType,
    expected: StructType,
    *,
    check_nullable: bool = False,
) -> None:
    """Compare two StructTypes field-by-field with a clear diff on mismatch.

    Args:
        actual: The actual schema.
        expected: The expected schema.
        check_nullable: Whether to compare nullable flags (default False).

    Raises:
        AssertionError: If the schemas differ.
    """
    actual_fields = actual.fields
    expected_fields = expected.fields

    if len(actual_fields) != len(expected_fields):
        raise AssertionError(
            f"Schema length mismatch: actual has {len(actual_fields)} fields, "
            f"expected {len(expected_fields)}"
        )

    for i, (a, e) in enumerate(zip(actual_fields, expected_fields, strict=True)):
        if a.name != e.name:
            raise AssertionError(
                f"Field {i}: name mismatch — actual={a.name!r}, expected={e.name!r}"
            )
        if a.dataType != e.dataType:
            raise AssertionError(
                f"Field {a.name!r}: type mismatch — actual={a.dataType}, expected={e.dataType}"
            )
        if check_nullable and a.nullable != e.nullable:
            raise AssertionError(
                f"Field {a.name!r}: nullable mismatch — "
                f"actual={a.nullable}, expected={e.nullable}"
            )


def assert_frame_equal(
    actual: DataFrame,
    expected: DataFrame,
    *,
    check_order: bool = False,
    check_schema: bool = True,
    check_nullable: bool = False,
) -> None:
    """Assert two DataFrames are equal.

    Uses Spark 3.5's ``assertDataFrameEqual`` when available, otherwise
    falls back to a manual row-by-row comparison.

    Args:
        actual: The actual DataFrame.
        expected: The expected DataFrame.
        check_order: Whether row order matters (default False).
        check_schema: Whether to compare schemas (default True).
        check_nullable: Whether to compare nullable flags (default False).

    Raises:
        AssertionError: If the DataFrames differ.
    """
    if check_schema:
        assert_schema_equal(actual.schema, expected.schema, check_nullable=check_nullable)

    try:
        from pyspark.testing.utils import assertDataFrameEqual

        assertDataFrameEqual(actual, expected, checkRowOrder=check_order)
    except ImportError:
        _manual_frame_compare(actual, expected, check_order=check_order)


def _manual_frame_compare(
    actual: DataFrame,
    expected: DataFrame,
    *,
    check_order: bool,
) -> None:
    actual_rows = actual.collect()
    expected_rows = expected.collect()

    if len(actual_rows) != len(expected_rows):
        raise AssertionError(
            f"Row count mismatch: actual={len(actual_rows)}, expected={len(expected_rows)}"
        )

    if check_order:
        for i, (a, e) in enumerate(zip(actual_rows, expected_rows, strict=True)):
            if a != e:
                raise AssertionError(f"Row {i} mismatch:\n  actual:   {a}\n  expected: {e}")
    else:
        actual_sorted = sorted(actual_rows, key=str)
        expected_sorted = sorted(expected_rows, key=str)
        if actual_sorted != expected_sorted:
            raise AssertionError(
                f"DataFrames differ (ignoring order):\n"
                f"  actual:   {actual_sorted}\n"
                f"  expected: {expected_sorted}"
            )
