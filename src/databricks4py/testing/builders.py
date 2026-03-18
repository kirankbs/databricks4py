"""Fluent builder for test DataFrames."""

from __future__ import annotations

import random
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

__all__ = ["DataFrameBuilder"]

_TYPE_MAP: dict[str, DataType] = {
    "string": StringType(),
    "int": IntegerType(),
    "integer": IntegerType(),
    "long": LongType(),
    "bigint": LongType(),
    "short": ShortType(),
    "smallint": ShortType(),
    "float": FloatType(),
    "double": DoubleType(),
    "boolean": BooleanType(),
    "date": DateType(),
    "timestamp": TimestampType(),
}


def _resolve_type(type_str: str) -> DataType:
    resolved = _TYPE_MAP.get(type_str.lower())
    if resolved is not None:
        return resolved
    from pyspark.sql.types import _parse_datatype_string

    return _parse_datatype_string(type_str)


class DataFrameBuilder:
    """Fluent builder for constructing test DataFrames.

    Example::

        df = (
            DataFrameBuilder(spark)
            .with_columns({"id": "int", "name": "string"})
            .with_rows((1, "alice"), (2, "bob"))
            .build()
        )
    """

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark
        self._schema: StructType | None = None
        self._rows: list[tuple] = []

    def with_columns(self, schema: dict[str, str]) -> DataFrameBuilder:
        """Define columns from a dict of ``{name: type_string}``."""
        fields = [StructField(name, _resolve_type(t)) for name, t in schema.items()]
        self._schema = StructType(fields)
        return self

    def with_schema(self, schema: StructType) -> DataFrameBuilder:
        """Define columns from a StructType directly."""
        self._schema = schema
        return self

    def with_rows(self, *rows: tuple) -> DataFrameBuilder:
        """Add explicit data rows."""
        self._rows.extend(rows)
        return self

    def with_sequential(
        self, column: str, start: int = 1, count: int = 10
    ) -> DataFrameBuilder:
        """Generate sequential integer rows for a single column.

        If rows already exist, this replaces them.
        """
        if self._schema is None:
            self._schema = StructType([StructField(column, IntegerType())])
        self._rows = [(i,) for i in range(start, start + count)]
        return self

    def with_nulls(
        self, column: str, frequency: float = 0.1, *, seed: int | None = None
    ) -> DataFrameBuilder:
        """Inject nulls into a column at the given frequency.

        Must be called after rows are populated.
        """
        if self._schema is None:
            raise ValueError("Schema must be defined before injecting nulls")

        col_idx = next(
            (i for i, f in enumerate(self._schema.fields) if f.name == column), None
        )
        if col_idx is None:
            raise ValueError(f"Column {column!r} not found in schema")

        rng = random.Random(seed)
        new_rows: list[tuple[Any, ...]] = []
        for row in self._rows:
            row_list = list(row)
            if rng.random() < frequency:
                row_list[col_idx] = None
            new_rows.append(tuple(row_list))
        self._rows = new_rows
        return self

    def build(self) -> DataFrame:
        """Construct the DataFrame.

        Raises:
            ValueError: If no schema has been defined.
        """
        if self._schema is None:
            raise ValueError("No schema defined — call with_columns() or with_schema() first")
        return self._spark.createDataFrame(self._rows, schema=self._schema)
