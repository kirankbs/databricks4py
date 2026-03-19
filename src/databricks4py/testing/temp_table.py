"""Temporary Delta table context manager for testing."""

from __future__ import annotations

import uuid
from types import TracebackType
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType

from databricks4py.testing.builders import _resolve_type

__all__ = ["TempDeltaTable"]


class TempDeltaTable:
    """Context manager that creates a temporary Delta table and drops it on exit.

    Example::

        with TempDeltaTable(spark, schema={"id": "int"}, data=[(1,), (2,)]) as table:
            df = table.dataframe()
            assert df.count() == 2
    """

    def __init__(
        self,
        spark: SparkSession,
        *,
        table_name: str | None = None,
        schema: dict[str, str] | None = None,
        data: list[tuple[Any, ...]] | None = None,
    ) -> None:
        self._spark = spark
        self._table_name = table_name or f"tmp_{uuid.uuid4().hex[:12]}"
        self._schema = schema
        self._data = data
        self._delta_table: Any = None

    @property
    def table_name(self) -> str:
        return self._table_name

    def __enter__(self) -> Any:
        from databricks4py.io.delta import DeltaTable

        if self._schema is None:
            raise ValueError("schema is required to create a TempDeltaTable")

        struct = StructType(
            [StructField(name, _resolve_type(t)) for name, t in self._schema.items()]
        )

        self._delta_table = DeltaTable(
            table_name=self._table_name,
            schema=struct,
            spark=self._spark,
        )

        if self._data:
            df = self._spark.createDataFrame(self._data, schema=struct)
            self._delta_table.write(df, mode="overwrite")

        return self._delta_table

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self._spark.sql(f"DROP TABLE IF EXISTS {self._table_name}")
