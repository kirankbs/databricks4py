"""Tests for TempDeltaTable."""

from __future__ import annotations

import pyspark.sql
import pytest
from pyspark.sql.utils import AnalysisException

from databricks4py.io.delta import DeltaTable
from databricks4py.testing.temp_table import TempDeltaTable


@pytest.mark.integration
class TestTempDeltaTable:
    def test_creates_and_drops(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        table_name = "test_temp_creates_drops"
        with TempDeltaTable(
            spark_session_function,
            table_name=table_name,
            schema={"id": "int"},
            data=[(1,), (2,)],
        ) as table:
            assert table.dataframe().count() == 2

        # Table should be dropped after exiting context
        with pytest.raises(AnalysisException):
            spark_session_function.read.table(table_name)

    def test_auto_generates_table_name(
        self, spark_session_function: pyspark.sql.SparkSession
    ) -> None:
        t = TempDeltaTable(spark_session_function, schema={"id": "int"})
        assert t.table_name.startswith("tmp_")
        assert len(t.table_name) == 16  # "tmp_" + 12 hex chars

    def test_custom_table_name(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        t = TempDeltaTable(
            spark_session_function,
            table_name="my_custom_table",
            schema={"id": "int"},
        )
        assert t.table_name == "my_custom_table"

    def test_returns_delta_table(self, spark_session_function: pyspark.sql.SparkSession) -> None:
        with TempDeltaTable(
            spark_session_function,
            schema={"id": "int", "name": "string"},
            data=[(1, "a")],
        ) as table:
            assert isinstance(table, DeltaTable)
