"""Tests for Delta Lake table utilities."""

import pyspark.sql
import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from databricks4py.io.delta import (
    DeltaTable,
    DeltaTableAppender,
    DeltaTableOverwriter,
    GeneratedColumn,
    optimize_table,
    vacuum_table,
)


@pytest.mark.no_pyspark
class TestGeneratedColumn:
    def test_creation(self) -> None:
        gc = GeneratedColumn("date_col", "DATE", "CAST(ts AS DATE)")
        assert gc.name == "date_col"
        assert gc.data_type == "DATE"
        assert gc.expression == "CAST(ts AS DATE)"
        assert gc.comment is None

    def test_with_comment(self) -> None:
        gc = GeneratedColumn("date_col", "DATE", "CAST(ts AS DATE)", comment="Derived date")
        assert gc.comment == "Derived date"

    def test_frozen(self) -> None:
        gc = GeneratedColumn("a", "STRING", "expr")
        with pytest.raises(AttributeError):
            gc.name = "b"  # type: ignore[misc]


SIMPLE_SCHEMA = StructType(
    [
        StructField("id", IntegerType()),
        StructField("name", StringType()),
    ]
)


@pytest.mark.integration
class TestDeltaTable:
    def test_create_table(
        self,
        spark_session_function: pyspark.sql.SparkSession,
        tmp_path,
    ) -> None:
        location = str(tmp_path / "test_table")
        table = DeltaTable(
            table_name="default.test_create",
            schema=SIMPLE_SCHEMA,
            location=location,
            spark=spark_session_function,
        )
        assert table.table_name == "default.test_create"

    def test_write_and_read(
        self,
        spark_session_function: pyspark.sql.SparkSession,
        tmp_path,
    ) -> None:
        location = str(tmp_path / "test_rw")
        table = DeltaTable(
            table_name="default.test_rw",
            schema=SIMPLE_SCHEMA,
            location=location,
            spark=spark_session_function,
        )

        df = spark_session_function.createDataFrame(
            [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
            schema=SIMPLE_SCHEMA,
        )
        table.write(df, mode="overwrite")

        result = table.dataframe()
        assert result.count() == 2

    def test_detail(
        self,
        spark_session_function: pyspark.sql.SparkSession,
        tmp_path,
    ) -> None:
        location = str(tmp_path / "test_detail")
        table = DeltaTable(
            table_name="default.test_detail",
            schema=SIMPLE_SCHEMA,
            location=location,
            spark=spark_session_function,
        )
        detail = table.detail()
        assert detail.count() == 1

    def test_location(
        self,
        spark_session_function: pyspark.sql.SparkSession,
        tmp_path,
    ) -> None:
        location = str(tmp_path / "test_loc")
        table = DeltaTable(
            table_name="default.test_loc",
            schema=SIMPLE_SCHEMA,
            location=location,
            spark=spark_session_function,
        )
        assert "test_loc" in table.location()

    def test_partition_columns(
        self,
        spark_session_function: pyspark.sql.SparkSession,
        tmp_path,
    ) -> None:
        location = str(tmp_path / "test_part")
        table = DeltaTable(
            table_name="default.test_part",
            schema=SIMPLE_SCHEMA,
            location=location,
            partition_by="id",
            spark=spark_session_function,
        )
        assert table.partition_columns() == ["id"]

    def test_from_data(
        self,
        spark_session_function: pyspark.sql.SparkSession,
        tmp_path,
    ) -> None:
        location = str(tmp_path / "test_from_data")
        table = DeltaTable.from_data(
            [{"id": 1, "name": "x"}, {"id": 2, "name": "y"}],
            table_name="default.test_from_data",
            schema=SIMPLE_SCHEMA,
            location=location,
            spark=spark_session_function,
        )
        assert table.dataframe().count() == 2

    def test_repr(
        self,
        spark_session_function: pyspark.sql.SparkSession,
        tmp_path,
    ) -> None:
        location = str(tmp_path / "test_repr")
        table = DeltaTable(
            table_name="default.test_repr",
            schema=SIMPLE_SCHEMA,
            location=location,
            spark=spark_session_function,
        )
        assert "test_repr" in repr(table)


@pytest.mark.integration
class TestDeltaTableAppender:
    def test_append(
        self,
        spark_session_function: pyspark.sql.SparkSession,
        tmp_path,
    ) -> None:
        location = str(tmp_path / "test_appender")
        appender = DeltaTableAppender(
            table_name="default.test_appender",
            schema=SIMPLE_SCHEMA,
            location=location,
            spark=spark_session_function,
        )

        df1 = spark_session_function.createDataFrame(
            [{"id": 1, "name": "a"}], schema=SIMPLE_SCHEMA
        )
        df2 = spark_session_function.createDataFrame(
            [{"id": 2, "name": "b"}], schema=SIMPLE_SCHEMA
        )

        appender.append(df1)
        appender.append(df2)
        assert appender.dataframe().count() == 2


@pytest.mark.integration
class TestDeltaTableOverwriter:
    def test_overwrite(
        self,
        spark_session_function: pyspark.sql.SparkSession,
        tmp_path,
    ) -> None:
        location = str(tmp_path / "test_overwriter")
        overwriter = DeltaTableOverwriter(
            table_name="default.test_overwriter",
            schema=SIMPLE_SCHEMA,
            location=location,
            spark=spark_session_function,
        )

        df1 = spark_session_function.createDataFrame(
            [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}],
            schema=SIMPLE_SCHEMA,
        )
        df2 = spark_session_function.createDataFrame(
            [{"id": 3, "name": "c"}], schema=SIMPLE_SCHEMA
        )

        overwriter.overwrite(df1)
        assert overwriter.dataframe().count() == 2

        overwriter.overwrite(df2)
        assert overwriter.dataframe().count() == 1


@pytest.mark.integration
class TestOptimizeVacuum:
    def test_optimize(
        self,
        spark_session_function: pyspark.sql.SparkSession,
        tmp_path,
    ) -> None:
        location = str(tmp_path / "test_opt")
        DeltaTable.from_data(
            [{"id": 1, "name": "a"}],
            table_name="default.test_opt",
            schema=SIMPLE_SCHEMA,
            location=location,
            spark=spark_session_function,
        )
        # Should not raise
        optimize_table("default.test_opt", spark=spark_session_function)

    def test_vacuum(
        self,
        spark_session_function: pyspark.sql.SparkSession,
        tmp_path,
    ) -> None:
        location = str(tmp_path / "test_vac")
        DeltaTable.from_data(
            [{"id": 1, "name": "a"}],
            table_name="default.test_vac",
            schema=SIMPLE_SCHEMA,
            location=location,
            spark=spark_session_function,
        )
        # Need to disable retention check for test
        spark_session_function.conf.set(
            "spark.databricks.delta.retentionDurationCheck.enabled", "false"
        )
        vacuum_table("default.test_vac", retention_hours=0, spark=spark_session_function)


@pytest.mark.integration
class TestDeltaTableDictSchema:
    def test_dict_schema(
        self,
        spark_session_function: pyspark.sql.SparkSession,
        tmp_path,
    ) -> None:
        table = DeltaTable(
            "default.test_dict_schema",
            schema={"id": "INT", "name": "STRING"},
            location=str(tmp_path / "dict_schema"),
            spark=spark_session_function,
        )
        df = spark_session_function.createDataFrame([(1, "alice")], SIMPLE_SCHEMA)
        table.write(df)
        assert table.dataframe().count() == 1


@pytest.mark.integration
class TestDeltaTableMerge:
    def test_merge_returns_builder(
        self,
        spark_session_function: pyspark.sql.SparkSession,
        tmp_path,
    ) -> None:
        from databricks4py.io.merge import MergeBuilder

        table = DeltaTable(
            "default.test_merge_dt",
            schema=SIMPLE_SCHEMA,
            location=str(tmp_path / "merge_dt"),
            spark=spark_session_function,
        )
        df = spark_session_function.createDataFrame([(1, "alice")], SIMPLE_SCHEMA)
        table.write(df, mode="overwrite")
        builder = table.merge(df)
        assert isinstance(builder, MergeBuilder)

    def test_upsert(
        self,
        spark_session_function: pyspark.sql.SparkSession,
        tmp_path,
    ) -> None:
        table = DeltaTable(
            "default.test_upsert_dt",
            schema=SIMPLE_SCHEMA,
            location=str(tmp_path / "upsert_dt"),
            spark=spark_session_function,
        )
        initial = spark_session_function.createDataFrame(
            [(1, "alice"), (2, "bob")], SIMPLE_SCHEMA
        )
        table.write(initial, mode="overwrite")
        incoming = spark_session_function.createDataFrame(
            [(2, "robert"), (3, "charlie")], SIMPLE_SCHEMA
        )
        result = table.upsert(incoming, keys=["id"])
        assert result.rows_updated == 1
        assert result.rows_inserted == 1

    def test_schema_check_blocks_breaking(
        self,
        spark_session_function: pyspark.sql.SparkSession,
        tmp_path,
    ) -> None:
        from databricks4py.migrations.schema_diff import SchemaEvolutionError

        table = DeltaTable(
            "default.test_schema_block",
            schema=SIMPLE_SCHEMA,
            location=str(tmp_path / "schema_block"),
            spark=spark_session_function,
        )
        df = spark_session_function.createDataFrame([(1, "alice")], SIMPLE_SCHEMA)
        table.write(df, mode="overwrite")
        narrow_schema = StructType([StructField("id", IntegerType())])
        narrow = spark_session_function.createDataFrame([(2,)], narrow_schema)
        with pytest.raises(SchemaEvolutionError, match="Breaking"):
            table.write(narrow, schema_check=True)

    def test_schema_check_disabled(
        self,
        spark_session_function: pyspark.sql.SparkSession,
        tmp_path,
    ) -> None:
        table = DeltaTable(
            "default.test_schema_skip",
            schema=SIMPLE_SCHEMA,
            location=str(tmp_path / "schema_skip"),
            spark=spark_session_function,
        )
        df = spark_session_function.createDataFrame([(1, "alice")], SIMPLE_SCHEMA)
        table.write(df, mode="overwrite")
        narrow_schema = StructType([StructField("id", IntegerType())])
        narrow = spark_session_function.createDataFrame([(2,)], narrow_schema)
        table.write(narrow, mode="overwrite", schema_check=False)
