"""Tests for migration validators."""

import pyspark.sql
import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from databricks4py.io.delta import DeltaTable
from databricks4py.migrations.validators import (
    MigrationError,
    TableValidator,
    ValidationResult,
)

SCHEMA = StructType(
    [
        StructField("id", IntegerType()),
        StructField("name", StringType()),
    ]
)


@pytest.mark.no_pyspark
class TestValidationResult:
    def test_valid_result(self) -> None:
        result = ValidationResult(is_valid=True)
        result.raise_if_invalid("test")  # should not raise

    def test_invalid_raises(self) -> None:
        result = ValidationResult(is_valid=False, errors=["missing col"])
        with pytest.raises(MigrationError, match="missing col"):
            result.raise_if_invalid("test_table")

    def test_warnings(self) -> None:
        result = ValidationResult(is_valid=True, warnings=["check this"])
        assert result.warnings == ["check this"]


@pytest.mark.no_pyspark
class TestMigrationError:
    def test_message_format(self) -> None:
        err = MigrationError("my_table", ["err1", "err2"])
        assert "my_table" in str(err)
        assert "err1" in str(err)
        assert "err2" in str(err)

    def test_attributes(self) -> None:
        err = MigrationError("t", ["e"])
        assert err.table_name == "t"
        assert err.errors == ["e"]


@pytest.mark.integration
class TestTableValidator:
    def test_validate_existing_table(
        self,
        spark_session_function: pyspark.sql.SparkSession,
        tmp_path,
    ) -> None:
        location = str(tmp_path / "val_exists")
        DeltaTable(
            table_name="default.val_exists",
            schema=SCHEMA,
            location=location,
            spark=spark_session_function,
        )

        validator = TableValidator(
            table_name="default.val_exists",
            expected_columns=["id", "name"],
            spark=spark_session_function,
        )
        result = validator.validate()
        assert result.is_valid

    def test_validate_missing_columns(
        self,
        spark_session_function: pyspark.sql.SparkSession,
        tmp_path,
    ) -> None:
        location = str(tmp_path / "val_missing")
        DeltaTable(
            table_name="default.val_missing",
            schema=SCHEMA,
            location=location,
            spark=spark_session_function,
        )

        validator = TableValidator(
            table_name="default.val_missing",
            expected_columns=["id", "nonexistent"],
            spark=spark_session_function,
        )
        result = validator.validate()
        assert not result.is_valid
        assert any("nonexistent" in e for e in result.errors)

    def test_validate_nonexistent_table(
        self,
        spark_session_function: pyspark.sql.SparkSession,
    ) -> None:
        validator = TableValidator(
            table_name="default.does_not_exist_at_all",
            spark=spark_session_function,
        )
        result = validator.validate()
        assert not result.is_valid
        assert any("does not exist" in e for e in result.errors)

    def test_validate_partitions(
        self,
        spark_session_function: pyspark.sql.SparkSession,
        tmp_path,
    ) -> None:
        location = str(tmp_path / "val_part")
        DeltaTable(
            table_name="default.val_part",
            schema=SCHEMA,
            location=location,
            partition_by="id",
            spark=spark_session_function,
        )

        validator = TableValidator(
            table_name="default.val_part",
            expected_partition_columns=["id"],
            spark=spark_session_function,
        )
        result = validator.validate()
        assert result.is_valid

    def test_validate_wrong_partitions(
        self,
        spark_session_function: pyspark.sql.SparkSession,
        tmp_path,
    ) -> None:
        location = str(tmp_path / "val_wrong_part")
        DeltaTable(
            table_name="default.val_wrong_part",
            schema=SCHEMA,
            location=location,
            partition_by="id",
            spark=spark_session_function,
        )

        validator = TableValidator(
            table_name="default.val_wrong_part",
            expected_partition_columns=["name"],
            spark=spark_session_function,
        )
        result = validator.validate()
        assert not result.is_valid
        assert any("Partition mismatch" in e for e in result.errors)
