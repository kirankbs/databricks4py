"""Tests for DBFS utilities."""

import pytest

from databricks4py.io import dbfs
from databricks4py.testing.mocks import MockDBUtilsModule


class TestCopyFromRemote:
    @pytest.fixture(autouse=True)
    def _reset_dbutils(self) -> None:
        original = dbfs._dbutils_module
        yield
        dbfs._dbutils_module = original

    @pytest.mark.no_pyspark
    def test_raises_without_injection(self) -> None:
        dbfs._dbutils_module = None
        with pytest.raises(RuntimeError, match="not injected"):
            dbfs.copy_from_remote("/src", "/dst")

    @pytest.mark.integration
    def test_copy_success(self, spark_session) -> None:
        module = MockDBUtilsModule()
        dbfs.inject_dbutils_module(module)
        result = dbfs.copy_from_remote("/remote/file.csv", "/local/file.csv")
        assert result is True
        # Verify the mock recorded the copy
        mock = module.DBUtils(spark_session)
        assert ("/remote/file.csv", "/local/file.csv", False) in mock.fs._copies

    @pytest.mark.integration
    def test_copy_recurse(self, spark_session) -> None:
        module = MockDBUtilsModule()
        dbfs.inject_dbutils_module(module)
        result = dbfs.copy_from_remote("/remote/dir", "/local/dir", recurse=True)
        assert result is True
        mock = module.DBUtils(spark_session)
        assert ("/remote/dir", "/local/dir", True) in mock.fs._copies


class TestInjectDbutils:
    @pytest.fixture(autouse=True)
    def _reset_dbutils(self) -> None:
        original = dbfs._dbutils_module
        yield
        dbfs._dbutils_module = original

    @pytest.mark.no_pyspark
    def test_injects_module(self) -> None:
        module = MockDBUtilsModule()
        dbfs.inject_dbutils_module(module)
        assert dbfs._dbutils_module is module
