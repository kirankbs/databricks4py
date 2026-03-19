"""Tests for DBFS utilities."""

import pytest

from databricks4py.io import dbfs
from databricks4py.testing.mocks import MockDBUtils, MockDBUtilsModule


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


class TestDbfsOperations:
    @pytest.fixture(autouse=True)
    def _setup(self) -> None:
        original = dbfs._dbutils_module
        self.mock = MockDBUtils()
        dbfs._set_dbutils_module(MockDBUtilsModule(self.mock))
        yield
        dbfs._dbutils_module = original

    @pytest.mark.no_pyspark
    def test_ls(self) -> None:
        self.mock.fs._ls_results["/mnt/data"] = ["file1", "file2"]
        result = dbfs.ls("/mnt/data")
        assert result == ["file1", "file2"]

    @pytest.mark.no_pyspark
    def test_ls_empty(self) -> None:
        result = dbfs.ls("/empty")
        assert result == []

    @pytest.mark.no_pyspark
    def test_mv(self) -> None:
        dbfs.mv("/src", "/dst")
        assert self.mock.fs._moves == [("/src", "/dst", False)]

    @pytest.mark.no_pyspark
    def test_mv_recurse(self) -> None:
        dbfs.mv("/src", "/dst", recurse=True)
        assert self.mock.fs._moves == [("/src", "/dst", True)]

    @pytest.mark.no_pyspark
    def test_rm(self) -> None:
        dbfs.rm("/path")
        assert self.mock.fs._removes == [("/path", False)]

    @pytest.mark.no_pyspark
    def test_rm_recurse(self) -> None:
        dbfs.rm("/path", recurse=True)
        assert self.mock.fs._removes == [("/path", True)]

    @pytest.mark.no_pyspark
    def test_mkdirs(self) -> None:
        dbfs.mkdirs("/new/dir")
        assert self.mock.fs._mkdirs == ["/new/dir"]
