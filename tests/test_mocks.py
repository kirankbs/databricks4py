"""Tests for mock objects."""

import pytest

from databricks4py.testing.mocks import MockDBUtils, MockDBUtilsModule


class TestMockDBUtils:
    @pytest.mark.no_pyspark
    def test_secrets_put_and_get(self) -> None:
        mock = MockDBUtils()
        mock.secrets.put("scope", "key", "value")
        assert mock.secrets.get("scope", "key") == "value"

    @pytest.mark.no_pyspark
    def test_secrets_missing_raises(self) -> None:
        mock = MockDBUtils()
        with pytest.raises(KeyError, match="scope='missing'"):
            mock.secrets.get("missing", "key")

    @pytest.mark.no_pyspark
    def test_fs_cp(self) -> None:
        mock = MockDBUtils()
        assert mock.fs.cp("/src", "/dst") is True
        assert mock.fs._copies == [("/src", "/dst", False)]

    @pytest.mark.no_pyspark
    def test_fs_cp_recurse(self) -> None:
        mock = MockDBUtils()
        mock.fs.cp("/src", "/dst", recurse=True)
        assert mock.fs._copies == [("/src", "/dst", True)]

    @pytest.mark.no_pyspark
    def test_fs_ls_empty(self) -> None:
        mock = MockDBUtils()
        assert mock.fs.ls("/path") == []


class TestMockDBUtilsModule:
    @pytest.mark.no_pyspark
    def test_dbutils_factory(self) -> None:
        module = MockDBUtilsModule()
        dbutils = module.DBUtils(None)
        assert hasattr(dbutils, "secrets")
        assert hasattr(dbutils, "fs")

    @pytest.mark.no_pyspark
    def test_shared_mock(self) -> None:
        mock = MockDBUtils()
        mock.secrets.put("s", "k", "v")
        module = MockDBUtilsModule(mock)
        dbutils = module.DBUtils()
        assert dbutils.secrets.get("s", "k") == "v"
