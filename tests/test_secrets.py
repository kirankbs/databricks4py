"""Tests for secret management."""

import pytest

from databricks4py.secrets import SecretFetcher, inject_dbutils
from databricks4py.testing.mocks import MockDBUtils, MockDBUtilsModule


class TestSecretFetcher:
    @pytest.fixture(autouse=True)
    def _reset_dbutils(self) -> None:
        original = SecretFetcher.dbutils
        yield
        SecretFetcher.dbutils = original

    @pytest.mark.integration
    def test_fetch_secret(self, spark_session) -> None:
        mock = MockDBUtils()
        mock.secrets.put("vault", "api-key", "secret-123")
        module = MockDBUtilsModule(mock)
        inject_dbutils(module)

        result = SecretFetcher.fetch_secret("vault", "api-key", spark=spark_session)
        assert result == "secret-123"

    @pytest.mark.no_pyspark
    def test_raises_without_injection(self) -> None:
        SecretFetcher.dbutils = None
        with pytest.raises(RuntimeError, match="not been set"):
            SecretFetcher.fetch_secret("scope", "key")


class TestInjectDbutils:
    @pytest.fixture(autouse=True)
    def _reset_dbutils(self) -> None:
        original = SecretFetcher.dbutils
        yield
        SecretFetcher.dbutils = original

    @pytest.mark.no_pyspark
    def test_injects_module(self) -> None:
        module = MockDBUtilsModule()
        inject_dbutils(module)
        assert SecretFetcher.dbutils is module
