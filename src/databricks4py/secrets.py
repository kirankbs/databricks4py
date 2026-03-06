"""Databricks secret management via dbutils."""

from __future__ import annotations

import logging
from typing import Any

import pyspark.sql

from databricks4py.spark_session import active_fallback

__all__ = ["SecretFetcher", "inject_dbutils"]

logger = logging.getLogger(__name__)


class SecretFetcher:
    """Fetch secrets from Databricks secret vaults via dbutils.

    The ``dbutils`` module must be injected before use, either by calling
    :func:`inject_dbutils` or by setting :attr:`dbutils` directly.

    Example::

        from databricks4py.secrets import SecretFetcher, inject_dbutils
        import pyspark.dbutils  # only available on Databricks

        inject_dbutils(pyspark.dbutils)
        value = SecretFetcher.fetch_secret("my-scope", "api-key")
    """

    dbutils: Any = None

    @staticmethod
    def fetch_secret(
        secret_scope: str,
        secret_key: str,
        *,
        spark: pyspark.sql.SparkSession | None = None,
    ) -> str:
        """Fetch a secret from the Databricks secret vault.

        Args:
            secret_scope: The secret scope name.
            secret_key: The secret key name.
            spark: Optional SparkSession (used to create DBUtils instance).

        Raises:
            RuntimeError: If dbutils has not been injected.
        """
        if SecretFetcher.dbutils is None:
            raise RuntimeError(
                "SecretFetcher.dbutils has not been set. "
                "Call inject_dbutils(pyspark.dbutils) first."
            )

        spark = active_fallback(spark)
        _dbutils = SecretFetcher.dbutils.DBUtils(spark)
        return _dbutils.secrets.get(scope=secret_scope, key=secret_key)


def inject_dbutils(dbutils_module: Any) -> None:
    """Inject the dbutils module for secret fetching.

    Args:
        dbutils_module: The ``pyspark.dbutils`` module (only available on Databricks).
    """
    SecretFetcher.dbutils = dbutils_module
    logger.debug("Injected dbutils module: %s", dbutils_module)
