"""Base configuration for Databricks jobs."""

from __future__ import annotations

import logging
import os
from enum import Enum

__all__ = ["Environment", "JobConfig"]

logger = logging.getLogger(__name__)


class Environment(Enum):
    """Deployment environment. Resolved automatically from Databricks widgets or env vars."""

    DEV = "dev"
    STAGING = "staging"
    PROD = "prod"


class JobConfig:
    """Configuration container for Databricks job parameters.

    Resolves the deployment environment from (in priority order):
    1. ``spark.databricks.widget.env`` conf (Databricks widget)
    2. ``ENV`` or ``ENVIRONMENT`` environment variable
    3. Defaults to ``DEV``

    Example::

        config = JobConfig(
            tables={"events": "catalog.bronze.events", "users": "catalog.silver.users"},
            secret_scope="my-scope",
            spark_configs={"spark.sql.shuffle.partitions": "8"},
        )
        table_name = config.table("events")  # "catalog.bronze.events"

    Args:
        tables: Mapping of logical names to fully qualified table names.
        secret_scope: Databricks secret scope for :meth:`secret` lookups.
        storage_root: Optional root path for storage operations.
        spark_configs: Spark configuration overrides applied by
            :meth:`~databricks4py.workflow.Workflow.execute`.
    """

    def __init__(
        self,
        tables: dict[str, str],
        *,
        secret_scope: str | None = None,
        storage_root: str | None = None,
        spark_configs: dict[str, str] | None = None,
    ) -> None:
        self.tables = tables
        self.secret_scope = secret_scope
        self.storage_root = storage_root
        self.spark_configs = spark_configs or {}
        self.env = self._resolve_env()

    def _resolve_env(self) -> Environment:
        raw: str | None = None

        # Try Databricks widget parameter first
        try:
            from pyspark.sql import SparkSession

            spark = SparkSession.getActiveSession()
            if spark is not None:
                raw = spark.conf.get("spark.databricks.widget.env", None)
        except Exception:
            pass

        if raw is None:
            raw = os.getenv("ENV") or os.getenv("ENVIRONMENT")

        if raw is None:
            return Environment.DEV

        try:
            return Environment(raw.lower())
        except ValueError:
            logger.warning("Unknown environment '%s', defaulting to DEV", raw)
            return Environment.DEV

    def table(self, name: str) -> str:
        """Look up a fully qualified table name by logical key.

        Raises:
            KeyError: If *name* is not in the configured tables.
        """
        try:
            return self.tables[name]
        except KeyError:
            available = sorted(self.tables.keys())
            raise KeyError(f"Table '{name}' not configured. Available: {available}") from None

    def secret(self, key: str) -> str:
        """Fetch a secret from Databricks using the configured scope.

        Raises:
            ValueError: If no ``secret_scope`` was configured.
        """
        if self.secret_scope is None:
            raise ValueError("No secret_scope configured on this JobConfig")

        from databricks4py.secrets import SecretFetcher

        return SecretFetcher.fetch_secret(self.secret_scope, key)

    @classmethod
    def from_env(cls, **kwargs) -> JobConfig:
        return cls(**kwargs)

    def __repr__(self) -> str:
        return (
            f"JobConfig(env={self.env.value!r}, tables={len(self.tables)}, "
            f"secret_scope={self.secret_scope!r})"
        )
