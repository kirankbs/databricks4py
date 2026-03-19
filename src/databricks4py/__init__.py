"""databricks4py: Spark, Delta Lake, and Databricks utility library.

A collection of reusable abstractions for building PySpark applications
on Databricks and locally.
"""

__version__ = "0.2.0"

from databricks4py.catalog import CatalogSchema
from databricks4py.config import Environment, JobConfig, UnityConfig
from databricks4py.logging import configure_logging, get_logger
from databricks4py.metrics import CompositeMetricsSink, LoggingMetricsSink, MetricEvent, MetricsSink
from databricks4py.retry import RetryConfig, retry
from databricks4py.secrets import SecretFetcher
from databricks4py.spark_session import active_fallback, get_active, get_or_create_local_session
from databricks4py.workflow import Workflow


def inject_dbutils(dbutils_module):
    """Unified dbutils injection for secrets and DBFS operations."""
    from databricks4py.io.dbfs import _set_dbutils_module

    SecretFetcher.dbutils = dbutils_module
    _set_dbutils_module(dbutils_module)


__all__ = [
    "__version__",
    # SparkSession
    "get_active",
    "active_fallback",
    "get_or_create_local_session",
    # Catalog
    "CatalogSchema",
    # Config
    "Environment",
    "JobConfig",
    "UnityConfig",
    # Logging
    "configure_logging",
    "get_logger",
    # Metrics
    "CompositeMetricsSink",
    "LoggingMetricsSink",
    "MetricEvent",
    "MetricsSink",
    # Retry
    "RetryConfig",
    "retry",
    # Secrets
    "SecretFetcher",
    # Workflow
    "Workflow",
    # Utilities
    "inject_dbutils",
]
