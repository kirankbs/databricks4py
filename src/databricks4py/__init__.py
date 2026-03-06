"""databricks4py: Spark, Delta Lake, and Databricks utility library.

A collection of reusable abstractions for building PySpark applications
on Databricks and locally.
"""

__version__ = "0.1.0"

from databricks4py.catalog import CatalogSchema
from databricks4py.logging import configure_logging, get_logger
from databricks4py.secrets import SecretFetcher
from databricks4py.spark_session import active_fallback, get_active, get_or_create_local_session
from databricks4py.workflow import Workflow

__all__ = [
    "__version__",
    # SparkSession
    "get_active",
    "active_fallback",
    "get_or_create_local_session",
    # Catalog
    "CatalogSchema",
    # Logging
    "configure_logging",
    "get_logger",
    # Secrets
    "SecretFetcher",
    # Workflow
    "Workflow",
]
