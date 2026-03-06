"""SparkSession management utilities."""

from __future__ import annotations

import pyspark.sql

__all__ = ["get_active", "active_fallback", "get_or_create_local_session"]


def get_active() -> pyspark.sql.SparkSession:
    """Get the currently active SparkSession.

    Raises:
        RuntimeError: If no active SparkSession exists.
    """
    spark = pyspark.sql.SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError(
            "No active SparkSession found. "
            "Create one with SparkSession.builder.getOrCreate() or "
            "use get_or_create_local_session()."
        )
    return spark


def active_fallback(spark: pyspark.sql.SparkSession | None = None) -> pyspark.sql.SparkSession:
    """Return the given SparkSession, or fall back to the active one.

    This is the standard pattern for library functions that accept an
    optional ``spark`` parameter::

        def my_function(data, *, spark=None):
            spark = active_fallback(spark)
            ...

    Args:
        spark: An explicit SparkSession, or None to use the active session.

    Raises:
        RuntimeError: If spark is None and no active session exists.
    """
    return spark if spark is not None else get_active()


def get_or_create_local_session() -> pyspark.sql.SparkSession:
    """Create a local SparkSession configured for Delta Lake.

    Suitable for local development and testing. Configures:
    - ``local[*]`` master
    - Delta Lake SQL extensions and catalog
    - Local Derby metastore

    Returns:
        A SparkSession with Delta Lake support.
    """
    from delta import configure_spark_with_delta_pip

    builder = (
        pyspark.sql.SparkSession.builder.master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", "spark-warehouse")
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()
