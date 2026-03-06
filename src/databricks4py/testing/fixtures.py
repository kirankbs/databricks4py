"""Pytest fixtures for Spark and Delta Lake testing."""

from __future__ import annotations

import os
import shutil
from collections.abc import Generator

import pyspark.sql
import pytest

__all__ = [
    "clear_env",
    "spark_session",
    "spark_session_function",
]


@pytest.fixture(scope="session")
def spark_session(
    tmp_path_factory: pytest.TempPathFactory,
) -> Generator[pyspark.sql.SparkSession, None, None]:
    """Session-scoped SparkSession with Delta Lake support.

    Creates a single SparkSession for the entire test session to avoid
    the overhead of starting/stopping the JVM repeatedly. Uses a temporary
    directory for the Derby metastore and Spark warehouse.
    """
    from delta import configure_spark_with_delta_pip

    warehouse_dir = str(tmp_path_factory.mktemp("spark-warehouse"))
    derby_dir = str(tmp_path_factory.mktemp("derby"))

    builder = (
        pyspark.sql.SparkSession.builder.master("local[*]")
        .appName("databricks4py-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("javax.jdo.option.ConnectionURL", f"jdbc:derby:{derby_dir}/metastore;create=true")
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={derby_dir}")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture()
def spark_session_function(
    spark_session: pyspark.sql.SparkSession,
) -> Generator[pyspark.sql.SparkSession, None, None]:
    """Function-scoped SparkSession that cleans up between tests.

    Reuses the session-scoped SparkSession but clears the catalog
    and cache after each test to ensure isolation.
    """
    yield spark_session

    # Clean up tables
    for db in spark_session.catalog.listDatabases():
        for table in spark_session.catalog.listTables(db.name):
            spark_session.sql(f"DROP TABLE IF EXISTS {db.name}.{table.name}")

    spark_session.catalog.clearCache()

    # Clean up any leftover warehouse files
    warehouse = spark_session.conf.get("spark.sql.warehouse.dir")
    if warehouse and os.path.exists(warehouse):
        for item in os.listdir(warehouse):
            item_path = os.path.join(warehouse, item)
            if os.path.isdir(item_path):
                shutil.rmtree(item_path, ignore_errors=True)


@pytest.fixture(autouse=True)
def clear_env() -> Generator[None, None, None]:
    """Auto-use fixture that restores environment variables after each test."""
    original = os.environ.copy()
    yield
    os.environ.clear()
    os.environ.update(original)
