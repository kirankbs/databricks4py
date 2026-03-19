"""Guide: Testing Spark code with databricks4py fixtures and mocks.

Shows how to set up pytest fixtures, use MockDBUtils, and write
isolated tests for Spark code. This file documents the patterns —
for actual tests, see the tests/ directory.

This example runs as a script to demonstrate the mock APIs.
No Java or Spark needed for the mock parts.

Run:
    python docs/examples/testing_guide.py
"""

from databricks4py.testing import MockDBUtils, MockDBUtilsModule

# ============================================================
# Part 1: MockDBUtils — test code that uses dbutils
# ============================================================

mock = MockDBUtils()

# --- Secrets ---

# Set up test secrets
mock.secrets.put("production-vault", "db-password", "hunter2")
mock.secrets.put("production-vault", "api-key", "sk-test-abc123")
mock.secrets.put("staging-vault", "db-password", "staging-pw")

# Fetch them just like real dbutils
password = mock.secrets.get("production-vault", "db-password")
api_key = mock.secrets.get("production-vault", "api-key")
print(f"Secret (db-password): {password}")
print(f"Secret (api-key):     {api_key}")

# Missing secrets raise KeyError
try:
    mock.secrets.get("production-vault", "nonexistent")
except KeyError as e:
    print(f"Expected error: {e}")

# --- File system ---

# fs.cp records copy operations
mock.fs.cp("/dbfs/source/data.csv", "/local/data.csv")
mock.fs.cp("/dbfs/source/dir/", "/local/dir/", recurse=True)
print(f"\nRecorded copies: {mock.fs._copies}")

# fs.ls returns files (empty by default, you populate in tests)
files = mock.fs.ls("/dbfs/some/path")
print(f"Files at path: {files}")

# ============================================================
# Part 2: MockDBUtilsModule — mimics pyspark.dbutils module
# ============================================================

# On Databricks, you'd do:
#   import pyspark.dbutils
#   dbutils = pyspark.dbutils.DBUtils(spark)
#
# In tests, use MockDBUtilsModule as a drop-in replacement:

mock_module = MockDBUtilsModule(mock)
dbutils = mock_module.DBUtils(None)  # spark arg is ignored

print(f"\nModule-based access: {dbutils.secrets.get('production-vault', 'db-password')}")
print(f"Module fs.cp: {dbutils.fs.cp('/a', '/b')}")

# Without pre-existing MockDBUtils, a fresh one is created:
fresh_module = MockDBUtilsModule()
fresh_dbutils = fresh_module.DBUtils(None)
fresh_dbutils.secrets.put("scope", "key", "value")
print(f"Fresh module: {fresh_dbutils.secrets.get('scope', 'key')}")


# ============================================================
# Part 3: Fixture setup patterns (for your conftest.py)
# ============================================================

print(
    """
# ---- conftest.py ----
# Add this to your test project's conftest.py:

from databricks4py.testing.fixtures import *  # noqa: F401,F403

# This registers three fixtures:
#   spark_session           — session-scoped SparkSession (one JVM per test run)
#   spark_session_function  — function-scoped cleanup (tables/cache cleared between tests)
#   clear_env               — auto-restores env vars after each test


# ---- Example test ----

# def test_my_etl(spark_session_function, tmp_path):
#     from databricks4py.io import DeltaTable
#     from pyspark.sql.types import StructType, StructField, IntegerType
#
#     schema = StructType([StructField("id", IntegerType())])
#     location = str(tmp_path / "test_table")
#
#     table = DeltaTable.from_data(
#         [{"id": 1}, {"id": 2}],
#         table_name="default.test_etl",
#         schema=schema,
#         location=location,
#         spark=spark_session_function,
#     )
#     assert table.dataframe().count() == 2


# ---- Test with mocks ----

# def test_secret_fetcher():
#     from databricks4py.secrets import SecretFetcher, inject_dbutils
#     from databricks4py.testing import MockDBUtils, MockDBUtilsModule
#
#     mock = MockDBUtils()
#     mock.secrets.put("vault", "key", "secret-value")
#     module = MockDBUtilsModule(mock)
#     inject_dbutils(module)
#
#     result = SecretFetcher.fetch_secret("vault", "key")
#     assert result == "secret-value"


# ---- Test markers ----
# databricks4py uses these pytest markers:
#
#   @pytest.mark.no_pyspark    — runs without Java/Spark (fast)
#   @pytest.mark.unit          — quick tests that need Spark
#   @pytest.mark.integration   — full integration tests with Delta Lake
#
# Run by category:
#   pytest -m no_pyspark       # Fast, no JVM
#   pytest -m "integration or unit"  # Needs Java 17+
"""
)

print("Done!")
