"""Quickstart: Run locally to see every databricks4py interface in action.

Prerequisites:
    pip install databricks4py
    Java 17+ must be installed for Spark

Run:
    python docs/examples/quickstart.py
"""

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# --- 1. Spark session ---
from databricks4py import get_or_create_local_session  # noqa: E402

spark = get_or_create_local_session()
print(f"SparkSession ready: {spark.version}")

# --- 2. Logging ---
from databricks4py import configure_logging, get_logger  # noqa: E402

configure_logging(level="INFO")
logger = get_logger("quickstart")
logger.info("databricks4py quickstart running")

# --- 3. CatalogSchema for table name management ---
from databricks4py import CatalogSchema  # noqa: E402

sales = CatalogSchema(
    "default",
    tables=["orders", "customers"],
    versioned_tables={"metrics": "metrics_v3"},
)
print(f"Table names: orders={sales.orders}, metrics={sales.metrics}")

# --- 4. Delta tables ---
from databricks4py.io import (  # noqa: E402
    DeltaTable,
    DeltaTableAppender,
    DeltaTableOverwriter,
    optimize_table,
    vacuum_table,
)

schema = StructType(
    [
        StructField("id", IntegerType()),
        StructField("name", StringType()),
    ]
)

# Create from in-memory data
table = DeltaTable.from_data(
    [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
    table_name="default.demo_users",
    schema=schema,
    spark=spark,
)
print(f"\nCreated {table.table_name}, rows={table.dataframe().count()}")
print(f"  location: {table.location()}")
print(f"  size: {table.size_in_bytes()} bytes")

# Appender: append-only writes
appender = DeltaTableAppender(
    table_name="default.demo_events",
    schema=schema,
    spark=spark,
)
df1 = spark.createDataFrame([{"id": 10, "name": "event_a"}], schema=schema)
df2 = spark.createDataFrame([{"id": 11, "name": "event_b"}], schema=schema)
appender.append(df1)
appender.append(df2)
print(f"\nAppender: {appender.dataframe().count()} rows after 2 appends")

# Overwriter: full replacement
overwriter = DeltaTableOverwriter(
    table_name="default.demo_snapshots",
    schema=schema,
    spark=spark,
)
overwriter.overwrite(df1)
print(f"Overwriter after first write: {overwriter.dataframe().count()} row")
overwriter.overwrite(df2)
print(f"Overwriter after second write: {overwriter.dataframe().count()} row (replaced)")

# Optimize and vacuum
optimize_table("default.demo_users", spark=spark)
optimize_table("default.demo_users", zorder_by="name", spark=spark)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
vacuum_table("default.demo_users", retention_hours=0, spark=spark)
print("Optimize + vacuum complete")

# --- 5. Filters ---
from databricks4py.filters import (  # noqa: E402
    ColumnFilter,
    DropDuplicates,
    FilterPipeline,
    WhereFilter,
)

raw = spark.createDataFrame(
    [
        {"id": 1, "name": "alice", "score": 90},
        {"id": 1, "name": "alice", "score": 90},  # duplicate
        {"id": 2, "name": "bob", "score": 30},
        {"id": 3, "name": "carol", "score": 75},
    ]
)

pipeline = FilterPipeline(
    [
        DropDuplicates(subset=["id"]),
        WhereFilter("score > 50"),
        ColumnFilter(columns=["id", "name"]),
    ]
)
result = pipeline(raw)
print(f"\nFilter pipeline: {raw.count()} rows -> {result.count()} rows")
result.show()

# --- 6. Migration validation ---
from databricks4py.migrations import TableValidator  # noqa: E402

validator = TableValidator(
    table_name="default.demo_users",
    expected_columns=["id", "name"],
    spark=spark,
)
validation = validator.validate()
print(f"\nValidation passed: {validation.is_valid}")
if validation.warnings:
    print(f"  Warnings: {validation.warnings}")

# --- 7. Mocks for testing (no Spark needed for this part) ---
from databricks4py.testing import MockDBUtils, MockDBUtilsModule  # noqa: E402

mock = MockDBUtils()
mock.secrets.put("my-vault", "api-key", "sk-test-12345")
print(f"\nMock secret: {mock.secrets.get('my-vault', 'api-key')}")

mock_module = MockDBUtilsModule(mock)
dbutils = mock_module.DBUtils(spark)
print(f"Mock fs.cp result: {dbutils.fs.cp('/src', '/dst')}")

# --- Cleanup ---
spark.stop()
print("\nDone! All databricks4py interfaces demonstrated.")
