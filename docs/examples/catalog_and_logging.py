"""Deep dive: CatalogSchema and logging with databricks4py.

Covers schema-qualified table names, versioned tables, and logging config.
This example doesn't need Spark — it runs without Java.

Run:
    python docs/examples/catalog_and_logging.py
"""

import logging

from databricks4py import CatalogSchema, configure_logging, get_logger

# ---- Logging ----

# Default: reads LOG_LEVEL env var, falls back to INFO
configure_logging(level="DEBUG")

logger = get_logger("my_app")
logger.debug("This is a debug message")
logger.info("This is an info message")
logger.warning("This is a warning")

# Change level at runtime
configure_logging(level=logging.WARNING)
logger.info("This won't appear — level is WARNING now")
logger.warning("But this will")

# Reset for the rest of the example
configure_logging(level="INFO")

# ---- CatalogSchema: organize table names ----

# Define schemas with their tables
bronze = CatalogSchema(
    "bronze",
    tables=["raw_events", "raw_users", "raw_orders"],
)

silver = CatalogSchema(
    "silver",
    tables=["cleaned_events", "enriched_users"],
    versioned_tables={
        "orders": "orders_v2",  # logical name → physical name
    },
)

gold = CatalogSchema(
    "gold",
    tables=["daily_metrics", "user_summary"],
)

# Access table names via attributes
print(f"Bronze events:  {bronze.raw_events}")  # bronze.raw_events
print(f"Silver orders:  {silver.orders}")  # silver.orders_v2 (versioned!)
print(f"Gold metrics:   {gold.daily_metrics}")  # gold.daily_metrics

# Schema name
print(f"\nSchema name: {silver.schema_name}")

# repr shows available tables
print(f"\n{bronze!r}")
print(f"{silver!r}")
print(f"{gold!r}")

# ---- Error handling: missing tables ----

try:
    _ = bronze.nonexistent_table
except AttributeError as e:
    print(f"\nExpected error: {e}")

# ---- Pattern: use in ETL code ----

# Instead of hardcoding table names throughout your code:
#   spark.read.table("bronze.raw_events")
#   spark.read.table("silver.orders_v2")
#
# Use CatalogSchema for centralized management:
#   spark.read.table(bronze.raw_events)
#   spark.read.table(silver.orders)
#
# When you version-bump a table (orders_v2 → orders_v3),
# change it in one place:
#   versioned_tables={"orders": "orders_v3"}

logger.info("CatalogSchema and logging demo complete")
print("\nDone!")
