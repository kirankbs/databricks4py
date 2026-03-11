"""Deep dive: Delta table management with databricks4py.

Covers every DeltaTable method including partitioning, generated columns,
table replacement, optimize, vacuum, and loading from parquet.

Prerequisites:
    pip install databricks4py
    Java 17+ must be installed for Spark

Run:
    python docs/examples/delta_tables.py
"""

import tempfile
from pathlib import Path

from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from databricks4py import get_or_create_local_session
from databricks4py.io import (
    DeltaTable,
    DeltaTableAppender,
    DeltaTableOverwriter,
    GeneratedColumn,
    optimize_table,
    vacuum_table,
)

spark = get_or_create_local_session()

# ---- Basic table creation ----

schema = StructType(
    [
        StructField("user_id", IntegerType()),
        StructField("username", StringType()),
        StructField("score", IntegerType()),
    ]
)

with tempfile.TemporaryDirectory() as tmpdir:
    location = str(Path(tmpdir) / "users")

    table = DeltaTable(
        table_name="default.users",
        schema=schema,
        location=location,
        spark=spark,
    )
    print(f"Created: {table}")
    print(f"  table_name: {table.table_name}")
    print(f"  location:   {table.location()}")

    # Write data
    df = spark.createDataFrame(
        [
            {"user_id": 1, "username": "alice", "score": 100},
            {"user_id": 2, "username": "bob", "score": 85},
            {"user_id": 3, "username": "carol", "score": 92},
        ],
        schema=schema,
    )
    table.write(df, mode="overwrite")
    print(f"  rows:       {table.dataframe().count()}")
    print(f"  size:       {table.size_in_bytes()} bytes")

    # ---- Partitioned table ----

    partitioned = DeltaTable(
        table_name="default.users_partitioned",
        schema=schema,
        location=str(Path(tmpdir) / "users_part"),
        partition_by="score",  # string or list
        spark=spark,
    )
    partitioned.write(df, mode="overwrite")
    print(f"\nPartitioned by: {partitioned.partition_columns()}")

    # ---- Generated columns ----

    event_schema = StructType(
        [
            StructField("event_id", IntegerType()),
            StructField("event_ts", TimestampType()),
            StructField("event_date", DateType()),  # generated
        ]
    )

    gen_cols = [
        GeneratedColumn(
            name="event_date",
            data_type="DATE",
            expression="CAST(event_ts AS DATE)",
            comment="Auto-derived from event_ts",
        ),
    ]

    event_table = DeltaTable(
        table_name="default.events_gen",
        schema=event_schema,
        location=str(Path(tmpdir) / "events_gen"),
        partition_by="event_date",
        generated_columns=gen_cols,
        spark=spark,
    )
    print(f"\nGenerated columns table: {event_table}")

    # ---- from_data: quick table creation ----

    quick = DeltaTable.from_data(
        [{"user_id": 10, "username": "quick", "score": 50}],
        table_name="default.quick_table",
        schema=schema,
        location=str(Path(tmpdir) / "quick"),
        spark=spark,
    )
    print(f"\nfrom_data: {quick.dataframe().count()} row in {quick.table_name}")

    # ---- from_parquet: load existing parquet ----

    parquet_path = str(Path(tmpdir) / "source.parquet")
    df.write.parquet(parquet_path)

    from_pq = DeltaTable.from_parquet(
        parquet_path,
        table_name="default.from_parquet",
        schema=schema,
        location=str(Path(tmpdir) / "from_pq"),
        spark=spark,
    )
    print(f"from_parquet: {from_pq.dataframe().count()} rows loaded")

    # ---- DeltaTableAppender ----

    appender = DeltaTableAppender(
        table_name="default.append_demo",
        schema=schema,
        location=str(Path(tmpdir) / "appender"),
        spark=spark,
    )
    batch1 = spark.createDataFrame([{"user_id": 1, "username": "a", "score": 1}], schema=schema)
    batch2 = spark.createDataFrame([{"user_id": 2, "username": "b", "score": 2}], schema=schema)
    appender.append(batch1)
    appender.append(batch2)
    print(f"\nAppender: {appender.dataframe().count()} rows after 2 appends")

    # ---- DeltaTableOverwriter ----

    overwriter = DeltaTableOverwriter(
        table_name="default.overwrite_demo",
        schema=schema,
        location=str(Path(tmpdir) / "overwriter"),
        spark=spark,
    )
    overwriter.overwrite(batch1)
    print(f"Overwriter after write 1: {overwriter.dataframe().count()} row")
    overwriter.overwrite(batch2)
    print(f"Overwriter after write 2: {overwriter.dataframe().count()} row (replaced)")

    # ---- Table metadata ----

    detail_df = table.detail()
    print(f"\nTable detail columns: {detail_df.columns}")

    # ---- Optimize and Vacuum ----

    optimize_table("default.users", spark=spark)
    print("\nOptimize complete")

    optimize_table("default.users", zorder_by="username", spark=spark)
    print("Optimize with Z-ORDER complete")

    optimize_table("default.users", zorder_by=["user_id", "username"], spark=spark)
    print("Optimize with multi-column Z-ORDER complete")

    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    vacuum_table("default.users", retention_hours=0, spark=spark)
    print("Vacuum complete")

    # ---- replace_data (atomic swap) ----

    replacement = DeltaTable.from_data(
        [{"user_id": 99, "username": "replaced", "score": 999}],
        table_name="default.users_replacement",
        schema=schema,
        location=str(Path(tmpdir) / "replacement"),
        spark=spark,
    )

    table.replace_data(
        replacement_table_name="default.users_replacement",
        recovery_table_name="default.users_backup",
    )
    print("\nAfter replace_data:")
    print(f"  users now has:  {spark.read.table('default.users').count()} row (replacement)")
    print(f"  backup has:     {spark.read.table('default.users_backup').count()} rows (original)")

spark.stop()
print("\nDone!")
