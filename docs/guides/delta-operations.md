# Delta Operations

## When to use this

Whenever you manage Delta tables. `DeltaTable` handles creation, reads, writes, merges, upserts, and SCD Type 2 through a single object. It auto-creates the table on instantiation if it doesn't exist.

## Table creation

### StructType schema

```python
from pyspark.sql.types import StructType, StructField, StringType, LongType
from databricks4py.io import DeltaTable

schema = StructType([
    StructField("id", LongType()),
    StructField("name", StringType()),
])

table = DeltaTable("catalog.schema.users", schema=schema)
```

### Dict schema (shorthand)

```python
table = DeltaTable(
    "catalog.schema.users",
    schema={"id": "long", "name": "string", "score": "double"},
)
```

Type strings map to PySpark types: `"int"`, `"long"`, `"string"`, `"double"`, `"boolean"`, `"date"`, `"timestamp"`, etc.

### With partitioning and generated columns

```python
from databricks4py.io import DeltaTable, GeneratedColumn

table = DeltaTable(
    "catalog.schema.events",
    schema={"event_id": "long", "event_ts": "timestamp", "event_date": "date", "payload": "string"},
    partition_by="event_date",
    generated_columns=[
        GeneratedColumn("event_date", "DATE", "CAST(event_ts AS DATE)"),
    ],
)
```

## Read and write

```python
# Read
df = table.dataframe()

# Write (append)
table.write(df, mode="append")

# Write (overwrite)
table.write(df, mode="overwrite")

# Skip schema check
table.write(df, mode="append", schema_check=False)
```

`schema_check=True` (the default) runs `SchemaDiff` before writing. If there are breaking schema changes (column removals, type changes), it raises `SchemaEvolutionError` instead of silently corrupting data.

## MergeBuilder fluent API

For complex merge operations:

```python
result = (
    table.merge(source_df)
    .on("id", "region")
    .when_matched_update(["name", "score"])
    .when_not_matched_insert()
    .when_not_matched_by_source_delete()
    .execute()
)

print(result.rows_inserted, result.rows_updated, result.rows_deleted)
```

Methods chain naturally:

- `.on(*keys)` -- equality-based join on one or more columns
- `.on_condition("target.id = source.id AND target.active = true")` -- custom condition
- `.when_matched_update(columns=None)` -- update all or specific columns
- `.when_matched_delete(condition=None)` -- delete matched rows
- `.when_not_matched_insert(columns=None)` -- insert new rows
- `.when_not_matched_by_source_delete(condition=None)` -- delete orphans

## upsert() one-liner

Most merge operations are upserts. Skip the builder:

```python
result = table.upsert(
    source_df,
    keys=["order_id"],
    update_columns=["amount", "status"],  # optional, defaults to all
)
```

Returns a `MergeResult(rows_inserted, rows_updated, rows_deleted)`.

## scd_type2() pattern

Slowly Changing Dimension Type 2 with automatic effective/end date management:

```python
result = table.scd_type2(
    source_df,
    keys=["customer_id"],
    effective_date_col="effective_date",
    end_date_col="end_date",
    active_col="is_active",
)
```

The target table needs `effective_date` (timestamp), `end_date` (timestamp, nullable), and `is_active` (boolean) columns. Source data should not include these -- they're added automatically.

On merge:
- Changed records get `end_date = current_timestamp()` and `is_active = false`
- All source rows are inserted as new active records

## Metadata

```python
table.location()          # physical storage path
table.size_in_bytes()     # table size
table.partition_columns() # partition column names
table.detail()            # full detail DataFrame
```

## optimize and vacuum

Standalone functions, not tied to a DeltaTable instance:

```python
from databricks4py.io import optimize_table, vacuum_table

optimize_table("catalog.schema.events", zorder_by=["event_date", "customer_id"])
vacuum_table("catalog.schema.events", retention_hours=168)  # 7 days
```

## Convenience subclasses

```python
from databricks4py.io import DeltaTableAppender, DeltaTableOverwriter

# Append-only table
appender = DeltaTableAppender("catalog.schema.logs", schema={"msg": "string"})
appender.append(df)

# Overwrite table
overwriter = DeltaTableOverwriter("catalog.schema.snapshots", schema={"day": "date", "count": "long"})
overwriter.overwrite(df)
```

## Complete example

```python
from databricks4py.io import DeltaTable, optimize_table

# Create or open table
orders = DeltaTable(
    "warehouse.silver.orders",
    schema={
        "order_id": "long",
        "customer_id": "long",
        "amount": "double",
        "order_date": "date",
    },
    partition_by="order_date",
)

# Read incoming data
incoming = spark.read.table("warehouse.bronze.raw_orders")

# Upsert
result = orders.upsert(incoming, keys=["order_id"])
print(f"Inserted {result.rows_inserted}, updated {result.rows_updated}")

# Maintenance
optimize_table("warehouse.silver.orders", zorder_by="customer_id")
```
