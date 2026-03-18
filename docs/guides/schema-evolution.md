# Schema Evolution

## When to use this

When you need to detect schema changes before they break downstream consumers. `SchemaDiff` compares two StructTypes and tells you exactly what changed, at what severity level. `DeltaTable.write()` uses it as a pre-write guard by default.

## SchemaDiff between StructTypes

```python
from databricks4py.migrations.schema_diff import SchemaDiff
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

current = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("score", IntegerType()),
])

incoming = StructType([
    StructField("id", LongType()),          # type changed
    StructField("name", StringType()),      # unchanged
    StructField("email", StringType()),     # added
    # score removed
])

diff = SchemaDiff(current, incoming)

for change in diff.changes():
    print(f"{change.column}: {change.change_type} ({change.severity})")
# email: added (info)
# id: type_changed (breaking)
# score: removed (breaking)
```

## from_tables() for existing table comparison

Compare a live Delta table's schema against an incoming DataFrame:

```python
diff = SchemaDiff.from_tables(
    "warehouse.silver.users",
    incoming_df,
)

print(diff.summary())
# Column                         Change               Severity   Details
# --------------------------------------------------------------------------------
# email                          added                info       StringType()
# id                             type_changed         breaking   IntegerType() -> LongType()
# score                          removed              breaking   IntegerType()

if diff.has_breaking_changes():
    print("Cannot proceed -- breaking changes detected")
```

## Severity levels

| Level | Meaning | Examples |
|---|---|---|
| **info** | Safe, additive change | New column added |
| **warning** | Potentially problematic | Nullable changed |
| **breaking** | Will cause failures or data loss | Column removed, type changed |

## ColumnChange dataclass

Each detected change is a `ColumnChange`:

```python
@dataclass(frozen=True)
class ColumnChange:
    column: str
    change_type: Literal["added", "removed", "type_changed", "nullable_changed"]
    old_value: str | None
    new_value: str | None
    severity: Literal["info", "warning", "breaking"]
```

## Pre-write guard on DeltaTable.write()

`DeltaTable.write()` runs `SchemaDiff` automatically when `schema_check=True` (the default). Breaking changes raise `SchemaEvolutionError`:

```python
from databricks4py.io import DeltaTable

table = DeltaTable("warehouse.silver.users", schema={"id": "int", "name": "string"})

# This will raise SchemaEvolutionError if df has removed or type-changed columns
table.write(df, mode="append")

# Skip the check if you know what you're doing
table.write(df, mode="append", schema_check=False)
```

## Complete example

```python
from databricks4py.migrations.schema_diff import SchemaDiff

# Before a migration, check what would change
diff = SchemaDiff.from_tables("warehouse.silver.orders", new_orders_df)

if diff.has_breaking_changes():
    print("BLOCKED: breaking schema changes")
    print(diff.summary())
    raise SystemExit(1)

for change in diff.changes():
    if change.severity == "warning":
        print(f"WARNING: {change.column} - {change.change_type}")

# Safe to proceed
new_orders_df.write.format("delta").mode("overwrite").option(
    "mergeSchema", "true"
).saveAsTable("warehouse.silver.orders")
```
