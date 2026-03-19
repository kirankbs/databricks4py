# Testing

## When to use this

When writing pytest tests for PySpark code. The testing module provides fixtures that manage SparkSession lifecycle (one JVM per test run), a fluent DataFrame builder, temp Delta tables that clean up after themselves, assertion helpers, and mock objects for dbutils.

## DataFrameBuilder

Fluent API for constructing test DataFrames without boilerplate schema definitions:

```python
from databricks4py.testing import DataFrameBuilder

df = (
    DataFrameBuilder(spark)
    .with_columns({"id": "int", "name": "string", "score": "double"})
    .with_rows(
        (1, "alice", 95.0),
        (2, "bob", 82.0),
        (3, "carol", None),
    )
    .build()
)
```

### Sequential data generation

```python
df = (
    DataFrameBuilder(spark)
    .with_columns({"id": "int"})
    .with_sequential("id", start=1, count=100)
    .build()
)
# 100 rows with id 1..100
```

### Null injection

```python
df = (
    DataFrameBuilder(spark)
    .with_columns({"id": "int", "value": "double"})
    .with_sequential("id", count=1000)
    .with_nulls("value", frequency=0.1, seed=42)
    .build()
)
# ~10% of "value" cells are null, deterministic with seed
```

### StructType schema

```python
from pyspark.sql.types import StructType, StructField, IntegerType

df = (
    DataFrameBuilder(spark)
    .with_schema(StructType([StructField("id", IntegerType())]))
    .with_rows((1,), (2,))
    .build()
)
```

## TempDeltaTable

Context manager that creates a Delta table and drops it on exit:

```python
from databricks4py.testing import TempDeltaTable

with TempDeltaTable(spark, schema={"id": "int", "name": "string"}, data=[(1, "alice")]) as table:
    df = table.dataframe()
    assert df.count() == 1
    # table is a full DeltaTable instance -- merge, upsert, etc. all work

# table is dropped here
```

Auto-generates a unique table name unless you specify one:

```python
with TempDeltaTable(spark, table_name="test_orders", schema={"id": "int"}) as table:
    assert table.table_name == "test_orders"
```

## Assertions

### assert_frame_equal

```python
from databricks4py.testing import assert_frame_equal

assert_frame_equal(actual_df, expected_df)
assert_frame_equal(actual_df, expected_df, check_order=True)
assert_frame_equal(actual_df, expected_df, check_schema=False)
assert_frame_equal(actual_df, expected_df, check_nullable=True)
```

Uses Spark 3.5's `assertDataFrameEqual` when available, falls back to manual row comparison.

### assert_schema_equal

```python
from databricks4py.testing import assert_schema_equal

assert_schema_equal(df.schema, expected_schema)
assert_schema_equal(df.schema, expected_schema, check_nullable=True)
```

Reports the exact field that differs: name, type, or nullable flag.

## Fixtures

Register all fixtures by importing in your `conftest.py`:

```python
# conftest.py
from databricks4py.testing.fixtures import *  # noqa: F401,F403
```

### spark_session (session-scoped)

One SparkSession for the entire test run. Delta Lake configured, UI disabled, shuffle partitions set to 2 for speed. Uses temp directories for Derby metastore and warehouse.

```python
def test_read_table(spark_session):
    df = spark_session.createDataFrame([(1, "a")], ["id", "name"])
    assert df.count() == 1
```

### spark_session_function (function-scoped)

Same SparkSession, but cleans up all tables and cache after each test:

```python
def test_isolation(spark_session_function):
    spark_session_function.sql("CREATE TABLE test_tbl (id INT) USING delta")
    # table is dropped after this test
```

### df_builder

Pre-configured DataFrameBuilder:

```python
def test_with_builder(df_builder):
    df = df_builder.with_columns({"x": "int"}).with_rows((1,), (2,)).build()
    assert df.count() == 2
```

### temp_delta (factory)

Factory fixture that creates TempDeltaTables and cleans up all of them after the test:

```python
def test_upsert(temp_delta):
    with temp_delta(schema={"id": "int", "val": "string"}, data=[(1, "a")]) as table:
        new_data = spark.createDataFrame([(1, "b"), (2, "c")], ["id", "val"])
        result = table.upsert(new_data, keys=["id"])
        assert result.rows_updated == 1
        assert result.rows_inserted == 1
```

### clear_env (autouse)

Auto-use fixture that snapshots `os.environ` before each test and restores it after. Any env vars set during a test won't leak into other tests.

## MockDBUtils

Test code that depends on `dbutils` without running on Databricks:

```python
from databricks4py.testing import MockDBUtils

mock = MockDBUtils()
mock.secrets.put("my-scope", "api-key", "test-secret-value")
assert mock.secrets.get("my-scope", "api-key") == "test-secret-value"

# fs operations are recorded but don't touch the filesystem
mock.fs.cp("/source", "/dest", recurse=True)
assert mock.fs._copies == [("/source", "/dest", True)]
```

### MockDBUtilsModule

Mimics the `pyspark.dbutils` module for injection:

```python
from databricks4py.testing import MockDBUtilsModule, MockDBUtils
from databricks4py import inject_dbutils

mock_dbutils = MockDBUtils()
mock_dbutils.secrets.put("scope", "key", "value")

inject_dbutils(MockDBUtilsModule(mock_dbutils))
# Now SecretFetcher and DBFS operations use the mock
```

## Complete example

```python
import pytest
from databricks4py.testing import DataFrameBuilder, TempDeltaTable, assert_frame_equal
from databricks4py.quality import QualityGate, NotNull

def test_quality_gate_rejects_nulls(spark_session):
    df = (
        DataFrameBuilder(spark_session)
        .with_columns({"id": "int", "email": "string"})
        .with_rows((1, "a@b.com"), (2, None), (3, "c@d.com"))
        .build()
    )

    gate = QualityGate(NotNull("email"), on_fail="quarantine", quarantine_handler=lambda _: None)
    clean = gate.enforce(df)

    expected = (
        DataFrameBuilder(spark_session)
        .with_columns({"id": "int", "email": "string"})
        .with_rows((1, "a@b.com"), (3, "c@d.com"))
        .build()
    )

    assert_frame_equal(clean, expected)

def test_upsert_merge(spark_session_function, temp_delta):
    with temp_delta(
        schema={"id": "int", "name": "string"},
        data=[(1, "alice"), (2, "bob")],
    ) as table:
        updates = spark_session_function.createDataFrame(
            [(2, "robert"), (3, "carol")], ["id", "name"]
        )
        result = table.upsert(updates, keys=["id"])

        assert result.rows_inserted == 1
        assert result.rows_updated == 1
        assert table.dataframe().count() == 3
```
