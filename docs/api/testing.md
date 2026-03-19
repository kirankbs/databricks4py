# API: Testing

## DataFrameBuilder

```python
class DataFrameBuilder:
    def __init__(self, spark: SparkSession) -> None
    def with_columns(self, schema: dict[str, str]) -> DataFrameBuilder
    def with_schema(self, schema: StructType) -> DataFrameBuilder
    def with_rows(self, *rows: tuple) -> DataFrameBuilder
    def with_sequential(self, column: str, start: int = 1, count: int = 10) -> DataFrameBuilder
    def with_nulls(self, column: str, frequency: float = 0.1, *, seed: int | None = None) -> DataFrameBuilder
    def build(self) -> DataFrame
```

`with_nulls()` must be called after rows are populated. `with_sequential()` replaces any existing rows.

---

## TempDeltaTable

```python
class TempDeltaTable:
    def __init__(
        self,
        spark: SparkSession,
        *,
        table_name: str | None = None,
        schema: dict[str, str] | None = None,
        data: list[tuple[Any, ...]] | None = None,
    ) -> None

    @property
    def table_name(self) -> str

    def __enter__(self) -> DeltaTable
    def __exit__(self, ...) -> None   # drops the table
```

---

## Assertions

### assert_frame_equal(actual, expected, *, check_order=False, check_schema=True, check_nullable=False)

Compare two DataFrames. Raises `AssertionError` on mismatch.

### assert_schema_equal(actual, expected, *, check_nullable=False)

Compare two StructTypes field-by-field. Reports the exact differing field.

---

## Fixtures

Import all fixtures in your `conftest.py`:

```python
from databricks4py.testing.fixtures import *  # noqa: F401,F403
```

| Fixture | Scope | Description |
|---|---|---|
| `spark_session` | session | SparkSession with Delta support, temp directories |
| `spark_session_function` | function | Reuses session, cleans tables/cache after each test |
| `clear_env` | function (autouse) | Restores `os.environ` after each test |
| `df_builder` | function | `DataFrameBuilder` bound to `spark_session` |
| `temp_delta` | function | Factory callable returning `TempDeltaTable` instances |

---

## MockDBUtils

```python
class MockDBUtils:
    secrets: _MockSecrets   # .get(scope, key), .put(scope, key, value)
    fs: _MockFS             # .cp(), .mv(), .rm(), .mkdirs(), .ls()
```

`fs` operations are recorded in `_copies`, `_moves`, `_removes`, `_mkdirs` lists for assertion.

## MockDBUtilsModule

```python
class MockDBUtilsModule:
    def __init__(self, mock_dbutils: MockDBUtils | None = None)
    def DBUtils(self, spark=None) -> MockDBUtils
```

Mimics `pyspark.dbutils` module structure. Pass to `inject_dbutils()` for testing.
