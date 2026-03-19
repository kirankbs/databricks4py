# API: I/O

## DeltaTable

```python
class DeltaTable:
    def __init__(
        self,
        table_name: str,
        schema: StructType | dict[str, str],
        *,
        location: str | None = None,
        partition_by: str | Sequence[str] | None = None,
        generated_columns: Sequence[GeneratedColumn] | None = None,
        spark: SparkSession | None = None,
    ) -> None
```

Auto-creates the table if it doesn't exist.

### dataframe() -> DataFrame

Read the table contents.

### write(df, mode="append", *, schema_check=True) -> None

Write a DataFrame. Modes: `"append"`, `"overwrite"`. `schema_check` runs `SchemaDiff` and blocks on breaking changes.

### merge(source, *, metrics_sink=None) -> MergeBuilder

Start a fluent merge operation against this table.

### upsert(source, keys, *, update_columns=None, metrics_sink=None) -> MergeResult

Update-or-insert by key columns. Returns `MergeResult`.

### scd_type2(source, keys, *, effective_date_col, end_date_col, active_col, metrics_sink) -> MergeResult

SCD Type 2 merge. Expires changed records and inserts new versions.

### detail() -> DataFrame

Delta table detail metadata.

### location() -> str, size_in_bytes() -> int, partition_columns() -> list[str]

Metadata accessors.

### replace_data(replacement_table_name, recovery_table_name) -> None

Atomic two-step rename swap.

### from_parquet(*paths, table_name, schema, ...) -> DeltaTable

Class method. Load from Parquet files into a new DeltaTable.

### from_data(data, *, table_name, schema, ...) -> DeltaTable

Class method. Create from in-memory data (list of dicts or tuples).

---

## DeltaTableAppender / DeltaTableOverwriter

Subclasses with `append(df)` / `overwrite(df)` convenience methods.

---

## GeneratedColumn

```python
@dataclass(frozen=True)
class GeneratedColumn:
    name: str
    data_type: str
    expression: str
    comment: str | None = None
```

---

## MergeBuilder

```python
class MergeBuilder:
    def __init__(self, target_table_name, source, spark=None, *, metrics_sink=None)
    def on(self, *keys: str) -> MergeBuilder
    def on_condition(self, condition: str) -> MergeBuilder
    def when_matched_update(self, columns: list[str] | None = None) -> MergeBuilder
    def when_matched_delete(self, condition: str | None = None) -> MergeBuilder
    def when_not_matched_insert(self, columns: list[str] | None = None) -> MergeBuilder
    def when_not_matched_by_source_delete(self, condition: str | None = None) -> MergeBuilder
    def execute(self) -> MergeResult
```

## MergeResult

```python
@dataclass(frozen=True)
class MergeResult:
    rows_inserted: int
    rows_updated: int
    rows_deleted: int
```

---

## StreamingTableReader

```python
class StreamingTableReader(ABC):
    def __init__(
        self,
        source_table: str,
        trigger: StreamingTriggerOptions | dict | None = None,
        checkpoint_location: str | None = None,
        *,
        source_format: str = "delta",
        filter: Filter | None = None,
        skip_empty_batches: bool = True,
        read_options: dict[str, str] | None = None,
        checkpoint_manager: CheckpointManager | None = None,
        metrics_sink: MetricsSink | None = None,
        spark: SparkSession | None = None,
    ) -> None

    @abstractmethod
    def process_batch(self, df: DataFrame, batch_id: int) -> None: ...

    def start(self) -> StreamingQuery
```

## StreamingTriggerOptions

Enum with values: `PROCESSING_TIME_10S`, `PROCESSING_TIME_30S`, `PROCESSING_TIME_1M`, `PROCESSING_TIME_5M`, `PROCESSING_TIME_10M`, `AVAILABLE_NOW`.

---

## CheckpointManager

```python
class CheckpointManager:
    def __init__(self, base_path: str, *, spark: SparkSession | None = None)
    def path_for(self, source: str, sink: str) -> str
    def exists(self, path: str) -> bool
    def reset(self, path: str) -> None
    def info(self, path: str) -> CheckpointInfo
```

## CheckpointInfo

```python
@dataclass(frozen=True)
class CheckpointInfo:
    path: str
    last_batch_id: int | None
    offsets: dict | None
    size_bytes: int
```

---

## DBFS operations

```python
def copy_from_remote(remote_path: str, local_path: str, recurse: bool = False) -> bool
def ls(path: str) -> list
def mv(source: str, dest: str, *, recurse: bool = False) -> None
def rm(path: str, *, recurse: bool = False) -> None
def mkdirs(path: str) -> None
```

All require dbutils injection via `inject_dbutils()` or `Workflow(dbutils=...)`.

---

## optimize_table / vacuum_table

```python
def optimize_table(table_name: str, *, zorder_by: str | Sequence[str] | None = None, spark=None) -> None
def vacuum_table(table_name: str, *, retention_hours: int = 168, spark=None) -> None
```
