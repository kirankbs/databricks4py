# databricks4py v0.2 — Workflow-Centric Redesign

**Date:** 2026-03-18
**Status:** Approved
**Approach:** Workflow-Centric Framework (Approach 2)

## Context

databricks4py v0.1 provides solid foundational abstractions for Spark/Delta/Databricks — session management, catalog naming, filters, basic delta operations, workflow ABC, and test fixtures. The library targets data engineers building medallion-architecture ETL pipelines and ML engineers building feature/training pipelines, all deployed on Databricks.

Research into real-world pain points surfaced 8 major gaps that data engineers consistently struggle with. This redesign adds those capabilities while making `Workflow` the cohesive spine that wires everything together.

**Key architectural principle:** Every module works standalone, but gets better when used inside a Workflow.

## Target Audience

- Data engineers building bronze/silver/gold ETL pipelines deployed as Databricks jobs
- ML engineers building feature engineering and training pipelines on Databricks
- Teams using Unity Catalog with multi-environment deployments

## Module Structure

```
src/databricks4py/
├── config/
│   ├── __init__.py
│   ├── base.py            # JobConfig, Environment enum
│   └── unity.py           # UnityConfig (opinionated UC conventions)
├── metrics/
│   ├── __init__.py
│   ├── base.py            # MetricsSink ABC, MetricEvent dataclass
│   ├── logging_sink.py    # LoggingMetricsSink (JSON structured logs)
│   └── delta_sink.py      # DeltaMetricsSink (writes to control table)
├── quality/
│   ├── __init__.py
│   ├── base.py            # Expectation ABC, ExpectationResult, QualityReport
│   ├── expectations.py    # NotNull, InRange, Unique, RowCount, MatchesRegex, ColumnExists
│   └── gate.py            # QualityGate (composable, raise/warn/quarantine)
├── io/
│   ├── __init__.py
│   ├── delta.py           # DeltaTable (existing + merge/upsert/scd2 methods)
│   ├── merge.py           # MergeBuilder (fluent MERGE INTO), MergeResult
│   ├── streaming.py       # StreamingTableReader (existing + metrics hooks)
│   ├── checkpoint.py      # CheckpointManager, CheckpointInfo
│   └── dbfs.py            # DBFS operations (expanded: ls, mv, rm, mkdirs)
├── filters/
│   └── base.py            # Existing — no changes
├── migrations/
│   ├── __init__.py
│   ├── validators.py      # Existing — fix bare exceptions to AnalysisException
│   └── schema_diff.py     # SchemaDiff, ColumnChange, SchemaEvolutionError
├── testing/
│   ├── __init__.py
│   ├── fixtures.py        # Existing + df_builder, temp_delta fixtures
│   ├── mocks.py           # Existing — no changes
│   ├── builders.py        # DataFrameBuilder (fluent test data construction)
│   ├── temp_table.py      # TempDeltaTable context manager
│   └── assertions.py      # assert_frame_equal, assert_schema_equal
├── workflow.py             # Workflow ABC (enhanced: config, metrics, quality_check, retry)
├── spark_session.py        # Existing — no changes
├── catalog.py              # Existing — no changes
├── logging.py              # Existing — no changes
├── secrets.py              # Existing — fix dbutils injection consistency
├── retry.py                # RetryConfig, @retry decorator
└── __init__.py             # Top-level exports (heavy modules still lazy)
```

**Dependency direction:**

```
workflow.py (top — orchestrates everything)
    ├── config/     (environment resolution)
    ├── metrics/    (pluggable sinks)
    ├── quality/    (pre-write gates)
    ├── io/         (delta, merge, streaming, checkpoint)
    └── retry.py    (transient failure handling)
```

---

## 1. Config Module

### Environment Enum

```python
class Environment(Enum):
    DEV = "dev"
    STAGING = "staging"
    PROD = "prod"
```

### JobConfig (flexible, no conventions assumed)

```python
class JobConfig:
    def __init__(self, tables: dict[str, str], secret_scope: str | None = None,
                 storage_root: str | None = None, spark_configs: dict[str, str] | None = None):
        self.env = self._resolve_env()
        self.tables = tables
        self.secret_scope = secret_scope
        self.storage_root = storage_root
        self.spark_configs = spark_configs or {}

    def _resolve_env(self) -> Environment:
        """Resolution order:
        1. Databricks widget parameter 'env'
        2. ENV or ENVIRONMENT environment variable
        3. Defaults to DEV
        """

    def table(self, name: str) -> str:
        """Resolve logical table name to fully-qualified name. Raises KeyError with
        helpful message listing available tables if not found."""

    def secret(self, key: str) -> str:
        """Fetch secret from configured scope via SecretFetcher."""

    @classmethod
    def from_env(cls, **overrides) -> "JobConfig":
        """Build config from environment variables / Databricks widgets."""
```

### UnityConfig (opinionated convention layer)

```python
class UnityConfig(JobConfig):
    """Convention: catalog = {prefix}_{env}, tables auto-resolve.

    UnityConfig(catalog_prefix="analytics", schemas=["bronze", "silver", "gold"])

    In DEV:  config.table("bronze.events") → "analytics_dev.bronze.events"
    In PROD: config.table("bronze.events") → "analytics_prod.bronze.events"
    """

    def __init__(self, catalog_prefix: str, schemas: list[str],
                 secret_scope: str | None = None,
                 storage_root: str | None = None,
                 spark_configs: dict[str, str] | None = None):
        ...
```

**Environment is never hardcoded in user code.** It is resolved at runtime from Databricks widgets, environment variables, or defaults to DEV.

---

## 2. Metrics Module

### MetricEvent

```python
@dataclass(frozen=True)
class MetricEvent:
    job_name: str
    event_type: str          # "batch_complete", "write_complete", "quality_check", "job_complete"
    timestamp: datetime
    duration_ms: float | None = None
    row_count: int | None = None
    table_name: str | None = None
    batch_id: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
```

### MetricsSink ABC

```python
class MetricsSink(ABC):
    @abstractmethod
    def emit(self, event: MetricEvent) -> None: ...

    def flush(self) -> None:
        """Optional — sinks that buffer can override."""
        pass
```

### Built-in Sinks

- **LoggingMetricsSink**: Emits JSON-structured log lines via Python logging.
- **DeltaMetricsSink**: Buffers events, appends to a Delta control table on `flush()` or buffer threshold.
- **CompositeMetricsSink**: Fan-out to multiple sinks simultaneously.

### Auto-capture in Workflow

`Workflow.execute()` automatically emits:
- `job_start` — when execute begins
- `job_complete` — with total duration on success
- `job_failed` — with exception details on failure

Manual `self.emit_metric(event_type, **kwargs)` for custom events.

---

## 3. Quality Module

### Expectation ABC

```python
class Expectation(ABC):
    @abstractmethod
    def validate(self, df: DataFrame) -> ExpectationResult: ...

@dataclass(frozen=True)
class ExpectationResult:
    expectation: str       # human-readable description
    passed: bool
    total_rows: int
    failing_rows: int = 0
    sample: list[Row] | None = None  # optional failing row samples
```

### Built-in Expectations

| Class | Purpose |
|-------|---------|
| `NotNull(*columns)` | No nulls in specified columns |
| `InRange(column, min_val, max_val)` | Values within bounds (inclusive) |
| `Unique(*columns)` | No duplicate combinations |
| `RowCount(min, max)` | Row count within bounds |
| `MatchesRegex(column, pattern)` | All values match regex |
| `ColumnExists(*columns, dtype)` | Columns exist, optionally correct type |

### QualityGate

```python
class QualityGate:
    def __init__(self, *expectations: Expectation,
                 on_fail: Literal["raise", "warn", "quarantine"] = "raise"):
        ...

    def check(self, df: DataFrame) -> QualityReport:
        """Run all expectations, return report."""

    def enforce(self, df: DataFrame) -> DataFrame:
        """Run checks. On failure: raise, warn, or split quarantine rows."""
```

**Quarantine mode:** `enforce()` returns only clean rows. Failing rows routed via `quarantine_handler`:

```python
gate = QualityGate(
    NotNull("event_id"), InRange("amount", min_val=0),
    on_fail="quarantine",
    quarantine_handler=lambda bad_df: bad_df.write.mode("append").saveAsTable("bronze.quarantine")
)
clean_df = gate.enforce(raw_df)
```

### QualityReport

```python
@dataclass(frozen=True)
class QualityReport:
    results: list[ExpectationResult]
    passed: bool

    def summary(self) -> str:
        """Human-readable table of results."""
```

---

## 4. Merge / Upsert / SCD

### MergeBuilder (fluent API)

```python
class MergeBuilder:
    def __init__(self, target: DeltaTable, source: DataFrame): ...
    def on(self, *keys: str) -> "MergeBuilder": ...
    def on_condition(self, condition: str) -> "MergeBuilder": ...
    def when_matched_update(self, columns: list[str] | None = None) -> "MergeBuilder": ...
    def when_matched_delete(self, condition: str | None = None) -> "MergeBuilder": ...
    def when_not_matched_insert(self, columns: list[str] | None = None) -> "MergeBuilder": ...
    def when_not_matched_by_source_delete(self, condition: str | None = None) -> "MergeBuilder": ...
    def execute(self) -> MergeResult: ...

@dataclass(frozen=True)
class MergeResult:
    rows_inserted: int
    rows_updated: int
    rows_deleted: int
```

### Shortcuts on DeltaTable

```python
class DeltaTable:
    def merge(self, source: DataFrame) -> MergeBuilder: ...

    def upsert(self, source: DataFrame, keys: list[str],
               update_columns: list[str] | None = None) -> MergeResult:
        """One-liner: update on match, insert on no match."""

    def scd_type2(self, source: DataFrame, keys: list[str],
                  effective_date_col: str = "effective_date",
                  end_date_col: str = "end_date",
                  active_col: str = "is_active") -> MergeResult:
        """SCD Type 2: expire old records, insert new versions."""
```

Metrics are explicit, not magical: `upsert()`, `scd_type2()`, and `MergeBuilder.execute()` accept an optional `metrics_sink` parameter. When used inside a Workflow, pass `self.metrics` explicitly. See Section 14 for details.

---

## 5. Schema Diff & Evolution

### ColumnChange

```python
@dataclass(frozen=True)
class ColumnChange:
    column: str
    change_type: Literal["added", "removed", "type_changed", "nullable_changed"]
    old_value: str | None = None
    new_value: str | None = None
    severity: Literal["info", "warning", "breaking"] = "info"
```

### SchemaDiff

```python
class SchemaDiff:
    def __init__(self, current: StructType, incoming: StructType): ...

    @classmethod
    def from_tables(cls, table_name: str, incoming_df: DataFrame,
                    spark: SparkSession | None = None) -> "SchemaDiff": ...

    def changes(self) -> list[ColumnChange]: ...
    def has_breaking_changes(self) -> bool: ...
    def summary(self) -> str: ...
```

**Severity rules:**
- **info**: column added, comment changed — safe with `mergeSchema`
- **warning**: nullable changed (NOT NULL → NULLABLE)
- **breaking**: column removed, type narrowed/changed

**Wired into DeltaTable.write():** `schema_check=True` by default. Breaking changes raise `SchemaEvolutionError` with a clear diff summary. Users opt out with `schema_check=False`.

---

## 6. Streaming Enhancements

### CheckpointManager

```python
class CheckpointManager:
    def __init__(self, base_path: str, spark: SparkSession | None = None): ...
    def path_for(self, source: str, sink: str) -> str: ...
    def exists(self, path: str) -> bool: ...
    def reset(self, path: str) -> None: ...
    def info(self, path: str) -> CheckpointInfo: ...

@dataclass(frozen=True)
class CheckpointInfo:
    path: str
    last_batch_id: int | None
    offsets: dict | None
    size_bytes: int
```

### StreamingTableReader enhancements

- Accepts optional `checkpoint_manager` — auto-generates checkpoint path from source table + class name if no explicit `checkpoint_location` given.
- Accepts optional `metrics_sink` — auto-emits per-batch metrics: `{batch_id, row_count, duration_ms, source_table}`.

---

## 7. Testing Enhancements

### DataFrameBuilder

```python
class DataFrameBuilder:
    def __init__(self, spark: SparkSession): ...
    def with_columns(self, schema: dict[str, str]) -> "DataFrameBuilder": ...
    def with_schema(self, schema: StructType) -> "DataFrameBuilder": ...
    def with_rows(self, *rows: tuple) -> "DataFrameBuilder": ...
    def with_sequential(self, column: str, start: int = 1, count: int = 10) -> "DataFrameBuilder": ...
    def with_nulls(self, column: str, frequency: float = 0.1) -> "DataFrameBuilder": ...
    def build(self) -> DataFrame: ...
```

### TempDeltaTable

```python
class TempDeltaTable:
    """Context manager that creates a temp Delta table and drops it on exit."""
    def __init__(self, spark, table_name=None, schema=None, data=None): ...
    def __enter__(self) -> DeltaTable: ...
    def __exit__(self, *exc): ...
```

### Assertion Helpers

- `assert_frame_equal(actual, expected, check_order, check_schema, check_nullable)` — wraps Spark 3.5 `assertDataFrameEqual` with clearer diffs, falls back for Spark < 3.5.
- `assert_schema_equal(actual, expected, check_nullable)` — schema-only comparison with diff output.

### New Fixtures

- `df_builder` — pre-wired `DataFrameBuilder`
- `temp_delta` — factory fixture returning `DeltaTable` from `TempDeltaTable`, auto-cleanup

---

## 8. Retry

### RetryConfig

```python
@dataclass
class RetryConfig:
    max_attempts: int = 3
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 60.0
    backoff_factor: float = 2.0
    retryable_exceptions: tuple[type[Exception], ...] = (...)  # Spark/Databricks transient failures
```

### @retry decorator

```python
def retry(config: RetryConfig | None = None):
    """Decorator for retrying transient failures with exponential backoff.
    Logs each retry attempt. Emits retry metrics if MetricsSink available.
    Raises original exception after max_attempts exhausted."""
```

When `Workflow` has a `retry_config`, `execute()` wraps `run()` with retry automatically.

---

## 9. Enhanced Workflow

```python
class Workflow(ABC):
    def __init__(
        self,
        *,
        spark: SparkSession | None = None,
        dbutils: Any | None = None,              # retained from v0.1
        log_level: int = logging.INFO,            # retained from v0.1
        config: JobConfig | None = None,          # new in v0.2
        metrics: MetricsSink | None = None,       # new in v0.2
        retry_config: RetryConfig | None = None,  # new in v0.2
    ) -> None:
        ...

    @abstractmethod
    def run(self) -> None: ...

    def execute(self) -> None:
        """inject dbutils → apply spark configs → emit job_start →
        run_at_time() with retry → emit job_complete/job_failed → flush metrics"""

    def run_at_time(self, execution_time: datetime | None = None) -> None:
        """Retained from v0.1 — sets execution_time, then calls run()."""

    def emit_metric(self, event_type: str, **kwargs) -> None:
        """Convenience — builds MetricEvent with job_name and timestamp pre-filled.
        No-op if no metrics sink configured."""

    def quality_check(self, df: DataFrame, gate: QualityGate,
                      table_name: str | None = None) -> DataFrame:
        """Run gate, emit quality metrics, return clean df."""

    @property
    def config(self) -> JobConfig: ...

    @property
    def metrics(self) -> MetricsSink | None: ...
```

**Backward compatible:** All v0.1 parameters (`spark`, `dbutils`, `log_level`) are retained with same defaults. Keyword-only (`*`) enforced. New parameters are additive. See Section 12 for full backward compatibility details.

---

## 10. Existing Code Fixes

1. **Bare `except Exception` → `AnalysisException`** in `delta.py` (`_ensure_table_exists`) and `validators.py` (`_table_exists`). Note: the bare `except Exception` in `workflow.py` line 66 is **intentionally kept** — dbutils injection can fail for non-Spark reasons (import errors, attribute errors outside Databricks), so broad catch is correct there.
2. **Unify dbutils injection** — single `inject_dbutils()` that sets both `SecretFetcher` and `dbfs` module. The existing `dbfs.inject_dbutils_module()` is renamed to `dbfs._set_dbutils_module()` (private, since the public API is the new top-level function).
3. **Expand DBFS module** — add `ls()`, `mv()`, `rm()`, `mkdirs()`
4. **Add StreamingTableReader integration tests**
5. **Add `replace_data()` tests** for DeltaTable

---

## 11. Documentation Plan

Comprehensive documentation published alongside the code:

```
docs/
├── index.md                    # Library overview, installation, quick start
├── getting-started/
│   ├── installation.md
│   ├── quick-start.md          # First workflow in 5 minutes
│   └── concepts.md             # Core concepts: Workflow, Config, Metrics, Quality
├── guides/
│   ├── config.md               # JobConfig vs UnityConfig, environment resolution
│   ├── delta-operations.md     # Read, write, merge, upsert, SCD Type 2
│   ├── quality-gates.md        # Expectations, QualityGate, quarantine pattern
│   ├── metrics.md              # MetricsSink, logging sink, delta sink, custom sinks
│   ├── streaming.md            # StreamingTableReader, CheckpointManager
│   ├── schema-evolution.md     # SchemaDiff, pre-write checks, migration patterns
│   ├── testing.md              # DataFrameBuilder, TempDeltaTable, assertions, fixtures
│   ├── retry.md                # RetryConfig, transient failure handling
│   └── workflow.md             # Full workflow lifecycle, wiring it all together
├── api/                        # Auto-generated or hand-written API reference
│   ├── config.md
│   ├── metrics.md
│   ├── quality.md
│   ├── io.md
│   ├── testing.md
│   └── workflow.md
└── examples/
    ├── bronze_to_silver.py     # Complete medallion ETL example
    ├── streaming_pipeline.py   # Streaming with checkpoint management
    ├── scd_type2.py            # SCD Type 2 merge pattern
    ├── quality_quarantine.py   # Quality gate with quarantine routing
    ├── multi_env_config.py     # UnityConfig across dev/staging/prod
    ├── custom_metrics_sink.py  # Implementing a custom MetricsSink
    ├── feature_pipeline.py     # ML feature engineering workflow
    └── testing_patterns.py     # Test patterns with builders and temp tables
```

---

## 12. Backward Compatibility & Migration

### Workflow constructor

The existing `Workflow.__init__` uses keyword-only arguments (`*`) and accepts `dbutils` and `log_level`. The v0.2 signature retains these for backward compatibility:

```python
class Workflow(ABC):
    def __init__(
        self,
        *,
        spark: SparkSession | None = None,
        dbutils: Any | None = None,
        log_level: int = logging.INFO,
        config: JobConfig | None = None,
        metrics: MetricsSink | None = None,
        retry_config: RetryConfig | None = None,
    ) -> None:
```

- `dbutils` and `log_level` are retained and work as before.
- If both `dbutils` and `config` are provided, `dbutils` is used for injection and `config.secret()` delegates to it.
- `execute()` still calls `run_at_time()` internally, preserving `execution_time` behavior. The new lifecycle wraps around the existing one: `inject dbutils → apply spark configs → emit job_start → run_at_time() with retry → emit job_complete/job_failed → flush metrics`. The "apply spark configs" step only activates when `config` is provided and `config.spark_configs` is non-empty — when `config` is `None` (v0.1 usage), this step is a no-op.

### DeltaTableAppender / DeltaTableOverwriter

These subclasses are retained and inherit new methods (`merge()`, `upsert()`, `scd_type2()`) from `DeltaTable`. They are not deprecated — they remain the recommended shorthand for single-mode write patterns.

### CatalogSchema vs UnityConfig

These serve different purposes and coexist:
- `CatalogSchema` — attribute-based access to known table names within a single schema. Used in code that references specific tables by attribute (e.g., `schema.events`).
- `UnityConfig` — environment-aware catalog resolution across schemas. Used for multi-environment deployment where catalog prefix changes per env.

They can be combined: `UnityConfig` resolves the catalog prefix, `CatalogSchema` provides attribute access within a resolved schema.

### DeltaTable.write() signature

Updated to include `schema_check`:

```python
def write(self, df: DataFrame, mode: str = "append", schema_check: bool = True) -> None:
```

`schema_check` also applies to `upsert()` and `scd_type2()` (both accept it as an optional parameter, defaulting to `True`). `MergeBuilder.execute()` does not auto-check schema since the merge condition itself defines the expected shape.

---

## 13. Dbutils Unification

A single top-level function in `databricks4py/__init__.py`:

```python
def inject_dbutils(dbutils_module: Any) -> None:
    """One-call injection that configures both SecretFetcher and DBFS.

    Call once at job startup (or let Workflow.__init__ handle it).
    """
    from databricks4py.secrets import SecretFetcher
    from databricks4py.io.dbfs import _set_dbutils_module

    SecretFetcher.dbutils = dbutils_module
    _set_dbutils_module(dbutils_module)
```

- `Workflow.__init__` calls this unified function when `dbutils` is provided.
- `secrets.inject_dbutils()` and `dbfs.inject_dbutils_module()` are retained as internal functions but the public API is the single top-level `inject_dbutils()`.
- `JobConfig.secret()` delegates to `SecretFetcher.fetch_secret()` — it does not import `SecretFetcher` at module level, avoiding circular dependencies. The import happens inside the method body.

---

## 14. Metrics Wiring (Explicit, No Magic)

`MergeResult` does **not** auto-discover a metrics sink. Instead, metrics emission is explicit at each integration point:

- **DeltaTable methods** (`upsert`, `scd_type2`, `merge().execute()`) accept an optional `metrics_sink: MetricsSink | None = None` parameter. When provided, they emit `MergeResult` metrics.
- **Workflow.quality_check()** emits quality metrics to `self.metrics` if set.
- **StreamingTableReader** accepts `metrics_sink` in constructor.

When used inside a `Workflow`, the workflow passes its sink explicitly:

```python
# Inside Workflow.run():
result = table.upsert(df, keys=["id"], metrics_sink=self.metrics)
```

No thread-locals, no context variables, no globals. Standalone callers just omit `metrics_sink`.

---

## 15. CompositeMetricsSink Interface

Lives in `metrics/base.py` alongside `MetricsSink`:

```python
class CompositeMetricsSink(MetricsSink):
    def __init__(self, *sinks: MetricsSink):
        self._sinks = sinks

    def emit(self, event: MetricEvent) -> None:
        for sink in self._sinks:
            sink.emit(event)

    def flush(self) -> None:
        for sink in self._sinks:
            sink.flush()
```

---

## 16. QualityGate Quarantine Handler

Changed from attribute assignment to constructor parameter:

```python
class QualityGate:
    def __init__(self, *expectations: Expectation,
                 on_fail: Literal["raise", "warn", "quarantine"] = "raise",
                 quarantine_handler: Callable[[DataFrame], None] | None = None):
        ...
```

`quarantine_handler` is required when `on_fail="quarantine"` — raises `ValueError` at construction if missing. This gives IDE completion, type checking, and fails fast.

```python
gate = QualityGate(
    NotNull("event_id"), InRange("amount", min_val=0),
    on_fail="quarantine",
    quarantine_handler=lambda bad_df: bad_df.write.mode("append").saveAsTable("bronze.quarantine")
)
clean_df = gate.enforce(raw_df)
```

---

## 17. RetryConfig Default Exceptions

```python
from py4j.protocol import Py4JNetworkError
from pyspark.sql.utils import AnalysisException

@dataclass
class RetryConfig:
    max_attempts: int = 3
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 60.0
    backoff_factor: float = 2.0
    retryable_exceptions: tuple[type[Exception], ...] = (
        Py4JNetworkError,          # JVM communication failures
        ConnectionError,           # network-level failures
        TimeoutError,              # storage/API timeouts
        OSError,                   # filesystem transient errors
    )
```

`AnalysisException` is intentionally excluded — it usually indicates a real error (missing table, bad SQL), not a transient failure. Users can add it to their config if they have a specific transient catalog scenario.

---

## 18. DataFrameBuilder Schema Input

`with_columns()` accepts Spark SQL type strings (same strings used in `CREATE TABLE` DDL):

```python
builder.with_columns({"id": "int", "name": "string", "ts": "timestamp", "amount": "double"})
```

For complex schemas, an overload accepts `StructType` directly:

```python
builder.with_schema(StructType([StructField("id", IntegerType(), False), ...]))
```

---

## 19. CheckpointManager Path Template

Auto-generated paths follow: `{base_path}/{source_sanitized}__{sink_sanitized}/`

Sanitization replaces `.` with `_` and strips non-alphanumeric characters:

```python
# source="catalog.bronze.events", sink="catalog.silver.events"
# → "{base_path}/catalog_bronze_events__catalog_silver_events/"
```

---

## 20. Module __init__.py Export Plan

Heavy modules remain lazy (not imported at top-level `__init__.py`):

| Module | Top-level import? | Reason |
|--------|-------------------|--------|
| `config/` | Yes | No JVM dependency |
| `metrics/` | Yes | No JVM dependency |
| `quality/` | No | Imports `pyspark.sql.DataFrame` |
| `io/` | No | Triggers JVM (existing pattern) |
| `filters/` | No | Existing pattern |
| `migrations/` | No | Existing pattern |
| `testing/` | No | Dev dependency only |
| `retry` | Yes | No JVM dependency |
| `workflow` | Yes | Already imported at top level |

---

## 21. v0.1 → v0.2 Migration Guide

### No breaking changes for existing code

All v0.1 code continues to work without modification:

```python
# This v0.1 code still works in v0.2:
class MyETL(Workflow):
    def run(self):
        df = self.spark.read.table("source")
        df.write.format("delta").saveAsTable("target")

MyETL(dbutils=pyspark.dbutils).execute()
```

### Adopting v0.2 features (opt-in)

1. **Add config:** Pass `config=UnityConfig(...)` to `super().__init__()`
2. **Add metrics:** Pass `metrics=LoggingMetricsSink()` to `super().__init__()`
3. **Add quality gates:** Call `self.quality_check(df, gate)` before writes
4. **Use merge/upsert:** Call `table.upsert(df, keys=[...])` instead of manual MERGE SQL
5. **Add retry:** Pass `retry_config=RetryConfig()` to `super().__init__()`

Each can be adopted independently. No "all or nothing."

---

## Non-Goals (out of scope for v0.2)

- Pipeline DSL / declarative DAG API (future v0.3 consideration)
- Databricks Connect integration
- Notebook-specific utilities
- DLT interop
- Multi-source streaming joins
- Stateful streaming operations
