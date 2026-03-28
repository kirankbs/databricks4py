# databricks4py

Spark, Delta Lake, and Databricks utility library for Python.

The patterns you keep re-implementing across PySpark jobs — Delta table management, streaming foreachBatch wiring, schema migrations, data quality checks — packaged as a library. Runs on Databricks and locally with open-source Spark + Delta Lake.

## Install

```bash
pip install databricks4py
```

**Requirements:** Python >= 3.10, Java 17+ (for PySpark). `pyspark.dbutils` only available on Databricks Runtime.

## Core modules

### Delta table management

Wraps the delta-spark API into a single object that handles table creation, reads, writes, metadata, and schema validation.

```python
from databricks4py.io import DeltaTable, GeneratedColumn

# Create a table with generated columns and partitioning
table = DeltaTable(
    table_name="catalog.schema.events",
    schema={"id": "int", "name": "string", "event_ts": "timestamp", "event_date": "date"},
    location="/data/events",
    partition_by="event_date",
    generated_columns=[GeneratedColumn("event_date", "DATE", "CAST(event_ts AS DATE)")],
)

df = table.dataframe()              # Read
table.write(df, mode="append")      # Write (schema_check=True by default)
table.detail()                      # Metadata DataFrame
table.partition_columns()           # ["event_date"]
table.size_in_bytes()               # Physical size in bytes
```

**Convenience wrappers:**

```python
from databricks4py.io import DeltaTableAppender, DeltaTableOverwriter

appender = DeltaTableAppender("target", schema=my_schema)
appender.append(df)

overwriter = DeltaTableOverwriter("target", schema=my_schema)
overwriter.overwrite(df)
```

**Table maintenance:**

```python
from databricks4py.io.delta import optimize_table, vacuum_table

optimize_table("catalog.schema.events", z_order_by=["id"])
vacuum_table("catalog.schema.events", retention_hours=168)
```

### Merge operations

Fluent builder for Delta `MERGE INTO`. Chain conditions, execute, get back row counts.

```python
from databricks4py.io import MergeBuilder

result = (
    MergeBuilder("catalog.schema.target", source_df, spark)
    .on("id", "date")                         # Join keys
    .when_matched_update(["name", "value"])    # Update specific columns
    .when_not_matched_insert()                 # Insert new rows
    .when_not_matched_by_source_delete()       # Remove stale rows
    .execute()
)

print(f"Inserted: {result.rows_inserted}, Updated: {result.rows_updated}")
```

Custom join conditions:

```python
result = (
    MergeBuilder("target", source_df, spark)
    .on_condition("target.id = source.id AND target.region = source.region")
    .when_matched_update()
    .when_not_matched_insert()
    .execute()
)
```

SCD Type 2 is built into DeltaTable:

```python
table.scd_type2(source_df, keys=["id"])
```

### Streaming

Subclass `StreamingTableReader`, implement `process_batch`, and the base class handles the `foreachBatch` wiring, empty-batch skipping, row filtering, metrics, and DLQ routing.

```python
from databricks4py.io import StreamingTableReader, StreamingTriggerOptions

class EventProcessor(StreamingTableReader):
    def process_batch(self, df, batch_id):
        clean = df.where("status IS NOT NULL")
        clean.write.format("delta").mode("append").saveAsTable("output")

reader = EventProcessor(
    source_table="catalog.schema.raw_events",
    trigger=StreamingTriggerOptions.PROCESSING_TIME_1M,
    checkpoint_location="/checkpoints/events",
    dead_letter_table="catalog.schema.dlq",   # Failed batches go here
)
query = reader.start()
reader.stop(timeout_seconds=30)  # Graceful shutdown
```

If `dead_letter_table` is set and `process_batch` raises, the batch DataFrame is written to the DLQ table with `_dlq_error_message`, `_dlq_error_timestamp`, and `_dlq_batch_id` columns appended. The stream keeps running.

**Checkpoint management:**

```python
from databricks4py.io import CheckpointManager

mgr = CheckpointManager(base_path="/checkpoints")
reader = EventProcessor(
    source_table="input",
    checkpoint_manager=mgr,  # Auto-generates checkpoint path
)
```

### Migration framework

Ordered migration runner, same idea as Flyway or Alembic but for Delta tables. Steps are Python callables, history is tracked in a Delta table, and each version runs exactly once.

```python
from databricks4py.migrations import MigrationRunner, MigrationStep

def add_audit_columns(spark):
    spark.sql("ALTER TABLE catalog.schema.events ADD COLUMNS (created_at TIMESTAMP)")

def backfill_defaults(spark):
    spark.sql("UPDATE catalog.schema.events SET created_at = current_timestamp()")

runner = MigrationRunner(history_table="catalog.schema._migrations")
runner.register(
    MigrationStep(version="V001", description="Add audit columns", up=add_audit_columns),
    MigrationStep(version="V002", description="Backfill defaults", up=backfill_defaults),
)

# Check what needs to run
pending = runner.pending()  # [MigrationStep(V002, ...)]

# Execute (idempotent — already-applied steps are skipped)
result = runner.run()
print(result.applied)   # ["V002"]
print(result.skipped)   # ["V001"]

# Dry run (logs what would run, no side effects)
result = runner.run(dry_run=True)
```

**Pre/post validation per step:**

```python
MigrationStep(
    version="V003",
    description="Rename column",
    up=lambda spark: spark.sql("ALTER TABLE t RENAME COLUMN old TO new"),
    pre_validate=lambda spark: spark.catalog.tableExists("t"),
    post_validate=lambda spark: "new" in spark.read.table("t").columns,
)
```

### Table alter (fluent DDL)

Queue up `ALTER TABLE` operations and apply them in one go.

```python
from databricks4py.migrations import TableAlter

TableAlter("catalog.schema.events") \
    .add_column("region", "STRING", comment="ISO-3166 region code") \
    .set_property("delta.enableChangeDataFeed", "true") \
    .apply()
```

Rename and drop require Delta column mapping:

```python
TableAlter("catalog.schema.events") \
    .set_property("delta.columnMapping.mode", "name") \
    .set_property("delta.minReaderVersion", "2") \
    .set_property("delta.minWriterVersion", "5") \
    .apply()

TableAlter("catalog.schema.events") \
    .rename_column("old_name", "new_name") \
    .drop_column("deprecated_col") \
    .apply()
```

### Schema diff

Compare two schemas (or a live table vs. an incoming DataFrame) and get back a list of column-level changes with severity.

```python
from databricks4py.migrations import SchemaDiff

diff = SchemaDiff.from_tables("catalog.schema.events", new_df)
for change in diff.changes():
    print(f"{change.column}: {change.change_type} [{change.severity}]")

if diff.has_breaking_changes():
    raise RuntimeError(diff.summary())
```

### Table validation

Check that a Delta table matches expected columns, partitions, and location before or after a migration.

```python
from databricks4py.migrations import TableValidator

validator = TableValidator(
    table_name="catalog.schema.events",
    expected_columns=["id", "name", "event_date"],
    expected_partition_columns=["event_date"],
    expected_location_contains="/data/events",
)
result = validator.validate()
if not result.is_valid:
    print(result.errors)    # ["Missing required columns: ['event_date']"]
    print(result.warnings)  # ["Unexpected extra columns: ['debug_flag']"]
result.raise_if_invalid("catalog.schema.events")  # Raises MigrationError
```

### Data quality

Row-level expectations you can run individually or bundle into a gate that raises, warns, or quarantines bad rows.

```python
from databricks4py.quality.expectations import NotNull, InRange, Unique, RowCount, MatchesRegex
from databricks4py.quality.gate import QualityGate

# Individual expectations
result = NotNull("id", "name").validate(df)
result = InRange("score", min_val=0, max_val=100).validate(df)
result = Unique("id").validate(df)
result = RowCount(min_count=1, max_count=1_000_000).validate(df)
result = MatchesRegex("email", r"^.+@.+\..+$").validate(df)
# result.passed, result.failing_rows, result.total_rows

# Quality gate — enforce multiple expectations
gate = QualityGate(
    NotNull("id"),
    InRange("score", min_val=0, max_val=100),
    on_fail="raise",  # or "warn" or "quarantine"
)
clean_df = gate.enforce(df)  # Raises QualityError if checks fail
```

**Quarantine mode** splits bad rows and routes them to a handler:

```python
gate = QualityGate(
    NotNull("id"),
    on_fail="quarantine",
    quarantine_handler=lambda bad_df: bad_df.write.saveAsTable("quarantine_table"),
)
clean_df = gate.enforce(df)  # Returns only clean rows
```

### Filter pipeline

Chain DataFrame transformations. Each filter is a callable that takes and returns a DataFrame.

```python
from databricks4py.filters import FilterPipeline, DropDuplicates, WhereFilter, ColumnFilter

pipeline = FilterPipeline([
    DropDuplicates(subset=["id"]),
    WhereFilter("status = 'active'"),
    ColumnFilter(columns=["id", "name", "status"]),
])
clean_df = pipeline(raw_df)
```

### Workflow

Base class for Databricks jobs. Handles SparkSession init, config application, lifecycle metrics (`job_start`/`job_complete`/`job_failed`), quality gates, and optional retry.

```python
from databricks4py import Workflow
from databricks4py.quality.expectations import NotNull
from databricks4py.quality.gate import QualityGate

class MyETL(Workflow):
    def run(self):
        source = self.spark.read.table("raw_events")

        # Quality check with metrics emission
        gate = QualityGate(NotNull("id", "event_ts"), on_fail="raise")
        clean = self.quality_check(source, gate, table_name="raw_events")

        clean.write.format("delta").mode("append").saveAsTable("clean_events")
        self.emit_metric("write_complete", row_count=clean.count())

# With config and metrics
from databricks4py.config import JobConfig
from databricks4py.metrics import DeltaMetricsSink

config = JobConfig(tables={"source": "raw_events"}, spark_configs={"spark.sql.shuffle.partitions": "8"})
sink = DeltaMetricsSink("catalog.schema.job_metrics")
MyETL(config=config, metrics=sink).execute()
```

### Configuration

Environment auto-detected from Databricks widgets or `ENV`/`ENVIRONMENT` env vars, defaults to DEV.

```python
from databricks4py.config import JobConfig, Environment

config = JobConfig(
    tables={"events": "catalog.bronze.events", "users": "catalog.silver.users"},
    secret_scope="my-scope",
    spark_configs={"spark.sql.shuffle.partitions": "8"},
)
config.env          # Environment.DEV (auto-detected)
config.table("events")  # "catalog.bronze.events"
```

**Unity Catalog** — environment-aware catalog resolution:

```python
from databricks4py.config import UnityConfig

config = UnityConfig(catalog_prefix="myapp", schemas=["bronze", "silver"])
config.catalog                    # "myapp_dev" (or myapp_prod in production)
config.table("bronze.events")     # "myapp_dev.bronze.events"
```

### Metrics

Buffer events and write them to a Delta table, log them as JSON, or both.

```python
from databricks4py.metrics import DeltaMetricsSink, LoggingMetricsSink, CompositeMetricsSink, MetricEvent

# Write metrics to a Delta table
delta_sink = DeltaMetricsSink("catalog.schema.metrics", buffer_size=50)

# Log metrics as JSON
log_sink = LoggingMetricsSink()

# Fan out to multiple sinks
sink = CompositeMetricsSink(delta_sink, log_sink)
sink.emit(MetricEvent(job_name="etl", event_type="batch_complete", timestamp=now, row_count=1000))
sink.flush()
```

### Observability

**Structured batch logging** — JSON log records per batch with correlation IDs, queryable in any log aggregation system.

```python
from databricks4py.observability import BatchContext, BatchLogger

logger = BatchLogger(extra_fields={"pipeline": "events", "env": "prod"})

# Inside your StreamingTableReader.process_batch:
ctx = BatchContext.create(batch_id=batch_id, source_table="catalog.schema.events")
logger.batch_start(ctx)
# ... process ...
logger.batch_complete(ctx, row_count=df.count(), duration_ms=ctx.elapsed_ms())
# On error:
logger.batch_error(ctx, error=str(exc))
```

Each log line is single-line JSON: `{"event": "batch_complete", "batch_id": 42, "correlation_id": "a1b2c3d4e5f6", "row_count": 1000, ...}`

**Query progress listener** — wraps PySpark 3.4+ `StreamingQueryListener` to collect progress snapshots and route them to a MetricsSink.

```python
from databricks4py.observability import QueryProgressObserver

observer = QueryProgressObserver(metrics_sink=my_sink, query_name_filter="events_processor")
observer.attach()

# After the stream runs:
latest = observer.latest_progress()
print(f"Batch {latest.batch_id}: {latest.processed_rows_per_second} rows/sec")

history = observer.history(limit=10)
observer.detach()
```

**Health checks** — poll a streaming query for stuck detection, slow batches, and low throughput.

```python
from databricks4py.observability import StreamingHealthCheck, HealthStatus

check = StreamingHealthCheck(
    query,
    max_batch_duration_ms=60_000,     # DEGRADED if batch > 60s
    min_processing_rate=100.0,         # DEGRADED if < 100 rows/sec
    stale_timeout_seconds=300,         # UNHEALTHY if no progress for 5min
)
result = check.evaluate()
if result.status != HealthStatus.HEALTHY:
    print(result.summary())
```

### Retry

```python
from databricks4py.retry import retry, RetryConfig

@retry(RetryConfig(max_attempts=5, base_delay_seconds=2.0, backoff_factor=3.0))
def fetch_from_api():
    return requests.get(url).json()
```

### Testing utilities

Session-scoped SparkSession (one JVM per test run), function-scoped cleanup, and helpers for building test data.

```python
# conftest.py — register fixtures
from databricks4py.testing.fixtures import *  # noqa: F401,F403
```

**DataFrameBuilder** — fluent test data construction:

```python
def test_my_transform(spark_session_function):
    df = (
        DataFrameBuilder(spark_session_function)
        .with_columns({"id": "int", "name": "string", "score": "int"})
        .with_rows((1, "Alice", 95), (2, "Bob", 80))
        .build()
    )
    assert df.count() == 2
```

**TempDeltaTable** — ephemeral Delta tables for test isolation:

```python
def test_merge(spark_session_function, tmp_path):
    with TempDeltaTable(spark_session_function, schema={"id": "int"}, data=[(1,), (2,)]) as table:
        assert table.dataframe().count() == 2
    # Table is auto-dropped after the context exits
```

**Assertions:**

```python
from databricks4py.testing.assertions import assert_frame_equal, assert_schema_equal

assert_frame_equal(actual_df, expected_df, check_order=False)
assert_schema_equal(actual_df.schema, expected_schema, check_nullable=False)
```

**Mock Databricks utilities:**

```python
from databricks4py.testing.mocks import MockDBUtils, MockDBUtilsModule

mock = MockDBUtils()
mock.secrets.put("scope", "key", "secret-value")
assert mock.secrets.get("scope", "key") == "secret-value"
```

## Project structure

```
src/databricks4py/
├── __init__.py              # Top-level exports
├── spark_session.py         # get_active(), active_fallback(), get_or_create_local_session()
├── catalog.py               # CatalogSchema for schema-qualified table access
├── logging.py               # configure_logging(), get_logger()
├── secrets.py               # SecretFetcher with injectable dbutils
├── retry.py                 # retry() decorator with exponential backoff
├── workflow.py              # Workflow ABC for Databricks job entry points
├── config/
│   ├── base.py              # JobConfig, Environment
│   └── unity.py             # UnityConfig (Unity Catalog-aware)
├── io/
│   ├── delta.py             # DeltaTable, Appender, Overwriter, optimize, vacuum
│   ├── merge.py             # MergeBuilder, MergeResult
│   ├── streaming.py         # StreamingTableReader, StreamingTriggerOptions
│   ├── checkpoint.py        # CheckpointManager, CheckpointInfo
│   └── dbfs.py              # DBFS file operations (Databricks only)
├── filters/
│   └── base.py              # Filter, FilterPipeline, DropDuplicates, WhereFilter, ColumnFilter
├── migrations/
│   ├── runner.py            # MigrationRunner, MigrationStep, MigrationRunResult
│   ├── alter.py             # TableAlter (fluent DDL builder)
│   ├── validators.py        # TableValidator, ValidationResult, MigrationError
│   └── schema_diff.py       # SchemaDiff, ColumnChange, SchemaEvolutionError
├── quality/
│   ├── base.py              # Expectation, ExpectationResult, QualityReport
│   ├── expectations.py      # NotNull, InRange, Unique, RowCount, MatchesRegex, ColumnExists
│   └── gate.py              # QualityGate, QualityError
├── metrics/
│   ├── base.py              # MetricEvent, MetricsSink, CompositeMetricsSink
│   ├── delta_sink.py        # DeltaMetricsSink (buffered Delta table writer)
│   └── logging_sink.py      # LoggingMetricsSink (JSON to logger)
├── observability/
│   ├── batch_context.py     # BatchContext, BatchLogger (structured per-batch JSON logging)
│   ├── query_listener.py    # QueryProgressObserver (StreamingQueryListener wrapper)
│   └── health.py            # StreamingHealthCheck, HealthStatus, HealthResult
└── testing/
    ├── fixtures.py          # spark_session, spark_session_function, df_builder, temp_delta
    ├── builders.py          # DataFrameBuilder (fluent test data)
    ├── temp_table.py        # TempDeltaTable (auto-cleanup context manager)
    ├── assertions.py        # assert_frame_equal, assert_schema_equal
    └── mocks.py             # MockDBUtils, MockDBUtilsModule
```

## Development setup

```bash
git clone https://github.com/kirankbs/databricks4py.git
cd databricks4py
pip install -e ".[dev]"

# Lint
ruff check src/ tests/ docs/
ruff format --check src/ tests/ docs/

# Tests
pytest -m no_pyspark --timeout=30                  # Fast, no Spark/Java
pytest -m "integration or unit" --timeout=120      # Integration (requires Java 17+)
pytest -v --timeout=120                            # Everything
```

## Compatibility matrix

| PySpark | delta-spark | Python |
|---------|-------------|--------|
| 3.5.x   | 3.2.x       | >= 3.10 |
| 3.4.x   | 2.4.x       | >= 3.10 |
| 4.x     | 4.x         | >= 3.10 |

## License

MIT
