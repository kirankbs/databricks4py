# databricks4py

Spark, Delta Lake, and Databricks utility library for Python.

A collection of reusable abstractions for building PySpark applications on Databricks and locally.

## Features

- **SparkSession Management** — `get_active()`, `active_fallback()`, `get_or_create_local_session()`
- **DeltaTable Abstraction** — structured API for creating, reading, writing, and managing Delta tables with generated columns, partitioning, and atomic table replacement
- **Streaming Utilities** — `StreamingTableReader` ABC for structured streaming micro-batch processing with trigger options
- **Filter Pipeline** — composable `Filter` chain with built-in `DropDuplicates`, `WhereFilter`, `ColumnFilter`
- **Migration Framework** — `TableValidator` for structural validation before/after Delta table migrations
- **Secrets Management** — `SecretFetcher` for Databricks dbutils secrets with injectable dbutils
- **Catalog Management** — `CatalogSchema` for schema-qualified table naming with versioned table support
- **Workflow Base Class** — `Workflow` ABC with auto-initialized SparkSession, logging, and dbutils
- **Test Fixtures** — pytest fixtures for SparkSession, mock dbutils, and environment isolation

## Installation

```bash
pip install databricks4py
```

### Prerequisites

- **Python** >= 3.10
- **Java 17+** — required by PySpark for running Spark locally and in tests
- **pyspark.dbutils** — only available on [Databricks Runtime](https://docs.databricks.com/dev-tools/databricks-utils.html); not needed for local development

## Quick Start

### Workflow

```python
from databricks4py import Workflow
from databricks4py.io import DeltaTableAppender

class MyETL(Workflow):
    def run(self):
        df = self.spark.read.table("source")
        output = DeltaTableAppender(
            table_name="target",
            schema=df.schema,
            spark=self.spark,
        )
        output.append(df)

# Entry point for Databricks job
def main():
    import pyspark.dbutils  # only on Databricks Runtime
    MyETL(dbutils=pyspark.dbutils).execute()
```

### DeltaTable

```python
from databricks4py.io import DeltaTable, GeneratedColumn

table = DeltaTable(
    table_name="catalog.schema.events",
    schema=events_schema,
    location="/data/events",
    partition_by="event_date",
    generated_columns=[
        GeneratedColumn("event_date", "DATE", "CAST(event_ts AS DATE)"),
    ],
)

df = table.dataframe()          # Read
table.write(df, mode="append")  # Write
table.detail()                  # Metadata
table.partition_columns()       # ["event_date"]
table.size_in_bytes()           # Physical size
```

### Filter Pipeline

```python
from databricks4py.filters import FilterPipeline, DropDuplicates, WhereFilter

pipeline = FilterPipeline([
    DropDuplicates(subset=["id"]),
    WhereFilter("status = 'active'"),
])
clean_df = pipeline(raw_df)
```

### Streaming

```python
from databricks4py.io import StreamingTableReader, StreamingTriggerOptions

class MyProcessor(StreamingTableReader):
    def process_batch(self, df, batch_id):
        df.write.format("delta").mode("append").saveAsTable("output")

reader = MyProcessor(
    source_table="input_stream",
    trigger=StreamingTriggerOptions.PROCESSING_TIME_1M,
    checkpoint_location="/checkpoints/my_reader",
)
query = reader.start()
query.awaitTermination()
```

### Migration Validation

```python
from databricks4py.migrations import TableValidator

validator = TableValidator(
    table_name="catalog.schema.events",
    expected_columns=["id", "name", "event_date"],
    expected_partition_columns=["event_date"],
)
result = validator.validate()
result.raise_if_invalid("catalog.schema.events")
```

### Test Fixtures

```python
# In your conftest.py
from databricks4py.testing.fixtures import *  # noqa: F401,F403

# In your tests
def test_my_etl(spark_session_function):
    df = spark_session_function.createDataFrame([{"id": 1}])
    assert df.count() == 1
```

## Examples

The `docs/examples/` directory contains runnable examples:

| File | Runs locally? | Description |
|------|:---:|-------------|
| `quickstart.py` | Yes | All core interfaces in one script |
| `delta_tables.py` | Yes | DeltaTable, Appender, Overwriter, optimize, vacuum, replace_data |
| `filters_and_pipelines.py` | Yes | Filter, FilterPipeline, custom filters |
| `catalog_and_logging.py` | Yes (no Java) | CatalogSchema, configure_logging |
| `testing_guide.py` | Yes (no Java) | MockDBUtils, fixtures, test patterns |
| `simple_etl.py` | Databricks | Workflow-based ETL job |
| `streaming_job.py` | Databricks | StreamingTableReader micro-batch job |
| `migration_check.py` | Databricks | Two-stage migration with validation |

```bash
# Run locally (requires Java 17+)
python docs/examples/quickstart.py

# No Java needed
python docs/examples/catalog_and_logging.py
python docs/examples/testing_guide.py
```

## Compatibility

| PySpark | delta-spark | Python |
|---------|-------------|--------|
| 3.5.x   | 3.2.x       | >= 3.10 |
| 3.4.x   | 2.4.x       | >= 3.10 |
| 4.x     | 4.x         | >= 3.10 |

## Development

```bash
git clone https://github.com/kirankbs/databricks4py.git
cd databricks4py
pip install -e ".[dev]"

# Lint
ruff check src/ tests/ docs/
ruff format --check src/ tests/ docs/

# Run all tests (requires Java 17+)
pytest -v --timeout=120

# Run by category
pytest -m no_pyspark --timeout=30          # Fast, no Spark/Java
pytest -m "integration or unit" --timeout=120  # Requires Java 17+
```

## License

MIT
