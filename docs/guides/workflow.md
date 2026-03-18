# Workflow

## When to use this

As the entry point for every Databricks job. `Workflow` gives you a structured lifecycle with SparkSession management, optional config/metrics/quality/retry wiring, and consistent error handling.

## Full lifecycle

```
Workflow.__init__()
    ├── configure_logging()
    ├── active_fallback(spark)       # get or accept SparkSession
    └── inject_dbutils(dbutils)      # optional

Workflow.execute()
    ├── apply spark_configs from JobConfig
    ├── emit "job_start"
    ├── run_at_time()                # sets execution_time, calls run()
    │   └── run()                    # YOUR CODE
    ├── emit "job_complete"          # on success
    ├── emit "job_failed"            # on exception, then re-raise
    └── metrics.flush()              # always, in finally
```

If `retry_config` is set, `run_at_time()` is wrapped with the retry decorator.

## Minimal example

```python
from databricks4py import Workflow

class SimpleJob(Workflow):
    def run(self):
        df = self.spark.read.table("source_table")
        df.write.format("delta").mode("overwrite").saveAsTable("target_table")

SimpleJob().execute()
```

## Wiring config + metrics + quality + retry

```python
from databricks4py import Workflow, JobConfig, LoggingMetricsSink, RetryConfig
from databricks4py.quality import QualityGate, NotNull, Unique

class FullFeaturedJob(Workflow):
    def run(self):
        source = self.spark.read.table(self.config.table("source"))

        gate = QualityGate(
            NotNull("id"), Unique("id"),
            on_fail="raise",
        )
        clean = self.quality_check(source, gate, table_name="source")

        clean.write.format("delta").mode("append").saveAsTable(
            self.config.table("target")
        )
        self.emit_metric("write_complete", row_count=clean.count())

config = JobConfig(
    tables={"source": "raw.events", "target": "clean.events"},
    secret_scope="prod-secrets",
    spark_configs={"spark.sql.shuffle.partitions": "200"},
)

FullFeaturedJob(
    config=config,
    metrics=LoggingMetricsSink(),
    retry_config=RetryConfig(max_attempts=3),
).execute()
```

## Available properties

| Property | Type | Description |
|---|---|---|
| `self.spark` | `SparkSession` | The active SparkSession |
| `self.dbutils` | `Any \| None` | dbutils module, or None outside Databricks |
| `self.execution_time` | `datetime` | Logical execution time (set by `run_at_time`) |
| `self.config` | `JobConfig \| None` | The config object |
| `self.metrics` | `MetricsSink \| None` | The metrics sink |

## run_at_time for backfills

```python
from datetime import datetime

job = MyJob(config=config)

# Process as if it's a specific date
job.run_at_time(datetime(2024, 1, 15))

# Inside run(), self.execution_time == datetime(2024, 1, 15)
```

## Backward compatibility with v0.1

v0.1 had the same `Workflow` ABC with `spark`, `dbutils`, and `run()`. v0.2 adds optional `config`, `metrics`, and `retry_config` parameters -- all defaulting to None. Existing v0.1 subclasses work without changes:

```python
# v0.1 code -- still works in v0.2
class LegacyJob(Workflow):
    def run(self):
        self.spark.read.table("source").write.saveAsTable("target")

LegacyJob().execute()
```

## Complete example

```python
from databricks4py import (
    Workflow, JobConfig, CompositeMetricsSink,
    LoggingMetricsSink, RetryConfig,
)
from databricks4py.metrics import DeltaMetricsSink
from databricks4py.io import DeltaTable
from databricks4py.quality import QualityGate, NotNull, InRange

class OrderPipeline(Workflow):
    def run(self):
        # Read
        raw = self.spark.read.table(self.config.table("bronze"))
        self.emit_metric("read_complete", row_count=raw.count(), table_name="bronze")

        # Quality
        gate = QualityGate(
            NotNull("order_id", "customer_id"),
            InRange("amount", min_val=0),
            on_fail="raise",
        )
        clean = self.quality_check(raw, gate, table_name="bronze")

        # Write
        silver = DeltaTable(
            self.config.table("silver"),
            schema={"order_id": "long", "customer_id": "long", "amount": "double"},
        )
        result = silver.upsert(clean, keys=["order_id"])

        self.emit_metric(
            "upsert_complete",
            row_count=result.rows_inserted + result.rows_updated,
            table_name="silver",
            metadata={
                "inserted": result.rows_inserted,
                "updated": result.rows_updated,
            },
        )

config = JobConfig(
    tables={
        "bronze": "warehouse.raw.orders",
        "silver": "warehouse.clean.orders",
    },
    secret_scope="etl-secrets",
)

metrics = CompositeMetricsSink(
    LoggingMetricsSink(),
    DeltaMetricsSink("warehouse.ops.pipeline_metrics"),
)

OrderPipeline(
    config=config,
    metrics=metrics,
    retry_config=RetryConfig(max_attempts=3, base_delay_seconds=5.0),
).execute()
```
