# Metrics

## When to use this

When you need visibility into job execution: durations, row counts, success/failure, quality check results. Metrics flow through a simple `emit()` / `flush()` interface, and the sink decides where they land.

## MetricEvent

Every metric is a frozen dataclass:

```python
from databricks4py.metrics import MetricEvent
from datetime import datetime

event = MetricEvent(
    job_name="IngestOrders",
    event_type="upsert_complete",
    timestamp=datetime.now(),
    duration_ms=1234,
    row_count=50000,
    table_name="warehouse.silver.orders",
    batch_id=None,
    metadata={"inserted": 30000, "updated": 20000},
)
```

Fields `duration_ms`, `row_count`, `table_name`, `batch_id`, and `metadata` are all optional.

## MetricsSink interface

```python
from databricks4py.metrics import MetricsSink, MetricEvent

class MyCustomSink(MetricsSink):
    def emit(self, event: MetricEvent) -> None:
        # send it somewhere
        ...

    def flush(self) -> None:
        # optional: finalize any buffered writes
        ...
```

## Built-in sinks

### LoggingMetricsSink

Serializes each event as JSON to the standard Python logger. Good for development and debugging.

```python
from databricks4py import LoggingMetricsSink

sink = LoggingMetricsSink()
sink.emit(event)
# [INFO] {"job_name": "IngestOrders", "event_type": "upsert_complete", ...}
```

### DeltaMetricsSink

Buffers events and writes them to a Delta table. Flushes automatically when the buffer hits `buffer_size`, and on explicit `flush()`.

```python
from databricks4py.metrics import DeltaMetricsSink

sink = DeltaMetricsSink(
    "warehouse.ops.job_metrics",
    buffer_size=100,  # default
)
```

The Delta table is created automatically on first flush if it doesn't exist.

### CompositeMetricsSink

Fan out to multiple sinks simultaneously:

```python
from databricks4py import CompositeMetricsSink, LoggingMetricsSink
from databricks4py.metrics import DeltaMetricsSink

sink = CompositeMetricsSink(
    LoggingMetricsSink(),
    DeltaMetricsSink("warehouse.ops.metrics"),
)
```

`flush()` calls `flush()` on every child sink.

## Auto-capture in Workflow

When you pass a `MetricsSink` to a `Workflow`, it auto-emits:

| Event | When |
|---|---|
| `job_start` | Before `run()` |
| `job_complete` | After successful `run()`, includes `duration_ms` |
| `job_failed` | On exception, includes `duration_ms` |
| `quality_check` | On `self.quality_check()`, includes pass/fail metadata |

`flush()` is called in the `finally` block of `execute()`, so buffered sinks like `DeltaMetricsSink` always write their remaining events.

## emit_metric() usage

Inside a Workflow subclass:

```python
class MyJob(Workflow):
    def run(self):
        df = self.spark.read.table("source")
        count = df.count()

        self.emit_metric("read_complete", row_count=count, table_name="source")

        # ... transform and write ...

        self.emit_metric(
            "transform_complete",
            row_count=output_count,
            metadata={"dropped": count - output_count},
        )
```

If no metrics sink is configured, `emit_metric()` is a no-op.

## Complete example

```python
from databricks4py import Workflow, JobConfig, CompositeMetricsSink, LoggingMetricsSink
from databricks4py.metrics import DeltaMetricsSink

sink = CompositeMetricsSink(
    LoggingMetricsSink(),
    DeltaMetricsSink("ops.etl.run_metrics"),
)

class OrderPipeline(Workflow):
    def run(self):
        raw = self.spark.read.table(self.config.table("raw"))
        self.emit_metric("source_read", row_count=raw.count())

        cleaned = raw.dropDuplicates(["order_id"]).where("amount > 0")
        self.emit_metric(
            "cleaning_complete",
            row_count=cleaned.count(),
            metadata={"dropped": raw.count() - cleaned.count()},
        )

        cleaned.write.format("delta").mode("append").saveAsTable(
            self.config.table("silver")
        )

config = JobConfig(tables={"raw": "lake.raw.orders", "silver": "lake.silver.orders"})
OrderPipeline(config=config, metrics=sink).execute()
```
