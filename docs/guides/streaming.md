# Streaming

## When to use this

For micro-batch streaming jobs that read from Delta tables (or other sources) and process each batch with custom logic. `StreamingTableReader` handles the `foreachBatch` wiring, empty batch skipping, filtering, and per-batch metrics.

## StreamingTableReader

Subclass it and implement `process_batch`:

```python
from databricks4py.io import StreamingTableReader, StreamingTriggerOptions

class OrderProcessor(StreamingTableReader):
    def process_batch(self, df, batch_id):
        cleaned = df.dropDuplicates(["order_id"])
        cleaned.write.format("delta").mode("append").saveAsTable("warehouse.silver.orders")

reader = OrderProcessor(
    source_table="warehouse.bronze.raw_orders",
    trigger=StreamingTriggerOptions.PROCESSING_TIME_1M,
    checkpoint_location="/checkpoints/order_processor",
)
query = reader.start()
query.awaitTermination()
```

### Constructor parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `source_table` | `str` | required | Table or path to read as a stream |
| `trigger` | `StreamingTriggerOptions \| dict` | 10 seconds | Trigger configuration |
| `checkpoint_location` | `str \| None` | None | Checkpoint path (required if no manager) |
| `source_format` | `str` | `"delta"` | Source format |
| `filter` | `Filter \| None` | None | Filter to apply before processing |
| `skip_empty_batches` | `bool` | True | Skip batches with 0 rows |
| `read_options` | `dict` | `{}` | Additional readStream options |
| `checkpoint_manager` | `CheckpointManager \| None` | None | Auto-generates checkpoint paths |
| `metrics_sink` | `MetricsSink \| None` | None | Emits `batch_complete` metrics |

### Trigger options

```python
from databricks4py.io import StreamingTriggerOptions

StreamingTriggerOptions.PROCESSING_TIME_10S   # {"processingTime": "10 seconds"}
StreamingTriggerOptions.PROCESSING_TIME_30S   # {"processingTime": "30 seconds"}
StreamingTriggerOptions.PROCESSING_TIME_1M    # {"processingTime": "1 minute"}
StreamingTriggerOptions.PROCESSING_TIME_5M    # {"processingTime": "5 minutes"}
StreamingTriggerOptions.PROCESSING_TIME_10M   # {"processingTime": "10 minutes"}
StreamingTriggerOptions.AVAILABLE_NOW         # {"availableNow": True}
```

Or pass a raw dict: `trigger={"processingTime": "2 minutes"}`.

## CheckpointManager

Generates deterministic checkpoint paths and provides lifecycle operations:

```python
from databricks4py.io import CheckpointManager

mgr = CheckpointManager("/mnt/checkpoints")

# Auto-generate a path from source and sink names
path = mgr.path_for("warehouse.bronze.orders", "OrderProcessor")
# "/mnt/checkpoints/warehouse_bronze_orders__OrderProcessor"

# Use with StreamingTableReader
reader = OrderProcessor(
    source_table="warehouse.bronze.orders",
    checkpoint_manager=mgr,
)

# Inspect checkpoint state
info = mgr.info(path)
info.last_batch_id   # latest committed batch
info.offsets          # offset JSON from the log
info.size_bytes       # total size on disk

# Reset (delete) a checkpoint
mgr.reset(path)
```

When you pass a `checkpoint_manager` to `StreamingTableReader` and omit `checkpoint_location`, the path is auto-generated from `source_table` and the class name.

## Metrics hooks per batch

Pass a `MetricsSink` to get `batch_complete` events after each micro-batch:

```python
from databricks4py import LoggingMetricsSink

reader = OrderProcessor(
    source_table="warehouse.bronze.orders",
    checkpoint_location="/checkpoints/orders",
    metrics_sink=LoggingMetricsSink(),
)
```

Each event includes `batch_id`, `row_count`, `duration_ms`, and `table_name`.

## Pre-batch filtering

Apply a `Filter` before each batch reaches `process_batch`:

```python
from databricks4py.filters import WhereFilter

reader = OrderProcessor(
    source_table="warehouse.bronze.orders",
    checkpoint_location="/checkpoints/orders",
    filter=WhereFilter("amount > 0"),
)
```

If filtering removes all rows and `skip_empty_batches=True` (default), the batch is skipped.

## Complete example

```python
from databricks4py.io import StreamingTableReader, StreamingTriggerOptions, CheckpointManager
from databricks4py.io import DeltaTable
from databricks4py import LoggingMetricsSink
from databricks4py.quality import QualityGate, NotNull

class EventEnricher(StreamingTableReader):
    def process_batch(self, df, batch_id):
        gate = QualityGate(NotNull("event_id", "user_id"), on_fail="warn")
        report = gate.check(df)
        if not report.passed:
            print(report.summary())

        enriched = df.withColumn("processed_at", F.current_timestamp())
        enriched.write.format("delta").mode("append").saveAsTable(
            "warehouse.silver.enriched_events"
        )

mgr = CheckpointManager("/mnt/checkpoints")

reader = EventEnricher(
    source_table="warehouse.bronze.raw_events",
    trigger=StreamingTriggerOptions.PROCESSING_TIME_5M,
    checkpoint_manager=mgr,
    metrics_sink=LoggingMetricsSink(),
    skip_empty_batches=True,
)

query = reader.start()
query.awaitTermination()
```
