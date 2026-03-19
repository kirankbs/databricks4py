# Quick Start

A complete workflow: read a bronze table, run quality checks, upsert to silver, emit metrics.

```python
from databricks4py import Workflow, JobConfig, LoggingMetricsSink, RetryConfig
from databricks4py.io import DeltaTable
from databricks4py.quality import QualityGate, NotNull, InRange, Unique

class BronzeToSilver(Workflow):
    def run(self):
        # Read source
        bronze = self.spark.read.table(self.config.table("bronze"))

        # Quality gate -- rejects bad rows before they hit silver
        gate = QualityGate(
            NotNull("order_id", "customer_id"),
            InRange("amount", min_val=0),
            Unique("order_id"),
            on_fail="raise",
        )
        clean = self.quality_check(bronze, gate, table_name="bronze")

        # Upsert into silver
        silver = DeltaTable(
            self.config.table("silver"),
            schema={"order_id": "long", "customer_id": "long", "amount": "double"},
        )
        result = silver.upsert(clean, keys=["order_id"])

        self.emit_metric(
            "upsert_complete",
            row_count=result.rows_inserted + result.rows_updated,
            metadata={"inserted": result.rows_inserted, "updated": result.rows_updated},
        )

# Wire it up and run
config = JobConfig(
    tables={
        "bronze": "warehouse.raw.orders",
        "silver": "warehouse.clean.orders",
    },
    secret_scope="etl-secrets",
)

BronzeToSilver(
    config=config,
    metrics=LoggingMetricsSink(),
    retry_config=RetryConfig(max_attempts=3),
).execute()
```

## What happens when you call `.execute()`

1. Spark configs from `JobConfig.spark_configs` get applied
2. `job_start` metric fires
3. `run_at_time()` calls `run()` (wrapped in retry if configured)
4. On success: `job_complete` metric with duration
5. On failure: `job_failed` metric, then re-raises
6. `metrics.flush()` in the `finally` block

## Section breakdown

**Config** resolves table names and environment. `config.table("bronze")` returns `"warehouse.raw.orders"`. If you pass a name that's not configured, it raises `KeyError` with the list of available tables.

**QualityGate** runs each expectation against the DataFrame. `on_fail="raise"` throws a `QualityError` if any check fails. Other modes: `"warn"` logs and continues, `"quarantine"` splits bad rows to a handler function.

**DeltaTable** creates the silver table if it doesn't exist (using the dict schema), then `upsert()` does a MERGE by the specified keys.

**Metrics** go to the logging sink here, but swap in `DeltaMetricsSink` or `CompositeMetricsSink` for production persistence.
