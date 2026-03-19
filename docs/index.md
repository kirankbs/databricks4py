# databricks4py

Reusable building blocks for PySpark and Delta Lake pipelines. Handles the boilerplate you'd otherwise copy-paste across every Databricks job: config resolution, Delta table management, merge operations, quality gates, metrics collection, retry logic, and streaming -- all with a consistent API that works identically on Databricks and locally.

## Key Features

- **Workflow** -- structured entry point for job scripts with lifecycle hooks
- **Config** -- environment-aware table name resolution with Unity Catalog support
- **DeltaTable** -- schema-driven table creation, read/write, merge, upsert, SCD Type 2
- **Quality gates** -- row-level expectations with raise/warn/quarantine enforcement
- **Metrics** -- pluggable sinks (logging, Delta, composite) auto-wired to workflows
- **Retry** -- exponential backoff decorator, built into the workflow lifecycle
- **Streaming** -- micro-batch processor with checkpoint management and per-batch metrics
- **Schema evolution** -- diff detection with severity levels and pre-write guards
- **Testing** -- DataFrameBuilder, TempDeltaTable, assertions, and pytest fixtures

## Install

```bash
pip install databricks4py
```

## Hello World

```python
from databricks4py import Workflow, JobConfig, LoggingMetricsSink

class IngestOrders(Workflow):
    def run(self):
        raw = self.spark.read.table(self.config.table("bronze_orders"))
        cleaned = raw.dropDuplicates(["order_id"]).where("amount > 0")
        cleaned.write.format("delta").mode("append").saveAsTable(
            self.config.table("silver_orders")
        )

config = JobConfig(
    tables={"bronze_orders": "catalog.raw.orders", "silver_orders": "catalog.clean.orders"}
)
IngestOrders(config=config, metrics=LoggingMetricsSink()).execute()
```

## Guides

- [Installation](getting-started/installation.md)
- [Quick Start](getting-started/quick-start.md)
- [Core Concepts](getting-started/concepts.md)
- [Config](guides/config.md)
- [Delta Operations](guides/delta-operations.md)
- [Quality Gates](guides/quality-gates.md)
- [Metrics](guides/metrics.md)
- [Streaming](guides/streaming.md)
- [Schema Evolution](guides/schema-evolution.md)
- [Retry](guides/retry.md)
- [Workflow](guides/workflow.md)
- [Testing](guides/testing.md)

## API Reference

- [Config](api/config.md)
- [I/O](api/io.md)
- [Quality](api/quality.md)
- [Metrics](api/metrics.md)
- [Workflow](api/workflow.md)
- [Testing](api/testing.md)
