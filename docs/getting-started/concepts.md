# Core Concepts

## Workflow is the spine

Every job extends `Workflow`. It gives you a SparkSession, lifecycle hooks, and optional wiring for config, metrics, quality, and retry. You implement `run()`, call `.execute()` from your entry point.

```python
class MyJob(Workflow):
    def run(self):
        # all your logic goes here
        ...

MyJob().execute()
```

You can also skip `Workflow` entirely and use individual modules standalone. Nothing forces you into the framework.

## Config resolves environment at runtime

`JobConfig` and `UnityConfig` figure out whether you're in dev, staging, or prod by checking (in order):

1. Databricks widget parameter `spark.databricks.widget.env`
2. `ENV` or `ENVIRONMENT` environment variable
3. Falls back to `dev`

Table name resolution goes through `config.table("name")` -- which is a simple dict lookup for `JobConfig`, and a `schema.table` qualified name for `UnityConfig`.

## Metrics are pluggable sinks

The library emits `MetricEvent` dataclasses. Where they go depends on which `MetricsSink` you wire in:

- `LoggingMetricsSink` -- JSON to the standard logger
- `DeltaMetricsSink` -- buffered writes to a Delta table
- `CompositeMetricsSink` -- fan out to multiple sinks

Workflow auto-emits `job_start`, `job_complete`, and `job_failed` events. You add custom ones with `self.emit_metric()`.

## Quality gates run before writes

`QualityGate` wraps a list of `Expectation` instances. Each expectation validates a DataFrame and returns pass/fail with row counts. The gate enforces policy:

- **raise** -- throw `QualityError` on failure (default)
- **warn** -- log a warning and continue
- **quarantine** -- split bad rows to a handler, pass clean rows through

Quality gates are standalone objects. `Workflow.quality_check()` is a convenience that also emits a metric.

## Every module works standalone

You don't need `Workflow` to use `DeltaTable`, `QualityGate`, `RetryConfig`, or any other component. Each module accepts an optional `spark` parameter and falls back to the active session.

```python
# Standalone DeltaTable usage -- no Workflow needed
from databricks4py.io import DeltaTable

table = DeltaTable("catalog.schema.users", schema={"id": "int", "name": "string"})
df = table.dataframe()
```

```python
# Standalone quality check
from databricks4py.quality import QualityGate, NotNull

gate = QualityGate(NotNull("email"), on_fail="raise")
report = gate.check(df)
print(report.summary())
```

```python
# Standalone retry
from databricks4py import retry, RetryConfig

@retry(RetryConfig(max_attempts=5, base_delay_seconds=2.0))
def fetch_data():
    ...
```
