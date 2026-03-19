# Retry

## When to use this

Transient failures -- network blips, Py4J disconnects, temporary cloud storage errors. The retry decorator wraps any callable with exponential backoff and configurable exception matching.

## RetryConfig

```python
from databricks4py import RetryConfig

config = RetryConfig(
    max_attempts=3,               # default
    base_delay_seconds=1.0,       # default
    max_delay_seconds=60.0,       # default
    backoff_factor=2.0,           # default
    retryable_exceptions=(),      # empty = use defaults
)
```

### Default retryable exceptions

When `retryable_exceptions` is empty (the default), it auto-detects:

- `ConnectionError`
- `TimeoutError`
- `OSError`
- `py4j.protocol.Py4JNetworkError` (if py4j is installed)

### Backoff behavior

Delay on attempt N = `min(base_delay * backoff_factor^(N-1), max_delay)`:

- Attempt 1: fails, wait 1s
- Attempt 2: fails, wait 2s
- Attempt 3: fails, raises

## @retry decorator standalone

```python
from databricks4py import retry, RetryConfig

@retry(RetryConfig(max_attempts=5, base_delay_seconds=0.5))
def call_external_api():
    response = requests.get("https://api.example.com/data")
    response.raise_for_status()
    return response.json()

# Or with defaults:
@retry()
def fetch_data():
    ...
```

The decorator preserves the wrapped function's name and docstring via `functools.wraps`.

## Workflow integration

Pass a `RetryConfig` to `Workflow` and `execute()` wraps `run_at_time()` with retry:

```python
from databricks4py import Workflow, RetryConfig

class FlakySyncJob(Workflow):
    def run(self):
        # This might fail transiently
        external_data = call_external_api()
        self.spark.createDataFrame(external_data).write.format("delta").saveAsTable("target")

FlakySyncJob(
    retry_config=RetryConfig(max_attempts=3, base_delay_seconds=5.0)
).execute()
```

On each retry attempt, a warning is logged with the attempt number, exception, and delay before the next try. After exhausting all attempts, the original exception is re-raised.

## Custom exception list

```python
from pyspark.sql.utils import AnalysisException

config = RetryConfig(
    max_attempts=3,
    retryable_exceptions=(ConnectionError, AnalysisException),
)
```

Only exceptions in this tuple trigger retries. Everything else propagates immediately.

## Complete example

```python
from databricks4py import Workflow, JobConfig, RetryConfig, LoggingMetricsSink

class ApiIngestJob(Workflow):
    def run(self):
        # External API call -- might fail transiently
        import requests
        resp = requests.get(self.config.secret("api_url"))
        resp.raise_for_status()
        data = resp.json()

        df = self.spark.createDataFrame(data)
        df.write.format("delta").mode("append").saveAsTable(
            self.config.table("events")
        )
        self.emit_metric("ingest_complete", row_count=len(data))

config = JobConfig(
    tables={"events": "warehouse.raw.api_events"},
    secret_scope="api-secrets",
)

ApiIngestJob(
    config=config,
    metrics=LoggingMetricsSink(),
    retry_config=RetryConfig(max_attempts=5, base_delay_seconds=2.0, max_delay_seconds=30.0),
).execute()
```
