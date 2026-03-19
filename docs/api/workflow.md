# API: Workflow

## Workflow (ABC)

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
    ) -> None
```

### Properties

| Property | Type | Description |
|---|---|---|
| `spark` | `SparkSession` | Active SparkSession |
| `dbutils` | `Any \| None` | Injected dbutils module |
| `execution_time` | `datetime` | Logical execution timestamp |
| `config` | `JobConfig \| None` | Job configuration |
| `metrics` | `MetricsSink \| None` | Metrics sink |

### run() -> None [abstract]

Implement your business logic here.

### execute() -> None

Standard entry point. Applies spark configs, emits lifecycle metrics, wraps `run_at_time()` in retry if configured, calls `metrics.flush()` in `finally`.

### run_at_time(execution_time: datetime | None = None) -> None

Sets `self.execution_time` and calls `run()`. Used for backfills and by `execute()`.

### emit_metric(event_type: str, **kwargs) -> None

Emit a custom metric event. No-op if no sink is configured. Accepts `duration_ms`, `row_count`, `table_name`, `batch_id`, `metadata` as keyword arguments.

### quality_check(df, gate, *, table_name=None) -> DataFrame

Run a `QualityGate` on a DataFrame. Emits a `quality_check` metric. Returns the clean DataFrame (or raises/warns per gate policy).

---

## RetryConfig

```python
@dataclass(frozen=True)
class RetryConfig:
    max_attempts: int = 3
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 60.0
    backoff_factor: float = 2.0
    retryable_exceptions: tuple[type[BaseException], ...] = ()  # empty = auto-detect
```

## retry(config: RetryConfig | None = None) -> Callable

Decorator factory. Returns a decorator that wraps a function with exponential backoff retry.

```python
@retry(RetryConfig(max_attempts=5))
def my_function(): ...
```
