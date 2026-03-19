# API: Metrics

## MetricEvent

```python
@dataclass(frozen=True)
class MetricEvent:
    job_name: str
    event_type: str
    timestamp: datetime
    duration_ms: int | None = None
    row_count: int | None = None
    table_name: str | None = None
    batch_id: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
```

## MetricsSink (ABC)

```python
class MetricsSink(ABC):
    @abstractmethod
    def emit(self, event: MetricEvent) -> None: ...

    def flush(self) -> None:
        pass
```

## LoggingMetricsSink

Emits events as JSON via `logging.info()`. No buffering.

```python
class LoggingMetricsSink(MetricsSink):
    def emit(self, event: MetricEvent) -> None
```

## DeltaMetricsSink

Buffers events and writes to a Delta table.

```python
class DeltaMetricsSink(MetricsSink):
    def __init__(
        self,
        table_name: str,
        *,
        spark: SparkSession | None = None,
        buffer_size: int = 100,
    ) -> None

    def emit(self, event: MetricEvent) -> None    # auto-flushes at buffer_size
    def flush(self) -> None                        # writes buffered events
```

## CompositeMetricsSink

Fans out to multiple sinks.

```python
class CompositeMetricsSink(MetricsSink):
    def __init__(self, *sinks: MetricsSink) -> None
    def emit(self, event: MetricEvent) -> None
    def flush(self) -> None
```
