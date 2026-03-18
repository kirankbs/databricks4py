"""Core metric types and sink abstractions."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

__all__ = ["CompositeMetricsSink", "MetricEvent", "MetricsSink"]


@dataclass(frozen=True)
class MetricEvent:
    """A single metrics observation."""

    job_name: str
    event_type: str
    timestamp: datetime
    duration_ms: int | None = None
    row_count: int | None = None
    table_name: str | None = None
    batch_id: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


class MetricsSink(ABC):
    """Abstract base for metrics destinations."""

    @abstractmethod
    def emit(self, event: MetricEvent) -> None: ...

    def flush(self) -> None:  # noqa: B027
        pass


class CompositeMetricsSink(MetricsSink):
    """Fans out events to multiple sinks."""

    def __init__(self, *sinks: MetricsSink) -> None:
        self._sinks = sinks

    def emit(self, event: MetricEvent) -> None:
        for sink in self._sinks:
            sink.emit(event)

    def flush(self) -> None:
        for sink in self._sinks:
            sink.flush()
