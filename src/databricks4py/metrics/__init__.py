"""Metrics collection with pluggable sinks."""

from __future__ import annotations

from databricks4py.metrics.base import CompositeMetricsSink, MetricEvent, MetricsSink
from databricks4py.metrics.logging_sink import LoggingMetricsSink

__all__ = [
    "CompositeMetricsSink",
    "DeltaMetricsSink",
    "LoggingMetricsSink",
    "MetricEvent",
    "MetricsSink",
]


def __getattr__(name: str):
    if name == "DeltaMetricsSink":
        from databricks4py.metrics.delta_sink import DeltaMetricsSink

        return DeltaMetricsSink
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
