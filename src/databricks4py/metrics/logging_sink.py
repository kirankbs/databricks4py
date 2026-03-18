"""Logging-based metrics sink."""

from __future__ import annotations

import json
import logging
from dataclasses import asdict

from databricks4py.metrics.base import MetricEvent, MetricsSink

__all__ = ["LoggingMetricsSink"]

logger = logging.getLogger(__name__)


class LoggingMetricsSink(MetricsSink):
    """Emits metric events as JSON via the standard logger."""

    def emit(self, event: MetricEvent) -> None:
        logger.info(json.dumps(asdict(event), default=str))
