"""Structured per-batch logging with correlation IDs.

Produces JSON-structured log records for each batch lifecycle event
(start, complete, error, skip). Designed for use inside
:class:`~databricks4py.io.streaming.StreamingTableReader` or any
``foreachBatch`` processor where you need queryable, machine-parseable logs.

Example::

    logger = BatchLogger()

    ctx = BatchContext.create(batch_id=42, source_table="catalog.schema.events")
    logger.batch_start(ctx)
    # ... process ...
    logger.batch_complete(ctx, row_count=1000, duration_ms=345.2)
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

__all__ = ["BatchContext", "BatchLogger"]


@dataclass(frozen=True)
class BatchContext:
    """Immutable context for a single streaming micro-batch.

    Carries the batch identifier, source table, a unique correlation ID,
    and the batch start time. Thread-safe (frozen).

    Args:
        batch_id: Spark-assigned batch identifier.
        source_table: Fully qualified source table or path.
        correlation_id: Unique ID for correlating logs, metrics, and DLQ
            records across systems. Auto-generated if not provided.
        start_time: UTC timestamp when the batch started processing.
    """

    batch_id: int
    source_table: str
    correlation_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    start_time: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))

    @classmethod
    def create(
        cls,
        batch_id: int,
        source_table: str,
        *,
        correlation_id: str | None = None,
    ) -> BatchContext:
        """Factory with optional explicit correlation ID."""
        kwargs: dict[str, Any] = {"batch_id": batch_id, "source_table": source_table}
        if correlation_id is not None:
            kwargs["correlation_id"] = correlation_id
        return cls(**kwargs)

    def elapsed_ms(self) -> float:
        """Milliseconds since ``start_time``."""
        return (datetime.now(tz=timezone.utc) - self.start_time).total_seconds() * 1000


class BatchLogger:
    """Structured JSON logger for streaming batch lifecycle events.

    Each log record is a single-line JSON object with a consistent schema,
    making it easy to query in log aggregation systems (Datadog, Splunk,
    CloudWatch, etc.).

    Args:
        logger_name: Python logger name. Defaults to ``"databricks4py.batch"``.
        extra_fields: Static fields added to every log record (e.g. environment,
            pipeline name).
    """

    def __init__(
        self,
        logger_name: str = "databricks4py.batch",
        extra_fields: dict[str, Any] | None = None,
    ) -> None:
        self._logger = logging.getLogger(logger_name)
        self._extra = extra_fields or {}

    def _emit(self, event: str, ctx: BatchContext, level: int, **fields: Any) -> None:
        record = {
            "event": event,
            "batch_id": ctx.batch_id,
            "source_table": ctx.source_table,
            "correlation_id": ctx.correlation_id,
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            **self._extra,
            **fields,
        }
        self._logger.log(level, json.dumps(record, default=str))

    def batch_start(self, ctx: BatchContext) -> None:
        """Log that batch processing has started."""
        self._emit("batch_start", ctx, logging.INFO)

    def batch_complete(
        self,
        ctx: BatchContext,
        row_count: int,
        duration_ms: float,
    ) -> None:
        """Log successful batch completion with row count and duration."""
        self._emit(
            "batch_complete",
            ctx,
            logging.INFO,
            row_count=row_count,
            duration_ms=round(duration_ms, 2),
        )

    def batch_error(self, ctx: BatchContext, error: str) -> None:
        """Log a batch processing failure."""
        self._emit("batch_error", ctx, logging.ERROR, error=error[:2000])

    def batch_skip(self, ctx: BatchContext, reason: str) -> None:
        """Log that a batch was skipped (empty, filtered out, etc.)."""
        self._emit("batch_skip", ctx, logging.DEBUG, reason=reason)

    def batch_dlq(self, ctx: BatchContext, dlq_table: str, error: str) -> None:
        """Log that a failed batch was routed to the dead-letter queue."""
        self._emit(
            "batch_dlq",
            ctx,
            logging.WARNING,
            dlq_table=dlq_table,
            error=error[:2000],
        )
