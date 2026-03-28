"""Health checks for streaming queries and checkpoints.

Polls a ``StreamingQuery`` or ``QueryProgressObserver`` and evaluates
configurable thresholds to produce a health status. Use in monitoring
dashboards, alerting hooks, or as a pre-flight check before scaling down.

Example::

    check = StreamingHealthCheck(
        query,
        max_batch_duration_ms=60_000,
        min_processing_rate=100.0,
        stale_timeout_seconds=300,
    )
    result = check.evaluate()
    if result.status == HealthStatus.UNHEALTHY:
        alert(result.summary())
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any

from databricks4py.observability._utils import parse_duration_ms as _parse_batch_duration

if TYPE_CHECKING:
    from pyspark.sql.streaming import StreamingQuery

__all__ = ["CheckDetail", "HealthResult", "HealthStatus", "StreamingHealthCheck"]

logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    """Overall health of a monitored component."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


@dataclass(frozen=True)
class CheckDetail:
    """Result of a single health check rule.

    Attributes:
        name: Short identifier for the check (e.g. ``"stuck_query"``).
        status: Pass/degraded/fail for this individual check.
        message: Human-readable explanation.
    """

    name: str
    status: HealthStatus
    message: str


@dataclass(frozen=True)
class HealthResult:
    """Aggregated health across all check rules.

    ``status`` is the worst status among all ``checks``. If any check is
    UNHEALTHY the result is UNHEALTHY; if any is DEGRADED it's DEGRADED.

    Attributes:
        status: Worst-case status across all checks.
        checks: Individual check results.
        timestamp: UTC time the evaluation ran.
    """

    status: HealthStatus
    checks: list[CheckDetail] = field(default_factory=list)
    timestamp: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))

    def summary(self) -> str:
        """One-line-per-check summary string."""
        lines = [f"Overall: {self.status.value}"]
        for c in self.checks:
            lines.append(f"  [{c.status.value}] {c.name}: {c.message}")
        return "\n".join(lines)


def _worst(statuses: list[HealthStatus]) -> HealthStatus:
    if HealthStatus.UNHEALTHY in statuses:
        return HealthStatus.UNHEALTHY
    if HealthStatus.DEGRADED in statuses:
        return HealthStatus.DEGRADED
    return HealthStatus.HEALTHY


class StreamingHealthCheck:
    """Evaluates a streaming query's health by polling its progress.

    Checks (all optional — configure the thresholds you care about):

    - **Stuck query**: No progress events for ``stale_timeout_seconds``.
    - **Slow batches**: ``batch_duration_ms`` exceeds ``max_batch_duration_ms``.
    - **Low throughput**: ``processedRowsPerSecond`` below ``min_processing_rate``.
    - **Query inactive**: The query has stopped unexpectedly.

    Args:
        query: A PySpark ``StreamingQuery`` to monitor.
        max_batch_duration_ms: DEGRADED if last batch took longer than this.
        min_processing_rate: DEGRADED if processed rows/sec drops below this.
        stale_timeout_seconds: UNHEALTHY if no progress for this many seconds.
    """

    def __init__(
        self,
        query: StreamingQuery,
        *,
        max_batch_duration_ms: int | None = None,
        min_processing_rate: float | None = None,
        stale_timeout_seconds: int = 600,
    ) -> None:
        self._query = query
        self._max_batch_duration_ms = max_batch_duration_ms
        self._min_processing_rate = min_processing_rate
        self._stale_timeout_seconds = stale_timeout_seconds
        self._last_progress_time: float = time.monotonic()
        self._last_batch_id: int | None = None

    def _get_progress(self) -> dict[str, Any] | None:
        progress = self._query.lastProgress
        if progress is None:
            return None
        import json as _json

        return _json.loads(progress.json)

    def evaluate(self) -> HealthResult:
        """Run all configured checks and return the aggregated result."""
        checks: list[CheckDetail] = []

        # Check 1: query still active
        if not self._query.isActive:
            checks.append(
                CheckDetail(
                    name="query_active",
                    status=HealthStatus.UNHEALTHY,
                    message="Query is no longer active",
                )
            )
            return HealthResult(status=_worst([c.status for c in checks]), checks=checks)

        checks.append(
            CheckDetail(
                name="query_active",
                status=HealthStatus.HEALTHY,
                message="Query is running",
            )
        )

        progress = self._get_progress()
        if progress is None:
            elapsed = time.monotonic() - self._last_progress_time
            if elapsed > self._stale_timeout_seconds:
                checks.append(
                    CheckDetail(
                        name="stale_progress",
                        status=HealthStatus.UNHEALTHY,
                        message=f"No progress events for {elapsed:.0f}s "
                        f"(threshold: {self._stale_timeout_seconds}s)",
                    )
                )
            else:
                checks.append(
                    CheckDetail(
                        name="stale_progress",
                        status=HealthStatus.HEALTHY,
                        message="Waiting for first progress event",
                    )
                )
            return HealthResult(status=_worst([c.status for c in checks]), checks=checks)

        # Track progress advancement
        batch_id = progress.get("batchId", -1)
        if batch_id != self._last_batch_id:
            self._last_progress_time = time.monotonic()
            self._last_batch_id = batch_id
        else:
            elapsed = time.monotonic() - self._last_progress_time
            if elapsed > self._stale_timeout_seconds:
                checks.append(
                    CheckDetail(
                        name="stale_progress",
                        status=HealthStatus.UNHEALTHY,
                        message=f"Batch {batch_id} unchanged for {elapsed:.0f}s",
                    )
                )

        # Check 2: batch duration
        if self._max_batch_duration_ms is not None:
            duration = _parse_batch_duration(progress.get("batchDuration", "0 ms"))
            if duration > self._max_batch_duration_ms:
                checks.append(
                    CheckDetail(
                        name="batch_duration",
                        status=HealthStatus.DEGRADED,
                        message=f"Batch took {duration}ms (max: {self._max_batch_duration_ms}ms)",
                    )
                )
            else:
                checks.append(
                    CheckDetail(
                        name="batch_duration",
                        status=HealthStatus.HEALTHY,
                        message=f"Batch took {duration}ms",
                    )
                )

        # Check 3: processing rate
        if self._min_processing_rate is not None:
            rate = progress.get("processedRowsPerSecond", 0.0)
            if rate < self._min_processing_rate:
                checks.append(
                    CheckDetail(
                        name="processing_rate",
                        status=HealthStatus.DEGRADED,
                        message=f"Processing {rate:.1f} rows/s "
                        f"(min: {self._min_processing_rate:.1f})",
                    )
                )
            else:
                checks.append(
                    CheckDetail(
                        name="processing_rate",
                        status=HealthStatus.HEALTHY,
                        message=f"Processing {rate:.1f} rows/s",
                    )
                )

        return HealthResult(status=_worst([c.status for c in checks]), checks=checks)
