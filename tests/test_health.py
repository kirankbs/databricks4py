"""Tests for StreamingHealthCheck and health types."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from databricks4py.observability.health import (
    CheckDetail,
    HealthResult,
    HealthStatus,
    StreamingHealthCheck,
    _worst,
)


def _mock_query(*, active=True, progress_json=None):
    q = MagicMock()
    q.isActive = active
    if progress_json:
        p = MagicMock()
        p.json = progress_json
        q.lastProgress = p
    else:
        q.lastProgress = None
    return q


HEALTHY_PROGRESS = json.dumps(
    {
        "batchId": 5,
        "batchDuration": "200 ms",
        "inputRowsPerSecond": 1000.0,
        "processedRowsPerSecond": 950.0,
        "numInputRows": 5000,
    }
)

SLOW_PROGRESS = json.dumps(
    {
        "batchId": 5,
        "batchDuration": "120000 ms",
        "inputRowsPerSecond": 10.0,
        "processedRowsPerSecond": 5.0,
        "numInputRows": 50,
    }
)


@pytest.mark.no_pyspark
class TestHealthStatus:
    def test_values(self) -> None:
        assert HealthStatus.HEALTHY.value == "healthy"
        assert HealthStatus.DEGRADED.value == "degraded"
        assert HealthStatus.UNHEALTHY.value == "unhealthy"


@pytest.mark.no_pyspark
class TestCheckDetail:
    def test_frozen(self) -> None:
        d = CheckDetail(name="test", status=HealthStatus.HEALTHY, message="ok")
        with pytest.raises(AttributeError, match="cannot assign"):
            d.name = "x"  # type: ignore[misc]


@pytest.mark.no_pyspark
class TestHealthResult:
    def test_summary_format(self) -> None:
        r = HealthResult(
            status=HealthStatus.DEGRADED,
            checks=[
                CheckDetail("a", HealthStatus.HEALTHY, "fine"),
                CheckDetail("b", HealthStatus.DEGRADED, "slow"),
            ],
        )
        s = r.summary()
        assert "Overall: degraded" in s
        assert "[healthy] a: fine" in s
        assert "[degraded] b: slow" in s


@pytest.mark.no_pyspark
class TestWorst:
    def test_all_healthy(self) -> None:
        assert _worst([HealthStatus.HEALTHY, HealthStatus.HEALTHY]) == HealthStatus.HEALTHY

    def test_degraded_wins(self) -> None:
        assert _worst([HealthStatus.HEALTHY, HealthStatus.DEGRADED]) == HealthStatus.DEGRADED

    def test_unhealthy_wins(self) -> None:
        assert (
            _worst([HealthStatus.HEALTHY, HealthStatus.DEGRADED, HealthStatus.UNHEALTHY])
            == HealthStatus.UNHEALTHY
        )


@pytest.mark.no_pyspark
class TestStreamingHealthCheck:
    def test_inactive_query_is_unhealthy(self) -> None:
        q = _mock_query(active=False)
        check = StreamingHealthCheck(q)
        result = check.evaluate()
        assert result.status == HealthStatus.UNHEALTHY
        assert any(c.name == "query_active" for c in result.checks)

    def test_healthy_progress(self) -> None:
        q = _mock_query(active=True, progress_json=HEALTHY_PROGRESS)
        check = StreamingHealthCheck(q)
        result = check.evaluate()
        assert result.status == HealthStatus.HEALTHY

    def test_slow_batch_degraded(self) -> None:
        q = _mock_query(active=True, progress_json=SLOW_PROGRESS)
        check = StreamingHealthCheck(q, max_batch_duration_ms=60_000)
        result = check.evaluate()

        duration_checks = [c for c in result.checks if c.name == "batch_duration"]
        assert len(duration_checks) == 1
        assert duration_checks[0].status == HealthStatus.DEGRADED

    def test_low_throughput_degraded(self) -> None:
        q = _mock_query(active=True, progress_json=SLOW_PROGRESS)
        check = StreamingHealthCheck(q, min_processing_rate=100.0)
        result = check.evaluate()

        rate_checks = [c for c in result.checks if c.name == "processing_rate"]
        assert len(rate_checks) == 1
        assert rate_checks[0].status == HealthStatus.DEGRADED

    def test_no_progress_within_timeout_healthy(self) -> None:
        q = _mock_query(active=True, progress_json=None)
        check = StreamingHealthCheck(q, stale_timeout_seconds=600)
        result = check.evaluate()
        # No progress yet but within timeout
        stale = [c for c in result.checks if c.name == "stale_progress"]
        assert stale[0].status == HealthStatus.HEALTHY

    def test_no_progress_exceeds_timeout_unhealthy(self) -> None:
        q = _mock_query(active=True, progress_json=None)
        check = StreamingHealthCheck(q, stale_timeout_seconds=0)
        # Force time to appear stale
        check._last_progress_time = 0.0
        result = check.evaluate()
        stale = [c for c in result.checks if c.name == "stale_progress"]
        assert stale[0].status == HealthStatus.UNHEALTHY

    def test_healthy_batch_under_threshold(self) -> None:
        q = _mock_query(active=True, progress_json=HEALTHY_PROGRESS)
        check = StreamingHealthCheck(q, max_batch_duration_ms=500)
        result = check.evaluate()
        duration_checks = [c for c in result.checks if c.name == "batch_duration"]
        assert duration_checks[0].status == HealthStatus.HEALTHY

    def test_healthy_rate_above_minimum(self) -> None:
        q = _mock_query(active=True, progress_json=HEALTHY_PROGRESS)
        check = StreamingHealthCheck(q, min_processing_rate=100.0)
        result = check.evaluate()
        rate_checks = [c for c in result.checks if c.name == "processing_rate"]
        assert rate_checks[0].status == HealthStatus.HEALTHY

    def test_multiple_checks_worst_wins(self) -> None:
        q = _mock_query(active=True, progress_json=SLOW_PROGRESS)
        check = StreamingHealthCheck(q, max_batch_duration_ms=60_000, min_processing_rate=100.0)
        result = check.evaluate()
        # Both should be DEGRADED, overall DEGRADED
        assert result.status == HealthStatus.DEGRADED
