"""Tests for metrics module."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from databricks4py.metrics.base import CompositeMetricsSink, MetricEvent, MetricsSink
from databricks4py.metrics.logging_sink import LoggingMetricsSink


def _make_event(**overrides) -> MetricEvent:
    defaults = {
        "job_name": "test_job",
        "event_type": "batch_complete",
        "timestamp": datetime(2026, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
    }
    defaults.update(overrides)
    return MetricEvent(**defaults)


class TestMetricEvent:
    @pytest.mark.no_pyspark
    def test_creation(self):
        event = _make_event()
        assert event.job_name == "test_job"
        assert event.event_type == "batch_complete"
        assert event.timestamp == datetime(2026, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        assert event.duration_ms is None
        assert event.row_count is None
        assert event.table_name is None
        assert event.batch_id is None
        assert event.metadata == {}

    @pytest.mark.no_pyspark
    def test_frozen(self):
        event = _make_event()
        with pytest.raises(AttributeError, match="cannot assign"):
            event.job_name = "other"

    @pytest.mark.no_pyspark
    def test_all_fields(self):
        event = MetricEvent(
            job_name="ingest",
            event_type="batch_complete",
            timestamp=datetime(2026, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
            duration_ms=1500,
            row_count=42000,
            table_name="catalog.schema.events",
            batch_id=7,
            metadata={"source": "kafka", "topic": "clicks"},
        )
        assert event.duration_ms == 1500
        assert event.row_count == 42000
        assert event.table_name == "catalog.schema.events"
        assert event.batch_id == 7
        assert event.metadata == {"source": "kafka", "topic": "clicks"}


class TestMetricsSinkABC:
    @pytest.mark.no_pyspark
    def test_cannot_instantiate(self):
        with pytest.raises(TypeError, match="abstract"):
            MetricsSink()

    @pytest.mark.no_pyspark
    def test_flush_is_optional(self):
        class DummySink(MetricsSink):
            def __init__(self):
                self.events: list[MetricEvent] = []

            def emit(self, event: MetricEvent) -> None:
                self.events.append(event)

        sink = DummySink()
        event = _make_event()
        sink.emit(event)
        sink.flush()  # should not raise
        assert sink.events == [event]


class TestLoggingMetricsSink:
    @pytest.mark.no_pyspark
    def test_emits_json_log(self, caplog):
        sink = LoggingMetricsSink()
        event = _make_event(duration_ms=200, row_count=100)

        with caplog.at_level(logging.INFO, logger="databricks4py.metrics.logging_sink"):
            sink.emit(event)

        assert len(caplog.records) == 1
        payload = json.loads(caplog.records[0].message)
        assert payload["job_name"] == "test_job"
        assert payload["event_type"] == "batch_complete"
        assert payload["duration_ms"] == 200
        assert payload["row_count"] == 100

    @pytest.mark.no_pyspark
    def test_handles_metadata(self, caplog):
        sink = LoggingMetricsSink()
        event = _make_event(metadata={"key": "value", "count": 42})

        with caplog.at_level(logging.INFO, logger="databricks4py.metrics.logging_sink"):
            sink.emit(event)

        payload = json.loads(caplog.records[0].message)
        assert payload["metadata"] == {"key": "value", "count": 42}


class TestCompositeMetricsSink:
    @pytest.mark.no_pyspark
    def test_fans_out_to_all_sinks(self):
        sink_a = MagicMock(spec=MetricsSink)
        sink_b = MagicMock(spec=MetricsSink)
        composite = CompositeMetricsSink(sink_a, sink_b)

        event = _make_event()
        composite.emit(event)

        sink_a.emit.assert_called_once_with(event)
        sink_b.emit.assert_called_once_with(event)

    @pytest.mark.no_pyspark
    def test_flushes_all_sinks(self):
        sink_a = MagicMock(spec=MetricsSink)
        sink_b = MagicMock(spec=MetricsSink)
        composite = CompositeMetricsSink(sink_a, sink_b)

        composite.flush()

        sink_a.flush.assert_called_once()
        sink_b.flush.assert_called_once()
