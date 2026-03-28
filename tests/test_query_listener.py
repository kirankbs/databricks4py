"""Tests for QueryProgressObserver and QueryProgressSnapshot."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from databricks4py.observability._utils import parse_duration_ms
from databricks4py.observability.query_listener import (
    QueryProgressObserver,
    QueryProgressSnapshot,
)

SAMPLE_PROGRESS_JSON = json.dumps(
    {
        "id": "abc-123",
        "name": "my_query",
        "batchId": 7,
        "batchDuration": "250 ms",
        "inputRowsPerSecond": 1500.0,
        "processedRowsPerSecond": 1200.0,
        "numInputRows": 3000,
        "sources": [{"description": "delta", "startOffset": "0", "endOffset": "100"}],
        "sink": {"description": "ForeachBatchSink"},
    }
)


def _mock_progress():
    p = MagicMock()
    p.json = SAMPLE_PROGRESS_JSON
    return p


@pytest.mark.no_pyspark
class TestQueryProgressSnapshot:
    def test_from_progress(self) -> None:
        snap = QueryProgressSnapshot.from_progress(_mock_progress())
        assert snap.query_id == "abc-123"
        assert snap.query_name == "my_query"
        assert snap.batch_id == 7
        assert snap.batch_duration_ms == 250
        assert snap.input_rows_per_second == 1500.0
        assert snap.processed_rows_per_second == 1200.0
        assert snap.num_input_rows == 3000
        assert len(snap.sources) == 1
        assert snap.raw_json == SAMPLE_PROGRESS_JSON

    def test_frozen(self) -> None:
        snap = QueryProgressSnapshot.from_progress(_mock_progress())
        with pytest.raises(AttributeError, match="cannot assign"):
            snap.batch_id = 99  # type: ignore[misc]

    def test_timestamp_is_utc(self) -> None:
        snap = QueryProgressSnapshot.from_progress(_mock_progress())
        assert snap.timestamp.tzinfo is not None


@pytest.mark.no_pyspark
class TestParseDuration:
    def test_ms_string(self) -> None:
        assert parse_duration_ms("250 ms") == 250

    def test_integer(self) -> None:
        assert parse_duration_ms(100) == 100

    def test_invalid(self) -> None:
        assert parse_duration_ms("unknown") == 0


@pytest.mark.no_pyspark
class TestQueryProgressObserver:
    def _make_observer(self, **kwargs):
        mock_spark = MagicMock()
        with patch(
            "databricks4py.observability.query_listener.active_fallback",
            return_value=mock_spark,
        ):
            obs = QueryProgressObserver(**kwargs)
        return obs, mock_spark

    def test_handle_progress_stores_snapshot(self) -> None:
        obs, _ = self._make_observer()
        obs._handle_progress(_mock_progress())
        assert len(obs.history()) == 1
        assert obs.latest_progress().batch_id == 7

    def test_history_limit(self) -> None:
        obs, _ = self._make_observer(history_size=3)
        for i in range(5):
            p = MagicMock()
            p.json = json.dumps({"batchId": i, "numInputRows": 0})
            obs._handle_progress(p)
        assert len(obs.history()) == 3
        assert obs.history()[0].batch_id == 2  # oldest retained

    def test_history_with_limit_param(self) -> None:
        obs, _ = self._make_observer()
        for i in range(10):
            p = MagicMock()
            p.json = json.dumps({"batchId": i, "numInputRows": 0})
            obs._handle_progress(p)
        assert len(obs.history(limit=3)) == 3
        assert obs.history(limit=3)[-1].batch_id == 9

    def test_latest_progress_none_when_empty(self) -> None:
        obs, _ = self._make_observer()
        assert obs.latest_progress() is None

    def test_metrics_sink_receives_event(self) -> None:
        sink = MagicMock()
        obs, _ = self._make_observer(metrics_sink=sink)
        obs._handle_progress(_mock_progress())

        sink.emit.assert_called_once()
        event = sink.emit.call_args[0][0]
        assert event.event_type == "query_progress"
        assert event.batch_id == 7
        assert event.row_count == 3000

    def test_on_progress_callback(self) -> None:
        received: list[QueryProgressSnapshot] = []
        obs, _ = self._make_observer(on_progress=lambda s: received.append(s))
        obs._handle_progress(_mock_progress())

        assert len(received) == 1
        assert received[0].batch_id == 7

    def test_query_name_filter(self) -> None:
        obs, _ = self._make_observer(query_name_filter="other_query")
        obs._handle_progress(_mock_progress())  # query_name = "my_query"
        assert obs.latest_progress() is None  # filtered out

    def test_query_name_filter_matches(self) -> None:
        obs, _ = self._make_observer(query_name_filter="my_query")
        obs._handle_progress(_mock_progress())
        assert obs.latest_progress() is not None

    def test_is_attached_false_by_default(self) -> None:
        obs, _ = self._make_observer()
        assert obs.is_attached is False

    def test_detach_when_not_attached_is_noop(self) -> None:
        obs, _ = self._make_observer()
        obs.detach()  # should not raise
