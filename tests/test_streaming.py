"""Tests for streaming utilities."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from databricks4py.io.streaming import StreamingTableReader, StreamingTriggerOptions


@pytest.mark.no_pyspark
class TestStreamingTriggerOptions:
    def test_processing_time_values(self) -> None:
        assert StreamingTriggerOptions.PROCESSING_TIME_10S.value == {
            "processingTime": "10 seconds"
        }
        assert StreamingTriggerOptions.PROCESSING_TIME_30S.value == {
            "processingTime": "30 seconds"
        }
        assert StreamingTriggerOptions.PROCESSING_TIME_1M.value == {
            "processingTime": "1 minute"
        }

    def test_available_now(self) -> None:
        assert StreamingTriggerOptions.AVAILABLE_NOW.value == {"availableNow": True}

    def test_all_options_are_dicts(self) -> None:
        for opt in StreamingTriggerOptions:
            assert isinstance(opt.value, dict)

    def test_enum_members_count(self) -> None:
        assert len(StreamingTriggerOptions) == 6


class _StubReader(StreamingTableReader):
    def process_batch(self, df, batch_id):
        pass


@pytest.mark.no_pyspark
class TestStreamingTableReaderConfig:
    def test_auto_checkpoint_path(self) -> None:
        from databricks4py.io.checkpoint import CheckpointManager

        mgr = CheckpointManager(base_path="/tmp/checkpoints")

        with patch("databricks4py.io.streaming.active_fallback", return_value=MagicMock()):
            reader = _StubReader(
                source_table="catalog.bronze.events",
                checkpoint_manager=mgr,
            )
        assert "catalog_bronze_events" in reader._checkpoint_location
        assert "_StubReader" in reader._checkpoint_location

    def test_explicit_checkpoint_location_takes_precedence(self) -> None:
        from databricks4py.io.checkpoint import CheckpointManager

        mgr = CheckpointManager(base_path="/tmp/checkpoints")

        with patch("databricks4py.io.streaming.active_fallback", return_value=MagicMock()):
            reader = _StubReader(
                source_table="catalog.bronze.events",
                checkpoint_location="/my/explicit/path",
                checkpoint_manager=mgr,
            )
        assert reader._checkpoint_location == "/my/explicit/path"

    def test_no_checkpoint_raises(self) -> None:
        with (
            patch("databricks4py.io.streaming.active_fallback", return_value=MagicMock()),
            pytest.raises(ValueError, match="checkpoint_location is required"),
        ):
            _StubReader(source_table="catalog.bronze.events")

    def test_trigger_enum_resolved(self) -> None:
        with patch("databricks4py.io.streaming.active_fallback", return_value=MagicMock()):
            reader = _StubReader(
                source_table="t",
                trigger=StreamingTriggerOptions.PROCESSING_TIME_1M,
                checkpoint_location="/cp",
            )
        assert reader._trigger_dict == {"processingTime": "1 minute"}

    def test_trigger_dict_passthrough(self) -> None:
        with patch("databricks4py.io.streaming.active_fallback", return_value=MagicMock()):
            reader = _StubReader(
                source_table="t",
                trigger={"once": True},
                checkpoint_location="/cp",
            )
        assert reader._trigger_dict == {"once": True}

    def test_trigger_none_uses_default(self) -> None:
        with patch("databricks4py.io.streaming.active_fallback", return_value=MagicMock()):
            reader = _StubReader(
                source_table="t",
                checkpoint_location="/cp",
            )
        assert reader._trigger_dict == {"processingTime": "10 seconds"}


@pytest.mark.no_pyspark
class TestStreamingMetrics:
    def test_metrics_emitted_on_batch(self) -> None:
        from databricks4py.metrics.base import MetricsSink

        sink = MagicMock(spec=MetricsSink)
        mock_df = MagicMock()
        mock_df.isEmpty.return_value = False
        mock_df.count.return_value = 42

        with patch("databricks4py.io.streaming.active_fallback", return_value=MagicMock()):
            reader = _StubReader(
                source_table="catalog.schema.tbl",
                checkpoint_location="/cp",
                metrics_sink=sink,
            )

        reader._foreach_batch_wrapper(mock_df, batch_id=7)

        sink.emit.assert_called_once()
        event = sink.emit.call_args[0][0]
        assert event.event_type == "batch_complete"
        assert event.row_count == 42
        assert event.batch_id == 7
        assert event.table_name == "catalog.schema.tbl"
        assert event.job_name == "_StubReader"

    def test_no_metrics_when_sink_is_none(self) -> None:
        mock_df = MagicMock()
        mock_df.isEmpty.return_value = False
        mock_df.count.return_value = 5

        with patch("databricks4py.io.streaming.active_fallback", return_value=MagicMock()):
            reader = _StubReader(
                source_table="t",
                checkpoint_location="/cp",
            )

        reader._foreach_batch_wrapper(mock_df, batch_id=0)
