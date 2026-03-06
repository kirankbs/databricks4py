"""Tests for streaming utilities."""

import pytest

from databricks4py.io.streaming import StreamingTriggerOptions


@pytest.mark.no_pyspark
class TestStreamingTriggerOptions:
    def test_processing_time_values(self) -> None:
        assert StreamingTriggerOptions.PROCESSING_TIME_10S.value == {"processingTime": "10 seconds"}
        assert StreamingTriggerOptions.PROCESSING_TIME_30S.value == {"processingTime": "30 seconds"}
        assert StreamingTriggerOptions.PROCESSING_TIME_1M.value == {"processingTime": "1 minute"}

    def test_available_now(self) -> None:
        assert StreamingTriggerOptions.AVAILABLE_NOW.value == {"availableNow": True}

    def test_all_options_are_dicts(self) -> None:
        for opt in StreamingTriggerOptions:
            assert isinstance(opt.value, dict)

    def test_enum_members_count(self) -> None:
        assert len(StreamingTriggerOptions) == 6
