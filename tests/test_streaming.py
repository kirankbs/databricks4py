"""Tests for streaming utilities."""

from unittest.mock import MagicMock, patch

import pytest

from databricks4py.io.streaming import StreamingTableReader, StreamingTriggerOptions


@pytest.mark.no_pyspark
class TestStreamingTriggerOptions:
    """Tests for StreamingTriggerOptions enum values."""

    def test_processing_time_values(self) -> None:
        assert StreamingTriggerOptions.PROCESSING_TIME_10S.value == {"processingTime": "10 seconds"}
        assert StreamingTriggerOptions.PROCESSING_TIME_30S.value == {"processingTime": "30 seconds"}
        assert StreamingTriggerOptions.PROCESSING_TIME_1M.value == {"processingTime": "1 minute"}
        assert StreamingTriggerOptions.PROCESSING_TIME_5M.value == {"processingTime": "5 minutes"}
        assert StreamingTriggerOptions.PROCESSING_TIME_10M.value == {"processingTime": "10 minutes"}

    def test_available_now(self) -> None:
        assert StreamingTriggerOptions.AVAILABLE_NOW.value == {"availableNow": True}

    def test_all_options_are_dicts(self) -> None:
        for opt in StreamingTriggerOptions:
            assert isinstance(opt.value, dict)

    def test_enum_members_count(self) -> None:
        assert len(StreamingTriggerOptions) == 6

    def test_trigger_values_usable_as_kwargs(self) -> None:
        """Trigger values must be valid kwargs for writeStream.trigger()."""
        for opt in StreamingTriggerOptions:
            assert len(opt.value) == 1, f"{opt.name} should have exactly one key"


@pytest.mark.no_pyspark
class TestStreamingTableReaderContract:
    """Tests for StreamingTableReader that don't require Spark."""

    def test_is_abstract(self) -> None:
        """StreamingTableReader cannot be instantiated directly."""
        with pytest.raises(TypeError, match="process_batch"):
            StreamingTableReader(  # type: ignore[abstract]
                source_table="test",
                trigger=StreamingTriggerOptions.AVAILABLE_NOW,
                checkpoint_location="/tmp/ckpt",
            )

    def test_subclass_with_mock_spark(self) -> None:
        """A concrete subclass can be created with a mocked SparkSession."""

        class ConcreteReader(StreamingTableReader):
            def process_batch(self, df, batch_id):
                pass

        mock_spark = MagicMock()

        with patch("databricks4py.io.streaming.active_fallback", return_value=mock_spark):
            reader = ConcreteReader(
                source_table="test.table",
                trigger=StreamingTriggerOptions.PROCESSING_TIME_1M,
                checkpoint_location="/checkpoints/test",
            )

        assert reader._source_table == "test.table"
        assert reader._trigger == StreamingTriggerOptions.PROCESSING_TIME_1M
        assert reader._checkpoint_location == "/checkpoints/test"
        assert reader._source_format == "delta"
        assert reader._skip_empty_batches is True
        assert reader._filter is None
        assert reader._read_options == {}

    def test_subclass_with_row_filter(self) -> None:
        """row_filter parameter is correctly assigned."""

        class ConcreteReader(StreamingTableReader):
            def process_batch(self, df, batch_id):
                pass

        mock_spark = MagicMock()
        mock_filter = MagicMock()

        with patch("databricks4py.io.streaming.active_fallback", return_value=mock_spark):
            reader = ConcreteReader(
                source_table="test.table",
                trigger=StreamingTriggerOptions.AVAILABLE_NOW,
                checkpoint_location="/checkpoints/test",
                row_filter=mock_filter,
            )

        assert reader._filter is mock_filter

    def test_subclass_with_custom_options(self) -> None:
        """Custom read_options and source_format are preserved."""

        class ConcreteReader(StreamingTableReader):
            def process_batch(self, df, batch_id):
                pass

        mock_spark = MagicMock()

        with patch("databricks4py.io.streaming.active_fallback", return_value=mock_spark):
            reader = ConcreteReader(
                source_table="s3://bucket/path",
                trigger=StreamingTriggerOptions.PROCESSING_TIME_10S,
                checkpoint_location="/ckpt",
                source_format="parquet",
                skip_empty_batches=False,
                read_options={"maxFilesPerTrigger": "10"},
            )

        assert reader._source_format == "parquet"
        assert reader._skip_empty_batches is False
        assert reader._read_options == {"maxFilesPerTrigger": "10"}

    def test_foreach_batch_wrapper_skips_empty(self) -> None:
        """Empty batches are skipped when skip_empty_batches=True."""

        class ConcreteReader(StreamingTableReader):
            def __init__(self, **kwargs):
                self.processed = []
                super().__init__(**kwargs)

            def process_batch(self, df, batch_id):
                self.processed.append(batch_id)

        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.isEmpty.return_value = True

        with patch("databricks4py.io.streaming.active_fallback", return_value=mock_spark):
            reader = ConcreteReader(
                source_table="t",
                trigger=StreamingTriggerOptions.AVAILABLE_NOW,
                checkpoint_location="/ckpt",
            )

        reader._foreach_batch_wrapper(mock_df, 0)
        assert reader.processed == []

    def test_foreach_batch_wrapper_processes_non_empty(self) -> None:
        """Non-empty batches are processed."""

        class ConcreteReader(StreamingTableReader):
            def __init__(self, **kwargs):
                self.processed = []
                super().__init__(**kwargs)

            def process_batch(self, df, batch_id):
                self.processed.append(batch_id)

        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.isEmpty.return_value = False

        with patch("databricks4py.io.streaming.active_fallback", return_value=mock_spark):
            reader = ConcreteReader(
                source_table="t",
                trigger=StreamingTriggerOptions.AVAILABLE_NOW,
                checkpoint_location="/ckpt",
            )

        reader._foreach_batch_wrapper(mock_df, 42)
        assert reader.processed == [42]

    def test_foreach_batch_wrapper_applies_filter(self) -> None:
        """Filter is applied before process_batch."""

        class ConcreteReader(StreamingTableReader):
            def __init__(self, **kwargs):
                self.received_dfs = []
                super().__init__(**kwargs)

            def process_batch(self, df, batch_id):
                self.received_dfs.append(df)

        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.isEmpty.return_value = False

        filtered_df = MagicMock()
        filtered_df.isEmpty.return_value = False

        mock_filter = MagicMock(return_value=filtered_df)

        with patch("databricks4py.io.streaming.active_fallback", return_value=mock_spark):
            reader = ConcreteReader(
                source_table="t",
                trigger=StreamingTriggerOptions.AVAILABLE_NOW,
                checkpoint_location="/ckpt",
                row_filter=mock_filter,
            )

        reader._foreach_batch_wrapper(mock_df, 1)
        mock_filter.assert_called_once_with(mock_df)
        assert reader.received_dfs == [filtered_df]

    def test_foreach_batch_wrapper_skips_after_filter_empties(self) -> None:
        """If filter empties the batch, it is skipped."""

        class ConcreteReader(StreamingTableReader):
            def __init__(self, **kwargs):
                self.processed = []
                super().__init__(**kwargs)

            def process_batch(self, df, batch_id):
                self.processed.append(batch_id)

        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.isEmpty.return_value = False

        filtered_df = MagicMock()
        filtered_df.isEmpty.return_value = True

        mock_filter = MagicMock(return_value=filtered_df)

        with patch("databricks4py.io.streaming.active_fallback", return_value=mock_spark):
            reader = ConcreteReader(
                source_table="t",
                trigger=StreamingTriggerOptions.AVAILABLE_NOW,
                checkpoint_location="/ckpt",
                row_filter=mock_filter,
            )

        reader._foreach_batch_wrapper(mock_df, 1)
        assert reader.processed == []
