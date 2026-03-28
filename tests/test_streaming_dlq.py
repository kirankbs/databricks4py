"""Tests for StreamingTableReader DLQ support and graceful stop."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from databricks4py.io.streaming import StreamingTableReader


class _FailingReader(StreamingTableReader):
    """Raises on every batch."""

    def process_batch(self, df, batch_id):
        raise ValueError("intentional failure")


class _OkReader(StreamingTableReader):
    def process_batch(self, df, batch_id):
        pass


@pytest.mark.no_pyspark
class TestDLQSupport:
    def _make_reader(self, dead_letter_table=None, cls=_OkReader):
        mock_spark = MagicMock()
        with patch("databricks4py.io.streaming.active_fallback", return_value=mock_spark):
            return cls(
                source_table="catalog.schema.events",
                checkpoint_location="/cp",
                dead_letter_table=dead_letter_table,
            )

    def test_dlq_param_stored(self) -> None:
        reader = self._make_reader(dead_letter_table="catalog.schema.dlq")
        assert reader._dead_letter_table == "catalog.schema.dlq"

    def test_no_dlq_by_default(self) -> None:
        reader = self._make_reader()
        assert reader._dead_letter_table is None

    def test_exception_reraises_without_dlq(self) -> None:
        reader = self._make_reader(cls=_FailingReader)
        mock_df = MagicMock()
        mock_df.isEmpty.return_value = False
        mock_df.count.return_value = 5

        with pytest.raises(ValueError, match="intentional failure"):
            reader._foreach_batch_wrapper(mock_df, 0)

    def test_exception_routed_to_dlq(self) -> None:
        reader = self._make_reader(dead_letter_table="catalog.schema.dlq", cls=_FailingReader)
        mock_df = MagicMock()
        mock_df.isEmpty.return_value = False
        mock_df.count.return_value = 5

        with patch.object(reader, "_write_to_dlq") as mock_dlq:
            reader._foreach_batch_wrapper(mock_df, 42)

        mock_dlq.assert_called_once()
        _, batch_id, error_msg = mock_dlq.call_args[0]
        assert batch_id == 42
        assert "intentional failure" in error_msg

    def test_dlq_write_adds_three_error_columns(self) -> None:
        mock_spark = MagicMock()
        with patch("databricks4py.io.streaming.active_fallback", return_value=mock_spark):
            reader = _FailingReader(
                source_table="t",
                checkpoint_location="/cp",
                dead_letter_table="catalog.schema.dlq",
            )

        # Build a chain: each withColumn returns an object supporting another withColumn
        chain = MagicMock()
        chain.withColumn.return_value = chain
        mock_df = MagicMock()
        mock_df.withColumn.return_value = chain

        with patch("pyspark.sql.functions.lit", return_value=MagicMock()):
            reader._write_to_dlq(mock_df, 7, "boom")

        # First withColumn is called on mock_df
        first_col_name = mock_df.withColumn.call_args[0][0]
        assert first_col_name == "_dlq_error_message"

        # Two more withColumn calls on the chain (_dlq_error_timestamp, _dlq_batch_id)
        col_names = [c[0][0] for c in chain.withColumn.call_args_list]
        assert "_dlq_error_timestamp" in col_names
        assert "_dlq_batch_id" in col_names

    def test_no_exception_no_dlq_called(self) -> None:
        reader = self._make_reader(dead_letter_table="catalog.schema.dlq", cls=_OkReader)
        mock_df = MagicMock()
        mock_df.isEmpty.return_value = False
        mock_df.count.return_value = 3

        with patch.object(reader, "_write_to_dlq") as mock_dlq:
            reader._foreach_batch_wrapper(mock_df, 1)

        mock_dlq.assert_not_called()

    def test_dlq_write_failure_reraises(self) -> None:
        """If the DLQ write itself fails, the exception propagates."""
        reader = self._make_reader(dead_letter_table="catalog.schema.dlq", cls=_FailingReader)
        mock_df = MagicMock()
        mock_df.isEmpty.return_value = False
        mock_df.count.return_value = 5

        with (
            patch.object(reader, "_write_to_dlq", side_effect=OSError("DLQ unavailable")),
            pytest.raises(OSError, match="DLQ unavailable"),
        ):
            reader._foreach_batch_wrapper(mock_df, 99)


@pytest.mark.no_pyspark
class TestStreamingStop:
    def _make_started_reader(self):
        mock_spark = MagicMock()
        with patch("databricks4py.io.streaming.active_fallback", return_value=mock_spark):
            reader = _OkReader(source_table="t", checkpoint_location="/cp")
        return reader

    def test_stop_before_start_raises(self) -> None:
        reader = self._make_started_reader()
        assert reader._query is None
        with pytest.raises(ValueError, match="No active query"):
            reader.stop()

    def test_query_property_none_before_start(self) -> None:
        reader = self._make_started_reader()
        assert reader.query is None

    def test_is_active_false_before_start(self) -> None:
        reader = self._make_started_reader()
        assert reader.is_active is False

    def test_stop_calls_query_stop_and_await(self) -> None:
        reader = self._make_started_reader()
        mock_query = MagicMock()
        reader._query = mock_query

        reader.stop(timeout_seconds=10)

        mock_query.stop.assert_called_once()
        mock_query.awaitTermination.assert_called_once_with(timeout=10)

    def test_query_property_after_start(self) -> None:
        reader = self._make_started_reader()
        mock_query = MagicMock()
        reader._query = mock_query
        assert reader.query is mock_query

    def test_is_active_delegates_to_query(self) -> None:
        reader = self._make_started_reader()
        mock_query = MagicMock()
        mock_query.isActive = True
        reader._query = mock_query
        assert reader.is_active is True

        mock_query.isActive = False
        assert reader.is_active is False

    def test_start_stores_query(self) -> None:
        mock_spark = MagicMock()
        mock_query = MagicMock()
        mock_spark.readStream.format.return_value.table.return_value.writeStream\
            .foreachBatch.return_value.trigger.return_value\
            .option.return_value.start.return_value = mock_query

        with patch("databricks4py.io.streaming.active_fallback", return_value=mock_spark):
            reader = _OkReader(source_table="t", checkpoint_location="/cp")

        reader.start()
        assert reader._query is mock_query
