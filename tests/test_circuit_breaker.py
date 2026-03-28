"""Tests for the streaming circuit breaker."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from databricks4py.io.streaming import CircuitBreakerError, StreamingTableReader


class _FailingReader(StreamingTableReader):
    """Reader that always raises on process_batch."""

    def __init__(self, fail_with: Exception | None = None, **kwargs):
        self._fail_with = fail_with or RuntimeError("batch failed")
        super().__init__(**kwargs)

    def process_batch(self, df, batch_id):
        raise self._fail_with


class _CountingReader(StreamingTableReader):
    """Reader that tracks calls and optionally fails on specified batches."""

    def __init__(self, fail_batch_ids: set[int] | None = None, **kwargs):
        self._fail_batch_ids = fail_batch_ids or set()
        self.processed: list[int] = []
        super().__init__(**kwargs)

    def process_batch(self, df, batch_id):
        if batch_id in self._fail_batch_ids:
            raise RuntimeError(f"batch {batch_id} failed")
        self.processed.append(batch_id)


def _make_df(empty: bool = False) -> MagicMock:
    df = MagicMock()
    df.isEmpty.return_value = empty
    df.count.return_value = 0 if empty else 5
    return df


@pytest.mark.no_pyspark
class TestCircuitBreakerError:
    def test_is_runtime_error(self) -> None:
        assert issubclass(CircuitBreakerError, RuntimeError)

    def test_can_be_raised_and_caught(self) -> None:
        with pytest.raises(CircuitBreakerError, match="tripped"):
            raise CircuitBreakerError("tripped after 3 failures")


@pytest.mark.no_pyspark
class TestCircuitBreakerConfig:
    def test_default_is_disabled(self) -> None:
        with patch("databricks4py.io.streaming.active_fallback", return_value=MagicMock()):
            reader = _FailingReader(source_table="t", checkpoint_location="/cp")
        assert reader._max_consecutive_failures is None
        assert reader._consecutive_failures == 0

    def test_explicit_threshold_stored(self) -> None:
        with patch("databricks4py.io.streaming.active_fallback", return_value=MagicMock()):
            reader = _FailingReader(
                source_table="t",
                checkpoint_location="/cp",
                max_consecutive_failures=3,
            )
        assert reader._max_consecutive_failures == 3

    def test_counter_initialised_to_zero(self) -> None:
        with patch("databricks4py.io.streaming.active_fallback", return_value=MagicMock()):
            reader = _FailingReader(
                source_table="t",
                checkpoint_location="/cp",
                max_consecutive_failures=5,
            )
        assert reader._consecutive_failures == 0


@pytest.mark.no_pyspark
class TestCircuitBreakerTripping:
    def _reader(self, max_consecutive_failures: int, **kwargs) -> _FailingReader:
        with patch("databricks4py.io.streaming.active_fallback", return_value=MagicMock()):
            return _FailingReader(
                source_table="t",
                checkpoint_location="/cp",
                max_consecutive_failures=max_consecutive_failures,
                **kwargs,
            )

    def test_trips_at_threshold(self) -> None:
        """Below threshold: original exception. At threshold: CircuitBreakerError."""
        reader = self._reader(max_consecutive_failures=3)
        df = _make_df()

        with pytest.raises(RuntimeError, match="batch failed"):
            reader._foreach_batch_wrapper(df, 0)  # failure 1 — re-raises original
        with pytest.raises(RuntimeError, match="batch failed"):
            reader._foreach_batch_wrapper(df, 1)  # failure 2 — re-raises original
        with pytest.raises(CircuitBreakerError, match="3 consecutive"):
            reader._foreach_batch_wrapper(df, 2)  # failure 3 — trips

    def test_no_trip_below_threshold(self) -> None:
        """All 4 failures re-raise the original exception; counter reaches 4."""
        reader = self._reader(max_consecutive_failures=5)
        df = _make_df()

        for i in range(4):
            with pytest.raises(RuntimeError, match="batch failed"):
                reader._foreach_batch_wrapper(df, i)

        assert reader._consecutive_failures == 4

    def test_counter_resets_on_success(self) -> None:
        with patch("databricks4py.io.streaming.active_fallback", return_value=MagicMock()):
            reader = _CountingReader(
                fail_batch_ids={0, 1},
                source_table="t",
                checkpoint_location="/cp",
                max_consecutive_failures=5,
            )
        df = _make_df()

        with pytest.raises(RuntimeError, match="batch 0 failed"):
            reader._foreach_batch_wrapper(df, 0)  # fail
        with pytest.raises(RuntimeError, match="batch 1 failed"):
            reader._foreach_batch_wrapper(df, 1)  # fail
        assert reader._consecutive_failures == 2

        reader._foreach_batch_wrapper(df, 2)  # success — resets counter
        assert reader._consecutive_failures == 0

    def test_counter_resets_then_trips_again(self) -> None:
        with patch("databricks4py.io.streaming.active_fallback", return_value=MagicMock()):
            reader = _CountingReader(
                fail_batch_ids={0, 1, 3, 4, 5},
                source_table="t",
                checkpoint_location="/cp",
                max_consecutive_failures=3,
            )
        df = _make_df()

        with pytest.raises(RuntimeError, match="batch 0 failed"):
            reader._foreach_batch_wrapper(df, 0)  # fail 1
        with pytest.raises(RuntimeError, match="batch 1 failed"):
            reader._foreach_batch_wrapper(df, 1)  # fail 2
        reader._foreach_batch_wrapper(df, 2)  # success — reset
        with pytest.raises(RuntimeError, match="batch 3 failed"):
            reader._foreach_batch_wrapper(df, 3)  # fail 1
        with pytest.raises(RuntimeError, match="batch 4 failed"):
            reader._foreach_batch_wrapper(df, 4)  # fail 2
        with pytest.raises(CircuitBreakerError, match="3 consecutive"):
            reader._foreach_batch_wrapper(df, 5)  # fail 3 — trips

    def test_disabled_when_none(self) -> None:
        """Without max_consecutive_failures, original exception propagates."""
        with patch("databricks4py.io.streaming.active_fallback", return_value=MagicMock()):
            reader = _FailingReader(
                source_table="t",
                checkpoint_location="/cp",
            )
        df = _make_df()
        for i in range(100):
            with pytest.raises(RuntimeError, match="batch failed"):
                reader._foreach_batch_wrapper(df, i)

    def test_error_message_includes_table(self) -> None:
        reader = self._reader(max_consecutive_failures=1)
        df = _make_df()
        with pytest.raises(CircuitBreakerError, match="t"):
            reader._foreach_batch_wrapper(df, 0)

    def test_cause_chained(self) -> None:
        reader = self._reader(max_consecutive_failures=1)
        df = _make_df()
        with pytest.raises(CircuitBreakerError) as exc_info:
            reader._foreach_batch_wrapper(df, 0)
        assert exc_info.value.__cause__ is not None


@pytest.mark.no_pyspark
class TestCircuitBreakerWithDLQ:
    def test_trips_with_dlq_after_threshold(self) -> None:
        """Circuit breaker fires even when DLQ is configured."""
        with patch("databricks4py.io.streaming.active_fallback", return_value=MagicMock()):
            reader = _FailingReader(
                source_table="t",
                checkpoint_location="/cp",
                max_consecutive_failures=2,
                dead_letter_table="catalog.dlq",
            )

        df = _make_df()

        with patch.object(reader, "_write_to_dlq"):
            reader._foreach_batch_wrapper(df, 0)  # fail 1, DLQ written
            with pytest.raises(CircuitBreakerError, match="2 consecutive"):
                reader._foreach_batch_wrapper(df, 1)  # fail 2, trips

    def test_dlq_written_before_trip(self) -> None:
        """DLQ write happens before CircuitBreakerError is raised."""
        with patch("databricks4py.io.streaming.active_fallback", return_value=MagicMock()):
            reader = _FailingReader(
                source_table="t",
                checkpoint_location="/cp",
                max_consecutive_failures=1,
                dead_letter_table="catalog.dlq",
            )
        df = _make_df()
        dlq_write_calls: list[int] = []

        def record_dlq(df_, batch_id, msg):
            dlq_write_calls.append(batch_id)

        with (
            patch.object(reader, "_write_to_dlq", side_effect=record_dlq),
            pytest.raises(CircuitBreakerError),
        ):
            reader._foreach_batch_wrapper(df, 7)

        assert 7 in dlq_write_calls

    def test_counter_resets_after_dlq_success(self) -> None:
        with patch("databricks4py.io.streaming.active_fallback", return_value=MagicMock()):
            reader = _CountingReader(
                fail_batch_ids={0},
                source_table="t",
                checkpoint_location="/cp",
                max_consecutive_failures=3,
                dead_letter_table="catalog.dlq",
            )
        df = _make_df()

        with patch.object(reader, "_write_to_dlq"):
            reader._foreach_batch_wrapper(df, 0)  # fail → DLQ
        assert reader._consecutive_failures == 1

        reader._foreach_batch_wrapper(df, 1)  # success
        assert reader._consecutive_failures == 0
