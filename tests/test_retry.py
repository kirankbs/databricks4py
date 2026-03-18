"""Tests for retry module."""

from __future__ import annotations

import time

import pytest

from databricks4py.retry import RetryConfig, retry


class TestRetryConfig:
    @pytest.mark.no_pyspark
    def test_defaults(self):
        config = RetryConfig()
        assert config.max_attempts == 3
        assert config.base_delay_seconds == 1.0
        assert config.max_delay_seconds == 60.0
        assert config.backoff_factor == 2.0
        assert ConnectionError in config.retryable_exceptions
        assert TimeoutError in config.retryable_exceptions
        assert OSError in config.retryable_exceptions

    @pytest.mark.no_pyspark
    def test_custom_config(self):
        config = RetryConfig(
            max_attempts=5,
            base_delay_seconds=0.5,
            max_delay_seconds=30.0,
            backoff_factor=3.0,
            retryable_exceptions=(ValueError,),
        )
        assert config.max_attempts == 5
        assert config.base_delay_seconds == 0.5
        assert config.max_delay_seconds == 30.0
        assert config.backoff_factor == 3.0
        assert ValueError in config.retryable_exceptions


class TestRetryDecorator:
    @pytest.mark.no_pyspark
    def test_no_retry_on_success(self):
        call_count = 0

        @retry(config=RetryConfig())
        def succeed():
            nonlocal call_count
            call_count += 1
            return "ok"

        assert succeed() == "ok"
        assert call_count == 1

    @pytest.mark.no_pyspark
    def test_retries_on_transient_failure(self):
        call_count = 0

        @retry(config=RetryConfig(base_delay_seconds=0.01, max_delay_seconds=0.1))
        def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("transient")
            return "recovered"

        assert flaky() == "recovered"
        assert call_count == 3

    @pytest.mark.no_pyspark
    def test_raises_after_max_attempts(self):
        @retry(config=RetryConfig(max_attempts=2, base_delay_seconds=0.01))
        def always_fails():
            raise TimeoutError("still broken")

        with pytest.raises(TimeoutError, match="still broken"):
            always_fails()

    @pytest.mark.no_pyspark
    def test_no_retry_on_non_retryable_exception(self):
        call_count = 0

        @retry(config=RetryConfig(base_delay_seconds=0.01))
        def bad_logic():
            nonlocal call_count
            call_count += 1
            raise ValueError("not retryable")

        with pytest.raises(ValueError, match="not retryable"):
            bad_logic()
        assert call_count == 1

    @pytest.mark.no_pyspark
    def test_exponential_backoff_timing(self):
        call_count = 0
        config = RetryConfig(
            max_attempts=3,
            base_delay_seconds=0.1,
            max_delay_seconds=10.0,
            backoff_factor=2.0,
        )

        @retry(config=config)
        def always_fails():
            nonlocal call_count
            call_count += 1
            raise ConnectionError("fail")

        start = time.monotonic()
        with pytest.raises(ConnectionError):
            always_fails()
        elapsed = time.monotonic() - start

        # delay_1 = 0.1, delay_2 = 0.2 → total ~0.3s
        assert elapsed >= 0.25
        assert elapsed < 1.0

    @pytest.mark.no_pyspark
    def test_default_config_when_none(self):
        call_count = 0

        @retry()
        def succeed():
            nonlocal call_count
            call_count += 1
            return "default"

        assert succeed() == "default"
        assert call_count == 1
