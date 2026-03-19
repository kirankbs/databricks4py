"""Tests for SparkSession management utilities."""

import pyspark.sql
import pytest

from databricks4py.spark_session import active_fallback, get_active, get_or_create_local_session


class TestGetActive:
    @pytest.mark.integration
    def test_returns_active_session(self, spark_session: pyspark.sql.SparkSession) -> None:
        result = get_active()
        assert result is spark_session

    @pytest.mark.no_pyspark
    def test_raises_when_no_session(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(pyspark.sql.SparkSession, "getActiveSession", lambda: None)
        with pytest.raises(RuntimeError, match="No active SparkSession"):
            get_active()


class TestActiveFallback:
    @pytest.mark.integration
    def test_returns_passed_session(self, spark_session: pyspark.sql.SparkSession) -> None:
        result = active_fallback(spark_session)
        assert result is spark_session

    @pytest.mark.integration
    def test_falls_back_to_active(self, spark_session: pyspark.sql.SparkSession) -> None:
        result = active_fallback(None)
        assert result is spark_session

    @pytest.mark.no_pyspark
    def test_raises_when_none_and_no_active(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(pyspark.sql.SparkSession, "getActiveSession", lambda: None)
        with pytest.raises(RuntimeError, match="No active SparkSession"):
            active_fallback(None)


class TestGetOrCreateLocalSession:
    @pytest.mark.integration
    def test_creates_session(self, spark_session: pyspark.sql.SparkSession) -> None:
        # With an existing session, this should return the same one
        result = get_or_create_local_session()
        assert isinstance(result, pyspark.sql.SparkSession)
