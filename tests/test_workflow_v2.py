"""Tests for Workflow v0.2 enhancements: config, metrics, retry, quality_check."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from databricks4py.workflow import Workflow


@pytest.mark.no_pyspark
class TestWorkflowV2Config:
    def test_backward_compat(self):
        class OldJob(Workflow):
            def run(self):
                pass

        job = OldJob(spark=MagicMock())
        assert job.config is None
        assert job.metrics is None

    def test_config_property(self):
        from databricks4py.config import JobConfig

        config = JobConfig(tables={"events": "cat.bronze.events"})

        class ConfigJob(Workflow):
            def run(self):
                pass

        job = ConfigJob(config=config, spark=MagicMock())
        assert job.config is config

    def test_metrics_property(self):
        from databricks4py.metrics import LoggingMetricsSink

        sink = LoggingMetricsSink()

        class MetricsJob(Workflow):
            def run(self):
                pass

        job = MetricsJob(metrics=sink, spark=MagicMock())
        assert job.metrics is sink

    def test_emit_metric_noop_without_sink(self):
        class NoSinkJob(Workflow):
            def run(self):
                pass

        job = NoSinkJob(spark=MagicMock())
        job.emit_metric("test_event")  # should not raise


@pytest.mark.integration
class TestWorkflowV2Execute:
    def test_lifecycle_metrics(self, spark_session):
        from databricks4py.metrics import MetricsSink

        events = []

        class CollectorSink(MetricsSink):
            def emit(self, event):
                events.append(event)

        class TestJob(Workflow):
            def run(self):
                self.emit_metric("custom_event", row_count=42)

        job = TestJob(spark=spark_session, metrics=CollectorSink())
        job.execute()
        types = [e.event_type for e in events]
        assert "job_start" in types
        assert "custom_event" in types
        assert "job_complete" in types

    def test_failure_metric(self, spark_session):
        from databricks4py.metrics import MetricsSink

        events = []

        class CollectorSink(MetricsSink):
            def emit(self, event):
                events.append(event)

        class FailJob(Workflow):
            def run(self):
                raise RuntimeError("boom")

        job = FailJob(spark=spark_session, metrics=CollectorSink())
        with pytest.raises(RuntimeError, match="boom"):
            job.execute()
        types = [e.event_type for e in events]
        assert "job_failed" in types

    def test_retry_integration(self, spark_session):
        from databricks4py.retry import RetryConfig

        count = 0

        class RetryJob(Workflow):
            def run(self):
                nonlocal count
                count += 1
                if count < 2:
                    raise ConnectionError("transient")

        job = RetryJob(
            spark=spark_session,
            retry_config=RetryConfig(
                max_attempts=3,
                base_delay_seconds=0.01,
                retryable_exceptions=(ConnectionError,),
            ),
        )
        job.execute()
        assert count == 2

    def test_spark_configs_applied(self, spark_session):
        from databricks4py.config import JobConfig

        class ConfigJob(Workflow):
            def run(self):
                assert self.spark.conf.get("spark.sql.shuffle.partitions") == "10"

        config = JobConfig(
            tables={},
            spark_configs={"spark.sql.shuffle.partitions": "10"},
        )
        job = ConfigJob(spark=spark_session, config=config)
        job.execute()
