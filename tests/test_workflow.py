"""Tests for the Workflow base class."""

from datetime import datetime

import pyspark.sql
import pytest

from databricks4py.workflow import Workflow


class _TestWorkflow(Workflow):
    """Concrete workflow for testing."""

    def __init__(self, **kwargs):
        self.run_called = False
        self.run_count = 0
        super().__init__(**kwargs)

    def run(self) -> None:
        self.run_called = True
        self.run_count += 1


class _FailingWorkflow(Workflow):
    def run(self) -> None:
        raise ValueError("intentional failure")


@pytest.mark.integration
class TestWorkflow:
    def test_run(self, spark_session: pyspark.sql.SparkSession) -> None:
        wf = _TestWorkflow(spark=spark_session)
        wf.run()
        assert wf.run_called

    def test_spark_property(self, spark_session: pyspark.sql.SparkSession) -> None:
        wf = _TestWorkflow(spark=spark_session)
        assert wf.spark is spark_session

    def test_dbutils_none_by_default(self, spark_session: pyspark.sql.SparkSession) -> None:
        wf = _TestWorkflow(spark=spark_session)
        assert wf.dbutils is None

    def test_run_at_time(self, spark_session: pyspark.sql.SparkSession) -> None:
        wf = _TestWorkflow(spark=spark_session)
        ts = datetime(2024, 1, 15, 10, 30)
        wf.run_at_time(execution_time=ts)
        assert wf.run_called
        assert wf.execution_time == ts

    def test_run_at_time_default(self, spark_session: pyspark.sql.SparkSession) -> None:
        wf = _TestWorkflow(spark=spark_session)
        before = datetime.now()
        wf.run_at_time()
        after = datetime.now()
        assert before <= wf.execution_time <= after

    def test_execute(self, spark_session: pyspark.sql.SparkSession) -> None:
        wf = _TestWorkflow(spark=spark_session)
        wf.execute()
        assert wf.run_called

    def test_execute_failure(self, spark_session: pyspark.sql.SparkSession) -> None:
        wf = _FailingWorkflow(spark=spark_session)
        with pytest.raises(ValueError, match="intentional failure"):
            wf.execute()

    def test_execution_time_default(self, spark_session: pyspark.sql.SparkSession) -> None:
        wf = _TestWorkflow(spark=spark_session)
        # Before run_at_time is called, execution_time is None in v0.2
        assert wf.execution_time is None
