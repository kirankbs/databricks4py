"""Tests for MigrationRunner and MigrationStep."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from databricks4py.migrations.runner import MigrationRunner, MigrationRunResult, MigrationStep
from databricks4py.migrations.validators import MigrationError


def _make_runner(applied_versions: set[str] | None = None) -> MigrationRunner:
    """Return a MigrationRunner backed by a mocked SparkSession."""
    applied_versions = applied_versions or set()
    mock_spark = MagicMock()
    # mock_spark.sql() returns a MagicMock, so _ensure_history_table is a no-op
    with patch("databricks4py.migrations.runner.active_fallback", return_value=mock_spark):
        runner = MigrationRunner("default._migration_history")
    # Patch _applied_versions so tests control what's already applied
    runner._applied_versions = lambda: set(applied_versions)  # type: ignore[method-assign]
    runner._record = MagicMock()  # type: ignore[method-assign]
    return runner


@pytest.mark.no_pyspark
class TestMigrationStep:
    def test_frozen_dataclass(self) -> None:
        step = MigrationStep(version="V001", description="test", up=lambda s: None)
        with pytest.raises(AttributeError, match="cannot assign"):
            step.version = "V002"  # type: ignore[misc]

    def test_optional_validators_default_none(self) -> None:
        step = MigrationStep(version="V001", description="d", up=lambda s: None)
        assert step.pre_validate is None
        assert step.post_validate is None


@pytest.mark.no_pyspark
class TestMigrationRunResult:
    def test_defaults(self) -> None:
        result = MigrationRunResult()
        assert result.applied == []
        assert result.skipped == []
        assert result.failed is None
        assert result.dry_run is False


@pytest.mark.no_pyspark
class TestMigrationRunnerPending:
    def test_all_pending_when_nothing_applied(self) -> None:
        runner = _make_runner()
        runner.register(
            MigrationStep(version="V001", description="a", up=lambda s: None),
            MigrationStep(version="V002", description="b", up=lambda s: None),
        )
        assert [s.version for s in runner.pending()] == ["V001", "V002"]

    def test_already_applied_excluded(self) -> None:
        runner = _make_runner(applied_versions={"V001"})
        runner.register(
            MigrationStep(version="V001", description="a", up=lambda s: None),
            MigrationStep(version="V002", description="b", up=lambda s: None),
        )
        assert [s.version for s in runner.pending()] == ["V002"]

    def test_pending_sorted_by_version(self) -> None:
        runner = _make_runner()
        runner.register(
            MigrationStep(version="V003", description="c", up=lambda s: None),
            MigrationStep(version="V001", description="a", up=lambda s: None),
            MigrationStep(version="V002", description="b", up=lambda s: None),
        )
        assert [s.version for s in runner.pending()] == ["V001", "V002", "V003"]


@pytest.mark.no_pyspark
class TestMigrationRunnerApplied:
    def test_applied_returns_sorted_versions(self) -> None:
        runner = _make_runner(applied_versions={"V003", "V001"})
        assert runner.applied() == ["V001", "V003"]

    def test_applied_empty_when_none(self) -> None:
        runner = _make_runner()
        assert runner.applied() == []


@pytest.mark.no_pyspark
class TestMigrationRunnerRun:
    def test_runs_pending_steps(self) -> None:
        runner = _make_runner()
        called = []
        runner.register(
            MigrationStep(version="V001", description="a", up=lambda s: called.append("V001"))
        )

        result = runner.run()

        assert result.applied == ["V001"]
        assert called == ["V001"]

    def test_skips_applied_steps(self) -> None:
        runner = _make_runner(applied_versions={"V001"})
        up_called = []
        runner.register(
            MigrationStep(version="V001", description="a", up=lambda s: up_called.append("V001")),
            MigrationStep(version="V002", description="b", up=lambda s: up_called.append("V002")),
        )

        result = runner.run()

        assert result.skipped == ["V001"]
        assert result.applied == ["V002"]
        assert up_called == ["V002"]

    def test_run_order_is_version_sorted(self) -> None:
        runner = _make_runner()
        order = []
        runner.register(
            MigrationStep(version="V003", description="c", up=lambda s: order.append("V003")),
            MigrationStep(version="V001", description="a", up=lambda s: order.append("V001")),
            MigrationStep(version="V002", description="b", up=lambda s: order.append("V002")),
        )

        runner.run()

        assert order == ["V001", "V002", "V003"]

    def test_halts_on_failure_records_and_returns(self) -> None:
        runner = _make_runner()
        order = []

        def _fail(spark):
            raise RuntimeError("boom")

        runner.register(
            MigrationStep(version="V001", description="a", up=lambda s: order.append("V001")),
            MigrationStep(version="V002", description="fail", up=_fail),
            MigrationStep(version="V003", description="c", up=lambda s: order.append("V003")),
        )

        result = runner.run()

        assert result.failed == "V002"
        assert "V003" not in result.applied
        assert order == ["V001"]
        runner._record.assert_called_with(runner._steps[1], success=False, error_message="boom")

    def test_pre_validate_failure_raises(self) -> None:
        runner = _make_runner()
        runner.register(
            MigrationStep(
                version="V001",
                description="guarded",
                up=lambda s: None,
                pre_validate=lambda s: False,
            )
        )

        with pytest.raises(MigrationError, match="Pre-validation failed"):
            runner.run()

    def test_pre_validate_success_proceeds(self) -> None:
        runner = _make_runner()
        called = []
        runner.register(
            MigrationStep(
                version="V001",
                description="guarded",
                up=lambda s: called.append(True),
                pre_validate=lambda s: True,
            )
        )

        result = runner.run()

        assert result.applied == ["V001"]
        assert called == [True]

    def test_post_validate_failure_records_and_halts(self) -> None:
        runner = _make_runner()
        runner.register(
            MigrationStep(
                version="V001",
                description="post-fail",
                up=lambda s: None,
                post_validate=lambda s: False,
            )
        )

        result = runner.run()

        assert result.failed == "V001"
        runner._record.assert_called_once_with(
            runner._steps[0],
            success=False,
            error_message="Post-validation failed for step V001",
        )

    def test_dry_run_does_not_call_up(self) -> None:
        runner = _make_runner()
        called = []
        runner.register(
            MigrationStep(version="V001", description="a", up=lambda s: called.append(True))
        )

        result = runner.run(dry_run=True)

        assert result.dry_run is True
        assert result.applied == ["V001"]
        assert called == []
        runner._record.assert_not_called()

    def test_dry_run_skips_already_applied(self) -> None:
        runner = _make_runner(applied_versions={"V001"})
        runner.register(
            MigrationStep(version="V001", description="a", up=lambda s: None),
            MigrationStep(version="V002", description="b", up=lambda s: None),
        )

        result = runner.run(dry_run=True)

        assert result.skipped == ["V001"]
        assert result.applied == ["V002"]

    def test_register_chaining(self) -> None:
        runner = _make_runner()
        step_a = MigrationStep(version="V001", description="a", up=lambda s: None)
        step_b = MigrationStep(version="V002", description="b", up=lambda s: None)
        returned = runner.register(step_a).register(step_b)
        assert returned is runner
        assert len(runner._steps) == 2

    def test_record_called_on_success(self) -> None:
        runner = _make_runner()
        step = MigrationStep(version="V001", description="a", up=lambda s: None)
        runner.register(step)

        runner.run()

        runner._record.assert_called_once_with(step, success=True)
