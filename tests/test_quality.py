"""Tests for the quality module (expectations and quality gate)."""

from dataclasses import FrozenInstanceError

import pytest

from databricks4py.quality.base import Expectation, ExpectationResult, QualityReport
from databricks4py.quality.expectations import (
    ColumnExists,
    InRange,
    MatchesRegex,
    NotNull,
    RowCount,
    Unique,
)
from databricks4py.quality.gate import QualityGate


@pytest.mark.no_pyspark
class TestExpectationResult:
    def test_passed(self) -> None:
        r = ExpectationResult(expectation="not_null", passed=True, total_rows=100)
        assert r.passed is True
        assert r.failing_rows == 0

    def test_failed(self) -> None:
        r = ExpectationResult(
            expectation="not_null", passed=False, total_rows=100, failing_rows=5
        )
        assert r.passed is False
        assert r.failing_rows == 5

    def test_frozen(self) -> None:
        r = ExpectationResult(expectation="not_null", passed=True, total_rows=100)
        with pytest.raises(FrozenInstanceError):
            r.passed = False  # type: ignore[misc]


@pytest.mark.no_pyspark
class TestQualityReport:
    def test_all_passed(self) -> None:
        results = [
            ExpectationResult(expectation="a", passed=True, total_rows=10),
            ExpectationResult(expectation="b", passed=True, total_rows=10),
        ]
        report = QualityReport(results=results, passed=True)
        assert report.passed is True

    def test_any_failed(self) -> None:
        results = [
            ExpectationResult(expectation="a", passed=True, total_rows=10),
            ExpectationResult(expectation="b", passed=False, total_rows=10, failing_rows=3),
        ]
        report = QualityReport(results=results, passed=False)
        assert report.passed is False

    def test_summary_returns_string(self) -> None:
        results = [
            ExpectationResult(expectation="a", passed=True, total_rows=10),
            ExpectationResult(expectation="b", passed=False, total_rows=10, failing_rows=2),
        ]
        report = QualityReport(results=results, passed=False)
        summary = report.summary()
        assert isinstance(summary, str)
        assert len(summary) > 0


@pytest.mark.no_pyspark
class TestExpectationABC:
    def test_cannot_instantiate(self) -> None:
        with pytest.raises(TypeError, match="instantiate"):
            Expectation()  # type: ignore[abstract]


@pytest.mark.no_pyspark
class TestExpectationRepr:
    def test_not_null(self) -> None:
        r = repr(NotNull("a", "b"))
        assert "NotNull" in r
        assert "a" in r
        assert "b" in r

    def test_in_range(self) -> None:
        r = repr(InRange("score", min_val=0, max_val=100))
        assert "InRange" in r
        assert "score" in r

    def test_unique(self) -> None:
        r = repr(Unique("id", "name"))
        assert "Unique" in r
        assert "id" in r

    def test_row_count(self) -> None:
        r = repr(RowCount(min_count=1, max_count=1000))
        assert "RowCount" in r

    def test_matches_regex(self) -> None:
        r = repr(MatchesRegex("email", r"^.+@.+$"))
        assert "MatchesRegex" in r
        assert "email" in r

    def test_column_exists(self) -> None:
        r = repr(ColumnExists("id", "name"))
        assert "ColumnExists" in r
        assert "id" in r


@pytest.mark.no_pyspark
class TestQualityGateConstruction:
    def test_quarantine_requires_handler(self) -> None:
        with pytest.raises(ValueError, match="quarantine_handler"):
            QualityGate(NotNull("id"), on_fail="quarantine")

    def test_quarantine_with_handler_works(self) -> None:
        handler = lambda df: None  # noqa: E731
        gate = QualityGate(NotNull("id"), on_fail="quarantine", quarantine_handler=handler)
        assert gate is not None

    def test_default_on_fail_is_raise(self) -> None:
        gate = QualityGate(NotNull("id"))
        assert gate._on_fail == "raise"
