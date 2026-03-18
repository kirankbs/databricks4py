# databricks4py v0.2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Evolve databricks4py from a utility bag into a workflow-centric framework with config, metrics, quality gates, merge/upsert, schema diff, streaming enhancements, testing builders, and retry — all backward compatible with v0.1.

**Architecture:** `Workflow` is the spine that wires config, metrics, quality, and retry together. Every new module works standalone but gets better inside a Workflow. Dependencies flow bottom-up: foundational modules (config, metrics, retry) → domain modules (quality, merge, schema_diff, checkpoint) → integration layer (enhanced Workflow). Existing v0.1 code is preserved with targeted fixes.

**Tech Stack:** Python 3.10+, PySpark 3.4+, Delta Lake 2.4+, pytest 8.0+

**Spec:** `docs/superpowers/specs/2026-03-18-library-redesign-design.md`

---

## File Structure

### New Files

```
src/databricks4py/
├── config/
│   ├── __init__.py          # Exports: Environment, JobConfig, UnityConfig
│   ├── base.py              # Environment enum, JobConfig class
│   └── unity.py             # UnityConfig class
├── metrics/
│   ├── __init__.py          # Exports: MetricEvent, MetricsSink, LoggingMetricsSink, DeltaMetricsSink, CompositeMetricsSink
│   ├── base.py              # MetricEvent dataclass, MetricsSink ABC, CompositeMetricsSink
│   ├── logging_sink.py      # LoggingMetricsSink
│   └── delta_sink.py        # DeltaMetricsSink
├── quality/
│   ├── __init__.py          # Exports: Expectation, ExpectationResult, QualityReport, QualityGate, NotNull, InRange, Unique, RowCount, MatchesRegex, ColumnExists
│   ├── base.py              # Expectation ABC, ExpectationResult, QualityReport
│   ├── expectations.py      # NotNull, InRange, Unique, RowCount, MatchesRegex, ColumnExists
│   └── gate.py              # QualityGate
├── io/
│   ├── merge.py             # MergeBuilder, MergeResult
│   └── checkpoint.py        # CheckpointManager, CheckpointInfo
├── migrations/
│   └── schema_diff.py       # SchemaDiff, ColumnChange, SchemaEvolutionError
├── testing/
│   ├── builders.py          # DataFrameBuilder
│   ├── temp_table.py        # TempDeltaTable
│   └── assertions.py        # assert_frame_equal, assert_schema_equal
└── retry.py                 # RetryConfig, retry decorator

tests/
├── test_config.py           # JobConfig, UnityConfig tests
├── test_metrics.py          # MetricsSink, LoggingMetricsSink, DeltaMetricsSink tests
├── test_quality.py          # Expectation, QualityGate tests
├── test_merge.py            # MergeBuilder, upsert, scd_type2 tests
├── test_schema_diff.py      # SchemaDiff tests
├── test_checkpoint.py       # CheckpointManager tests
├── test_builders.py         # DataFrameBuilder tests
├── test_temp_table.py       # TempDeltaTable tests
├── test_assertions.py       # assert_frame_equal tests
├── test_retry.py            # retry decorator tests
├── test_workflow_v2.py      # Enhanced Workflow integration tests

docs/
├── index.md
├── getting-started/
│   ├── installation.md
│   ├── quick-start.md
│   └── concepts.md
├── guides/
│   ├── config.md
│   ├── delta-operations.md
│   ├── quality-gates.md
│   ├── metrics.md
│   ├── streaming.md
│   ├── schema-evolution.md
│   ├── testing.md
│   ├── retry.md
│   └── workflow.md
├── api/
│   ├── config.md
│   ├── metrics.md
│   ├── quality.md
│   ├── io.md
│   ├── testing.md
│   └── workflow.md
└── examples/
    ├── bronze_to_silver.py
    ├── streaming_pipeline.py
    ├── scd_type2.py
    ├── quality_quarantine.py
    ├── multi_env_config.py
    ├── custom_metrics_sink.py
    ├── feature_pipeline.py
    └── testing_patterns.py
```

### Modified Files

```
src/databricks4py/__init__.py           # Add config, metrics, retry top-level exports + unified inject_dbutils
src/databricks4py/workflow.py           # Add config, metrics, retry_config params; quality_check method
src/databricks4py/io/__init__.py        # Add MergeBuilder, MergeResult, CheckpointManager exports
src/databricks4py/io/delta.py           # Add merge(), upsert(), scd_type2(); fix bare exceptions; add schema_check to write()
src/databricks4py/io/dbfs.py            # Rename inject_dbutils_module → _set_dbutils_module; add ls(), mv(), rm(), mkdirs()
src/databricks4py/io/streaming.py       # Add checkpoint_manager and metrics_sink optional params
src/databricks4py/migrations/__init__.py # Add SchemaDiff, ColumnChange, SchemaEvolutionError exports
src/databricks4py/migrations/validators.py # Fix bare except → AnalysisException
src/databricks4py/testing/__init__.py    # Add builders, temp_table, assertions exports + new fixtures
src/databricks4py/testing/fixtures.py    # Add df_builder, temp_delta fixtures
tests/test_dbfs.py                       # Add tests for ls, mv, rm, mkdirs
tests/test_streaming.py                  # Add StreamingTableReader integration tests
tests/test_delta.py                      # Add replace_data tests
```

---

## Task Dependency Order

```
Task 1: Fix existing code issues (bare exceptions, dbutils unification)
Task 2: Retry module (no dependencies)
Task 3: Config module (no dependencies)
Task 4: Metrics module (no dependencies)
Task 5: Quality module (no dependencies)
Task 6: Merge module (depends on existing DeltaTable)
Task 7: Schema diff module (no dependencies)
Task 8: Checkpoint module (no dependencies)
Task 9: Testing enhancements (depends on existing DeltaTable)
Task 10: DBFS expansion (no dependencies)
Task 11: Streaming enhancements (depends on Task 4 metrics, Task 8 checkpoint)
Task 12: DeltaTable enhancements (depends on Task 6 merge, Task 7 schema_diff, Task 4 metrics)
Task 13: Enhanced Workflow (depends on Task 2, 3, 4, 5)
Task 14: Top-level exports and __init__.py updates
Task 15: Documentation
Task 16: Examples
```

Tasks 2-8 can be parallelized. Task 9 (testing) must complete before Task 10 (DBFS) since both modify `testing/mocks.py` — Task 9 adds new mock methods that Task 10's tests depend on. Tasks 11-13 depend on earlier tasks. Tasks 14-16 are finalization.

---

### Task 1: Fix Existing Code Issues

**Files:**
- Modify: `src/databricks4py/io/delta.py:119-123` (bare exception)
- Modify: `src/databricks4py/migrations/validators.py:110-114` (bare exception)
- Modify: `src/databricks4py/io/dbfs.py:15-23` (rename function)
- Modify: `src/databricks4py/secrets.py` (no changes needed, just verify)
- Test: `tests/test_delta.py`, `tests/test_migrations.py`, `tests/test_dbfs.py`

- [ ] **Step 1: Fix bare exception in delta.py**

In `src/databricks4py/io/delta.py`, change `_ensure_table_exists`:

```python
# Before (line ~121):
except Exception:
# After:
from pyspark.sql.utils import AnalysisException
except AnalysisException:
```

Add the import at the top of the file with other pyspark imports.

- [ ] **Step 2: Fix bare exception in validators.py**

In `src/databricks4py/migrations/validators.py`, change `_table_exists`:

```python
# Before (line ~112):
except Exception:
# After:
from pyspark.sql.utils import AnalysisException
except AnalysisException:
```

Add the import at the top of the file.

- [ ] **Step 3: Run existing tests to verify no regressions**

Run: `pytest tests/test_delta.py tests/test_migrations.py -v --timeout=120`
Expected: All existing tests pass.

- [ ] **Step 4: Rename dbfs injection function**

In `src/databricks4py/io/dbfs.py`:
- Rename `inject_dbutils_module` to `_set_dbutils_module` (private)
- Keep the old name as a public alias for backward compat in `io/__init__.py`

```python
# In dbfs.py:
def _set_dbutils_module(dbutils_module) -> None:
    """Internal: set the dbutils module. Use top-level inject_dbutils() instead."""
    global _dbutils_module
    _dbutils_module = dbutils_module

# Backward compat alias
inject_dbutils_module = _set_dbutils_module
```

- [ ] **Step 5: Run dbfs tests**

Run: `pytest tests/test_dbfs.py -v --timeout=30`
Expected: All existing tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/databricks4py/io/delta.py src/databricks4py/migrations/validators.py src/databricks4py/io/dbfs.py
git commit -m "fix: use AnalysisException instead of bare Exception, prep dbutils unification"
```

---

### Task 2: Retry Module

**Files:**
- Create: `src/databricks4py/retry.py`
- Create: `tests/test_retry.py`

- [ ] **Step 1: Write failing tests for RetryConfig**

Create `tests/test_retry.py`:

```python
"""Tests for retry module."""

import pytest

from databricks4py.retry import RetryConfig, retry


pytestmark = pytest.mark.no_pyspark


class TestRetryConfig:
    def test_defaults(self):
        config = RetryConfig()
        assert config.max_attempts == 3
        assert config.base_delay_seconds == 1.0
        assert config.max_delay_seconds == 60.0
        assert config.backoff_factor == 2.0
        assert len(config.retryable_exceptions) > 0

    def test_custom_config(self):
        config = RetryConfig(max_attempts=5, base_delay_seconds=0.5)
        assert config.max_attempts == 5
        assert config.base_delay_seconds == 0.5


class TestRetryDecorator:
    def test_no_retry_on_success(self):
        call_count = 0

        @retry()
        def succeeds():
            nonlocal call_count
            call_count += 1
            return "ok"

        result = succeeds()
        assert result == "ok"
        assert call_count == 1

    def test_retries_on_transient_failure(self):
        call_count = 0

        @retry(RetryConfig(max_attempts=3, base_delay_seconds=0.01,
                           retryable_exceptions=(ConnectionError,)))
        def fails_twice():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("transient")
            return "recovered"

        result = fails_twice()
        assert result == "recovered"
        assert call_count == 3

    def test_raises_after_max_attempts(self):
        @retry(RetryConfig(max_attempts=2, base_delay_seconds=0.01,
                           retryable_exceptions=(ConnectionError,)))
        def always_fails():
            raise ConnectionError("permanent")

        with pytest.raises(ConnectionError, match="permanent"):
            always_fails()

    def test_no_retry_on_non_retryable_exception(self):
        call_count = 0

        @retry(RetryConfig(max_attempts=3, base_delay_seconds=0.01,
                           retryable_exceptions=(ConnectionError,)))
        def raises_value_error():
            nonlocal call_count
            call_count += 1
            raise ValueError("not retryable")

        with pytest.raises(ValueError, match="not retryable"):
            raises_value_error()
        assert call_count == 1

    def test_exponential_backoff_delays(self):
        """Verify delays increase exponentially (tested via timing)."""
        import time

        call_count = 0
        timestamps = []

        @retry(RetryConfig(max_attempts=3, base_delay_seconds=0.05,
                           backoff_factor=2.0, max_delay_seconds=10.0,
                           retryable_exceptions=(ConnectionError,)))
        def track_timing():
            nonlocal call_count
            call_count += 1
            timestamps.append(time.monotonic())
            if call_count < 3:
                raise ConnectionError("retry")
            return "done"

        track_timing()
        assert len(timestamps) == 3
        delay1 = timestamps[1] - timestamps[0]
        delay2 = timestamps[2] - timestamps[1]
        assert delay1 >= 0.04  # base_delay ~0.05s
        assert delay2 >= 0.08  # base_delay * backoff ~0.10s
        assert delay2 > delay1  # exponential growth

    def test_default_config_when_none(self):
        """retry() with no args uses default RetryConfig."""
        call_count = 0

        @retry()
        def succeeds():
            nonlocal call_count
            call_count += 1
            return "ok"

        assert succeeds() == "ok"
        assert call_count == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_retry.py -v --timeout=30`
Expected: FAIL — `ModuleNotFoundError: No module named 'databricks4py.retry'`

- [ ] **Step 3: Implement retry module**

Create `src/databricks4py/retry.py`:

```python
"""Retry decorator with exponential backoff for transient failures."""

from __future__ import annotations

import functools
import logging
import time
from dataclasses import dataclass, field

__all__ = ["RetryConfig", "retry"]

logger = logging.getLogger(__name__)


@dataclass
class RetryConfig:
    """Configuration for retry behavior.

    Args:
        max_attempts: Maximum number of attempts (including first try).
        base_delay_seconds: Initial delay before first retry.
        max_delay_seconds: Cap on delay between retries.
        backoff_factor: Multiplier applied to delay after each retry.
        retryable_exceptions: Exception types that trigger a retry.
    """

    max_attempts: int = 3
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 60.0
    backoff_factor: float = 2.0
    retryable_exceptions: tuple[type[Exception], ...] = (
        ConnectionError,
        TimeoutError,
        OSError,
    )

# Note: At runtime, extend defaults to include Py4JNetworkError if available:
# try:
#     from py4j.protocol import Py4JNetworkError
#     retryable_exceptions = retryable_exceptions + (Py4JNetworkError,)
# except ImportError:
#     pass
# This is done in __post_init__ to avoid hard dependency on py4j at import time.


def retry(config: RetryConfig | None = None):
    """Decorator that retries a function on transient failures.

    Args:
        config: Retry configuration. Uses defaults if None.
    """
    if config is None:
        config = RetryConfig()

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            delay = config.base_delay_seconds
            last_exception = None

            for attempt in range(1, config.max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except config.retryable_exceptions as exc:
                    last_exception = exc
                    if attempt == config.max_attempts:
                        logger.error(
                            "All %d attempts failed for %s: %s",
                            config.max_attempts,
                            func.__name__,
                            exc,
                        )
                        raise
                    logger.warning(
                        "Attempt %d/%d failed for %s: %s. Retrying in %.2fs",
                        attempt,
                        config.max_attempts,
                        func.__name__,
                        exc,
                        delay,
                    )
                    time.sleep(delay)
                    delay = min(delay * config.backoff_factor, config.max_delay_seconds)

            raise last_exception  # unreachable but satisfies type checker

        return wrapper

    return decorator
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_retry.py -v --timeout=30`
Expected: All 7 tests pass.

- [ ] **Step 5: Lint check**

Run: `ruff check src/databricks4py/retry.py tests/test_retry.py`
Expected: No errors.

- [ ] **Step 6: Commit**

```bash
git add src/databricks4py/retry.py tests/test_retry.py
git commit -m "feat: add retry decorator with exponential backoff"
```

---

### Task 3: Config Module

**Files:**
- Create: `src/databricks4py/config/__init__.py`
- Create: `src/databricks4py/config/base.py`
- Create: `src/databricks4py/config/unity.py`
- Create: `tests/test_config.py`

- [ ] **Step 1: Write failing tests for Environment and JobConfig**

Create `tests/test_config.py`:

```python
"""Tests for config module."""

import os

import pytest

from databricks4py.config import Environment, JobConfig, UnityConfig


pytestmark = pytest.mark.no_pyspark


class TestEnvironment:
    def test_values(self):
        assert Environment.DEV.value == "dev"
        assert Environment.STAGING.value == "staging"
        assert Environment.PROD.value == "prod"

    def test_all_environments_covered(self):
        assert len(Environment) == 3


class TestJobConfig:
    def test_table_resolution(self):
        config = JobConfig(tables={"events": "catalog.bronze.events"})
        assert config.table("events") == "catalog.bronze.events"

    def test_table_not_found_raises(self):
        config = JobConfig(tables={"events": "catalog.bronze.events"})
        with pytest.raises(KeyError, match="orders"):
            config.table("orders")

    def test_env_defaults_to_dev(self):
        config = JobConfig(tables={})
        assert config.env == Environment.DEV

    def test_env_from_env_var(self, monkeypatch):
        monkeypatch.setenv("ENV", "prod")
        config = JobConfig(tables={})
        assert config.env == Environment.PROD

    def test_env_from_environment_var(self, monkeypatch):
        monkeypatch.setenv("ENVIRONMENT", "staging")
        config = JobConfig(tables={})
        assert config.env == Environment.STAGING

    def test_env_var_precedence(self, monkeypatch):
        """ENV takes precedence over ENVIRONMENT."""
        monkeypatch.setenv("ENV", "prod")
        monkeypatch.setenv("ENVIRONMENT", "staging")
        config = JobConfig(tables={})
        assert config.env == Environment.PROD

    def test_spark_configs_default_empty(self):
        config = JobConfig(tables={})
        assert config.spark_configs == {}

    def test_spark_configs_set(self):
        configs = {"spark.sql.shuffle.partitions": "10"}
        config = JobConfig(tables={}, spark_configs=configs)
        assert config.spark_configs == configs

    def test_from_env(self, monkeypatch):
        monkeypatch.setenv("ENV", "staging")
        config = JobConfig.from_env(
            tables={"events": "cat.bronze.events"},
            secret_scope="my-scope",
        )
        assert config.env == Environment.STAGING
        assert config.secret_scope == "my-scope"

    def test_table_not_found_lists_available(self):
        config = JobConfig(tables={"events": "x", "users": "y"})
        with pytest.raises(KeyError, match="events"):
            config.table("orders")

    def test_invalid_env_defaults_to_dev(self, monkeypatch):
        monkeypatch.setenv("ENV", "invalid_env")
        config = JobConfig(tables={})
        assert config.env == Environment.DEV


class TestUnityConfig:
    def test_table_resolution_dev(self):
        config = UnityConfig(catalog_prefix="analytics", schemas=["bronze", "silver"])
        # Default env is DEV
        assert config.table("bronze.events") == "analytics_dev.bronze.events"

    def test_table_resolution_prod(self, monkeypatch):
        monkeypatch.setenv("ENV", "prod")
        config = UnityConfig(catalog_prefix="analytics", schemas=["bronze", "silver"])
        assert config.table("bronze.events") == "analytics_prod.bronze.events"

    def test_unknown_schema_raises(self):
        config = UnityConfig(catalog_prefix="analytics", schemas=["bronze"])
        with pytest.raises(KeyError, match="silver"):
            config.table("silver.events")

    def test_invalid_table_format_raises(self):
        config = UnityConfig(catalog_prefix="analytics", schemas=["bronze"])
        with pytest.raises(ValueError, match="schema.table"):
            config.table("events")  # missing schema prefix

    def test_secret_scope(self):
        config = UnityConfig(
            catalog_prefix="analytics",
            schemas=["bronze"],
            secret_scope="my-scope",
        )
        assert config.secret_scope == "my-scope"

    def test_storage_root(self):
        config = UnityConfig(
            catalog_prefix="analytics",
            schemas=["bronze"],
            storage_root="s3://bucket/path",
        )
        assert config.storage_root == "s3://bucket/path"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_config.py -v --timeout=30`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement config module**

Create `src/databricks4py/config/__init__.py`:

```python
"""Environment-aware configuration for Databricks jobs."""

from databricks4py.config.base import Environment, JobConfig
from databricks4py.config.unity import UnityConfig

__all__ = ["Environment", "JobConfig", "UnityConfig"]
```

Create `src/databricks4py/config/base.py`:

```python
"""Base configuration classes."""

from __future__ import annotations

import logging
import os
from enum import Enum

__all__ = ["Environment", "JobConfig"]

logger = logging.getLogger(__name__)


class Environment(Enum):
    DEV = "dev"
    STAGING = "staging"
    PROD = "prod"


class JobConfig:
    """Flexible job configuration with environment-aware table resolution.

    Environment is auto-resolved at runtime (never hardcoded):
    1. ENV environment variable
    2. ENVIRONMENT environment variable
    3. Defaults to DEV

    Args:
        tables: Mapping of logical names to fully-qualified table names.
        secret_scope: Databricks secret scope name.
        storage_root: Base storage path for this job.
        spark_configs: Spark configuration overrides to apply.
    """

    def __init__(
        self,
        tables: dict[str, str],
        *,
        secret_scope: str | None = None,
        storage_root: str | None = None,
        spark_configs: dict[str, str] | None = None,
    ) -> None:
        self.env = self._resolve_env()
        self.tables = tables
        self.secret_scope = secret_scope
        self.storage_root = storage_root
        self.spark_configs = spark_configs or {}

    def _resolve_env(self) -> Environment:
        """Resolution order:
        1. Databricks widget parameter 'env' (if available)
        2. ENV environment variable
        3. ENVIRONMENT environment variable
        4. Defaults to DEV
        """
        raw = None
        # Try Databricks widget parameter first
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            if spark:
                raw = spark.conf.get("spark.databricks.widget.env", None)
        except Exception:
            pass
        raw = raw or os.environ.get("ENV") or os.environ.get("ENVIRONMENT") or "dev"
        try:
            return Environment(raw.lower())
        except ValueError:
            logger.warning("Unknown environment '%s', defaulting to DEV", raw)
            return Environment.DEV

    def table(self, name: str) -> str:
        """Resolve a logical table name to its fully-qualified name.

        Raises:
            KeyError: If the table name is not in the configured tables mapping.
        """
        if name not in self.tables:
            available = ", ".join(sorted(self.tables.keys()))
            raise KeyError(
                f"Table '{name}' not found. Available tables: {available}"
            )
        return self.tables[name]

    def secret(self, key: str) -> str:
        """Fetch a secret from the configured scope.

        Raises:
            ValueError: If no secret_scope is configured.
        """
        if not self.secret_scope:
            raise ValueError("No secret_scope configured on this JobConfig")
        from databricks4py.secrets import SecretFetcher

        return SecretFetcher.fetch_secret(self.secret_scope, key)

    @classmethod
    def from_env(cls, **kwargs) -> JobConfig:
        """Build a JobConfig, inheriting environment from runtime context."""
        return cls(**kwargs)

    def __repr__(self) -> str:
        return f"JobConfig(env={self.env.value}, tables={list(self.tables.keys())})"
```

Create `src/databricks4py/config/unity.py`:

```python
"""Unity Catalog convention-based configuration."""

from __future__ import annotations

from databricks4py.config.base import JobConfig

__all__ = ["UnityConfig"]


class UnityConfig(JobConfig):
    """Convention-based config for Unity Catalog environments.

    Resolves table names using the pattern: {prefix}_{env}.{schema}.{table}

    The environment is auto-resolved at runtime — same code works
    across dev, staging, and prod without changes.

    Args:
        catalog_prefix: Prefix for catalog names (e.g., "analytics").
        schemas: List of valid schema names (e.g., ["bronze", "silver", "gold"]).
        secret_scope: Databricks secret scope name.
        storage_root: Base storage path.
        spark_configs: Spark configuration overrides.
    """

    def __init__(
        self,
        catalog_prefix: str,
        schemas: list[str],
        *,
        secret_scope: str | None = None,
        storage_root: str | None = None,
        spark_configs: dict[str, str] | None = None,
    ) -> None:
        super().__init__(
            tables={},
            secret_scope=secret_scope,
            storage_root=storage_root,
            spark_configs=spark_configs,
        )
        self._catalog_prefix = catalog_prefix
        self._schemas = set(schemas)
        self._catalog = f"{catalog_prefix}_{self.env.value}"

    def table(self, name: str) -> str:
        """Resolve schema.table to catalog_env.schema.table.

        Args:
            name: Table reference as "schema.table" (e.g., "bronze.events").

        Raises:
            ValueError: If name is not in "schema.table" format.
            KeyError: If schema is not in the configured schemas list.
        """
        parts = name.split(".", 1)
        if len(parts) != 2:
            raise ValueError(
                f"Table name must be in 'schema.table' format, got '{name}'"
            )
        schema, table = parts
        if schema not in self._schemas:
            available = ", ".join(sorted(self._schemas))
            raise KeyError(
                f"Schema '{schema}' not configured. Available: {available}"
            )
        return f"{self._catalog}.{schema}.{table}"

    def __repr__(self) -> str:
        return (
            f"UnityConfig(catalog={self._catalog}, "
            f"schemas={sorted(self._schemas)})"
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_config.py -v --timeout=30`
Expected: All tests pass.

- [ ] **Step 5: Lint check**

Run: `ruff check src/databricks4py/config/ tests/test_config.py`
Expected: No errors.

- [ ] **Step 6: Commit**

```bash
git add src/databricks4py/config/ tests/test_config.py
git commit -m "feat: add config module with JobConfig and UnityConfig"
```

---

### Task 4: Metrics Module

**Files:**
- Create: `src/databricks4py/metrics/__init__.py`
- Create: `src/databricks4py/metrics/base.py`
- Create: `src/databricks4py/metrics/logging_sink.py`
- Create: `src/databricks4py/metrics/delta_sink.py`
- Create: `tests/test_metrics.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_metrics.py`:

```python
"""Tests for metrics module."""

import json
import logging
from datetime import datetime

import pytest

from databricks4py.metrics import (
    CompositeMetricsSink,
    LoggingMetricsSink,
    MetricEvent,
    MetricsSink,
)


pytestmark = pytest.mark.no_pyspark


class TestMetricEvent:
    def test_creation(self):
        event = MetricEvent(
            job_name="my_job",
            event_type="batch_complete",
            timestamp=datetime(2026, 1, 1),
        )
        assert event.job_name == "my_job"
        assert event.event_type == "batch_complete"
        assert event.row_count is None
        assert event.metadata == {}

    def test_frozen(self):
        event = MetricEvent(
            job_name="my_job",
            event_type="test",
            timestamp=datetime(2026, 1, 1),
        )
        with pytest.raises(AttributeError):
            event.job_name = "other"

    def test_with_all_fields(self):
        event = MetricEvent(
            job_name="my_job",
            event_type="write_complete",
            timestamp=datetime(2026, 1, 1),
            duration_ms=150.5,
            row_count=1000,
            table_name="silver.events",
            batch_id=42,
            metadata={"custom_key": "value"},
        )
        assert event.duration_ms == 150.5
        assert event.row_count == 1000
        assert event.batch_id == 42
        assert event.metadata["custom_key"] == "value"


class TestMetricsSinkABC:
    def test_cannot_instantiate_abc(self):
        with pytest.raises(TypeError, match="abstract"):
            MetricsSink()

    def test_flush_is_optional(self):
        class TestSink(MetricsSink):
            def emit(self, event):
                pass

        sink = TestSink()
        sink.flush()  # should not raise


class TestLoggingMetricsSink:
    def test_emits_json_log(self, caplog):
        sink = LoggingMetricsSink()
        event = MetricEvent(
            job_name="test_job",
            event_type="test",
            timestamp=datetime(2026, 1, 1, 12, 0, 0),
        )
        with caplog.at_level(logging.INFO):
            sink.emit(event)

        assert len(caplog.records) == 1
        logged = json.loads(caplog.records[0].message)
        assert logged["job_name"] == "test_job"
        assert logged["event_type"] == "test"

    def test_handles_metadata(self, caplog):
        sink = LoggingMetricsSink()
        event = MetricEvent(
            job_name="test",
            event_type="custom",
            timestamp=datetime(2026, 1, 1),
            metadata={"key": "value"},
        )
        with caplog.at_level(logging.INFO):
            sink.emit(event)

        logged = json.loads(caplog.records[0].message)
        assert logged["metadata"]["key"] == "value"


class TestCompositeMetricsSink:
    def test_fans_out_to_all_sinks(self):
        events_a = []
        events_b = []

        class SinkA(MetricsSink):
            def emit(self, event):
                events_a.append(event)

        class SinkB(MetricsSink):
            def emit(self, event):
                events_b.append(event)

        composite = CompositeMetricsSink(SinkA(), SinkB())
        event = MetricEvent(
            job_name="test", event_type="test", timestamp=datetime(2026, 1, 1)
        )
        composite.emit(event)

        assert len(events_a) == 1
        assert len(events_b) == 1

    def test_flush_all_sinks(self):
        flushed = []

        class FlushableSink(MetricsSink):
            def __init__(self, name):
                self.name = name

            def emit(self, event):
                pass

            def flush(self):
                flushed.append(self.name)

        composite = CompositeMetricsSink(FlushableSink("a"), FlushableSink("b"))
        composite.flush()
        assert flushed == ["a", "b"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_metrics.py -v --timeout=30`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement metrics module**

Create `src/databricks4py/metrics/__init__.py`:

```python
"""Pluggable metrics collection for Databricks jobs."""

from databricks4py.metrics.base import (
    CompositeMetricsSink,
    MetricEvent,
    MetricsSink,
)
from databricks4py.metrics.delta_sink import DeltaMetricsSink
from databricks4py.metrics.logging_sink import LoggingMetricsSink

__all__ = [
    "CompositeMetricsSink",
    "DeltaMetricsSink",
    "LoggingMetricsSink",
    "MetricEvent",
    "MetricsSink",
]
```

Create `src/databricks4py/metrics/base.py`:

```python
"""Core metrics types and interfaces."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

__all__ = ["MetricEvent", "MetricsSink", "CompositeMetricsSink"]


@dataclass(frozen=True)
class MetricEvent:
    """A single metric measurement.

    Args:
        job_name: Name of the job emitting the metric.
        event_type: Category of event (e.g., "batch_complete", "write_complete").
        timestamp: When the event occurred.
        duration_ms: Operation duration in milliseconds.
        row_count: Number of rows processed.
        table_name: Table involved in the operation.
        batch_id: Streaming batch identifier.
        metadata: Arbitrary additional context.
    """

    job_name: str
    event_type: str
    timestamp: datetime
    duration_ms: float | None = None
    row_count: int | None = None
    table_name: str | None = None
    batch_id: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


class MetricsSink(ABC):
    """Interface for metrics destinations.

    Implement emit() to handle metric events. Override flush()
    if your sink buffers events.
    """

    @abstractmethod
    def emit(self, event: MetricEvent) -> None: ...

    def flush(self) -> None:
        pass


class CompositeMetricsSink(MetricsSink):
    """Fan-out sink that emits to multiple sinks simultaneously."""

    def __init__(self, *sinks: MetricsSink) -> None:
        self._sinks = sinks

    def emit(self, event: MetricEvent) -> None:
        for sink in self._sinks:
            sink.emit(event)

    def flush(self) -> None:
        for sink in self._sinks:
            sink.flush()
```

Create `src/databricks4py/metrics/logging_sink.py`:

```python
"""Structured logging metrics sink."""

from __future__ import annotations

import json
import logging
from dataclasses import asdict

from databricks4py.metrics.base import MetricEvent, MetricsSink

__all__ = ["LoggingMetricsSink"]

logger = logging.getLogger(__name__)


class LoggingMetricsSink(MetricsSink):
    """Emits metrics as JSON-structured log lines."""

    def emit(self, event: MetricEvent) -> None:
        logger.info(json.dumps(asdict(event), default=str))
```

Create `src/databricks4py/metrics/delta_sink.py`:

```python
"""Delta Lake metrics sink — writes metrics to a control table."""

from __future__ import annotations

import logging
from dataclasses import asdict
from typing import TYPE_CHECKING

from databricks4py.metrics.base import MetricEvent, MetricsSink
from databricks4py.spark_session import active_fallback

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

__all__ = ["DeltaMetricsSink"]

logger = logging.getLogger(__name__)

_BUFFER_THRESHOLD = 100


class DeltaMetricsSink(MetricsSink):
    """Appends metrics to a Delta control table.

    Buffers events and writes on flush() or when buffer exceeds threshold.

    Args:
        table_name: Fully-qualified Delta table name for metrics storage.
        spark: Optional SparkSession. Falls back to active session.
        buffer_size: Number of events to buffer before auto-flushing.
    """

    def __init__(
        self,
        table_name: str,
        *,
        spark: "SparkSession | None" = None,
        buffer_size: int = _BUFFER_THRESHOLD,
    ) -> None:
        self._table_name = table_name
        self._spark = spark
        self._buffer: list[MetricEvent] = []
        self._buffer_size = buffer_size

    def emit(self, event: MetricEvent) -> None:
        self._buffer.append(event)
        if len(self._buffer) >= self._buffer_size:
            self.flush()

    def flush(self) -> None:
        if not self._buffer:
            return
        spark = active_fallback(self._spark)
        rows = [asdict(e) for e in self._buffer]
        df = spark.createDataFrame(rows)
        df.write.mode("append").saveAsTable(self._table_name)
        logger.info(
            "Flushed %d metrics to %s", len(self._buffer), self._table_name
        )
        self._buffer.clear()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_metrics.py -v --timeout=30`
Expected: All tests pass.

- [ ] **Step 5: Lint check**

Run: `ruff check src/databricks4py/metrics/ tests/test_metrics.py`
Expected: No errors.

- [ ] **Step 6: Commit**

```bash
git add src/databricks4py/metrics/ tests/test_metrics.py
git commit -m "feat: add metrics module with pluggable sinks"
```

---

### Task 5: Quality Module

**Files:**
- Create: `src/databricks4py/quality/__init__.py`
- Create: `src/databricks4py/quality/base.py`
- Create: `src/databricks4py/quality/expectations.py`
- Create: `src/databricks4py/quality/gate.py`
- Create: `tests/test_quality.py`

- [ ] **Step 1: Write failing tests for expectations**

Create `tests/test_quality.py`:

```python
"""Tests for quality module."""

import pytest

from databricks4py.quality import (
    ColumnExists,
    Expectation,
    ExpectationResult,
    InRange,
    NotNull,
    QualityGate,
    QualityReport,
    RowCount,
    Unique,
    MatchesRegex,
)


pytestmark = pytest.mark.no_pyspark


class TestExpectationResult:
    def test_passed(self):
        result = ExpectationResult(
            expectation="NotNull(id)", passed=True, total_rows=100
        )
        assert result.passed is True
        assert result.failing_rows == 0

    def test_failed(self):
        result = ExpectationResult(
            expectation="NotNull(id)",
            passed=False,
            total_rows=100,
            failing_rows=5,
        )
        assert result.passed is False
        assert result.failing_rows == 5

    def test_frozen(self):
        result = ExpectationResult(
            expectation="test", passed=True, total_rows=10
        )
        with pytest.raises(AttributeError):
            result.passed = False


class TestQualityReport:
    def test_all_passed(self):
        results = [
            ExpectationResult("check1", True, 100),
            ExpectationResult("check2", True, 100),
        ]
        report = QualityReport(results=results, passed=True)
        assert report.passed is True

    def test_any_failed(self):
        results = [
            ExpectationResult("check1", True, 100),
            ExpectationResult("check2", False, 100, 5),
        ]
        report = QualityReport(results=results, passed=False)
        assert report.passed is False

    def test_summary_returns_string(self):
        results = [ExpectationResult("check1", True, 100)]
        report = QualityReport(results=results, passed=True)
        summary = report.summary()
        assert isinstance(summary, str)
        assert "check1" in summary


class TestExpectationABC:
    def test_cannot_instantiate(self):
        with pytest.raises(TypeError, match="abstract"):
            Expectation()


class TestExpectationRepr:
    def test_not_null_repr(self):
        assert repr(NotNull("id", "name")) == "NotNull(id, name)"

    def test_in_range_repr(self):
        assert repr(InRange("age", min_val=0, max_val=150)) == "InRange(age, 0..150)"

    def test_unique_repr(self):
        assert repr(Unique("id")) == "Unique(id)"

    def test_row_count_repr(self):
        assert repr(RowCount(min_count=1, max_count=1000)) == "RowCount(1..1000)"

    def test_matches_regex_repr(self):
        assert repr(MatchesRegex("email", r".*@.*")) == "MatchesRegex(email, .*@.*)"

    def test_column_exists_repr(self):
        assert repr(ColumnExists("id", "name")) == "ColumnExists(id, name)"


class TestQualityGateConstruction:
    def test_quarantine_requires_handler(self):
        with pytest.raises(ValueError, match="quarantine_handler"):
            QualityGate(NotNull("id"), on_fail="quarantine")

    def test_quarantine_with_handler(self):
        gate = QualityGate(
            NotNull("id"),
            on_fail="quarantine",
            quarantine_handler=lambda df: None,
        )
        assert gate._on_fail == "quarantine"

    def test_default_on_fail_is_raise(self):
        gate = QualityGate(NotNull("id"))
        assert gate._on_fail == "raise"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_quality.py -v --timeout=30`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement quality base module**

Create `src/databricks4py/quality/__init__.py`:

```python
"""Lightweight data quality checks for Spark DataFrames."""

from databricks4py.quality.base import (
    Expectation,
    ExpectationResult,
    QualityReport,
)
from databricks4py.quality.expectations import (
    ColumnExists,
    InRange,
    MatchesRegex,
    NotNull,
    RowCount,
    Unique,
)
from databricks4py.quality.gate import QualityGate

__all__ = [
    "ColumnExists",
    "Expectation",
    "ExpectationResult",
    "InRange",
    "MatchesRegex",
    "NotNull",
    "QualityGate",
    "QualityReport",
    "RowCount",
    "Unique",
]
```

Create `src/databricks4py/quality/base.py`:

```python
"""Core quality types and interfaces."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, Row

__all__ = ["Expectation", "ExpectationResult", "QualityReport"]


@dataclass(frozen=True)
class ExpectationResult:
    """Result of a single expectation check.

    Args:
        expectation: Human-readable description of the check.
        passed: Whether the check passed.
        total_rows: Total rows evaluated.
        failing_rows: Number of rows that failed the check.
        sample: Optional sample of failing rows for debugging.
    """

    expectation: str
    passed: bool
    total_rows: int
    failing_rows: int = 0
    sample: list[Row] | None = None


class Expectation(ABC):
    """Base class for data quality checks."""

    @abstractmethod
    def validate(self, df: DataFrame) -> ExpectationResult: ...

    def failing_condition(self):
        """Return a Column expression matching failing rows, or None if not row-level.

        Override in expectations that can identify failing rows individually
        (e.g., NotNull, InRange, MatchesRegex). Aggregate checks (RowCount,
        Unique) return None.
        """
        return None


@dataclass(frozen=True)
class QualityReport:
    """Aggregated results from multiple expectations.

    Args:
        results: Individual expectation results.
        passed: True if all expectations passed.
    """

    results: list[ExpectationResult]
    passed: bool

    def summary(self) -> str:
        lines = ["Expectation          | Passed | Total  | Failing"]
        lines.append("-" * 55)
        for r in self.results:
            status = "PASS" if r.passed else "FAIL"
            failing = str(r.failing_rows) if r.failing_rows else "-"
            lines.append(
                f"{r.expectation:<20} | {status:<6} | {r.total_rows:<6} | {failing}"
            )
        return "\n".join(lines)
```

- [ ] **Step 4: Implement expectations**

Create `src/databricks4py/quality/expectations.py`:

```python
"""Built-in expectation implementations."""

from __future__ import annotations

from typing import TYPE_CHECKING

from databricks4py.quality.base import Expectation, ExpectationResult

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

__all__ = [
    "ColumnExists",
    "InRange",
    "MatchesRegex",
    "NotNull",
    "RowCount",
    "Unique",
]


class NotNull(Expectation):
    """Check that specified columns contain no null values."""

    def __init__(self, *columns: str) -> None:
        if not columns:
            raise ValueError("At least one column required")
        self._columns = columns

    def validate(self, df: DataFrame) -> ExpectationResult:
        from pyspark.sql import functions as F

        total = df.count()
        condition = F.lit(False)
        for col in self._columns:
            condition = condition | F.col(col).isNull()
        failing = df.filter(condition).count()
        return ExpectationResult(
            expectation=repr(self),
            passed=failing == 0,
            total_rows=total,
            failing_rows=failing,
        )

    def failing_condition(self):
        from pyspark.sql import functions as F
        condition = F.lit(False)
        for col in self._columns:
            condition = condition | F.col(col).isNull()
        return condition

    def __repr__(self) -> str:
        return f"NotNull({', '.join(self._columns)})"


class InRange(Expectation):
    """Check that column values fall within bounds (inclusive)."""

    def __init__(
        self,
        column: str,
        *,
        min_val: float | int | None = None,
        max_val: float | int | None = None,
    ) -> None:
        if min_val is None and max_val is None:
            raise ValueError("At least one of min_val or max_val required")
        self._column = column
        self._min = min_val
        self._max = max_val

    def validate(self, df: DataFrame) -> ExpectationResult:
        from pyspark.sql import functions as F

        total = df.count()
        col = F.col(self._column)
        condition = F.lit(False)
        if self._min is not None:
            condition = condition | (col < self._min)
        if self._max is not None:
            condition = condition | (col > self._max)
        failing = df.filter(condition).count()
        return ExpectationResult(
            expectation=repr(self),
            passed=failing == 0,
            total_rows=total,
            failing_rows=failing,
        )

    def failing_condition(self):
        from pyspark.sql import functions as F
        col = F.col(self._column)
        condition = F.lit(False)
        if self._min is not None:
            condition = condition | (col < self._min)
        if self._max is not None:
            condition = condition | (col > self._max)
        return condition

    def __repr__(self) -> str:
        min_s = str(self._min) if self._min is not None else ""
        max_s = str(self._max) if self._max is not None else ""
        return f"InRange({self._column}, {min_s}..{max_s})"


class Unique(Expectation):
    """Check that column combinations have no duplicates."""

    def __init__(self, *columns: str) -> None:
        if not columns:
            raise ValueError("At least one column required")
        self._columns = columns

    def validate(self, df: DataFrame) -> ExpectationResult:
        total = df.count()
        distinct = df.select(*self._columns).distinct().count()
        failing = total - distinct
        return ExpectationResult(
            expectation=repr(self),
            passed=failing == 0,
            total_rows=total,
            failing_rows=failing,
        )

    def __repr__(self) -> str:
        return f"Unique({', '.join(self._columns)})"


class RowCount(Expectation):
    """Check that DataFrame row count is within bounds."""

    def __init__(
        self,
        *,
        min_count: int | None = None,
        max_count: int | None = None,
    ) -> None:
        if min_count is None and max_count is None:
            raise ValueError("At least one of min_count or max_count required")
        self._min = min_count
        self._max = max_count

    def validate(self, df: DataFrame) -> ExpectationResult:
        total = df.count()
        passed = True
        if self._min is not None and total < self._min:
            passed = False
        if self._max is not None and total > self._max:
            passed = False
        return ExpectationResult(
            expectation=repr(self),
            passed=passed,
            total_rows=total,
        )

    def __repr__(self) -> str:
        min_s = str(self._min) if self._min is not None else ""
        max_s = str(self._max) if self._max is not None else ""
        return f"RowCount({min_s}..{max_s})"


class MatchesRegex(Expectation):
    """Check that all values in a column match a regex pattern."""

    def __init__(self, column: str, pattern: str) -> None:
        self._column = column
        self._pattern = pattern

    def validate(self, df: DataFrame) -> ExpectationResult:
        from pyspark.sql import functions as F

        total = df.count()
        failing = df.filter(~F.col(self._column).rlike(self._pattern)).count()
        return ExpectationResult(
            expectation=repr(self),
            passed=failing == 0,
            total_rows=total,
            failing_rows=failing,
        )

    def failing_condition(self):
        from pyspark.sql import functions as F
        return ~F.col(self._column).rlike(self._pattern)

    def __repr__(self) -> str:
        return f"MatchesRegex({self._column}, {self._pattern})"


class ColumnExists(Expectation):
    """Check that specified columns exist in the DataFrame."""

    def __init__(self, *columns: str, dtype: str | None = None) -> None:
        if not columns:
            raise ValueError("At least one column required")
        self._columns = columns
        self._dtype = dtype

    def validate(self, df: DataFrame) -> ExpectationResult:
        existing = {f.name: f.dataType.simpleString() for f in df.schema.fields}
        missing = [c for c in self._columns if c not in existing]
        type_mismatches = []
        if self._dtype and not missing:
            type_mismatches = [
                c for c in self._columns if existing.get(c) != self._dtype
            ]
        passed = not missing and not type_mismatches
        total = df.count()
        return ExpectationResult(
            expectation=repr(self),
            passed=passed,
            total_rows=total,
            failing_rows=len(missing) + len(type_mismatches),
        )

    def __repr__(self) -> str:
        return f"ColumnExists({', '.join(self._columns)})"
```

- [ ] **Step 5: Implement QualityGate**

Create `src/databricks4py/quality/gate.py`:

```python
"""Quality gate — composable pre-write check."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Callable, Literal

from databricks4py.quality.base import Expectation, ExpectationResult, QualityReport

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

__all__ = ["QualityGate"]

logger = logging.getLogger(__name__)


class QualityGate:
    """Composable quality gate that runs expectations and acts on failures.

    Args:
        *expectations: Expectations to evaluate.
        on_fail: Action on failure — "raise", "warn", or "quarantine".
        quarantine_handler: Required when on_fail="quarantine". Called with
            a DataFrame of failing rows.

    Raises:
        ValueError: If on_fail="quarantine" but no quarantine_handler provided.
    """

    def __init__(
        self,
        *expectations: Expectation,
        on_fail: Literal["raise", "warn", "quarantine"] = "raise",
        quarantine_handler: Callable[[DataFrame], None] | None = None,
    ) -> None:
        if on_fail == "quarantine" and quarantine_handler is None:
            raise ValueError(
                "quarantine_handler is required when on_fail='quarantine'"
            )
        self._expectations = expectations
        self._on_fail = on_fail
        self._quarantine_handler = quarantine_handler

    def check(self, df: DataFrame) -> QualityReport:
        """Run all expectations and return a report."""
        results = [exp.validate(df) for exp in self._expectations]
        passed = all(r.passed for r in results)
        return QualityReport(results=results, passed=passed)

    def enforce(self, df: DataFrame) -> DataFrame:
        """Run checks and act on failures.

        Returns:
            The original DataFrame (raise/warn) or clean rows (quarantine).

        Raises:
            QualityError: If on_fail="raise" and any expectation fails.
        """
        report = self.check(df)

        if report.passed:
            return df

        if self._on_fail == "raise":
            raise QualityError(
                f"Quality gate failed:\n{report.summary()}", report=report
            )

        if self._on_fail == "warn":
            logger.warning("Quality gate warnings:\n%s", report.summary())
            return df

        if self._on_fail == "quarantine" and self._quarantine_handler:
            from pyspark.sql import functions as F

            # Build row-level filter from expectations that support it.
            # Each expectation provides a failing_condition() → Column | None.
            # NotNull, InRange, MatchesRegex can produce row-level conditions.
            # Unique, RowCount, ColumnExists cannot (they are aggregate checks).
            conditions = []
            for exp in self._expectations:
                cond = exp.failing_condition()
                if cond is not None:
                    conditions.append(cond)

            if conditions:
                combined = conditions[0]
                for c in conditions[1:]:
                    combined = combined | c
                bad = df.filter(combined)
                clean = df.filter(~combined)
                if bad.count() > 0:
                    self._quarantine_handler(bad)
                return clean

            # No row-level conditions available — pass all rows through
            return df

        return df


class QualityError(Exception):
    """Raised when a quality gate fails in raise mode."""

    def __init__(self, message: str, *, report: QualityReport) -> None:
        super().__init__(message)
        self.report = report
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `pytest tests/test_quality.py -v --timeout=30`
Expected: All tests pass.

- [ ] **Step 7: Lint check**

Run: `ruff check src/databricks4py/quality/ tests/test_quality.py`
Expected: No errors.

- [ ] **Step 8: Commit**

```bash
git add src/databricks4py/quality/ tests/test_quality.py
git commit -m "feat: add quality module with expectations and quality gate"
```

---

### Task 6: Merge Module

**Files:**
- Create: `src/databricks4py/io/merge.py`
- Create: `tests/test_merge.py`

- [ ] **Step 1: Write failing tests for MergeBuilder and MergeResult**

Create `tests/test_merge.py`:

```python
"""Tests for merge module."""

import pytest

from databricks4py.io.merge import MergeBuilder, MergeResult


pytestmark = pytest.mark.no_pyspark


class TestMergeResult:
    def test_creation(self):
        result = MergeResult(rows_inserted=10, rows_updated=5, rows_deleted=2)
        assert result.rows_inserted == 10
        assert result.rows_updated == 5
        assert result.rows_deleted == 2

    def test_frozen(self):
        result = MergeResult(rows_inserted=0, rows_updated=0, rows_deleted=0)
        with pytest.raises(AttributeError):
            result.rows_inserted = 1
```

- [ ] **Step 2: Write integration tests for merge operations**

Add to `tests/test_merge.py`:

```python
@pytest.mark.integration
class TestMergeBuilderIntegration:
    def test_basic_merge(self, spark_session):
        from databricks4py.io.delta import DeltaTable

        table = DeltaTable(
            "test_merge_target",
            schema={"id": "INT", "name": "STRING", "value": "INT"},
            spark=spark_session,
        )
        initial = spark_session.createDataFrame(
            [(1, "alice", 100), (2, "bob", 200)],
            ["id", "name", "value"],
        )
        table.write(initial, mode="overwrite")

        incoming = spark_session.createDataFrame(
            [(2, "robert", 250), (3, "charlie", 300)],
            ["id", "name", "value"],
        )

        result = (
            table.merge(incoming)
            .on("id")
            .when_matched_update(["name", "value"])
            .when_not_matched_insert()
            .execute()
        )

        assert result.rows_updated == 1
        assert result.rows_inserted == 1
        df = table.dataframe()
        assert df.count() == 3

    def test_upsert_shortcut(self, spark_session):
        from databricks4py.io.delta import DeltaTable

        table = DeltaTable(
            "test_upsert_target",
            schema={"id": "INT", "name": "STRING"},
            spark=spark_session,
        )
        initial = spark_session.createDataFrame(
            [(1, "alice"), (2, "bob")], ["id", "name"]
        )
        table.write(initial, mode="overwrite")

        incoming = spark_session.createDataFrame(
            [(2, "robert"), (3, "charlie")], ["id", "name"]
        )
        result = table.upsert(incoming, keys=["id"])
        assert result.rows_updated == 1
        assert result.rows_inserted == 1

    def test_merge_with_delete(self, spark_session):
        from databricks4py.io.delta import DeltaTable

        table = DeltaTable(
            "test_merge_delete",
            schema={"id": "INT", "name": "STRING"},
            spark=spark_session,
        )
        initial = spark_session.createDataFrame(
            [(1, "alice"), (2, "bob")], ["id", "name"]
        )
        table.write(initial, mode="overwrite")

        incoming = spark_session.createDataFrame(
            [(2, "robert")], ["id", "name"]
        )
        result = (
            table.merge(incoming)
            .on("id")
            .when_matched_update()
            .when_not_matched_by_source_delete()
            .execute()
        )
        assert result.rows_updated == 1
        assert result.rows_deleted == 1
        assert table.dataframe().count() == 1

    def test_upsert_specific_columns(self, spark_session):
        from databricks4py.io.delta import DeltaTable

        table = DeltaTable(
            "test_upsert_cols",
            schema={"id": "INT", "name": "STRING", "score": "INT"},
            spark=spark_session,
        )
        initial = spark_session.createDataFrame(
            [(1, "alice", 100)], ["id", "name", "score"]
        )
        table.write(initial, mode="overwrite")

        incoming = spark_session.createDataFrame(
            [(1, "alice_updated", 200)], ["id", "name", "score"]
        )
        result = table.upsert(incoming, keys=["id"], update_columns=["score"])
        row = table.dataframe().collect()[0]
        assert row["name"] == "alice"  # not updated
        assert row["score"] == 200  # updated
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `pytest tests/test_merge.py -v --timeout=120`
Expected: FAIL

- [ ] **Step 4: Implement MergeResult and MergeBuilder**

Create `src/databricks4py/io/merge.py`:

```python
"""Fluent MERGE INTO builder for Delta tables."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from databricks4py.metrics.base import MetricsSink

__all__ = ["MergeBuilder", "MergeResult"]

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MergeResult:
    """Result stats from a merge operation."""

    rows_inserted: int
    rows_updated: int
    rows_deleted: int


class MergeBuilder:
    """Fluent builder for Delta MERGE INTO operations.

    Usage::

        result = (table.merge(source_df)
            .on("id")
            .when_matched_update(["name", "value"])
            .when_not_matched_insert()
            .execute())
    """

    def __init__(
        self,
        target_table_name: str,
        source: DataFrame,
        spark,
        *,
        metrics_sink: MetricsSink | None = None,
    ) -> None:
        self._target_table_name = target_table_name
        self._source = source
        self._spark = spark
        self._metrics_sink = metrics_sink
        self._match_keys: list[str] = []
        self._match_condition: str | None = None
        self._matched_update_cols: list[str] | None = None
        self._has_matched_update = False
        self._matched_delete_condition: str | None = None
        self._has_matched_delete = False
        self._not_matched_insert_cols: list[str] | None = None
        self._has_not_matched_insert = False
        self._not_matched_by_source_delete = False
        self._not_matched_by_source_condition: str | None = None

    def on(self, *keys: str) -> MergeBuilder:
        self._match_keys = list(keys)
        return self

    def on_condition(self, condition: str) -> MergeBuilder:
        self._match_condition = condition
        return self

    def when_matched_update(
        self, columns: list[str] | None = None
    ) -> MergeBuilder:
        self._has_matched_update = True
        self._matched_update_cols = columns
        return self

    def when_matched_delete(
        self, condition: str | None = None
    ) -> MergeBuilder:
        self._has_matched_delete = True
        self._matched_delete_condition = condition
        return self

    def when_not_matched_insert(
        self, columns: list[str] | None = None
    ) -> MergeBuilder:
        self._has_not_matched_insert = True
        self._not_matched_insert_cols = columns
        return self

    def when_not_matched_by_source_delete(
        self, condition: str | None = None
    ) -> MergeBuilder:
        self._not_matched_by_source_delete = True
        self._not_matched_by_source_condition = condition
        return self

    def execute(self) -> MergeResult:
        from delta.tables import DeltaTable as _DeltaTable

        target = _DeltaTable.forName(self._spark, self._target_table_name)

        if self._match_condition:
            condition = self._match_condition
        elif self._match_keys:
            condition = " AND ".join(
                f"target.{k} = source.{k}" for k in self._match_keys
            )
        else:
            raise ValueError("Must call on() or on_condition() before execute()")

        merger = target.alias("target").merge(
            self._source.alias("source"), condition
        )

        if self._has_matched_update:
            if self._matched_update_cols:
                update_map = {
                    c: f"source.{c}" for c in self._matched_update_cols
                }
                merger = merger.whenMatchedUpdate(set=update_map)
            else:
                merger = merger.whenMatchedUpdateAll()

        if self._has_matched_delete:
            if self._matched_delete_condition:
                merger = merger.whenMatchedDelete(
                    condition=self._matched_delete_condition
                )
            else:
                merger = merger.whenMatchedDelete()

        if self._has_not_matched_insert:
            if self._not_matched_insert_cols:
                insert_map = {
                    c: f"source.{c}" for c in self._not_matched_insert_cols
                }
                merger = merger.whenNotMatchedInsert(values=insert_map)
            else:
                merger = merger.whenNotMatchedInsertAll()

        if self._not_matched_by_source_delete:
            if self._not_matched_by_source_condition:
                merger = merger.whenNotMatchedBySourceDelete(
                    condition=self._not_matched_by_source_condition
                )
            else:
                merger = merger.whenNotMatchedBySourceDelete()

        merger.execute()

        # Read merge metrics from the operation history
        result = self._extract_merge_metrics()

        if self._metrics_sink:
            from datetime import datetime

            from databricks4py.metrics.base import MetricEvent

            self._metrics_sink.emit(
                MetricEvent(
                    job_name="merge",
                    event_type="merge_complete",
                    timestamp=datetime.now(),
                    table_name=self._target_table_name,
                    metadata={
                        "rows_inserted": result.rows_inserted,
                        "rows_updated": result.rows_updated,
                        "rows_deleted": result.rows_deleted,
                    },
                )
            )

        return result

    def _extract_merge_metrics(self) -> MergeResult:
        history = (
            self._spark.sql(
                f"DESCRIBE HISTORY {self._target_table_name} LIMIT 1"
            )
            .collect()[0]
        )
        metrics = history["operationMetrics"]
        return MergeResult(
            rows_inserted=int(metrics.get("numTargetRowsInserted", 0)),
            rows_updated=int(metrics.get("numTargetRowsUpdated", 0)),
            rows_deleted=int(metrics.get("numTargetRowsDeleted", 0)),
        )
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/test_merge.py -v --timeout=120`
Expected: All tests pass.

- [ ] **Step 6: Lint check**

Run: `ruff check src/databricks4py/io/merge.py tests/test_merge.py`
Expected: No errors.

- [ ] **Step 7: Commit**

```bash
git add src/databricks4py/io/merge.py tests/test_merge.py
git commit -m "feat: add MergeBuilder with fluent MERGE INTO API"
```

---

### Task 7: Schema Diff Module

**Files:**
- Create: `src/databricks4py/migrations/schema_diff.py`
- Create: `tests/test_schema_diff.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_schema_diff.py`:

```python
"""Tests for schema diff module."""

import pytest

from databricks4py.migrations.schema_diff import (
    ColumnChange,
    SchemaDiff,
    SchemaEvolutionError,
)


pytestmark = pytest.mark.no_pyspark


class TestColumnChange:
    def test_creation(self):
        change = ColumnChange(
            column="email", change_type="added", new_value="string"
        )
        assert change.column == "email"
        assert change.severity == "info"

    def test_frozen(self):
        change = ColumnChange(column="x", change_type="added")
        with pytest.raises(AttributeError):
            change.column = "y"


class TestSchemaEvolutionError:
    def test_message(self):
        err = SchemaEvolutionError("breaking changes detected")
        assert "breaking changes" in str(err)


@pytest.mark.integration
class TestSchemaDiffIntegration:
    def test_no_changes(self, spark_session):
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType

        schema = StructType([
            StructField("id", IntegerType()),
            StructField("name", StringType()),
        ])
        diff = SchemaDiff(schema, schema)
        assert diff.changes() == []
        assert not diff.has_breaking_changes()

    def test_column_added(self, spark_session):
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType

        current = StructType([StructField("id", IntegerType())])
        incoming = StructType([
            StructField("id", IntegerType()),
            StructField("name", StringType()),
        ])
        diff = SchemaDiff(current, incoming)
        changes = diff.changes()
        assert len(changes) == 1
        assert changes[0].change_type == "added"
        assert changes[0].column == "name"
        assert changes[0].severity == "info"
        assert not diff.has_breaking_changes()

    def test_column_removed(self, spark_session):
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType

        current = StructType([
            StructField("id", IntegerType()),
            StructField("name", StringType()),
        ])
        incoming = StructType([StructField("id", IntegerType())])
        diff = SchemaDiff(current, incoming)
        changes = diff.changes()
        assert len(changes) == 1
        assert changes[0].change_type == "removed"
        assert changes[0].severity == "breaking"
        assert diff.has_breaking_changes()

    def test_type_changed(self, spark_session):
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType

        current = StructType([StructField("age", StringType())])
        incoming = StructType([StructField("age", IntegerType())])
        diff = SchemaDiff(current, incoming)
        changes = diff.changes()
        assert len(changes) == 1
        assert changes[0].change_type == "type_changed"
        assert changes[0].severity == "breaking"

    def test_nullable_changed(self, spark_session):
        from pyspark.sql.types import IntegerType, StructField, StructType

        current = StructType([StructField("id", IntegerType(), nullable=False)])
        incoming = StructType([StructField("id", IntegerType(), nullable=True)])
        diff = SchemaDiff(current, incoming)
        changes = diff.changes()
        assert len(changes) == 1
        assert changes[0].change_type == "nullable_changed"
        assert changes[0].severity == "warning"

    def test_summary_returns_string(self, spark_session):
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType

        current = StructType([StructField("id", IntegerType())])
        incoming = StructType([
            StructField("id", IntegerType()),
            StructField("name", StringType()),
        ])
        diff = SchemaDiff(current, incoming)
        summary = diff.summary()
        assert "name" in summary
        assert "added" in summary

    def test_from_tables(self, spark_session):
        from databricks4py.io.delta import DeltaTable

        table = DeltaTable(
            "test_schema_diff_table",
            schema={"id": "INT", "name": "STRING"},
            spark=spark_session,
        )
        initial = spark_session.createDataFrame([(1, "alice")], ["id", "name"])
        table.write(initial, mode="overwrite")

        incoming = spark_session.createDataFrame(
            [(1, "alice", 25)], ["id", "name", "age"]
        )
        diff = SchemaDiff.from_tables(
            "test_schema_diff_table", incoming, spark=spark_session
        )
        changes = diff.changes()
        assert any(c.column == "age" and c.change_type == "added" for c in changes)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_schema_diff.py -v --timeout=120`
Expected: FAIL

- [ ] **Step 3: Implement schema_diff module**

Create `src/databricks4py/migrations/schema_diff.py`:

```python
"""Schema comparison and evolution detection."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from databricks4py.spark_session import active_fallback

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType

__all__ = ["ColumnChange", "SchemaDiff", "SchemaEvolutionError"]


class SchemaEvolutionError(Exception):
    """Raised when a write would cause breaking schema changes."""


@dataclass(frozen=True)
class ColumnChange:
    """A single schema change between two StructTypes.

    Attributes:
        column: Column name affected.
        change_type: What changed.
        old_value: Previous value (type, nullable state).
        new_value: New value.
        severity: Impact level — "info", "warning", or "breaking".
    """

    column: str
    change_type: Literal["added", "removed", "type_changed", "nullable_changed"]
    old_value: str | None = None
    new_value: str | None = None
    severity: Literal["info", "warning", "breaking"] = "info"


class SchemaDiff:
    """Compare two Spark schemas and report changes with severity.

    Severity rules:
    - info: column added (safe with mergeSchema)
    - warning: nullable changed (NOT NULL → NULLABLE)
    - breaking: column removed, type changed
    """

    def __init__(self, current: StructType, incoming: StructType) -> None:
        self._current = current
        self._incoming = incoming
        self._changes: list[ColumnChange] | None = None

    @classmethod
    def from_tables(
        cls,
        table_name: str,
        incoming_df: DataFrame,
        *,
        spark: SparkSession | None = None,
    ) -> SchemaDiff:
        """Compare an existing table's schema against an incoming DataFrame.

        Uses DESCRIBE TABLE to read schema metadata (avoids full table scan).
        """
        sp = active_fallback(spark)
        current_schema = sp.read.table(table_name).schema
        return cls(current_schema, incoming_df.schema)

    def changes(self) -> list[ColumnChange]:
        if self._changes is not None:
            return self._changes

        self._changes = []
        current_fields = {f.name: f for f in self._current.fields}
        incoming_fields = {f.name: f for f in self._incoming.fields}

        for name, field in incoming_fields.items():
            if name not in current_fields:
                self._changes.append(
                    ColumnChange(
                        column=name,
                        change_type="added",
                        new_value=field.dataType.simpleString(),
                        severity="info",
                    )
                )
            else:
                cur = current_fields[name]
                if cur.dataType != field.dataType:
                    self._changes.append(
                        ColumnChange(
                            column=name,
                            change_type="type_changed",
                            old_value=cur.dataType.simpleString(),
                            new_value=field.dataType.simpleString(),
                            severity="breaking",
                        )
                    )
                if cur.nullable != field.nullable:
                    self._changes.append(
                        ColumnChange(
                            column=name,
                            change_type="nullable_changed",
                            old_value="NULLABLE" if cur.nullable else "NOT NULL",
                            new_value="NULLABLE" if field.nullable else "NOT NULL",
                            severity="warning",
                        )
                    )

        for name in current_fields:
            if name not in incoming_fields:
                self._changes.append(
                    ColumnChange(
                        column=name,
                        change_type="removed",
                        old_value=current_fields[name].dataType.simpleString(),
                        severity="breaking",
                    )
                )

        return self._changes

    def has_breaking_changes(self) -> bool:
        return any(c.severity == "breaking" for c in self.changes())

    def summary(self) -> str:
        lines = ["Column          | Change          | Old       | New       | Severity"]
        lines.append("-" * 72)
        for c in self.changes():
            lines.append(
                f"{c.column:<15} | {c.change_type:<15} | "
                f"{(c.old_value or '-'):<9} | {(c.new_value or '-'):<9} | {c.severity}"
            )
        return "\n".join(lines)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_schema_diff.py -v --timeout=120`
Expected: All tests pass.

- [ ] **Step 5: Lint check**

Run: `ruff check src/databricks4py/migrations/schema_diff.py tests/test_schema_diff.py`
Expected: No errors.

- [ ] **Step 6: Commit**

```bash
git add src/databricks4py/migrations/schema_diff.py tests/test_schema_diff.py
git commit -m "feat: add SchemaDiff for schema evolution detection"
```

---

### Task 8: Checkpoint Module

**Files:**
- Create: `src/databricks4py/io/checkpoint.py`
- Create: `tests/test_checkpoint.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_checkpoint.py`:

```python
"""Tests for checkpoint module."""

import os

import pytest

from databricks4py.io.checkpoint import CheckpointInfo, CheckpointManager


pytestmark = pytest.mark.no_pyspark


class TestCheckpointManager:
    def test_path_for(self, tmp_path):
        mgr = CheckpointManager(base_path=str(tmp_path))
        path = mgr.path_for("catalog.bronze.events", "catalog.silver.events")
        assert path == str(tmp_path / "catalog_bronze_events__catalog_silver_events")

    def test_path_sanitization(self, tmp_path):
        mgr = CheckpointManager(base_path=str(tmp_path))
        path = mgr.path_for("a.b.c", "x.y.z")
        assert "a_b_c__x_y_z" in path

    def test_exists_false(self, tmp_path):
        mgr = CheckpointManager(base_path=str(tmp_path))
        assert not mgr.exists(str(tmp_path / "nonexistent"))

    def test_exists_true(self, tmp_path):
        cp_dir = tmp_path / "checkpoint"
        cp_dir.mkdir()
        mgr = CheckpointManager(base_path=str(tmp_path))
        assert mgr.exists(str(cp_dir))

    def test_reset_deletes_directory(self, tmp_path):
        cp_dir = tmp_path / "checkpoint"
        cp_dir.mkdir()
        (cp_dir / "offsets").mkdir()
        (cp_dir / "offsets" / "0").write_text("{}")
        mgr = CheckpointManager(base_path=str(tmp_path))
        mgr.reset(str(cp_dir))
        assert not cp_dir.exists()

    def test_reset_nonexistent_is_noop(self, tmp_path):
        mgr = CheckpointManager(base_path=str(tmp_path))
        mgr.reset(str(tmp_path / "nonexistent"))  # should not raise


class TestCheckpointInfo:
    def test_creation(self):
        info = CheckpointInfo(
            path="/tmp/cp",
            last_batch_id=42,
            offsets={"source": "100"},
            size_bytes=1024,
        )
        assert info.last_batch_id == 42
        assert info.size_bytes == 1024

    def test_frozen(self):
        info = CheckpointInfo(path="/tmp", last_batch_id=0, offsets=None, size_bytes=0)
        with pytest.raises(AttributeError):
            info.path = "/other"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_checkpoint.py -v --timeout=30`
Expected: FAIL

- [ ] **Step 3: Implement checkpoint module**

Create `src/databricks4py/io/checkpoint.py`:

```python
"""Checkpoint lifecycle management for structured streaming."""

from __future__ import annotations

import json
import logging
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

__all__ = ["CheckpointInfo", "CheckpointManager"]

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CheckpointInfo:
    """Metadata about a streaming checkpoint.

    Attributes:
        path: Checkpoint directory path.
        last_batch_id: Last committed batch ID, or None if empty.
        offsets: Offset data from the last committed batch.
        size_bytes: Total size of the checkpoint directory.
    """

    path: str
    last_batch_id: int | None
    offsets: dict | None
    size_bytes: int


def _sanitize(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", name)


class CheckpointManager:
    """Manages streaming checkpoint directories.

    Provides deterministic path generation, existence checks,
    and reset capabilities for checkpoint lifecycle management.

    Args:
        base_path: Root directory for all checkpoint subdirectories.
        spark: Optional SparkSession (for HDFS/DBFS operations in future).
    """

    def __init__(
        self, base_path: str, *, spark: "SparkSession | None" = None
    ) -> None:
        self._base_path = base_path
        self._spark = spark

    def path_for(self, source: str, sink: str) -> str:
        """Generate a deterministic checkpoint path from source and sink names.

        Pattern: {base_path}/{source_sanitized}__{sink_sanitized}
        """
        return str(Path(self._base_path) / f"{_sanitize(source)}__{_sanitize(sink)}")

    def exists(self, path: str) -> bool:
        return Path(path).exists()

    def reset(self, path: str) -> None:
        """Delete a checkpoint directory for full reprocessing."""
        p = Path(path)
        if p.exists():
            shutil.rmtree(p)
            logger.info("Reset checkpoint at %s", path)

    def info(self, path: str) -> CheckpointInfo:
        """Read checkpoint metadata from the offset log."""
        p = Path(path)
        if not p.exists():
            return CheckpointInfo(
                path=path, last_batch_id=None, offsets=None, size_bytes=0
            )

        size = sum(f.stat().st_size for f in p.rglob("*") if f.is_file())

        offsets_dir = p / "offsets"
        last_batch_id = None
        offsets = None
        if offsets_dir.exists():
            batch_files = sorted(
                (f for f in offsets_dir.iterdir() if f.is_file()),
                key=lambda f: int(f.name) if f.name.isdigit() else -1,
            )
            if batch_files:
                last = batch_files[-1]
                last_batch_id = int(last.name) if last.name.isdigit() else None
                try:
                    content = last.read_text().strip()
                    lines = content.split("\n")
                    if len(lines) > 1:
                        offsets = json.loads(lines[-1])
                    else:
                        offsets = json.loads(content)
                except (json.JSONDecodeError, IndexError):
                    offsets = None

        return CheckpointInfo(
            path=path,
            last_batch_id=last_batch_id,
            offsets=offsets,
            size_bytes=size,
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_checkpoint.py -v --timeout=30`
Expected: All tests pass.

- [ ] **Step 5: Lint check**

Run: `ruff check src/databricks4py/io/checkpoint.py tests/test_checkpoint.py`
Expected: No errors.

- [ ] **Step 6: Commit**

```bash
git add src/databricks4py/io/checkpoint.py tests/test_checkpoint.py
git commit -m "feat: add CheckpointManager for streaming checkpoint lifecycle"
```

---

### Task 9: Testing Enhancements

**Files:**
- Create: `src/databricks4py/testing/builders.py`
- Create: `src/databricks4py/testing/temp_table.py`
- Create: `src/databricks4py/testing/assertions.py`
- Modify: `src/databricks4py/testing/fixtures.py`
- Modify: `src/databricks4py/testing/__init__.py`
- Create: `tests/test_builders.py`
- Create: `tests/test_temp_table.py`
- Create: `tests/test_assertions.py`

- [ ] **Step 1: Write failing tests for DataFrameBuilder**

Create `tests/test_builders.py`:

```python
"""Tests for DataFrameBuilder."""

import pytest

from databricks4py.testing.builders import DataFrameBuilder


@pytest.mark.integration
class TestDataFrameBuilder:
    def test_basic_build(self, spark_session):
        df = (
            DataFrameBuilder(spark_session)
            .with_columns({"id": "int", "name": "string"})
            .with_rows((1, "alice"), (2, "bob"))
            .build()
        )
        assert df.count() == 2
        assert df.columns == ["id", "name"]

    def test_with_sequential(self, spark_session):
        df = (
            DataFrameBuilder(spark_session)
            .with_columns({"id": "int"})
            .with_sequential("id", start=1, count=5)
            .build()
        )
        assert df.count() == 5
        rows = [r["id"] for r in df.orderBy("id").collect()]
        assert rows == [1, 2, 3, 4, 5]

    def test_with_schema_struct_type(self, spark_session):
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType

        schema = StructType([
            StructField("id", IntegerType()),
            StructField("name", StringType()),
        ])
        df = (
            DataFrameBuilder(spark_session)
            .with_schema(schema)
            .with_rows((1, "alice"))
            .build()
        )
        assert df.schema == schema
        assert df.count() == 1

    def test_with_nulls(self, spark_session):
        df = (
            DataFrameBuilder(spark_session)
            .with_columns({"id": "int", "name": "string"})
            .with_sequential("id", count=100)
            .with_nulls("name", frequency=1.0)
            .build()
        )
        null_count = df.filter(df.name.isNull()).count()
        assert null_count == 100

    def test_empty_build_raises(self, spark_session):
        with pytest.raises(ValueError, match="schema"):
            DataFrameBuilder(spark_session).build()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_builders.py -v --timeout=120`
Expected: FAIL

- [ ] **Step 3: Implement DataFrameBuilder**

Create `src/databricks4py/testing/builders.py`:

```python
"""Fluent test data construction for PySpark DataFrames."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType

__all__ = ["DataFrameBuilder"]

_SPARK_TYPE_MAP = {
    "int": "IntegerType",
    "integer": "IntegerType",
    "long": "LongType",
    "bigint": "LongType",
    "float": "FloatType",
    "double": "DoubleType",
    "string": "StringType",
    "boolean": "BooleanType",
    "date": "DateType",
    "timestamp": "TimestampType",
    "binary": "BinaryType",
    "short": "ShortType",
    "byte": "ByteType",
    "decimal": "DecimalType",
}


class DataFrameBuilder:
    """Fluent builder for constructing test DataFrames.

    Example::

        df = (DataFrameBuilder(spark)
            .with_columns({"id": "int", "name": "string"})
            .with_rows((1, "alice"), (2, "bob"))
            .build())
    """

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark
        self._schema: StructType | None = None
        self._column_defs: dict[str, str] | None = None
        self._rows: list[tuple] = []
        self._sequential: dict[str, tuple[int, int]] = {}
        self._null_cols: dict[str, float] = {}

    def with_columns(self, schema: dict[str, str]) -> DataFrameBuilder:
        """Define columns using Spark SQL type strings.

        Args:
            schema: Mapping of column name to type string
                (e.g., {"id": "int", "name": "string"}).
        """
        self._column_defs = schema
        return self

    def with_schema(self, schema: StructType) -> DataFrameBuilder:
        """Define columns using a PySpark StructType directly."""
        self._schema = schema
        return self

    def with_rows(self, *rows: tuple) -> DataFrameBuilder:
        """Add explicit data rows."""
        self._rows.extend(rows)
        return self

    def with_sequential(
        self, column: str, start: int = 1, count: int = 10
    ) -> DataFrameBuilder:
        """Generate sequential integer values for a column."""
        self._sequential[column] = (start, count)
        return self

    def with_nulls(self, column: str, frequency: float = 0.1) -> DataFrameBuilder:
        """Inject nulls into a column at the given frequency (0.0 to 1.0)."""
        self._null_cols[column] = frequency
        return self

    def build(self) -> DataFrame:
        """Construct the DataFrame."""
        schema = self._resolve_schema()
        if schema is None:
            raise ValueError(
                "No schema defined. Call with_columns() or with_schema() first."
            )

        rows = list(self._rows)

        # Generate sequential data
        if self._sequential:
            col_names = [f.name for f in schema.fields]
            for col, (start, count) in self._sequential.items():
                if not rows:
                    # Generate rows with sequential values
                    for i in range(count):
                        row = tuple(
                            start + i if c == col else None for c in col_names
                        )
                        rows.append(row)
                else:
                    # Fill sequential values into existing rows
                    col_idx = col_names.index(col)
                    new_rows = []
                    for i, row in enumerate(rows):
                        row_list = list(row)
                        row_list[col_idx] = start + i
                        new_rows.append(tuple(row_list))
                    rows = new_rows

        df = self._spark.createDataFrame(rows, schema=schema)

        # Apply null injection
        if self._null_cols:
            from pyspark.sql import functions as F

            for col, freq in self._null_cols.items():
                df = df.withColumn(
                    col,
                    F.when(F.rand() < freq, F.lit(None)).otherwise(F.col(col)),
                )

        return df

    def _resolve_schema(self) -> StructType | None:
        if self._schema is not None:
            return self._schema
        if self._column_defs is not None:
            from pyspark.sql import types as T

            fields = []
            for name, type_str in self._column_defs.items():
                type_class_name = _SPARK_TYPE_MAP.get(type_str.lower())
                if type_class_name is None:
                    # Try as-is (e.g., "array<string>")
                    spark_type = T._parse_datatype_string(type_str)
                else:
                    spark_type = getattr(T, type_class_name)()
                fields.append(T.StructField(name, spark_type, nullable=True))
            return T.StructType(fields)
        return None
```

- [ ] **Step 4: Run builder tests**

Run: `pytest tests/test_builders.py -v --timeout=120`
Expected: All tests pass.

- [ ] **Step 5: Write failing tests for TempDeltaTable**

Create `tests/test_temp_table.py`:

```python
"""Tests for TempDeltaTable."""

import pytest

from databricks4py.testing.temp_table import TempDeltaTable


@pytest.mark.integration
class TestTempDeltaTable:
    def test_creates_and_drops(self, spark_session):
        with TempDeltaTable(
            spark_session,
            schema={"id": "INT", "name": "STRING"},
            data=[(1, "alice"), (2, "bob")],
        ) as table:
            df = table.dataframe()
            assert df.count() == 2

        # Table should be dropped after context exit
        from pyspark.sql.utils import AnalysisException

        with pytest.raises(AnalysisException):
            spark_session.read.table(table.table_name)

    def test_auto_generates_table_name(self, spark_session):
        with TempDeltaTable(
            spark_session,
            schema={"id": "INT"},
            data=[(1,)],
        ) as table:
            assert table.table_name.startswith("tmp_")

    def test_custom_table_name(self, spark_session):
        with TempDeltaTable(
            spark_session,
            table_name="my_test_table",
            schema={"id": "INT"},
            data=[(1,)],
        ) as table:
            assert table.table_name == "my_test_table"

    def test_returns_delta_table(self, spark_session):
        from databricks4py.io.delta import DeltaTable

        with TempDeltaTable(
            spark_session,
            schema={"id": "INT"},
            data=[(1,)],
        ) as table:
            assert isinstance(table, DeltaTable)
```

- [ ] **Step 6: Implement TempDeltaTable**

Create `src/databricks4py/testing/temp_table.py`:

```python
"""Temporary Delta table for test isolation."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from databricks4py.io.delta import DeltaTable

__all__ = ["TempDeltaTable"]


class TempDeltaTable:
    """Context manager that creates a temporary Delta table and drops it on exit.

    Example::

        with TempDeltaTable(spark, schema={"id": "INT"}, data=[(1,)]) as table:
            df = table.dataframe()
            assert df.count() == 1
        # table is dropped here
    """

    def __init__(
        self,
        spark: SparkSession,
        *,
        table_name: str | None = None,
        schema: dict[str, str] | None = None,
        data: list[tuple] | None = None,
    ) -> None:
        self._spark = spark
        self._table_name = table_name or f"tmp_{uuid.uuid4().hex[:8]}"
        self._schema = schema
        self._data = data
        self._table: DeltaTable | None = None

    def __enter__(self) -> DeltaTable:
        from databricks4py.io.delta import DeltaTable

        self._table = DeltaTable(
            self._table_name, schema=self._schema, spark=self._spark
        )
        if self._data and self._schema:
            df = self._spark.createDataFrame(
                self._data, list(self._schema.keys())
            )
            self._table.write(df, mode="overwrite")
        return self._table

    def __exit__(self, *exc) -> None:
        try:
            self._spark.sql(f"DROP TABLE IF EXISTS {self._table_name}")
        except Exception:
            pass
```

- [ ] **Step 7: Run temp table tests**

Run: `pytest tests/test_temp_table.py -v --timeout=120`
Expected: All tests pass.

- [ ] **Step 8: Write failing tests for assertions**

Create `tests/test_assertions.py`:

```python
"""Tests for assertion helpers."""

import pytest

from databricks4py.testing.assertions import assert_frame_equal, assert_schema_equal


@pytest.mark.integration
class TestAssertFrameEqual:
    def test_equal_frames(self, spark_session):
        df1 = spark_session.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
        df2 = spark_session.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
        assert_frame_equal(df1, df2)  # should not raise

    def test_unequal_frames_raises(self, spark_session):
        df1 = spark_session.createDataFrame([(1, "a")], ["id", "name"])
        df2 = spark_session.createDataFrame([(1, "b")], ["id", "name"])
        with pytest.raises(AssertionError):
            assert_frame_equal(df1, df2)

    def test_schema_mismatch_raises(self, spark_session):
        df1 = spark_session.createDataFrame([(1,)], ["id"])
        df2 = spark_session.createDataFrame([("a",)], ["name"])
        with pytest.raises(AssertionError):
            assert_frame_equal(df1, df2)

    def test_ignore_order(self, spark_session):
        df1 = spark_session.createDataFrame([(2, "b"), (1, "a")], ["id", "name"])
        df2 = spark_session.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
        assert_frame_equal(df1, df2, check_order=False)


@pytest.mark.integration
class TestAssertSchemaEqual:
    def test_equal_schemas(self, spark_session):
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType

        s1 = StructType([StructField("id", IntegerType()), StructField("name", StringType())])
        s2 = StructType([StructField("id", IntegerType()), StructField("name", StringType())])
        assert_schema_equal(s1, s2)

    def test_different_types_raises(self, spark_session):
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType

        s1 = StructType([StructField("id", IntegerType())])
        s2 = StructType([StructField("id", StringType())])
        with pytest.raises(AssertionError, match="id"):
            assert_schema_equal(s1, s2)
```

- [ ] **Step 9: Implement assertions**

Create `src/databricks4py/testing/assertions.py`:

```python
"""DataFrame and schema assertion helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

__all__ = ["assert_frame_equal", "assert_schema_equal"]


def assert_frame_equal(
    actual: DataFrame,
    expected: DataFrame,
    *,
    check_order: bool = False,
    check_schema: bool = True,
    check_nullable: bool = False,
) -> None:
    """Assert two DataFrames are equal with clear diff output.

    Uses Spark 3.5+ assertDataFrameEqual when available,
    falls back to manual comparison for older versions.

    Raises:
        AssertionError: If the DataFrames differ.
    """
    if check_schema:
        assert_schema_equal(
            actual.schema, expected.schema, check_nullable=check_nullable
        )

    try:
        from pyspark.testing.utils import assertDataFrameEqual

        assertDataFrameEqual(actual, expected, checkRowOrder=check_order)
    except ImportError:
        _manual_frame_compare(actual, expected, check_order)


def assert_schema_equal(
    actual: StructType,
    expected: StructType,
    *,
    check_nullable: bool = False,
) -> None:
    """Assert two StructTypes are equal with clear diff output.

    Raises:
        AssertionError: If schemas differ, with details about each mismatch.
    """
    actual_fields = {f.name: f for f in actual.fields}
    expected_fields = {f.name: f for f in expected.fields}

    diffs = []

    for name in expected_fields:
        if name not in actual_fields:
            diffs.append(f"  Missing column: {name}")
        elif actual_fields[name].dataType != expected_fields[name].dataType:
            diffs.append(
                f"  Type mismatch for '{name}': "
                f"actual={actual_fields[name].dataType.simpleString()}, "
                f"expected={expected_fields[name].dataType.simpleString()}"
            )
        elif (
            check_nullable
            and actual_fields[name].nullable != expected_fields[name].nullable
        ):
            diffs.append(
                f"  Nullable mismatch for '{name}': "
                f"actual={actual_fields[name].nullable}, "
                f"expected={expected_fields[name].nullable}"
            )

    for name in actual_fields:
        if name not in expected_fields:
            diffs.append(f"  Extra column: {name}")

    if diffs:
        raise AssertionError("Schema mismatch:\n" + "\n".join(diffs))


def _manual_frame_compare(
    actual: DataFrame, expected: DataFrame, check_order: bool
) -> None:
    if check_order:
        actual_rows = actual.collect()
        expected_rows = expected.collect()
    else:
        cols = actual.columns
        actual_rows = sorted(actual.collect(), key=lambda r: tuple(str(r[c]) for c in cols))
        expected_rows = sorted(expected.collect(), key=lambda r: tuple(str(r[c]) for c in cols))

    if actual_rows != expected_rows:
        raise AssertionError(
            f"DataFrame mismatch:\n"
            f"  Actual rows:   {actual_rows}\n"
            f"  Expected rows: {expected_rows}"
        )
```

- [ ] **Step 10: Run assertion tests**

Run: `pytest tests/test_assertions.py -v --timeout=120`
Expected: All tests pass.

- [ ] **Step 11: Update testing __init__.py and fixtures**

Update `src/databricks4py/testing/__init__.py` to add new exports.

Update `src/databricks4py/testing/fixtures.py` to add `df_builder` and `temp_delta` fixtures:

```python
# Add at the bottom of fixtures.py:

@pytest.fixture
def df_builder(spark_session):
    """Pre-wired DataFrameBuilder."""
    from databricks4py.testing.builders import DataFrameBuilder
    return DataFrameBuilder(spark_session)


@pytest.fixture
def temp_delta(spark_session):
    """Factory fixture for TempDeltaTable with automatic cleanup."""
    from databricks4py.testing.temp_table import TempDeltaTable

    tables = []

    def _create(**kwargs):
        t = TempDeltaTable(spark_session, **kwargs)
        entered = t.__enter__()
        tables.append(t)
        return entered

    yield _create

    for t in reversed(tables):
        t.__exit__(None, None, None)
```

- [ ] **Step 12: Lint check**

Run: `ruff check src/databricks4py/testing/ tests/test_builders.py tests/test_temp_table.py tests/test_assertions.py`
Expected: No errors.

- [ ] **Step 13: Run all testing tests together**

Run: `pytest tests/test_builders.py tests/test_temp_table.py tests/test_assertions.py tests/test_mocks.py -v --timeout=120`
Expected: All pass.

- [ ] **Step 14: Commit**

```bash
git add src/databricks4py/testing/ tests/test_builders.py tests/test_temp_table.py tests/test_assertions.py
git commit -m "feat: add DataFrameBuilder, TempDeltaTable, and assertion helpers"
```

---

### Task 10: DBFS Expansion

**Files:**
- Modify: `src/databricks4py/io/dbfs.py`
- Modify: `tests/test_dbfs.py`

- [ ] **Step 1: Write failing tests for new DBFS operations**

Add to `tests/test_dbfs.py`:

```python
class TestDbfsOperations:
    def test_ls(self):
        from databricks4py.io.dbfs import _set_dbutils_module, ls

        mock_dbutils = MockDBUtils()
        _set_dbutils_module(MockDBUtilsModule(mock_dbutils))
        result = ls("/mnt/data")
        assert isinstance(result, list)

    def test_mv(self):
        from databricks4py.io.dbfs import _set_dbutils_module, mv

        mock_dbutils = MockDBUtils()
        _set_dbutils_module(MockDBUtilsModule(mock_dbutils))
        mv("/src", "/dst")  # should not raise

    def test_rm(self):
        from databricks4py.io.dbfs import _set_dbutils_module, rm

        mock_dbutils = MockDBUtils()
        _set_dbutils_module(MockDBUtilsModule(mock_dbutils))
        rm("/path")  # should not raise

    def test_mkdirs(self):
        from databricks4py.io.dbfs import _set_dbutils_module, mkdirs

        mock_dbutils = MockDBUtils()
        _set_dbutils_module(MockDBUtilsModule(mock_dbutils))
        mkdirs("/new/dir")  # should not raise
```

Note: This requires updating `MockDBUtils._MockFS` to support `mv`, `rm`, `mkdirs`. Update `src/databricks4py/testing/mocks.py` first to add these methods.

- [ ] **Step 2: Update MockDBUtils to support new operations**

Add `mv()`, `rm()`, `mkdirs()` to `_MockFS` in `src/databricks4py/testing/mocks.py`, and update `ls()` to be configurable:

```python
# In _MockFS class:
def ls(self, path: str) -> list:
    """Return empty list (override in tests for specific behavior)."""
    return self._ls_results.get(path, [])

def mv(self, source: str, dest: str, recurse: bool = False) -> None:
    self._copies.append({"source": source, "dest": dest, "op": "mv"})

def rm(self, path: str, recurse: bool = False) -> None:
    self._copies.append({"path": path, "op": "rm"})

def mkdirs(self, path: str) -> None:
    self._copies.append({"path": path, "op": "mkdirs"})
```

Also add `self._ls_results: dict[str, list] = {}` to `_MockFS.__init__`.
```

- [ ] **Step 3: Implement new DBFS functions**

In `src/databricks4py/io/dbfs.py`, add `ls()`, `mv()`, `rm()`, `mkdirs()`:

```python
def ls(path: str) -> list:
    """List files at the given path."""
    dbutils = _get_dbutils()
    return dbutils.fs.ls(path)


def mv(source: str, dest: str, *, recurse: bool = False) -> None:
    """Move a file or directory."""
    dbutils = _get_dbutils()
    logger.info("Moving %s → %s (recurse=%s)", source, dest, recurse)
    dbutils.fs.mv(source, dest, recurse)


def rm(path: str, *, recurse: bool = False) -> None:
    """Remove a file or directory."""
    dbutils = _get_dbutils()
    logger.info("Removing %s (recurse=%s)", path, recurse)
    dbutils.fs.rm(path, recurse)


def mkdirs(path: str) -> None:
    """Create a directory (and parents)."""
    dbutils = _get_dbutils()
    logger.info("Creating directory %s", path)
    dbutils.fs.mkdirs(path)
```

These use the existing `_get_dbutils()` helper (which creates a `DBUtils` instance with the active SparkSession), not `_dbutils_module.DBUtils(None)`.

```python
```

- [ ] **Step 4: Run tests**

Run: `pytest tests/test_dbfs.py -v --timeout=30`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/databricks4py/io/dbfs.py src/databricks4py/testing/mocks.py tests/test_dbfs.py
git commit -m "feat: expand DBFS module with ls, mv, rm, mkdirs"
```

---

### Task 11: Streaming Enhancements

**Files:**
- Modify: `src/databricks4py/io/streaming.py`
- Modify: `tests/test_streaming.py`

- [ ] **Step 1: Update StreamingTableReader with optional checkpoint_manager and metrics_sink params**

In `src/databricks4py/io/streaming.py`, update `__init__` to add new keyword-only params while preserving existing positional args:

```python
def __init__(
    self,
    source_table: str,
    trigger: StreamingTriggerOptions | dict | None = None,  # keep existing type, add None
    checkpoint_location: str | None = None,
    *,
    source_format: str = "delta",
    row_filter: Filter | None = None,
    skip_empty_batches: bool = True,
    read_options: dict[str, str] | None = None,
    spark: SparkSession | None = None,
    checkpoint_manager: CheckpointManager | None = None,  # new
    metrics_sink: MetricsSink | None = None,               # new
) -> None:
```

The `trigger` type is widened to accept `StreamingTriggerOptions | dict | None` — existing code passing `StreamingTriggerOptions.AVAILABLE_NOW` still works. Internally, resolve to dict via `.value` if enum is passed. The `start()` method must handle `trigger=None` by using a sensible default (e.g., `processingTime="10 seconds"`).

If `checkpoint_manager` is provided and no `checkpoint_location`, auto-generate:

```python
if checkpoint_location is None and checkpoint_manager is not None:
    checkpoint_location = checkpoint_manager.path_for(
        source_table, self.__class__.__name__
    )
```

In `_foreach_batch_wrapper`, add timing and metrics emission:

```python
import time
start = time.monotonic()
# ... existing batch processing ...
duration_ms = (time.monotonic() - start) * 1000
if self._metrics_sink:
    from databricks4py.metrics.base import MetricEvent
    from datetime import datetime
    self._metrics_sink.emit(MetricEvent(
        job_name=self.__class__.__name__,
        event_type="batch_complete",
        timestamp=datetime.now(),
        duration_ms=duration_ms,
        row_count=df.count() if not df.isEmpty() else 0,
        batch_id=batch_id,
        table_name=self._source_table,
    ))
```

- [ ] **Step 2: Write tests for enhanced streaming**

Add to `tests/test_streaming.py`:

```python
@pytest.mark.no_pyspark
class TestStreamingTableReaderConfig:
    def test_auto_checkpoint_path(self):
        """Verify checkpoint path is auto-generated from checkpoint_manager."""
        from unittest.mock import MagicMock
        from databricks4py.io.checkpoint import CheckpointManager

        mgr = CheckpointManager(base_path="/tmp/checkpoints")

        class TestReader(StreamingTableReader):
            def process_batch(self, df, batch_id):
                pass

        reader = TestReader(
            source_table="catalog.bronze.events",
            checkpoint_manager=mgr,
            spark=MagicMock(),
        )
        assert "catalog_bronze_events" in reader._checkpoint_location
```

- [ ] **Step 3: Run tests**

Run: `pytest tests/test_streaming.py -v --timeout=120`
Expected: All tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/databricks4py/io/streaming.py tests/test_streaming.py
git commit -m "feat: add checkpoint manager and metrics hooks to streaming"
```

---

### Task 12: DeltaTable Enhancements

**Files:**
- Modify: `src/databricks4py/io/delta.py`
- Modify: `src/databricks4py/io/__init__.py`
- Modify: `tests/test_delta.py`

- [ ] **Step 0: Add dict schema support to DeltaTable.__init__**

The existing `DeltaTable.__init__` requires `schema: StructType`. Add support for `dict[str, str]` as an alternative (e.g., `{"id": "INT", "name": "STRING"}`). This makes the API more ergonomic and aligns with `DataFrameBuilder.with_columns()`.

In `src/databricks4py/io/delta.py`, update the `__init__` type hint and add conversion:

```python
def __init__(
    self,
    table_name: str,
    schema: StructType | dict[str, str],  # accept dict or StructType
    *,
    location: str | None = None,
    partition_by: str | Sequence[str] | None = None,
    generated_columns: Sequence[GeneratedColumn] | None = None,
    spark: SparkSession | None = None,
) -> None:
    self._spark = active_fallback(spark)
    self._table_name = table_name
    self._schema = self._resolve_schema(schema)
    # ... rest unchanged

@staticmethod
def _resolve_schema(schema: StructType | dict[str, str]) -> StructType:
    if isinstance(schema, dict):
        from pyspark.sql import types as T
        fields = []
        for name, type_str in schema.items():
            spark_type = T._parse_datatype_string(type_str)
            fields.append(T.StructField(name, spark_type, nullable=True))
        return T.StructType(fields)
    return schema
```

This is backward compatible — existing code passing `StructType` still works.

- [ ] **Step 1: Add merge(), upsert(), scd_type2() methods to DeltaTable**

In `src/databricks4py/io/delta.py`, add to the `DeltaTable` class:

```python
def merge(self, source: DataFrame, *, metrics_sink=None) -> MergeBuilder:
    """Start a merge operation against this table."""
    from databricks4py.io.merge import MergeBuilder
    self._ensure_table_exists()
    return MergeBuilder(
        self._table_name, source, self._spark, metrics_sink=metrics_sink
    )

def upsert(
    self,
    source: DataFrame,
    keys: list[str],
    *,
    update_columns: list[str] | None = None,
    metrics_sink=None,
) -> MergeResult:
    """One-liner upsert: update on match, insert on no match."""
    builder = self.merge(source, metrics_sink=metrics_sink).on(*keys)
    builder = builder.when_matched_update(update_columns)
    builder = builder.when_not_matched_insert()
    return builder.execute()

def scd_type2(
    self,
    source: DataFrame,
    keys: list[str],
    *,
    effective_date_col: str = "effective_date",
    end_date_col: str = "end_date",
    active_col: str = "is_active",
    metrics_sink=None,
) -> MergeResult:
    """SCD Type 2: expire old records, insert new versions.

    On match (active record with changed values):
      - Sets end_date to current_timestamp and active=False on old record
      - Inserts new version with active=True

    On no match: inserts new record with active=True.
    """
    from pyspark.sql import functions as F
    from databricks4py.io.merge import MergeBuilder, MergeResult

    self._ensure_table_exists()

    # Determine non-key columns (the ones we check for changes)
    all_cols = [f.name for f in source.schema.fields]
    value_cols = [c for c in all_cols
                  if c not in keys and c not in (effective_date_col, end_date_col, active_col)]

    # Build change detection condition
    change_condition = " OR ".join(
        f"target.{c} <> source.{c}" for c in value_cols
    )

    # Step 1: Identify rows that need to be expired (matched + changed)
    # Step 2: Merge — expire old, insert new
    key_condition = " AND ".join(f"target.{k} = source.{k}" for k in keys)
    merge_condition = f"{key_condition} AND target.{active_col} = true"

    from delta.tables import DeltaTable as _DeltaTable
    target = _DeltaTable.forName(self._spark, self._table_name)

    # Add SCD2 columns to source if not present
    source_with_scd = source
    if effective_date_col not in all_cols:
        source_with_scd = source_with_scd.withColumn(effective_date_col, F.current_timestamp())
    if end_date_col not in all_cols:
        source_with_scd = source_with_scd.withColumn(end_date_col, F.lit(None).cast("timestamp"))
    if active_col not in all_cols:
        source_with_scd = source_with_scd.withColumn(active_col, F.lit(True))

    # Execute merge: expire matched-and-changed, insert all from source
    (target.alias("target")
        .merge(source_with_scd.alias("source"), merge_condition)
        .whenMatchedUpdate(
            condition=change_condition,
            set={
                end_date_col: "current_timestamp()",
                active_col: "false",
            }
        )
        .whenNotMatchedInsertAll()
        .execute())

    # Extract metrics from history
    history = self._spark.sql(
        f"DESCRIBE HISTORY {self._table_name} LIMIT 1"
    ).collect()[0]
    metrics = history["operationMetrics"]
    result = MergeResult(
        rows_inserted=int(metrics.get("numTargetRowsInserted", 0)),
        rows_updated=int(metrics.get("numTargetRowsUpdated", 0)),
        rows_deleted=int(metrics.get("numTargetRowsDeleted", 0)),
    )

    if metrics_sink:
        from datetime import datetime as dt
        from databricks4py.metrics.base import MetricEvent
        metrics_sink.emit(MetricEvent(
            job_name="scd_type2", event_type="merge_complete",
            timestamp=dt.now(), table_name=self._table_name,
            metadata={"rows_inserted": result.rows_inserted, "rows_updated": result.rows_updated},
        ))

    return result
```

- [ ] **Step 2: Add schema_check to write()**

Update `DeltaTable.write()` signature:

```python
def write(self, df: DataFrame, mode: str = "append", *, schema_check: bool = True) -> None:
    if schema_check and self._table_exists():
        from databricks4py.migrations.schema_diff import SchemaDiff, SchemaEvolutionError
        diff = SchemaDiff.from_tables(self._table_name, df, spark=self._spark)
        if diff.has_breaking_changes():
            raise SchemaEvolutionError(
                f"Breaking schema changes detected for {self._table_name}:\n{diff.summary()}"
            )
        if diff.changes():
            logger.info("Schema evolution for %s:\n%s", self._table_name, diff.summary())
    # ... existing write logic
```

Add a helper `_table_exists()` that returns bool (extracted from `_ensure_table_exists`).

- [ ] **Step 3: Update io/__init__.py exports**

Add `MergeBuilder`, `MergeResult`, `CheckpointManager`, `CheckpointInfo` to `src/databricks4py/io/__init__.py`.

- [ ] **Step 4: Write tests for new DeltaTable methods**

Add to `tests/test_delta.py`:

```python
@pytest.mark.integration
class TestDeltaTableMerge:
    def test_merge_method_returns_builder(self, spark_session):
        from databricks4py.io.merge import MergeBuilder
        table = DeltaTable("test_merge_dt", schema={"id": "INT"}, spark=spark_session)
        df = spark_session.createDataFrame([(1,)], ["id"])
        table.write(df, mode="overwrite")
        builder = table.merge(df)
        assert isinstance(builder, MergeBuilder)

    def test_schema_check_blocks_breaking_change(self, spark_session):
        table = DeltaTable(
            "test_schema_block",
            schema={"id": "INT", "name": "STRING"},
            spark=spark_session,
        )
        df = spark_session.createDataFrame([(1, "alice")], ["id", "name"])
        table.write(df, mode="overwrite")

        # Try writing with a dropped column
        bad_df = spark_session.createDataFrame([(1,)], ["id"])
        from databricks4py.migrations.schema_diff import SchemaEvolutionError
        with pytest.raises(SchemaEvolutionError, match="Breaking"):
            table.write(bad_df, schema_check=True)

    def test_schema_check_disabled(self, spark_session):
        table = DeltaTable(
            "test_schema_skip",
            schema={"id": "INT", "name": "STRING"},
            spark=spark_session,
        )
        df = spark_session.createDataFrame([(1, "alice")], ["id", "name"])
        table.write(df, mode="overwrite")

        # This would break with schema_check, but we disable it
        narrow_df = spark_session.createDataFrame([(2,)], ["id"])
        table.write(narrow_df, mode="overwrite", schema_check=False)
```

- [ ] **Step 5: Run tests**

Run: `pytest tests/test_delta.py tests/test_merge.py -v --timeout=120`
Expected: All tests pass.

- [ ] **Step 6: Lint check**

Run: `ruff check src/databricks4py/io/ tests/test_delta.py tests/test_merge.py`
Expected: No errors.

- [ ] **Step 7: Commit**

```bash
git add src/databricks4py/io/ tests/test_delta.py
git commit -m "feat: add merge/upsert/scd2 and schema_check to DeltaTable"
```

---

### Task 13: Enhanced Workflow

**Files:**
- Modify: `src/databricks4py/workflow.py`
- Create: `tests/test_workflow_v2.py`

- [ ] **Step 1: Write failing tests for enhanced Workflow**

Create `tests/test_workflow_v2.py`:

```python
"""Tests for enhanced Workflow with config, metrics, retry."""

import pytest

from databricks4py.config import JobConfig
from databricks4py.metrics import LoggingMetricsSink, MetricsSink
from databricks4py.retry import RetryConfig
from databricks4py.workflow import Workflow


@pytest.mark.no_pyspark
class TestWorkflowV2Config:
    def test_backward_compat_no_new_params(self):
        """v0.1 subclass still works with no new params."""
        from unittest.mock import MagicMock

        class OldJob(Workflow):
            def run(self):
                pass

        # v0.1 usage — only spark, no new params
        job = OldJob(spark=MagicMock())
        assert job.config is None
        assert job.metrics is None

    def test_config_property(self):
        class ConfigJob(Workflow):
            def run(self):
                pass

        config = JobConfig(tables={"events": "cat.bronze.events"})
        job = ConfigJob(config=config, spark=__import__("unittest").mock.MagicMock())
        assert job.config is config

    def test_config_none_by_default(self):
        class SimpleJob(Workflow):
            def run(self):
                pass

        job = SimpleJob(spark=__import__("unittest").mock.MagicMock())
        assert job.config is None

    def test_metrics_property(self):
        class MetricsJob(Workflow):
            def run(self):
                pass

        sink = LoggingMetricsSink()
        job = MetricsJob(metrics=sink, spark=__import__("unittest").mock.MagicMock())
        assert job.metrics is sink

    def test_emit_metric_noop_without_sink(self):
        class NoSinkJob(Workflow):
            def run(self):
                self.emit_metric("test_event")

        job = NoSinkJob(spark=__import__("unittest").mock.MagicMock())
        job.emit_metric("test_event")  # should not raise


@pytest.mark.integration
class TestWorkflowV2Execute:
    def test_execute_emits_lifecycle_metrics(self, spark_session):
        events = []

        class CollectorSink(MetricsSink):
            def emit(self, event):
                events.append(event)

        class TestJob(Workflow):
            def run(self):
                self.emit_metric("custom_event", row_count=42)

        job = TestJob(spark=spark_session, metrics=CollectorSink())
        job.execute()

        event_types = [e.event_type for e in events]
        assert "job_start" in event_types
        assert "custom_event" in event_types
        assert "job_complete" in event_types

    def test_execute_emits_failure_metric(self, spark_session):
        events = []

        class CollectorSink(MetricsSink):
            def emit(self, event):
                events.append(event)

        class FailingJob(Workflow):
            def run(self):
                raise RuntimeError("boom")

        job = FailingJob(spark=spark_session, metrics=CollectorSink())
        with pytest.raises(RuntimeError, match="boom"):
            job.execute()

        event_types = [e.event_type for e in events]
        assert "job_start" in event_types
        assert "job_failed" in event_types

    def test_execute_with_retry(self, spark_session):
        call_count = 0

        class RetryJob(Workflow):
            def run(self):
                nonlocal call_count
                call_count += 1
                if call_count < 2:
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
        assert call_count == 2

    def test_execute_applies_spark_configs(self, spark_session):
        class ConfigJob(Workflow):
            def run(self):
                val = self.spark.conf.get("spark.sql.shuffle.partitions")
                assert val == "10"

        config = JobConfig(
            tables={},
            spark_configs={"spark.sql.shuffle.partitions": "10"},
        )
        job = ConfigJob(spark=spark_session, config=config)
        job.execute()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_workflow_v2.py -v --timeout=120`
Expected: FAIL

- [ ] **Step 3: Update Workflow implementation**

In `src/databricks4py/workflow.py`, update the class:

```python
class Workflow(ABC):
    def __init__(
        self,
        *,
        spark: SparkSession | None = None,
        dbutils: Any | None = None,
        log_level: int = logging.INFO,
        config: JobConfig | None = None,
        metrics: MetricsSink | None = None,
        retry_config: RetryConfig | None = None,
    ) -> None:
        configure_logging(level=log_level)
        self._spark = active_fallback(spark)
        self._dbutils: Any | None = None
        self._execution_time: datetime | None = None
        self._config = config
        self._metrics = metrics
        self._retry_config = retry_config

        if dbutils is not None:
            try:
                inject_dbutils(dbutils)
                self._dbutils = dbutils
            except Exception:
                logger.info("dbutils injection failed (running outside Databricks)")

    @property
    def config(self) -> JobConfig | None:
        return self._config

    @property
    def metrics(self) -> MetricsSink | None:
        return self._metrics

    def emit_metric(self, event_type: str, **kwargs) -> None:
        if self._metrics is None:
            return
        from databricks4py.metrics.base import MetricEvent
        self._metrics.emit(MetricEvent(
            job_name=self.__class__.__name__,
            event_type=event_type,
            timestamp=datetime.now(),
            **kwargs,
        ))

    def quality_check(self, df, gate, *, table_name=None):
        """Run quality gate, emit metrics, return clean DataFrame."""
        # Run check once, then enforce based on the result
        report = gate.check(df)
        self.emit_metric(
            "quality_check",
            table_name=table_name,
            metadata={"passed": report.passed, "checks": len(report.results)},
        )
        # Emit metrics BEFORE enforce (which may raise)
        if not report.passed:
            return gate.enforce(df)  # handles raise/warn/quarantine
        return df

    def execute(self) -> None:
        workflow_name = self.__class__.__name__

        # Apply spark configs
        if self._config and self._config.spark_configs:
            for k, v in self._config.spark_configs.items():
                self._spark.conf.set(k, v)

        self.emit_metric("job_start")
        logger.info("Starting workflow: %s", workflow_name)
        start = datetime.now()

        try:
            if self._retry_config:
                from databricks4py.retry import retry
                retryable_run = retry(self._retry_config)(self.run_at_time)
                retryable_run()
            else:
                self.run_at_time()

            duration_ms = (datetime.now() - start).total_seconds() * 1000
            self.emit_metric("job_complete", duration_ms=duration_ms)
            logger.info("Workflow %s completed successfully", workflow_name)
        except Exception:
            duration_ms = (datetime.now() - start).total_seconds() * 1000
            self.emit_metric("job_failed", duration_ms=duration_ms)
            logger.exception("Workflow %s failed", workflow_name)
            raise
        finally:
            if self._metrics:
                self._metrics.flush()
```

Use lazy imports for `JobConfig`, `MetricsSink`, `RetryConfig` in the type hints (use `TYPE_CHECKING` guard) to avoid circular imports.

**Important:** Update the import at the top of `workflow.py` — change `from databricks4py.secrets import inject_dbutils` to `from databricks4py import inject_dbutils` (the new unified function from `__init__.py`). The old `secrets.inject_dbutils` only sets `SecretFetcher.dbutils`; the new unified one sets both SecretFetcher and DBFS. If `inject_dbutils` is not yet available from the top-level package (Task 14 hasn't run), import both individually:

```python
def _inject_dbutils_unified(dbutils_module):
    from databricks4py.secrets import SecretFetcher
    from databricks4py.io.dbfs import _set_dbutils_module
    SecretFetcher.dbutils = dbutils_module
    _set_dbutils_module(dbutils_module)
```

Use `_inject_dbutils_unified` inside `Workflow.__init__` instead of the imported `inject_dbutils`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_workflow_v2.py tests/test_workflow.py -v --timeout=120`
Expected: All tests pass (both v1 and v2).

- [ ] **Step 5: Lint check**

Run: `ruff check src/databricks4py/workflow.py tests/test_workflow_v2.py`
Expected: No errors.

- [ ] **Step 6: Commit**

```bash
git add src/databricks4py/workflow.py tests/test_workflow_v2.py
git commit -m "feat: enhance Workflow with config, metrics, retry integration"
```

---

### Task 14: Top-Level Exports and __init__.py Updates

**Files:**
- Modify: `src/databricks4py/__init__.py`
- Modify: `src/databricks4py/io/__init__.py`
- Modify: `src/databricks4py/migrations/__init__.py`
- Modify: `src/databricks4py/testing/__init__.py`

- [ ] **Step 1: Update top-level __init__.py**

Add config, metrics, retry to top-level exports (no JVM dependency). Add unified `inject_dbutils`:

```python
# In __init__.py — add these top-level exports:
from databricks4py.config import Environment, JobConfig, UnityConfig
from databricks4py.metrics import (
    CompositeMetricsSink,
    LoggingMetricsSink,
    MetricEvent,
    MetricsSink,
)
from databricks4py.retry import RetryConfig, retry

def inject_dbutils(dbutils_module):
    """Unified dbutils injection for secrets and DBFS operations."""
    from databricks4py.secrets import SecretFetcher
    from databricks4py.io.dbfs import _set_dbutils_module
    SecretFetcher.dbutils = dbutils_module
    _set_dbutils_module(dbutils_module)
```

- [ ] **Step 2: Update io/__init__.py**

Add `MergeBuilder`, `MergeResult`, `CheckpointManager`, `CheckpointInfo` exports.

- [ ] **Step 3: Update migrations/__init__.py**

Add `ColumnChange`, `SchemaDiff`, `SchemaEvolutionError` exports.

- [ ] **Step 4: Update testing/__init__.py**

Add `DataFrameBuilder`, `TempDeltaTable`, `assert_frame_equal`, `assert_schema_equal`, `df_builder`, `temp_delta` exports.

- [ ] **Step 5: Bump version**

In `pyproject.toml`, change version from `"0.1.0"` to `"0.2.0"`. In `__init__.py`, update `__version__ = "0.2.0"`.

- [ ] **Step 6: Run full test suite**

Run: `pytest -v --timeout=120`
Expected: All tests pass.

- [ ] **Step 7: Lint full project**

Run: `ruff check src/ tests/ && ruff format --check src/ tests/`
Expected: No errors.

- [ ] **Step 8: Commit**

```bash
git add src/databricks4py/__init__.py src/databricks4py/io/__init__.py src/databricks4py/migrations/__init__.py src/databricks4py/testing/__init__.py pyproject.toml
git commit -m "feat: update exports and bump version to 0.2.0"
```

---

### Task 15: Documentation

**Files:**
- Create: All files in `docs/` as listed in the file structure above

- [ ] **Step 1: Write index.md**

Create `docs/index.md` with library overview, installation instructions, feature highlights, and links to guides.

- [ ] **Step 2: Write getting-started guides**

Create `docs/getting-started/installation.md`, `quick-start.md`, `concepts.md`.

`quick-start.md` should show a complete workflow in under 30 lines — read bronze, quality check, upsert to silver, with metrics.

`concepts.md` should explain: Workflow as spine, config as environment resolver, metrics as pluggable sinks, quality as pre-write gates.

- [ ] **Step 3: Write feature guides**

Create each guide in `docs/guides/`:
- `config.md` — JobConfig vs UnityConfig, environment resolution, from_env()
- `delta-operations.md` — DeltaTable read/write, merge, upsert, SCD Type 2
- `quality-gates.md` — Expectations, QualityGate, quarantine pattern
- `metrics.md` — MetricsSink, built-in sinks, custom sinks, auto-capture
- `streaming.md` — StreamingTableReader, CheckpointManager, auto-path
- `schema-evolution.md` — SchemaDiff, pre-write checks, migration patterns
- `testing.md` — DataFrameBuilder, TempDeltaTable, assertions, fixtures
- `retry.md` — RetryConfig, decorator usage, workflow integration
- `workflow.md` — Full lifecycle, wiring config + metrics + quality + retry

Each guide should include:
- When to use it
- Complete code examples
- Common patterns
- Edge cases

- [ ] **Step 4: Write API reference**

Create `docs/api/` files with class/function signatures, parameters, return types, and usage examples for each module.

- [ ] **Step 5: Commit documentation**

```bash
git add docs/
git commit -m "docs: add comprehensive library documentation"
```

---

### Task 16: Examples

**Files:**
- Create: All files in `docs/examples/`

- [ ] **Step 1: Write bronze_to_silver.py**

Complete medallion ETL example showing: UnityConfig, QualityGate, upsert, metrics.

- [ ] **Step 2: Write streaming_pipeline.py**

Streaming example with CheckpointManager, metrics, foreachBatch.

- [ ] **Step 3: Write scd_type2.py**

SCD Type 2 merge with effective dates and active flags.

- [ ] **Step 4: Write quality_quarantine.py**

Quality gate with quarantine routing to a dead-letter table.

- [ ] **Step 5: Write multi_env_config.py**

UnityConfig usage showing same code working across dev/staging/prod.

- [ ] **Step 6: Write custom_metrics_sink.py**

Custom MetricsSink implementation (e.g., sending to an HTTP endpoint).

- [ ] **Step 7: Write feature_pipeline.py**

ML feature engineering workflow example.

- [ ] **Step 8: Write testing_patterns.py**

Test patterns with DataFrameBuilder, TempDeltaTable, and assertion helpers.

- [ ] **Step 9: Commit examples**

```bash
git add docs/examples/
git commit -m "docs: add comprehensive usage examples"
```

---

## Final Verification

After all tasks are complete:

- [ ] Run full test suite: `pytest -v --timeout=120`
- [ ] Run lint: `ruff check src/ tests/ docs/ && ruff format --check src/ tests/ docs/`
- [ ] Verify all imports work: `python -c "import databricks4py; print(databricks4py.__version__)"`
- [ ] Verify lazy imports: `python -c "import databricks4py"` should not trigger JVM
