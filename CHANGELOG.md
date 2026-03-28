# Changelog

All notable changes to this project will be documented in this file.

Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Observability subpackage: `BatchContext`, `BatchLogger`, `QueryProgressObserver`, `StreamingHealthCheck`
- CI security scanning with Bandit and pip-audit
- PyPI publishing workflow with trusted publishers
- `StreamingTableReader`: `max_consecutive_failures` parameter — raises `CircuitBreakerError` after N consecutive batch failures, with or without a DLQ configured
- `MergeBuilder.when_matched_soft_delete()` — logical delete via `is_deleted=true` / `deleted_at=current_timestamp()` without a physical row removal
- `DeltaTable.history()`, `DeltaTable.restore()`, `DeltaTable.restore_to_timestamp()` — point-in-time rollback helpers
- `FreshnessExpectation(column, max_age)` — data quality check that a table's most-recent timestamp is within the allowed age window; integrates with `QualityGate`

### Fixed
- `DeltaTable.scd_type2()`: returned `None` implicitly when the post-merge history was empty; now returns `MergeResult(0, 0, 0)`
- `DeltaTable`: constructor now rejects SQL injection characters (`--`, `;`, `/*`) in table names

## [0.2.0] - 2026-03-28

### Added
- `StreamingTableReader`: dead-letter queue support (`dead_letter_table`), `stop()`, `query`/`is_active` properties
- `MigrationRunner`: ordered, idempotent migration runner backed by Delta history table
- `MigrationStep`: versioned migration step with pre/post validation
- `TableAlter`: fluent DDL builder for `ALTER TABLE` operations
- `DeltaMetricsSink`: buffered metrics writer with explicit schema
- Data quality expectations: `NotNull`, `InRange`, `Unique`, `RowCount`, `MatchesRegex`, `ColumnExists`
- `QualityGate`: enforce expectations with raise/warn/quarantine modes
- 64 integration tests covering streaming, quality, migrations, metrics, and e2e workflows
- Comprehensive README with usage examples for all modules

### Fixed
- `expectations.py`: PySpark Column truthiness check (`if condition` -> `if condition is not None`)
- `DeltaMetricsSink.flush()`: explicit schema prevents inference failure on None columns
- `dbfs.py`: operations no longer require active SparkSession when module is already injected

## [0.1.0] - 2026-03-27

### Added
- `DeltaTable`, `DeltaTableAppender`, `DeltaTableOverwriter` with schema validation
- `MergeBuilder` fluent API for Delta MERGE operations
- `StreamingTableReader` ABC for foreachBatch processing
- `Filter`, `FilterPipeline`, `DropDuplicates`, `WhereFilter`, `ColumnFilter`
- `Workflow` base class with lifecycle metrics and config
- `SchemaDiff` for detecting column-level schema changes
- `TableValidator` for migration pre/post checks
- `CheckpointManager` for streaming checkpoint lifecycle
- `JobConfig`, `UnityConfig` for environment-aware configuration
- `SecretFetcher` with injectable dbutils
- `CatalogSchema` for schema-qualified table naming
- Testing utilities: `DataFrameBuilder`, `TempDeltaTable`, assertions, fixtures, mocks
- `retry` decorator with exponential backoff
- Metrics infrastructure: `MetricEvent`, `MetricsSink`, `CompositeMetricsSink`, `LoggingMetricsSink`
