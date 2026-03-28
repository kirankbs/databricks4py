"""Tests for BatchContext and BatchLogger."""

from __future__ import annotations

import json
import logging

import pytest

from databricks4py.observability.batch_context import BatchContext, BatchLogger


@pytest.mark.no_pyspark
class TestBatchContext:
    def test_create_auto_generates_correlation_id(self) -> None:
        ctx = BatchContext.create(batch_id=0, source_table="t")
        assert ctx.correlation_id
        assert len(ctx.correlation_id) == 12

    def test_create_with_explicit_correlation_id(self) -> None:
        ctx = BatchContext.create(batch_id=0, source_table="t", correlation_id="my-trace-123")
        assert ctx.correlation_id == "my-trace-123"

    def test_frozen(self) -> None:
        ctx = BatchContext.create(batch_id=0, source_table="t")
        with pytest.raises(AttributeError, match="cannot assign"):
            ctx.batch_id = 1  # type: ignore[misc]

    def test_elapsed_ms_positive(self) -> None:
        ctx = BatchContext.create(batch_id=0, source_table="t")
        assert ctx.elapsed_ms() >= 0

    def test_start_time_is_utc(self) -> None:
        ctx = BatchContext.create(batch_id=0, source_table="t")
        assert ctx.start_time.tzinfo is not None

    def test_two_contexts_have_different_correlation_ids(self) -> None:
        a = BatchContext.create(batch_id=0, source_table="t")
        b = BatchContext.create(batch_id=1, source_table="t")
        assert a.correlation_id != b.correlation_id


@pytest.mark.no_pyspark
class TestBatchLogger:
    def test_batch_start_emits_json(self) -> None:
        bl = BatchLogger(logger_name="test.batch.start")
        log = logging.getLogger("test.batch.start")
        log.setLevel(logging.DEBUG)

        records: list[logging.LogRecord] = []

        class Capture(logging.Handler):
            def emit(self, record):
                records.append(record)

        log.addHandler(Capture())

        ctx = BatchContext.create(batch_id=5, source_table="catalog.schema.events")
        bl.batch_start(ctx)

        assert len(records) == 1
        data = json.loads(records[0].getMessage())
        assert data["event"] == "batch_start"
        assert data["batch_id"] == 5
        assert data["source_table"] == "catalog.schema.events"
        assert "correlation_id" in data
        assert "timestamp" in data

    def test_batch_complete_includes_metrics(self) -> None:
        records: list[logging.LogRecord] = []

        class Capture(logging.Handler):
            def emit(self, record):
                records.append(record)

        bl = BatchLogger(logger_name="test.batch.complete")
        logging.getLogger("test.batch.complete").setLevel(logging.DEBUG)
        logging.getLogger("test.batch.complete").addHandler(Capture())

        ctx = BatchContext.create(batch_id=7, source_table="t")
        bl.batch_complete(ctx, row_count=1000, duration_ms=345.678)

        data = json.loads(records[0].getMessage())
        assert data["event"] == "batch_complete"
        assert data["row_count"] == 1000
        assert data["duration_ms"] == 345.68

    def test_batch_error_truncates_long_error(self) -> None:
        records: list[logging.LogRecord] = []

        class Capture(logging.Handler):
            def emit(self, record):
                records.append(record)

        bl = BatchLogger(logger_name="test.batch.error")
        logging.getLogger("test.batch.error").setLevel(logging.DEBUG)
        logging.getLogger("test.batch.error").addHandler(Capture())

        ctx = BatchContext.create(batch_id=0, source_table="t")
        bl.batch_error(ctx, error="x" * 5000)

        data = json.loads(records[0].getMessage())
        assert len(data["error"]) == 2000

    def test_batch_skip_at_debug_level(self) -> None:
        records: list[logging.LogRecord] = []

        class Capture(logging.Handler):
            def emit(self, record):
                records.append(record)

        bl = BatchLogger(logger_name="test.batch.skip")
        logging.getLogger("test.batch.skip").setLevel(logging.DEBUG)
        logging.getLogger("test.batch.skip").addHandler(Capture())

        ctx = BatchContext.create(batch_id=0, source_table="t")
        bl.batch_skip(ctx, reason="empty")

        assert records[0].levelno == logging.DEBUG

    def test_extra_fields_included(self) -> None:
        records: list[logging.LogRecord] = []

        class Capture(logging.Handler):
            def emit(self, record):
                records.append(record)

        bl = BatchLogger(
            logger_name="test.batch.extra", extra_fields={"env": "prod", "pipeline": "events"}
        )
        logging.getLogger("test.batch.extra").setLevel(logging.DEBUG)
        logging.getLogger("test.batch.extra").addHandler(Capture())

        ctx = BatchContext.create(batch_id=0, source_table="t")
        bl.batch_start(ctx)

        data = json.loads(records[0].getMessage())
        assert data["env"] == "prod"
        assert data["pipeline"] == "events"

    def test_batch_dlq_includes_table(self) -> None:
        records: list[logging.LogRecord] = []

        class Capture(logging.Handler):
            def emit(self, record):
                records.append(record)

        bl = BatchLogger(logger_name="test.batch.dlq")
        logging.getLogger("test.batch.dlq").setLevel(logging.DEBUG)
        logging.getLogger("test.batch.dlq").addHandler(Capture())

        ctx = BatchContext.create(batch_id=3, source_table="src")
        bl.batch_dlq(ctx, dlq_table="catalog.schema.dlq", error="boom")

        data = json.loads(records[0].getMessage())
        assert data["event"] == "batch_dlq"
        assert data["dlq_table"] == "catalog.schema.dlq"
        assert data["error"] == "boom"
        assert records[0].levelno == logging.WARNING
