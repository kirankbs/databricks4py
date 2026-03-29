"""Tests for checkpoint compatibility checker."""

import json
import os

import pytest
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from databricks4py.io.checkpoint_compat import (
    CheckpointDiagnostic,
    CheckpointHealth,
    CompatibilityResult,
    check_compatibility,
    diagnose_checkpoint,
)


def _make_checkpoint(tmp_path, *, offsets=None, commits=None, sources=None, metadata=None):
    """Helper to create a synthetic checkpoint directory."""
    ckpt = tmp_path / "checkpoint"
    ckpt.mkdir()

    if offsets is not None:
        offsets_dir = ckpt / "offsets"
        offsets_dir.mkdir()
        for batch_id in offsets:
            (offsets_dir / str(batch_id)).write_text("{}")

    if commits is not None:
        commits_dir = ckpt / "commits"
        commits_dir.mkdir()
        for batch_id in commits:
            (commits_dir / str(batch_id)).write_text("{}")

    if sources is not None:
        sources_dir = ckpt / "sources"
        sources_dir.mkdir()
        for i, source_data in enumerate(sources):
            (sources_dir / str(i)).write_text(json.dumps(source_data))

    if metadata is not None:
        (ckpt / "metadata").write_text(json.dumps(metadata))

    return str(ckpt)


@pytest.mark.no_pyspark
class TestCompatibilityResult:
    def test_frozen(self) -> None:
        result = CompatibilityResult(compatible=True)
        with pytest.raises(AttributeError, match="cannot assign"):
            result.compatible = False  # type: ignore[misc]

    def test_defaults(self) -> None:
        result = CompatibilityResult(compatible=True)
        assert result.safe_to_resume is True
        assert result.added_columns == []
        assert result.removed_columns == []
        assert result.type_changes == []


@pytest.mark.no_pyspark
class TestCheckpointDiagnostic:
    def test_frozen(self) -> None:
        diag = CheckpointDiagnostic(path="/test", health=CheckpointHealth.HEALTHY)
        with pytest.raises(AttributeError, match="cannot assign"):
            diag.health = CheckpointHealth.CORRUPTED  # type: ignore[misc]

    def test_defaults(self) -> None:
        diag = CheckpointDiagnostic(path="/test", health=CheckpointHealth.MISSING)
        assert diag.has_offsets is False
        assert diag.has_commits is False
        assert diag.pending_batches == []
        assert diag.issues == []


@pytest.mark.no_pyspark
class TestDiagnoseCheckpoint:
    def test_missing_directory(self, tmp_path) -> None:
        result = diagnose_checkpoint(str(tmp_path / "nonexistent"))
        assert result.health == CheckpointHealth.MISSING
        assert "does not exist" in result.issues[0]

    def test_empty_checkpoint(self, tmp_path) -> None:
        ckpt = tmp_path / "checkpoint"
        ckpt.mkdir()
        result = diagnose_checkpoint(str(ckpt))
        assert result.health == CheckpointHealth.DEGRADED
        assert any("offsets" in i.lower() for i in result.issues)

    def test_healthy_checkpoint(self, tmp_path) -> None:
        path = _make_checkpoint(
            tmp_path,
            offsets=[0, 1, 2],
            commits=[0, 1, 2],
            metadata={"id": "test-query"},
        )
        result = diagnose_checkpoint(path)
        assert result.health == CheckpointHealth.HEALTHY
        assert result.has_offsets is True
        assert result.has_commits is True
        assert result.last_batch_id == 2
        assert result.pending_batches == []
        assert result.issues == []

    def test_pending_batches(self, tmp_path) -> None:
        path = _make_checkpoint(
            tmp_path,
            offsets=[0, 1, 2, 3],
            commits=[0, 1],
            metadata={"id": "test"},
        )
        result = diagnose_checkpoint(path)
        assert result.pending_batches == [2, 3]
        assert result.last_batch_id == 1

    def test_gap_in_commits(self, tmp_path) -> None:
        path = _make_checkpoint(
            tmp_path,
            offsets=[0, 1, 2, 3],
            commits=[0, 2, 3],  # gap at 1
            metadata={"id": "test"},
        )
        result = diagnose_checkpoint(path)
        assert result.health == CheckpointHealth.CORRUPTED
        assert any("Gaps" in i for i in result.issues)

    def test_orphaned_commits(self, tmp_path) -> None:
        path = _make_checkpoint(
            tmp_path,
            offsets=[0, 1],
            commits=[0, 1, 5],  # 5 has no offset
            metadata={"id": "test"},
        )
        result = diagnose_checkpoint(path)
        assert result.health == CheckpointHealth.CORRUPTED
        assert any("without matching offsets" in i for i in result.issues)

    def test_missing_metadata(self, tmp_path) -> None:
        path = _make_checkpoint(
            tmp_path,
            offsets=[0],
            commits=[0],
        )
        result = diagnose_checkpoint(path)
        assert any("metadata" in i.lower() for i in result.issues)

    def test_size_bytes(self, tmp_path) -> None:
        path = _make_checkpoint(
            tmp_path,
            offsets=[0],
            commits=[0],
            metadata={"id": "test"},
        )
        result = diagnose_checkpoint(path)
        assert result.size_bytes > 0

    def test_state_directory(self, tmp_path) -> None:
        path = _make_checkpoint(
            tmp_path,
            offsets=[0],
            commits=[0],
            metadata={"id": "test"},
        )
        os.makedirs(os.path.join(path, "state"))
        result = diagnose_checkpoint(path)
        assert result.has_state is True


@pytest.mark.no_pyspark
class TestCheckCompatibility:
    def test_nonexistent_checkpoint(self, tmp_path) -> None:
        schema = StructType([StructField("id", IntegerType())])
        result = check_compatibility(str(tmp_path / "missing"), schema)
        assert result.compatible is True
        assert result.safe_to_resume is True
        assert "start fresh" in result.recommendation.lower()

    def test_no_source_schema(self, tmp_path) -> None:
        path = _make_checkpoint(tmp_path, offsets=[0], commits=[0])
        schema = StructType([StructField("id", IntegerType())])
        result = check_compatibility(path, schema)
        assert result.compatible is True

    def test_identical_schema(self, tmp_path) -> None:
        source_schema = {
            "type": "struct",
            "fields": [
                {"name": "id", "type": "integer", "nullable": True, "metadata": {}},
                {"name": "name", "type": "string", "nullable": True, "metadata": {}},
            ],
        }
        path = _make_checkpoint(
            tmp_path,
            offsets=[0],
            commits=[0],
            sources=[source_schema],
        )
        current = StructType(
            [
                StructField("id", IntegerType()),
                StructField("name", StringType()),
            ]
        )
        result = check_compatibility(path, current)
        assert result.compatible is True
        assert result.safe_to_resume is True
        assert "identical" in result.recommendation.lower()

    def test_added_columns_safe(self, tmp_path) -> None:
        source_schema = {
            "type": "struct",
            "fields": [
                {"name": "id", "type": "integer", "nullable": True, "metadata": {}},
            ],
        }
        path = _make_checkpoint(
            tmp_path,
            offsets=[0],
            commits=[0],
            sources=[source_schema],
        )
        current = StructType(
            [
                StructField("id", IntegerType()),
                StructField("new_col", StringType()),
            ]
        )
        result = check_compatibility(path, current)
        assert result.compatible is True
        assert result.safe_to_resume is True
        assert result.added_columns == ["new_col"]

    def test_removed_columns_unsafe(self, tmp_path) -> None:
        source_schema = {
            "type": "struct",
            "fields": [
                {"name": "id", "type": "integer", "nullable": True, "metadata": {}},
                {"name": "old_col", "type": "string", "nullable": True, "metadata": {}},
            ],
        }
        path = _make_checkpoint(
            tmp_path,
            offsets=[0],
            commits=[0],
            sources=[source_schema],
        )
        current = StructType([StructField("id", IntegerType())])
        result = check_compatibility(path, current)
        assert result.compatible is False
        assert result.safe_to_resume is False
        assert result.removed_columns == ["old_col"]
        assert "reset required" in result.recommendation.lower()

    def test_type_change_unsafe(self, tmp_path) -> None:
        source_schema = {
            "type": "struct",
            "fields": [
                {"name": "id", "type": "integer", "nullable": True, "metadata": {}},
            ],
        }
        path = _make_checkpoint(
            tmp_path,
            offsets=[0],
            commits=[0],
            sources=[source_schema],
        )
        current = StructType([StructField("id", LongType())])
        result = check_compatibility(path, current)
        assert result.compatible is False
        assert result.safe_to_resume is False
        assert len(result.type_changes) == 1

    def test_nullable_change_flagged(self, tmp_path) -> None:
        source_schema = {
            "type": "struct",
            "fields": [
                {"name": "id", "type": "integer", "nullable": True, "metadata": {}},
            ],
        }
        path = _make_checkpoint(
            tmp_path,
            offsets=[0],
            commits=[0],
            sources=[source_schema],
        )
        current = StructType([StructField("id", IntegerType(), nullable=False)])
        result = check_compatibility(path, current)
        assert result.compatible is True  # nullable changes are non-breaking
        assert result.safe_to_resume is True
        assert len(result.nullable_changes) == 1
        assert "usually safe" in result.recommendation.lower()

    def test_spark_format_source_file(self, tmp_path) -> None:
        """Real Spark checkpoints have a version line before the JSON."""
        ckpt = tmp_path / "checkpoint"
        ckpt.mkdir()
        (ckpt / "offsets").mkdir()
        (ckpt / "offsets" / "0").write_text("{}")
        (ckpt / "commits").mkdir()
        (ckpt / "commits" / "0").write_text("{}")
        sources_dir = ckpt / "sources"
        sources_dir.mkdir()

        schema_json = json.dumps(
            {
                "type": "struct",
                "fields": [
                    {"name": "id", "type": "integer", "nullable": True, "metadata": {}},
                ],
            }
        )
        # Spark format: version line + JSON with schema as string
        (sources_dir / "0").write_text(f"v1\n{json.dumps({'schema': schema_json})}")

        current = StructType(
            [
                StructField("id", IntegerType()),
                StructField("new_col", StringType()),
            ]
        )
        result = check_compatibility(str(ckpt), current)
        assert result.compatible is True
        assert result.added_columns == ["new_col"]
