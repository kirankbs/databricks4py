"""Tests for checkpoint management."""

from __future__ import annotations

import json
from dataclasses import FrozenInstanceError

import pytest

from databricks4py.io.checkpoint import CheckpointInfo, CheckpointManager


class TestCheckpointInfo:
    @pytest.mark.no_pyspark
    def test_creation(self) -> None:
        info = CheckpointInfo(
            path="/tmp/ckpt",
            last_batch_id=5,
            offsets={"source": "offset_1"},
            size_bytes=1024,
        )
        assert info.path == "/tmp/ckpt"
        assert info.last_batch_id == 5
        assert info.offsets == {"source": "offset_1"}
        assert info.size_bytes == 1024

    @pytest.mark.no_pyspark
    def test_frozen(self) -> None:
        info = CheckpointInfo(
            path="/tmp/ckpt", last_batch_id=None, offsets=None, size_bytes=0
        )
        with pytest.raises(FrozenInstanceError, match="cannot assign"):
            info.path = "/other"  # type: ignore[misc]


class TestCheckpointManager:
    @pytest.mark.no_pyspark
    def test_path_for(self) -> None:
        mgr = CheckpointManager("/mnt/checkpoints")
        result = mgr.path_for("catalog.schema.source", "catalog.schema.sink")
        assert result == "/mnt/checkpoints/catalog_schema_source__catalog_schema_sink"

    @pytest.mark.no_pyspark
    def test_path_sanitization(self) -> None:
        mgr = CheckpointManager("/mnt/checkpoints")
        result = mgr.path_for("my-source/v2", "my.sink!table")
        assert result == "/mnt/checkpoints/my_source_v2__my_sink_table"

    @pytest.mark.no_pyspark
    def test_exists_false(self, tmp_path) -> None:
        mgr = CheckpointManager(str(tmp_path))
        assert mgr.exists(str(tmp_path / "nonexistent")) is False

    @pytest.mark.no_pyspark
    def test_exists_true(self, tmp_path) -> None:
        ckpt = tmp_path / "my_checkpoint"
        ckpt.mkdir()
        mgr = CheckpointManager(str(tmp_path))
        assert mgr.exists(str(ckpt)) is True

    @pytest.mark.no_pyspark
    def test_reset_deletes(self, tmp_path) -> None:
        ckpt = tmp_path / "my_checkpoint"
        ckpt.mkdir()
        (ckpt / "offsets").mkdir()
        (ckpt / "offsets" / "0").write_text("{}")
        mgr = CheckpointManager(str(tmp_path))
        mgr.reset(str(ckpt))
        assert not ckpt.exists()

    @pytest.mark.no_pyspark
    def test_reset_nonexistent_is_noop(self, tmp_path) -> None:
        mgr = CheckpointManager(str(tmp_path))
        mgr.reset(str(tmp_path / "nonexistent"))  # should not raise

    @pytest.mark.no_pyspark
    def test_info(self, tmp_path) -> None:
        ckpt = tmp_path / "my_checkpoint"
        ckpt.mkdir()
        offsets_dir = ckpt / "offsets"
        offsets_dir.mkdir()
        offset_data = {"source_table": {"start": 0, "end": 100}}
        (offsets_dir / "0").write_text(json.dumps(offset_data))
        (offsets_dir / "3").write_text(json.dumps({"source_table": {"start": 100, "end": 200}}))
        # add a non-numeric file that should be ignored
        (offsets_dir / ".metadata").write_text("{}")

        mgr = CheckpointManager(str(tmp_path))
        info = mgr.info(str(ckpt))
        assert info.path == str(ckpt)
        assert info.last_batch_id == 3
        assert info.offsets == {"source_table": {"start": 100, "end": 200}}
        assert info.size_bytes > 0

    @pytest.mark.no_pyspark
    def test_info_no_offsets_dir(self, tmp_path) -> None:
        ckpt = tmp_path / "empty_checkpoint"
        ckpt.mkdir()
        mgr = CheckpointManager(str(tmp_path))
        info = mgr.info(str(ckpt))
        assert info.last_batch_id is None
        assert info.offsets is None
