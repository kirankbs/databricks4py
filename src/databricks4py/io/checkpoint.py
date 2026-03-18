"""Streaming checkpoint lifecycle management."""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
from dataclasses import dataclass

from pyspark.sql import SparkSession

__all__ = [
    "CheckpointInfo",
    "CheckpointManager",
]

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CheckpointInfo:
    """Immutable snapshot of a streaming checkpoint's state."""

    path: str
    last_batch_id: int | None
    offsets: dict | None
    size_bytes: int


def _dir_size(path: str) -> int:
    total = 0
    for dirpath, _dirnames, filenames in os.walk(path):
        for fname in filenames:
            total += os.path.getsize(os.path.join(dirpath, fname))
    return total


def _sanitize(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]", "_", name)


class CheckpointManager:
    """Manages streaming checkpoint directories.

    Args:
        base_path: Root directory under which checkpoints are stored.
        spark: Optional SparkSession (reserved for future DBFS/cloud support).
    """

    def __init__(
        self,
        base_path: str,
        *,
        spark: SparkSession | None = None,
    ) -> None:
        self._base_path = base_path
        self._spark = spark

    def path_for(self, source: str, sink: str) -> str:
        """Generate a deterministic checkpoint path for a source/sink pair."""
        return f"{self._base_path}/{_sanitize(source)}__{_sanitize(sink)}"

    def exists(self, path: str) -> bool:
        return os.path.isdir(path)

    def reset(self, path: str) -> None:
        """Delete a checkpoint directory. No-op if it doesn't exist."""
        if os.path.exists(path):
            shutil.rmtree(path)
            logger.info("Deleted checkpoint at %s", path)

    def info(self, path: str) -> CheckpointInfo:
        """Read checkpoint metadata from the offset log."""
        offsets_dir = os.path.join(path, "offsets")
        last_batch_id: int | None = None
        offsets: dict | None = None

        if os.path.isdir(offsets_dir):
            batch_ids = []
            for entry in os.listdir(offsets_dir):
                if entry.isdigit():
                    batch_ids.append(int(entry))

            if batch_ids:
                last_batch_id = max(batch_ids)
                offset_file = os.path.join(offsets_dir, str(last_batch_id))
                with open(offset_file) as f:
                    offsets = json.load(f)

        return CheckpointInfo(
            path=path,
            last_batch_id=last_batch_id,
            offsets=offsets,
            size_bytes=_dir_size(path),
        )
