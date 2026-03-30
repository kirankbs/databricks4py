"""Checkpoint compatibility checking and diagnostics.

Provides health checks for Spark Structured Streaming checkpoints:
schema compatibility, corruption detection, and migration safety
assessment. No competitor library offers these utilities.

.. note::
    Currently supports local filesystem checkpoints only. Cloud storage
    paths (``dbfs:``, ``s3://``, ``abfss://``, ``gs://``) are not yet
    supported — they require Hadoop filesystem access via SparkSession.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from pyspark.sql.types import StructType

__all__ = [
    "CheckpointDiagnostic",
    "CheckpointHealth",
    "CompatibilityResult",
    "check_compatibility",
    "diagnose_checkpoint",
]

logger = logging.getLogger(__name__)


class CheckpointHealth(Enum):
    """Overall health status of a checkpoint directory."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    CORRUPTED = "corrupted"
    MISSING = "missing"


@dataclass(frozen=True)
class CompatibilityResult:
    """Result of checking a checkpoint against a schema.

    Attributes:
        compatible: Whether the checkpoint is compatible with the given schema.
        added_columns: New columns in the schema not present in the checkpoint.
        removed_columns: Columns in the checkpoint not present in the schema.
        type_changes: Columns whose data type has changed.
        nullable_changes: Columns whose nullable property changed.
        safe_to_resume: Whether the stream can safely resume from this checkpoint.
        recommendation: Human-readable recommendation.
    """

    compatible: bool
    added_columns: list[str] = field(default_factory=list)
    removed_columns: list[str] = field(default_factory=list)
    type_changes: list[str] = field(default_factory=list)
    nullable_changes: list[str] = field(default_factory=list)
    safe_to_resume: bool = True
    recommendation: str = ""


@dataclass(frozen=True)
class CheckpointDiagnostic:
    """Comprehensive diagnostic report for a checkpoint directory.

    Attributes:
        path: Checkpoint directory path.
        health: Overall health status.
        has_offsets: Whether the offsets directory exists and is populated.
        has_commits: Whether the commits directory exists and is populated.
        has_sources: Whether the sources directory exists.
        has_state: Whether a state directory exists (stateful operations).
        last_batch_id: Most recent committed batch ID, or None.
        pending_batches: Batch IDs present in offsets but not in commits.
        metadata: Raw metadata from the checkpoint metadata file.
        issues: List of detected problems.
        size_bytes: Total checkpoint directory size.
    """

    path: str
    health: CheckpointHealth
    has_offsets: bool = False
    has_commits: bool = False
    has_sources: bool = False
    has_state: bool = False
    last_batch_id: int | None = None
    pending_batches: list[int] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    issues: list[str] = field(default_factory=list)
    size_bytes: int = 0


def check_compatibility(
    checkpoint_path: str,
    current_schema: StructType,
) -> CompatibilityResult:
    """Check whether a streaming checkpoint is compatible with a schema.

    Reads the source schema stored in the checkpoint's ``sources/``
    directory and compares it against ``current_schema``. Determines
    whether the stream can safely resume or needs a checkpoint reset.

    Schema evolution rules for Structured Streaming:
    - **Adding columns**: Safe (additive change).
    - **Removing columns**: Unsafe — checkpoint references old offsets with those columns.
    - **Changing types**: Unsafe — deserialization will fail.
    - **Changing nullable**: Usually safe, but flagged for awareness.

    Args:
        checkpoint_path: Path to the checkpoint directory.
        current_schema: The schema the stream would use going forward.

    Returns:
        CompatibilityResult with detailed findings.
    """

    if not os.path.isdir(checkpoint_path):
        return CompatibilityResult(
            compatible=True,
            safe_to_resume=True,
            recommendation="Checkpoint does not exist — stream will start fresh.",
        )

    checkpoint_schema = _read_source_schema(checkpoint_path)
    if checkpoint_schema is None:
        return CompatibilityResult(
            compatible=True,
            safe_to_resume=True,
            recommendation="No source schema found in checkpoint — stream will start fresh.",
        )

    old_fields = {f.name: f for f in checkpoint_schema.fields}
    new_fields = {f.name: f for f in current_schema.fields}

    added = [n for n in new_fields if n not in old_fields]
    removed = [n for n in old_fields if n not in new_fields]
    type_changes = []
    nullable_changes = []

    for name in old_fields:
        if name in new_fields:
            old_f = old_fields[name]
            new_f = new_fields[name]
            if old_f.dataType != new_f.dataType:
                type_changes.append(
                    f"{name}: {old_f.dataType.simpleString()} -> {new_f.dataType.simpleString()}"
                )
            elif old_f.nullable != new_f.nullable:
                nullable_changes.append(f"{name}: nullable {old_f.nullable} -> {new_f.nullable}")

    has_breaking = bool(removed or type_changes)
    safe = not has_breaking

    parts = []
    if not has_breaking and not added and not nullable_changes:
        recommendation = "Schema is identical — safe to resume."
    else:
        if added:
            parts.append(f"New columns ({', '.join(added)}) are additive and safe.")
        if removed:
            parts.append(f"Removed columns ({', '.join(removed)}) — checkpoint reset required.")
        if type_changes:
            parts.append(f"Type changes ({'; '.join(type_changes)}) — checkpoint reset required.")
        if nullable_changes:
            parts.append(f"Nullable changes ({'; '.join(nullable_changes)}) — usually safe.")
        recommendation = " ".join(parts)

    return CompatibilityResult(
        compatible=not has_breaking,
        added_columns=added,
        removed_columns=removed,
        type_changes=type_changes,
        nullable_changes=nullable_changes,
        safe_to_resume=safe,
        recommendation=recommendation,
    )


def diagnose_checkpoint(checkpoint_path: str) -> CheckpointDiagnostic:
    """Run a comprehensive health check on a checkpoint directory.

    Inspects the directory structure, validates that offsets and commits
    are consistent, reads metadata, and flags any detected corruption.

    Args:
        checkpoint_path: Path to the checkpoint directory.

    Returns:
        CheckpointDiagnostic with full findings.
    """
    if not os.path.isdir(checkpoint_path):
        return CheckpointDiagnostic(
            path=checkpoint_path,
            health=CheckpointHealth.MISSING,
            issues=["Checkpoint directory does not exist."],
        )

    issues: list[str] = []
    offsets_dir = os.path.join(checkpoint_path, "offsets")
    commits_dir = os.path.join(checkpoint_path, "commits")
    sources_dir = os.path.join(checkpoint_path, "sources")
    state_dir = os.path.join(checkpoint_path, "state")
    metadata_file = os.path.join(checkpoint_path, "metadata")

    has_offsets = os.path.isdir(offsets_dir)
    has_commits = os.path.isdir(commits_dir)
    has_sources = os.path.isdir(sources_dir)
    has_state = os.path.isdir(state_dir)

    if not has_offsets:
        issues.append("Missing offsets directory — checkpoint may be empty or corrupted.")

    # Read batch IDs from offsets and commits
    offset_batches = _read_batch_ids(offsets_dir) if has_offsets else set()
    commit_batches = _read_batch_ids(commits_dir) if has_commits else set()

    # Pending = in offsets but not committed
    pending = sorted(offset_batches - commit_batches)
    if len(pending) > 1:
        issues.append(
            f"Multiple uncommitted batches: {pending}. This may indicate a crash during processing."
        )

    # Committed batches not in offsets (shouldn't happen)
    orphaned_commits = commit_batches - offset_batches
    if orphaned_commits:
        issues.append(
            f"Commit entries without matching offsets: {sorted(orphaned_commits)}. "
            "Checkpoint may be corrupted."
        )

    last_batch_id = max(commit_batches) if commit_batches else None

    # Check for gaps in committed batches
    gaps: list[int] = []
    if commit_batches:
        expected = set(range(min(commit_batches), max(commit_batches) + 1))
        gaps = sorted(expected - commit_batches)
        if gaps:
            issues.append(f"Gaps in commit sequence: {gaps}. Checkpoint may be corrupted.")

    # Read metadata
    metadata: dict[str, Any] = {}
    if os.path.isfile(metadata_file):
        metadata = _read_metadata(metadata_file)
    else:
        issues.append("Missing metadata file.")

    # Determine health
    if orphaned_commits or gaps:
        health = CheckpointHealth.CORRUPTED
    elif issues:
        health = CheckpointHealth.DEGRADED
    else:
        health = CheckpointHealth.HEALTHY

    size_bytes = _dir_size(checkpoint_path)

    return CheckpointDiagnostic(
        path=checkpoint_path,
        health=health,
        has_offsets=has_offsets,
        has_commits=has_commits,
        has_sources=has_sources,
        has_state=has_state,
        last_batch_id=last_batch_id,
        pending_batches=pending,
        metadata=metadata,
        issues=issues,
        size_bytes=size_bytes,
    )


def _read_source_schema(checkpoint_path: str) -> StructType | None:
    """Attempt to read the source schema from checkpoint sources directory.

    Spark checkpoint source files use a line-delimited format:
    - Line 1: version number (e.g. "v1")
    - Line 2+: JSON metadata (may contain schema)

    Also handles plain JSON files for compatibility with test fixtures.
    """
    sources_dir = os.path.join(checkpoint_path, "sources")
    if not os.path.isdir(sources_dir):
        return None

    for entry in sorted(os.listdir(sources_dir)):
        source_file = os.path.join(sources_dir, entry)
        if os.path.isfile(source_file):
            try:
                with open(source_file) as f:
                    lines = f.read().strip().splitlines()

                if not lines:
                    continue

                # Real Spark checkpoints: first line is version, rest is JSON
                # Test/synthetic: entire content is JSON
                json_content = None
                if lines[0].startswith("v") and len(lines) > 1:
                    json_content = "\n".join(lines[1:])
                else:
                    json_content = "\n".join(lines)

                data = json.loads(json_content)
                if isinstance(data, dict) and "schema" in data:
                    schema_val = data["schema"]
                    if isinstance(schema_val, str):
                        # Spark stores schema as a JSON string within the JSON
                        schema_val = json.loads(schema_val)
                    if isinstance(schema_val, dict) and "fields" in schema_val:
                        return StructType.fromJson(schema_val)
                    if isinstance(schema_val, list):
                        return StructType.fromJson({"type": "struct", "fields": schema_val})
                if isinstance(data, dict) and "fields" in data:
                    return StructType.fromJson(data)
            except (json.JSONDecodeError, KeyError, TypeError):
                continue
    return None


def _read_batch_ids(directory: str) -> set[int]:
    """Read numeric batch IDs from a checkpoint subdirectory."""
    ids = set()
    if os.path.isdir(directory):
        for entry in os.listdir(directory):
            if entry.isdigit():
                ids.add(int(entry))
    return ids


def _read_metadata(path: str) -> dict[str, Any]:
    """Read and parse the checkpoint metadata file."""
    try:
        with open(path) as f:
            content = f.read().strip()
        return json.loads(content)
    except (json.JSONDecodeError, OSError):
        return {}


def _dir_size(path: str) -> int:
    """Calculate total size of a directory tree in bytes."""
    total = 0
    for dirpath, _dirnames, filenames in os.walk(path):
        for fname in filenames:
            total += os.path.getsize(os.path.join(dirpath, fname))
    return total
