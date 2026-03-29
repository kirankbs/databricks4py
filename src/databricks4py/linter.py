"""PySpark performance anti-pattern linter.

Runtime query plan analysis that detects common performance problems
in Spark DataFrames. Unlike static AST linters (cylint), this operates
on the actual Spark logical/physical plan, catching issues that only
appear at runtime.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

__all__ = [
    "LintReport",
    "LintWarning",
    "Severity",
    "lint",
    "check_collect_safety",
]

logger = logging.getLogger(__name__)


class Severity(Enum):
    """Severity levels for lint warnings."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


@dataclass(frozen=True)
class LintWarning:
    """A single anti-pattern finding.

    Attributes:
        rule: Short identifier for the anti-pattern.
        severity: How serious the issue is.
        message: Human-readable description.
        suggestion: Recommended fix.
        plan_excerpt: Relevant portion of the query plan (if applicable).
    """

    rule: str
    severity: Severity
    message: str
    suggestion: str
    plan_excerpt: str = ""


@dataclass(frozen=True)
class LintReport:
    """Collection of lint warnings for a DataFrame.

    Attributes:
        warnings: All detected anti-patterns.
    """

    warnings: list[LintWarning] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        return any(w.severity == Severity.ERROR for w in self.warnings)

    @property
    def has_warnings(self) -> bool:
        return any(w.severity in (Severity.WARNING, Severity.ERROR) for w in self.warnings)

    def summary(self) -> str:
        if not self.warnings:
            return "No anti-patterns detected."

        lines = [f"Found {len(self.warnings)} issue(s):"]
        for w in self.warnings:
            lines.append(f"  [{w.severity.value.upper()}] {w.rule}: {w.message}")
            lines.append(f"    Suggestion: {w.suggestion}")
        return "\n".join(lines)


def lint(df: DataFrame) -> LintReport:
    """Analyze a DataFrame's query plan for performance anti-patterns.

    Inspects the logical and physical plans to detect common issues
    like cartesian joins, missing filter pushdown, broadcast candidates,
    and excessive shuffles.

    Args:
        df: The DataFrame to analyze. Must not be a streaming DataFrame.

    Returns:
        LintReport with all detected issues.

    Raises:
        TypeError: If df is a streaming DataFrame.
    """
    if df.isStreaming:
        raise TypeError("Cannot lint a streaming DataFrame — plan is not materialized.")

    warnings: list[LintWarning] = []

    # Get both logical and physical plans as strings
    logical_plan = _get_plan(df, extended=False)
    physical_plan = _get_plan(df, extended=True)

    warnings.extend(_check_cartesian_product(physical_plan))
    warnings.extend(_check_broadcast_nested_loop(physical_plan))
    warnings.extend(_check_no_filter_before_scan(logical_plan))
    warnings.extend(_check_sort_merge_on_small_table(physical_plan))
    warnings.extend(_check_excessive_columns(df))
    warnings.extend(_check_deeply_nested_plan(physical_plan))

    return LintReport(warnings=warnings)


def check_collect_safety(
    df: DataFrame,
    *,
    max_rows: int = 100_000,
    max_bytes: int | None = None,
) -> LintWarning | None:
    """Check whether it's safe to call ``collect()`` on a DataFrame.

    Samples the plan to estimate whether the result set is small enough
    to fit in driver memory. This does NOT execute the full query — it
    uses the logical plan's size estimates when available.

    Args:
        df: DataFrame to check.
        max_rows: Maximum safe row count (default 100k).
        max_bytes: Maximum safe byte count. If None, only row count is checked.

    Returns:
        A LintWarning if collect is unsafe, None if it's safe.
    """
    plan_str = _get_plan(df, extended=True)

    # Try to extract sizeInBytes from the plan's statistics
    estimated_rows = _extract_row_estimate(plan_str)
    estimated_bytes = _extract_size_estimate(plan_str)

    issues = []
    if estimated_rows is not None and estimated_rows > max_rows:
        issues.append(f"Estimated {estimated_rows:,} rows exceeds max_rows={max_rows:,}")
    if max_bytes is not None and estimated_bytes is not None and estimated_bytes > max_bytes:
        issues.append(f"Estimated {estimated_bytes:,} bytes exceeds max_bytes={max_bytes:,}")

    if issues:
        return LintWarning(
            rule="COLLECT_SAFETY",
            severity=Severity.ERROR,
            message=". ".join(issues) + ".",
            suggestion="Use .limit() before .collect(), or use .toPandas() with Arrow.",
        )
    return None


# --- Plan checkers ---


def _check_cartesian_product(plan: str) -> list[LintWarning]:
    if "CartesianProduct" in plan:
        excerpt = _extract_node(plan, "CartesianProduct")
        return [
            LintWarning(
                rule="CARTESIAN_PRODUCT",
                severity=Severity.ERROR,
                message="Cartesian product (cross join) detected in query plan.",
                suggestion="Add a join condition or use .crossJoin() explicitly if intentional.",
                plan_excerpt=excerpt,
            )
        ]
    return []


def _check_broadcast_nested_loop(plan: str) -> list[LintWarning]:
    if "BroadcastNestedLoopJoin" in plan:
        excerpt = _extract_node(plan, "BroadcastNestedLoopJoin")
        return [
            LintWarning(
                rule="BROADCAST_NESTED_LOOP",
                severity=Severity.WARNING,
                message=(
                    "BroadcastNestedLoopJoin detected — this is an O(n*m) join "
                    "used when no equi-join condition exists."
                ),
                suggestion="Add an equi-join condition (==) to enable hash or sort-merge join.",
                plan_excerpt=excerpt,
            )
        ]
    return []


def _check_no_filter_before_scan(plan: str) -> list[LintWarning]:
    """Detect full table scans without any filter pushdown."""
    warnings = []
    # Look for scan nodes that don't have a pushed filter
    scan_pattern = re.compile(
        r"(?:Relation|FileScan|Scan)\s+\S+.*?(?:PushedFilters:\s*\[(.*?)\])?",
        re.DOTALL,
    )
    for match in scan_pattern.finditer(plan):
        pushed = match.group(1)
        if pushed is not None and pushed.strip() == "":
            warnings.append(
                LintWarning(
                    rule="FULL_TABLE_SCAN",
                    severity=Severity.INFO,
                    message="Table scan with no pushed filters detected.",
                    suggestion=(
                        "Add a .where() / .filter() clause before joins or aggregations "
                        "to enable predicate pushdown."
                    ),
                    plan_excerpt=match.group(0)[:200],
                )
            )
    return warnings


def _check_sort_merge_on_small_table(plan: str) -> list[LintWarning]:
    """Detect SortMergeJoin that might benefit from a broadcast."""
    if "SortMergeJoin" not in plan:
        return []

    # Try to find size estimates near the join
    size_estimate = _extract_size_estimate(plan)
    # If we can determine one side is small, suggest broadcast
    # Conservative: only suggest if we see a small sizeInBytes
    if size_estimate is not None and size_estimate < 100 * 1024 * 1024:  # < 100MB
        return [
            LintWarning(
                rule="BROADCAST_CANDIDATE",
                severity=Severity.INFO,
                message=(
                    "SortMergeJoin on a potentially small table "
                    f"(~{size_estimate / (1024 * 1024):.0f}MB)."
                ),
                suggestion=(
                    "Consider using broadcast() on the smaller side "
                    "to avoid shuffle: F.broadcast(small_df)."
                ),
            )
        ]
    return []


def _check_excessive_columns(df: DataFrame) -> list[LintWarning]:
    """Flag DataFrames with very wide schemas that could cause memory pressure."""
    col_count = len(df.columns)
    if col_count > 500:
        return [
            LintWarning(
                rule="EXCESSIVE_COLUMNS",
                severity=Severity.WARNING,
                message=f"DataFrame has {col_count} columns.",
                suggestion=(
                    "Select only the columns you need with .select() to reduce "
                    "serialization overhead and memory usage."
                ),
            )
        ]
    return []


def _check_deeply_nested_plan(plan: str) -> list[LintWarning]:
    """Detect deeply nested plans (often from .withColumn() in a loop)."""
    # Count indentation levels as a proxy for plan depth
    max_depth = 0
    for line in plan.split("\n"):
        stripped = line.lstrip()
        if stripped:
            depth = len(line) - len(stripped)
            max_depth = max(max_depth, depth)

    if max_depth > 200:
        return [
            LintWarning(
                rule="DEEP_PLAN",
                severity=Severity.WARNING,
                message=(
                    f"Query plan is deeply nested (depth ~{max_depth}). "
                    "This often results from calling .withColumn() in a loop."
                ),
                suggestion=(
                    "Replace multiple .withColumn() calls with a single .select() "
                    "that computes all new columns at once."
                ),
            )
        ]
    return []


# --- Plan extraction helpers ---


def _get_plan(df: DataFrame, *, extended: bool = False) -> str:
    """Get the query plan as a string without printing to stdout."""
    if extended:
        return df._jdf.queryExecution().toString()
    return df._jdf.queryExecution().simpleString()


def _extract_node(plan: str, node_name: str) -> str:
    """Extract a plan node and a few surrounding lines for context."""
    lines = plan.split("\n")
    for i, line in enumerate(lines):
        if node_name in line:
            start = max(0, i - 1)
            end = min(len(lines), i + 3)
            return "\n".join(lines[start:end])
    return ""


def _extract_row_estimate(plan: str) -> int | None:
    """Try to extract row count estimates from plan statistics."""
    match = re.search(r"rowCount=(\d+)", plan)
    if match:
        return int(match.group(1))
    # Alternative format
    match = re.search(r"Statistics\(.*?(\d+)\s*rows", plan)
    if match:
        return int(match.group(1))
    return None


def _extract_size_estimate(plan: str) -> int | None:
    """Try to extract sizeInBytes from plan statistics."""
    match = re.search(r"sizeInBytes=(\d+(?:\.\d+)?)\s*(B|KB|KiB|MB|MiB|GB|GiB|TB|TiB)?", plan)
    if not match:
        return None

    value = float(match.group(1))
    unit = (match.group(2) or "B").upper()

    multipliers = {
        "B": 1,
        "KB": 1024,
        "KIB": 1024,
        "MB": 1024**2,
        "MIB": 1024**2,
        "GB": 1024**3,
        "GIB": 1024**3,
        "TB": 1024**4,
        "TIB": 1024**4,
    }
    return int(value * multipliers.get(unit, 1))
