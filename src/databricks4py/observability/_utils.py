"""Shared utilities for the observability subpackage."""

from __future__ import annotations


def parse_duration_ms(val: str | int) -> int:
    """Parse a Spark duration string to integer milliseconds.

    Handles ``'250 ms'``, ``'1 s'``, ``'2 m'``, and bare integers.
    Returns 0 if the value cannot be parsed.
    """
    if isinstance(val, int):
        return val
    try:
        stripped = val.strip()
        if stripped.endswith("ms"):
            return int(stripped.replace("ms", "").strip())
        if stripped.endswith("s"):
            return int(float(stripped.replace("s", "").strip()) * 1000)
        if stripped.endswith("m"):
            return int(float(stripped.replace("m", "").strip()) * 60_000)
        return int(stripped)
    except (ValueError, AttributeError):
        return 0
