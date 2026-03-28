"""Shared utilities for the observability subpackage."""

from __future__ import annotations


def parse_duration_ms(val: str | int) -> int:
    """Parse a Spark duration string like ``'250 ms'`` to integer milliseconds.

    Returns 0 if the value cannot be parsed.
    """
    if isinstance(val, int):
        return val
    try:
        return int(val.replace(" ms", "").strip())
    except (ValueError, AttributeError):
        return 0
