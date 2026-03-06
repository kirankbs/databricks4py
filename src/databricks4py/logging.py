"""Logging configuration for Spark applications."""

from __future__ import annotations

import logging
import os

__all__ = ["configure_logging", "get_logger"]


def configure_logging(level: int | str | None = None) -> None:
    """Configure root logger with standard formatting.

    Reads ``LOG_LEVEL`` environment variable if no level is provided.
    Silences noisy ``py4j`` logger.

    Args:
        level: Log level (e.g. ``logging.INFO``, ``"DEBUG"``).
            Defaults to ``LOG_LEVEL`` env var or ``INFO``.
    """
    if level is None:
        level = os.getenv("LOG_LEVEL", "INFO")

    formatting = "[%(levelname)s] [%(asctime)s] %(name)s - %(message)s"
    logging.basicConfig(level=level, format=formatting, force=True)

    # Silence py4j noise
    logging.getLogger("py4j").setLevel(logging.ERROR)


def get_logger(name: str) -> logging.Logger:
    """Get a named logger.

    Convenience wrapper around ``logging.getLogger``.

    Args:
        name: Logger name (typically ``__name__``).
    """
    return logging.getLogger(name)
