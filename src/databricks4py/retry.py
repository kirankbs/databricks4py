"""Retry decorator with exponential backoff for transient failures."""

from __future__ import annotations

import functools
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

__all__ = ["RetryConfig", "retry"]

logger = logging.getLogger(__name__)


def _default_retryable_exceptions() -> tuple[type[BaseException], ...]:
    exceptions: list[type[BaseException]] = [ConnectionError, TimeoutError, OSError]
    try:
        from py4j.protocol import Py4JNetworkError

        exceptions.append(Py4JNetworkError)
    except ImportError:
        pass
    return tuple(exceptions)


@dataclass(frozen=True)
class RetryConfig:
    """Configuration for :func:`retry` behaviour.

    Args:
        max_attempts: Total number of tries (including the first).
        base_delay_seconds: Initial delay before the first retry.
        max_delay_seconds: Upper cap on the exponentially increasing delay.
        backoff_factor: Multiplier applied to the delay after each failure.
        retryable_exceptions: Exception types that trigger a retry.
            Defaults to ``ConnectionError``, ``TimeoutError``, ``OSError``,
            and ``Py4JNetworkError`` (if py4j is installed).
    """

    max_attempts: int = 3
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 60.0
    backoff_factor: float = 2.0
    retryable_exceptions: tuple[type[BaseException], ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if not self.retryable_exceptions:
            object.__setattr__(self, "retryable_exceptions", _default_retryable_exceptions())


def retry(config: RetryConfig | None = None) -> Callable:
    """Decorator factory for retrying functions with exponential backoff.

    Example::

        @retry(RetryConfig(max_attempts=5, base_delay_seconds=2.0))
        def fetch_data():
            return requests.get(url).json()

    Args:
        config: Retry parameters. Uses defaults if ``None``.
    """
    if config is None:
        config = RetryConfig()

    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exception: BaseException | None = None
            for attempt in range(1, config.max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except config.retryable_exceptions as exc:
                    last_exception = exc
                    if attempt == config.max_attempts:
                        logger.error(
                            "%s failed after %d attempts: %s",
                            fn.__name__,
                            config.max_attempts,
                            exc,
                        )
                        raise
                    delay = min(
                        config.base_delay_seconds * (config.backoff_factor ** (attempt - 1)),
                        config.max_delay_seconds,
                    )
                    logger.warning(
                        "%s attempt %d/%d failed: %s. Retrying in %.1fs",
                        fn.__name__,
                        attempt,
                        config.max_attempts,
                        exc,
                        delay,
                    )
                    time.sleep(delay)
            raise last_exception  # type: ignore[misc]

        return wrapper

    return decorator
