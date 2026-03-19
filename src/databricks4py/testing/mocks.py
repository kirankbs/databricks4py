"""Mock objects for Databricks dbutils in local testing."""

from __future__ import annotations

from typing import Any

__all__ = ["MockDBUtils", "MockDBUtilsModule"]


class _MockSecrets:
    """Mock for dbutils.secrets."""

    def __init__(self) -> None:
        self._secrets: dict[tuple[str, str], str] = {}

    def get(self, scope: str, key: str) -> str:
        """Get a secret value.

        Args:
            scope: The secret scope.
            key: The secret key.

        Raises:
            KeyError: If the secret is not found.
        """
        try:
            return self._secrets[(scope, key)]
        except KeyError:
            raise KeyError(f"Secret not found: scope={scope!r}, key={key!r}") from None

    def put(self, scope: str, key: str, value: str) -> None:
        """Store a secret value (for test setup)."""
        self._secrets[(scope, key)] = value


class _MockFS:
    """Mock for dbutils.fs."""

    def __init__(self) -> None:
        self._copies: list[tuple[str, str, bool]] = []
        self._moves: list[tuple[str, str, bool]] = []
        self._removes: list[tuple[str, bool]] = []
        self._mkdirs: list[str] = []
        self._ls_results: dict[str, list[Any]] = {}

    def cp(self, source: str, dest: str, recurse: bool = False) -> bool:
        """Record a copy operation."""
        self._copies.append((source, dest, recurse))
        return True

    def mv(self, source: str, dest: str, recurse: bool = False) -> bool:
        """Record a move operation."""
        self._moves.append((source, dest, recurse))
        return True

    def rm(self, path: str, recurse: bool = False) -> bool:
        """Record a remove operation."""
        self._removes.append((path, recurse))
        return True

    def mkdirs(self, path: str) -> bool:
        """Record a mkdirs operation."""
        self._mkdirs.append(path)
        return True

    def ls(self, path: str) -> list[Any]:
        """List files at path."""
        return self._ls_results.get(path, [])


class MockDBUtils:
    """Mock Databricks DBUtils for local testing.

    Provides mock implementations of ``secrets`` and ``fs`` modules.

    Example::

        mock = MockDBUtils()
        mock.secrets.put("my-scope", "api-key", "secret-value")
        assert mock.secrets.get("my-scope", "api-key") == "secret-value"
    """

    def __init__(self) -> None:
        self.secrets = _MockSecrets()
        self.fs = _MockFS()


class MockDBUtilsModule:
    """Mock for the ``pyspark.dbutils`` module.

    Mimics the Databricks runtime's ``pyspark.dbutils`` module which
    provides a ``DBUtils`` class that accepts a SparkSession.

    Example::

        mock_module = MockDBUtilsModule()
        dbutils = mock_module.DBUtils(spark)
        dbutils.secrets.get("scope", "key")
    """

    def __init__(self, mock_dbutils: MockDBUtils | None = None) -> None:
        self._mock_dbutils = mock_dbutils or MockDBUtils()

    def DBUtils(self, spark: Any = None) -> MockDBUtils:  # noqa: N802
        """Create a DBUtils instance (ignores spark argument)."""
        return self._mock_dbutils
