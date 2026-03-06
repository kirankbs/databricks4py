"""Catalog and schema-qualified table name management."""

from __future__ import annotations

__all__ = ["CatalogSchema"]


class CatalogSchema:
    """Schema-qualified table name registry.

    Provides attribute-based access to fully qualified table names
    within a schema. Supports versioned table aliases.

    Example::

        sales = CatalogSchema(
            "sales",
            tables=["orders", "customers"],
            versioned_tables={"metrics": "metrics_v3"},
        )

        # Access table names
        sales.orders      # "sales.orders"
        sales.customers   # "sales.customers"
        sales.metrics     # "sales.metrics_v3"

    Args:
        name: The schema name.
        tables: List of table names in this schema.
        versioned_tables: Dict mapping logical names to versioned names.
    """

    def __init__(
        self,
        name: str,
        tables: list[str] | None = None,
        versioned_tables: dict[str, str] | None = None,
    ) -> None:
        self._name = name
        self._tables: dict[str, str] = {}

        for table in tables or []:
            self._tables[table] = f"{name}.{table}"

        for logical_name, versioned_name in (versioned_tables or {}).items():
            self._tables[logical_name] = f"{name}.{versioned_name}"

    @property
    def schema_name(self) -> str:
        """The schema name."""
        return self._name

    def __getattr__(self, name: str) -> str:
        if name.startswith("_"):
            raise AttributeError(name)
        try:
            return self._tables[name]
        except KeyError:
            raise AttributeError(
                f"'{type(self).__name__}' has no table '{name}'. "
                f"Available: {sorted(self._tables.keys())}"
            ) from None

    def __repr__(self) -> str:
        return f"CatalogSchema({self._name!r}, tables={sorted(self._tables.keys())})"
