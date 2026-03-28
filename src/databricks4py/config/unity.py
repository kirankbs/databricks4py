"""Unity Catalog-aware configuration."""

from __future__ import annotations

from databricks4py.config.base import JobConfig

__all__ = ["UnityConfig"]


class UnityConfig(JobConfig):
    """Environment-aware Unity Catalog configuration.

    Builds a catalog name from ``{catalog_prefix}_{env}`` (e.g. ``myapp_prod``)
    and resolves table references in ``schema.table`` format to fully qualified
    three-part names.

    Example::

        config = UnityConfig(
            catalog_prefix="myapp",
            schemas=["bronze", "silver"],
        )
        # In production: config.catalog == "myapp_prod"
        config.table("bronze.events")  # "myapp_prod.bronze.events"

    Args:
        catalog_prefix: Base name prepended to the resolved environment.
        schemas: Allowed schema names. :meth:`table` rejects unknown schemas.
        secret_scope: Forwarded to :class:`JobConfig`.
        storage_root: Forwarded to :class:`JobConfig`.
        spark_configs: Forwarded to :class:`JobConfig`.
    """

    def __init__(
        self,
        catalog_prefix: str,
        schemas: list[str],
        *,
        secret_scope: str | None = None,
        storage_root: str | None = None,
        spark_configs: dict[str, str] | None = None,
    ) -> None:
        super().__init__(
            tables={},
            secret_scope=secret_scope,
            storage_root=storage_root,
            spark_configs=spark_configs,
        )
        self.catalog_prefix = catalog_prefix
        self.schemas = schemas
        self.catalog = f"{catalog_prefix}_{self.env.value}"

    def table(self, name: str) -> str:
        """Resolve ``'schema.table'`` to ``'catalog.schema.table'``.

        Raises:
            ValueError: If *name* is not in ``schema.table`` format.
            KeyError: If the schema is not in the configured list.
        """
        parts = name.split(".")
        if len(parts) != 2:
            raise ValueError(f"Expected 'schema.table' format, got '{name}'")
        schema, table_name = parts
        if schema not in self.schemas:
            raise KeyError(f"Schema '{schema}' not in configured schemas: {sorted(self.schemas)}")
        return f"{self.catalog}.{schema}.{table_name}"

    def __repr__(self) -> str:
        return (
            f"UnityConfig(catalog={self.catalog!r}, schemas={self.schemas}, "
            f"secret_scope={self.secret_scope!r})"
        )
