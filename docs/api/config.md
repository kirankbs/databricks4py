# API: Config

## Environment

```python
class Environment(Enum):
    DEV = "dev"
    STAGING = "staging"
    PROD = "prod"
```

## JobConfig

```python
class JobConfig:
    def __init__(
        self,
        tables: dict[str, str],
        *,
        secret_scope: str | None = None,
        storage_root: str | None = None,
        spark_configs: dict[str, str] | None = None,
    ) -> None
```

**Properties:**
- `tables` -- dict mapping logical names to fully qualified table names
- `secret_scope` -- Databricks secret scope name
- `storage_root` -- base storage path
- `spark_configs` -- Spark configuration key-value pairs
- `env` -- resolved `Environment` (auto-detected)

**Methods:**

### table(name: str) -> str

Resolve a logical table name. Raises `KeyError` with available names on miss.

### secret(key: str) -> str

Fetch a secret from the configured scope via `SecretFetcher`. Raises `ValueError` if no scope is set.

### from_env(**kwargs) -> JobConfig

Class method. Alias for `JobConfig(**kwargs)`.

---

## UnityConfig

```python
class UnityConfig(JobConfig):
    def __init__(
        self,
        catalog_prefix: str,
        schemas: list[str],
        *,
        secret_scope: str | None = None,
        storage_root: str | None = None,
        spark_configs: dict[str, str] | None = None,
    ) -> None
```

**Properties:**
- `catalog_prefix` -- prefix for catalog name
- `schemas` -- list of allowed schema names
- `catalog` -- resolved catalog name (`"{prefix}_{env}"`)

### table(name: str) -> str

Expects `"schema.table"` format. Returns `"{catalog}.{schema}.{table}"`. Raises `ValueError` for wrong format, `KeyError` for unknown schema.

---

## CatalogSchema

```python
class CatalogSchema:
    def __init__(
        self,
        name: str,
        tables: list[str] | None = None,
        versioned_tables: dict[str, str] | None = None,
    ) -> None
```

Attribute access returns `"{name}.{table}"`. Versioned tables map a logical name to a different physical name.

```python
cs = CatalogSchema("sales", tables=["orders"], versioned_tables={"metrics": "metrics_v3"})
cs.orders   # "sales.orders"
cs.metrics  # "sales.metrics_v3"
```
