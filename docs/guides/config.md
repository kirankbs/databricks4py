# Config

## When to use this

Any time your job needs table names, secrets, or Spark configs that vary by environment. Config resolves `dev`/`staging`/`prod` automatically so your job code stays environment-agnostic.

## JobConfig vs UnityConfig

**JobConfig** -- explicit table name mapping. You list every table by a logical name.

```python
from databricks4py import JobConfig

config = JobConfig(
    tables={
        "source": "raw_db.events",
        "target": "clean_db.events_silver",
    },
    secret_scope="my-scope",
    storage_root="/mnt/lake",
    spark_configs={"spark.sql.shuffle.partitions": "200"},
)

config.table("source")   # "raw_db.events"
config.table("target")   # "clean_db.events_silver"
config.table("missing")  # KeyError: "Table 'missing' not configured. Available: ['source', 'target']"
```

**UnityConfig** -- for Unity Catalog. Generates `{prefix}_{env}.schema.table` names from a catalog prefix and schema list.

```python
from databricks4py.config import UnityConfig

config = UnityConfig(
    catalog_prefix="analytics",
    schemas=["raw", "curated"],
    secret_scope="analytics-secrets",
)

# In dev (ENV=dev or default):
config.table("raw.events")      # "analytics_dev.raw.events"
config.table("curated.orders")  # "analytics_dev.curated.orders"

# In prod (ENV=prod):
# "analytics_prod.raw.events"
```

Format must be `"schema.table"`. Passing just `"events"` raises `ValueError`. Referencing a schema not in the `schemas` list raises `KeyError`.

## Environment auto-resolution

Resolution order:

1. **Widget parameter** -- `spark.conf.get("spark.databricks.widget.env")` (Databricks job parameters)
2. **Environment variable** -- `ENV` or `ENVIRONMENT`
3. **Default** -- `Environment.DEV`

Unknown values log a warning and fall back to DEV.

```python
config.env  # Environment.DEV, Environment.STAGING, or Environment.PROD
```

## Secret access

```python
config = JobConfig(tables={}, secret_scope="my-scope")
api_key = config.secret("api-key")
```

Requires `dbutils` injection first (happens automatically when you pass `dbutils` to `Workflow`). Without it, `SecretFetcher` raises `RuntimeError`.

## spark_configs

Pass Spark configuration as a dict. When used with `Workflow.execute()`, configs are applied before `run()`:

```python
config = JobConfig(
    tables={},
    spark_configs={
        "spark.sql.shuffle.partitions": "400",
        "spark.sql.adaptive.enabled": "true",
    },
)
```

## Complete example

```python
from databricks4py import Workflow, JobConfig

class DailyAggregation(Workflow):
    def run(self):
        events = self.spark.read.table(self.config.table("events"))
        agg = events.groupBy("date").count()
        agg.write.format("delta").mode("overwrite").saveAsTable(
            self.config.table("daily_counts")
        )

config = JobConfig(
    tables={
        "events": "warehouse.raw.click_events",
        "daily_counts": "warehouse.agg.daily_clicks",
    },
    spark_configs={"spark.sql.shuffle.partitions": "100"},
)

DailyAggregation(config=config).execute()
```
