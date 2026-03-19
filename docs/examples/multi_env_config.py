"""Same code, different environments — zero config changes.

UnityConfig resolves the catalog name from the environment automatically.
Set ENV=dev/staging/prod or pass it via Databricks widgets. The rest of
the code stays identical across all three.
"""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from databricks4py import UnityConfig, Workflow
from databricks4py.io.delta import DeltaTable


class RevenueAggregation(Workflow):
    """Aggregates daily revenue by product category.

    Catalog resolution:
      ENV=dev   -> ecommerce_dev.sales.orders
      ENV=staging -> ecommerce_staging.sales.orders
      ENV=prod  -> ecommerce_prod.sales.orders
    """

    def run(self) -> None:
        config = UnityConfig(
            "ecommerce",
            schemas=["sales", "analytics", "reference"],
            secret_scope="ecommerce-secrets",
            spark_configs={
                "spark.sql.shuffle.partitions": "200",
            },
        )

        # config.table() returns the fully qualified name with the
        # environment-specific catalog prefix baked in
        orders = self.spark.read.table(config.table("sales.orders"))
        products = self.spark.read.table(config.table("reference.products"))

        daily_revenue = (
            orders.join(products, "product_id")
            .groupBy(
                F.date_trunc("day", F.col("order_ts")).alias("order_date"),
                F.col("category"),
            )
            .agg(
                F.sum("total_amount").alias("revenue"),
                F.countDistinct("order_id").alias("order_count"),
                F.avg("total_amount").alias("avg_order_value"),
            )
        )

        target_schema = StructType(
            [
                StructField("order_date", StringType()),
                StructField("category", StringType()),
                StructField("revenue", DoubleType()),
                StructField("order_count", IntegerType()),
                StructField("avg_order_value", DoubleType()),
            ]
        )
        target = DeltaTable(
            table_name=config.table("analytics.daily_revenue"),
            schema=target_schema,
            partition_by="order_date",
        )
        target.write(daily_revenue, mode="overwrite")


if __name__ == "__main__":
    RevenueAggregation().execute()
