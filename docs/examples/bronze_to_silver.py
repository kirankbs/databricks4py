"""Bronze-to-silver ETL with quality gates, upsert, and metrics.

Full workflow: read bronze events, validate with quality gates,
apply business transforms, upsert into silver, emit metrics.
"""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from databricks4py import CompositeMetricsSink, LoggingMetricsSink, UnityConfig, Workflow
from databricks4py.io.delta import DeltaTable
from databricks4py.quality import InRange, NotNull, QualityGate, RowCount

SILVER_SCHEMA = StructType(
    [
        StructField("order_id", IntegerType()),
        StructField("customer_id", IntegerType()),
        StructField("product_sku", StringType()),
        StructField("quantity", IntegerType()),
        StructField("unit_price", DoubleType()),
        StructField("total_amount", DoubleType()),
        StructField("order_ts", TimestampType()),
        StructField("processed_at", TimestampType()),
    ]
)


class BronzeToSilverOrders(Workflow):
    """Promotes raw order events from bronze to a clean silver table."""

    def run(self) -> None:
        config = UnityConfig("analytics", schemas=["bronze", "silver"])
        bronze_table = config.table("bronze.raw_orders")
        silver_table_name = config.table("silver.orders")

        # Only process records that arrived since last run
        bronze_df = self.spark.read.table(bronze_table).where(
            F.col("_ingested_at") > F.lit(self.execution_time)
        )

        gate = QualityGate(
            NotNull("order_id", "customer_id", "product_sku"),
            InRange("quantity", min_val=1, max_val=10_000),
            InRange("unit_price", min_val=0.01),
            RowCount(min_count=1),
            on_fail="raise",
        )
        report = gate.check(bronze_df)
        self.emit_metric(
            "quality_check",
            table_name=bronze_table,
            metadata={"passed": report.passed, "checks": len(report.results)},
        )
        if not report.passed:
            gate.enforce(bronze_df)

        silver_df = bronze_df.select(
            F.col("order_id").cast("int"),
            F.col("customer_id").cast("int"),
            F.col("product_sku"),
            F.col("quantity").cast("int"),
            F.col("unit_price").cast("double"),
            (F.col("quantity") * F.col("unit_price")).alias("total_amount"),
            F.col("event_timestamp").alias("order_ts"),
            F.current_timestamp().alias("processed_at"),
        )

        silver = DeltaTable(
            table_name=silver_table_name,
            schema=SILVER_SCHEMA,
            partition_by="order_ts",
        )
        result = silver.upsert(
            silver_df,
            keys=["order_id"],
            metrics_sink=self.metrics,
        )
        self.emit_metric(
            "upsert_complete",
            row_count=result.rows_inserted + result.rows_updated,
            table_name=silver_table_name,
        )


if __name__ == "__main__":
    metrics = CompositeMetricsSink(LoggingMetricsSink())
    BronzeToSilverOrders(metrics=metrics).execute()
