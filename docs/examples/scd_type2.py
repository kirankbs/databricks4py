"""Slowly changing dimension Type 2 with automatic history tracking.

Maintains a full history of customer records: changed rows get expired
(end_date set, is_active flipped to false) and new versions are inserted.
"""

from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from databricks4py import LoggingMetricsSink, Workflow
from databricks4py.io.delta import DeltaTable

# The target schema includes SCD2 tracking columns alongside business fields.
# DeltaTable.scd_type2() manages effective_date, end_date, and is_active
# automatically — you only supply the business columns in the source.
CUSTOMER_DIM_SCHEMA = StructType(
    [
        StructField("customer_id", StringType()),
        StructField("email", StringType()),
        StructField("name", StringType()),
        StructField("segment", StringType()),
        StructField("region", StringType()),
        StructField("effective_date", TimestampType()),
        StructField("end_date", TimestampType()),
        StructField("is_active", BooleanType()),
    ]
)


class CustomerDimensionLoad(Workflow):
    """Applies SCD Type 2 logic to the customer dimension table."""

    def run(self) -> None:
        # Source contains the latest snapshot of customer attributes.
        # No SCD columns here — scd_type2() adds them on merge.
        incoming = self.spark.read.table("core.staging.customers").select(
            "customer_id", "email", "name", "segment", "region"
        )

        dim_table = DeltaTable(
            table_name="core.dimensions.customer_dim",
            schema=CUSTOMER_DIM_SCHEMA,
        )

        result = dim_table.scd_type2(
            incoming,
            keys=["customer_id"],
            effective_date_col="effective_date",
            end_date_col="end_date",
            active_col="is_active",
            metrics_sink=self.metrics,
        )

        self.emit_metric(
            "scd2_complete",
            table_name="core.dimensions.customer_dim",
            metadata={
                "expired": result.rows_updated,
                "new_versions": result.rows_inserted,
            },
        )


if __name__ == "__main__":
    CustomerDimensionLoad(metrics=LoggingMetricsSink()).execute()
