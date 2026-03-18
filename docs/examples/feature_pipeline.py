"""Feature engineering pipeline for ML training data.

Computes user-level behavioral features from event data, validates
feature distributions with quality gates, and writes to a feature table.
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

from databricks4py import LoggingMetricsSink, Workflow
from databricks4py.io.delta import DeltaTable
from databricks4py.quality import InRange, NotNull, QualityGate, RowCount, Unique

FEATURE_SCHEMA = StructType(
    [
        StructField("user_id", StringType()),
        StructField("total_sessions_30d", IntegerType()),
        StructField("avg_session_duration_sec", DoubleType()),
        StructField("pages_per_session", DoubleType()),
        StructField("conversion_rate_30d", DoubleType()),
        StructField("days_since_last_visit", IntegerType()),
        StructField("computed_at", TimestampType()),
    ]
)


class UserFeaturePipeline(Workflow):
    """Computes user engagement features from raw session data."""

    def run(self) -> None:
        sessions = self.spark.read.table("ml.bronze.user_sessions").where(
            F.col("session_start") >= F.date_sub(F.current_date(), 30)
        )

        features = (
            sessions.groupBy("user_id")
            .agg(
                F.count("session_id").cast("int").alias("total_sessions_30d"),
                F.avg("duration_seconds").alias("avg_session_duration_sec"),
                F.avg("page_count").alias("pages_per_session"),
                # Conversion rate: sessions with a purchase / total sessions
                (
                    F.sum(F.when(F.col("has_purchase"), 1).otherwise(0)) / F.count("session_id")
                ).alias("conversion_rate_30d"),
                F.datediff(F.current_date(), F.max("session_start"))
                .cast("int")
                .alias("days_since_last_visit"),
            )
            .withColumn("computed_at", F.current_timestamp())
        )

        # Sanity checks on feature distributions — catch upstream data issues
        # before they silently degrade model training
        gate = QualityGate(
            NotNull("user_id", "total_sessions_30d"),
            Unique("user_id"),
            RowCount(min_count=100),
            InRange("conversion_rate_30d", min_val=0.0, max_val=1.0),
            InRange("avg_session_duration_sec", min_val=0.0),
            on_fail="raise",
        )
        features = self.quality_check(features, gate, table_name="ml.features.user_engagement")

        feature_table = DeltaTable(
            table_name="ml.features.user_engagement",
            schema=FEATURE_SCHEMA,
        )
        feature_table.write(features, mode="overwrite")

        self.emit_metric(
            "features_computed",
            row_count=features.count(),
            table_name="ml.features.user_engagement",
        )


if __name__ == "__main__":
    UserFeaturePipeline(metrics=LoggingMetricsSink()).execute()
