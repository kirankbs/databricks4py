"""Streaming pipeline with checkpoint management and batch metrics.

Reads clickstream events via Structured Streaming, deduplicates per
micro-batch, and appends to a silver table with per-batch metrics.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from databricks4py import LoggingMetricsSink, UnityConfig
from databricks4py.filters.base import DropDuplicates, FilterPipeline, WhereFilter
from databricks4py.io.checkpoint import CheckpointManager
from databricks4py.io.delta import DeltaTable
from databricks4py.io.streaming import StreamingTableReader, StreamingTriggerOptions

CLICKSTREAM_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("user_id", IntegerType()),
        StructField("page_url", StringType()),
        StructField("event_type", StringType()),
        StructField("event_ts", TimestampType()),
    ]
)

config = UnityConfig("web_analytics", schemas=["bronze", "silver"])

silver_table = DeltaTable(
    table_name=config.table("silver.clickstream"),
    schema=CLICKSTREAM_SCHEMA,
    partition_by="event_ts",
)


class ClickstreamProcessor(StreamingTableReader):
    """Deduplicates and writes clickstream events to silver."""

    def process_batch(self, df: DataFrame, batch_id: int) -> None:
        enriched = df.withColumn("processed_at", F.current_timestamp())
        silver_table.write(enriched, mode="append")


if __name__ == "__main__":
    checkpoints = CheckpointManager("/mnt/checkpoints/clickstream")
    metrics = LoggingMetricsSink()

    # Drop duplicate events and filter out bot traffic before processing
    pre_filter = FilterPipeline(
        [
            DropDuplicates(subset=["event_id"]),
            WhereFilter("event_type != 'bot_ping'"),
        ]
    )

    processor = ClickstreamProcessor(
        source_table=config.table("bronze.raw_clickstream"),
        trigger=StreamingTriggerOptions.PROCESSING_TIME_1M,
        checkpoint_manager=checkpoints,
        filter=pre_filter,
        metrics_sink=metrics,
    )

    query = processor.start()
    query.awaitTermination()
