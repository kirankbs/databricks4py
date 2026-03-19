"""Databricks pattern: Structured streaming micro-batch job.

Shows StreamingTableReader with foreachBatch processing, trigger options,
row filtering, and DeltaTableAppender as the output sink.

Note: Designed to run on Databricks Runtime. For local examples, see quickstart.py.
      pyspark.dbutils is only available on Databricks Runtime.
      Streaming requires a source that supports readStream (e.g. Delta table, Kafka).

Available triggers:
    StreamingTriggerOptions.PROCESSING_TIME_10S
    StreamingTriggerOptions.PROCESSING_TIME_30S
    StreamingTriggerOptions.PROCESSING_TIME_1M
    StreamingTriggerOptions.PROCESSING_TIME_5M
    StreamingTriggerOptions.PROCESSING_TIME_10M
    StreamingTriggerOptions.AVAILABLE_NOW
"""

from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

from databricks4py import Workflow
from databricks4py.filters import DropDuplicates
from databricks4py.io import DeltaTableAppender, StreamingTableReader, StreamingTriggerOptions

OUTPUT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("timestamp", TimestampType()),
        StructField("value", IntegerType()),
    ]
)


class EventStreamProcessor(StreamingTableReader):
    """Process streaming events into a Delta table."""

    def __init__(self, spark, checkpoint_location, output_location, trigger):
        super().__init__(
            source_table="bronze.raw_stream",
            trigger=trigger,
            checkpoint_location=checkpoint_location,
            row_filter=DropDuplicates(subset=["event_id"]),
            spark=spark,
        )
        self._output = DeltaTableAppender(
            table_name="silver.processed_events",
            schema=OUTPUT_SCHEMA,
            location=output_location,
            spark=spark,
        )

    def process_batch(self, df: DataFrame, batch_id: int) -> None:
        processed = df.select("event_id", "timestamp", "value")
        self._output.append(processed)


class StreamingWorkflow(Workflow):
    """Workflow that runs a streaming processor."""

    def run(self) -> None:
        processor = EventStreamProcessor(
            spark=self.spark,
            checkpoint_location="/checkpoints/event_stream",
            output_location="/data/silver/processed_events",
            trigger=StreamingTriggerOptions.PROCESSING_TIME_1M,
        )
        query = processor.start()
        query.awaitTermination()


def main():
    """Entry point for Databricks streaming job."""
    import pyspark.dbutils

    StreamingWorkflow(dbutils=pyspark.dbutils).execute()


if __name__ == "__main__":
    main()
