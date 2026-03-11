"""Databricks pattern: Simple ETL workflow.

Shows the Workflow pattern for Databricks job entry points:
read from a source table, apply filters, write to a curated Delta table.

Note: Designed to run on Databricks Runtime. For local examples, see quickstart.py.
      pyspark.dbutils is only available on Databricks Runtime.

Databricks job config:
    Entry point: simple_etl.py
    Function: main
"""

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from databricks4py import Workflow
from databricks4py.filters import DropDuplicates, FilterPipeline, WhereFilter
from databricks4py.io import DeltaTableAppender

OUTPUT_SCHEMA = StructType(
    [
        StructField("user_id", IntegerType()),
        StructField("event_type", StringType()),
        StructField("value", IntegerType()),
    ]
)


class EventETL(Workflow):
    """Process raw events into a curated Delta table."""

    def run(self) -> None:
        raw_events = self.spark.read.table("bronze.raw_events")

        pipeline = FilterPipeline(
            [
                DropDuplicates(subset=["user_id", "event_type"]),
                WhereFilter("value > 0"),
            ]
        )
        clean_events = pipeline(raw_events)

        output = DeltaTableAppender(
            table_name="silver.curated_events",
            schema=OUTPUT_SCHEMA,
            location="/data/silver/curated_events",
            partition_by="event_type",
            spark=self.spark,
        )
        output.append(clean_events)


def main():
    """Entry point for Databricks job.

    pyspark.dbutils is only available on Databricks Runtime — it provides
    access to Databricks secrets, DBFS, and other platform APIs.
    """
    import pyspark.dbutils

    EventETL(dbutils=pyspark.dbutils).execute()


if __name__ == "__main__":
    main()
