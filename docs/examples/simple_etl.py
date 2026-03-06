"""Example: Simple ETL workflow using databricks4py.

Demonstrates:
- Subclassing Workflow for structured entry points
- DeltaTableAppender for append-only writes
- FilterPipeline for composable data quality checks
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
        # Read source data
        raw_events = self.spark.read.table("bronze.raw_events")

        # Apply filters
        pipeline = FilterPipeline(
            [
                DropDuplicates(subset=["user_id", "event_type"]),
                WhereFilter("value > 0"),
            ]
        )
        clean_events = pipeline(raw_events)

        # Write to curated table
        output = DeltaTableAppender(
            table_name="silver.curated_events",
            schema=OUTPUT_SCHEMA,
            location="/data/silver/curated_events",
            partition_by="event_type",
            spark=self.spark,
        )
        output.append(clean_events)


def main():
    """Entry point for Databricks job."""
    import pyspark.dbutils

    EventETL(dbutils=pyspark.dbutils).execute()


if __name__ == "__main__":
    main()
