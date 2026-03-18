"""Quality gate that routes bad records to a quarantine table.

Records failing validation are split off into a quarantine table for
investigation, while clean records flow through to the target.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from databricks4py import Workflow
from databricks4py.io.delta import DeltaTable
from databricks4py.quality import InRange, MatchesRegex, NotNull, QualityGate

PAYMENT_SCHEMA = StructType(
    [
        StructField("txn_id", StringType()),
        StructField("account_id", IntegerType()),
        StructField("amount", DoubleType()),
        StructField("currency", StringType()),
        StructField("txn_ts", TimestampType()),
    ]
)

QUARANTINE_TABLE = "payments.quality.quarantined_transactions"


def write_to_quarantine(bad_rows: DataFrame) -> None:
    """Stamp bad rows with a reason marker and write to the quarantine table."""
    stamped = bad_rows.withColumn("quarantined_at", F.current_timestamp())
    quarantine = DeltaTable(
        table_name=QUARANTINE_TABLE,
        schema=stamped.schema,
        partition_by="txn_ts",
    )
    quarantine.write(stamped, mode="append")


class PaymentIngestion(Workflow):
    """Ingests payment transactions with quarantine-based quality enforcement."""

    def run(self) -> None:
        raw = self.spark.read.table("payments.bronze.raw_transactions")

        gate = QualityGate(
            NotNull("txn_id", "account_id", "amount"),
            InRange("amount", min_val=0.01, max_val=1_000_000),
            MatchesRegex("currency", r"^[A-Z]{3}$"),
            on_fail="quarantine",
            quarantine_handler=write_to_quarantine,
        )

        # enforce() splits the DataFrame: clean rows are returned,
        # bad rows are sent to write_to_quarantine
        clean = gate.enforce(raw)

        target = DeltaTable(
            table_name="payments.silver.transactions",
            schema=PAYMENT_SCHEMA,
            partition_by="txn_ts",
        )
        result = target.upsert(clean, keys=["txn_id"])

        self.emit_metric(
            "ingestion_complete",
            row_count=result.rows_inserted + result.rows_updated,
            table_name="payments.silver.transactions",
        )


if __name__ == "__main__":
    PaymentIngestion().execute()
