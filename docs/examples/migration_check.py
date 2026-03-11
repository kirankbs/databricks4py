"""Databricks pattern: Two-stage table migration with validation.

Shows the full migration workflow:
1. Create new table with updated schema (including generated columns)
2. Validate structure with TableValidator before swapping
3. Atomic replace_data() to swap production table

Note: Designed to run on Databricks Runtime. For local examples, see quickstart.py.
      pyspark.dbutils is only available on Databricks Runtime.
"""

from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

from databricks4py import Workflow
from databricks4py.io import DeltaTable, GeneratedColumn
from databricks4py.migrations import TableValidator

NEW_SCHEMA = StructType(
    [
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("created_date", DateType()),
    ]
)

GENERATED_COLS = [
    GeneratedColumn(
        name="created_date",
        data_type="DATE",
        expression="CAST(created_at AS DATE)",
        comment="Date derived from created_at timestamp",
    ),
]


class MigrateUsersTable(Workflow):
    """Two-stage migration: reprocess then replace."""

    def run(self) -> None:
        # Stage 1: Create migration table with new schema
        migration_table = DeltaTable(
            table_name="migrations.users_v2",
            schema=NEW_SCHEMA,
            location="/data/migrations/users_v2",
            partition_by="created_date",
            generated_columns=GENERATED_COLS,
            spark=self.spark,
        )

        # Write reprocessed data
        source_df = self.spark.read.table("production.users")
        migration_table.write(source_df, mode="overwrite")

        # Validate migration table
        validator = TableValidator(
            table_name="migrations.users_v2",
            expected_columns=["id", "name", "created_date"],
            expected_partition_columns=["created_date"],
            expected_generated_columns=GENERATED_COLS,
            spark=self.spark,
        )
        result = validator.validate()
        result.raise_if_invalid("migrations.users_v2")

        # Stage 2: Atomic swap
        production = DeltaTable(
            table_name="production.users",
            schema=NEW_SCHEMA,
            spark=self.spark,
        )
        production.replace_data(
            replacement_table_name="migrations.users_v2",
            recovery_table_name="recovery.users_backup_20240115",
        )

        print("Migration complete!")


def main():
    import pyspark.dbutils

    MigrateUsersTable(dbutils=pyspark.dbutils).execute()


if __name__ == "__main__":
    main()
