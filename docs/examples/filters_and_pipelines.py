"""Deep dive: Filter pipeline with databricks4py.

Covers every Filter subclass and pipeline composition. Shows how to
build custom filters and chain them together.

Prerequisites:
    pip install databricks4py
    Java 17+ must be installed for Spark

Run:
    python docs/examples/filters_and_pipelines.py
"""

from pyspark.sql import DataFrame

from databricks4py import get_or_create_local_session
from databricks4py.filters import (
    ColumnFilter,
    DropDuplicates,
    Filter,
    FilterPipeline,
    WhereFilter,
)

spark = get_or_create_local_session()

# Sample data with duplicates, nulls, and varied scores
data = [
    {"id": 1, "name": "alice", "department": "eng", "score": 95},
    {"id": 1, "name": "alice", "department": "eng", "score": 95},  # duplicate
    {"id": 2, "name": "bob", "department": "sales", "score": 40},
    {"id": 3, "name": "carol", "department": "eng", "score": 82},
    {"id": 4, "name": "dave", "department": "sales", "score": 70},
    {"id": 5, "name": "eve", "department": "hr", "score": 55},
]
raw = spark.createDataFrame(data)
print(f"Raw data: {raw.count()} rows")
raw.show()

# ---- DropDuplicates ----

deduped = DropDuplicates().apply(raw)
print(f"DropDuplicates (all columns): {deduped.count()} rows")

deduped_by_name = DropDuplicates(subset=["name"]).apply(raw)
print(f"DropDuplicates (by name): {deduped_by_name.count()} rows")

# ---- WhereFilter ----

high_scorers = WhereFilter("score >= 70").apply(raw)
print(f"\nWhereFilter (score >= 70): {high_scorers.count()} rows")
high_scorers.show()

eng_only = WhereFilter("department = 'eng'").apply(raw)
print(f"WhereFilter (department = 'eng'): {eng_only.count()} rows")

# ---- ColumnFilter ----

slim = ColumnFilter(columns=["id", "name"]).apply(raw)
print(f"ColumnFilter (id, name): columns = {slim.columns}")
slim.show()

# ---- FilterPipeline: compose filters ----

pipeline = FilterPipeline(
    [
        DropDuplicates(subset=["id"]),
        WhereFilter("score > 50"),
        ColumnFilter(columns=["id", "name", "score"]),
    ]
)
result = pipeline(raw)
print(f"\nPipeline (dedup → where → select): {raw.count()} → {result.count()} rows")
result.show()

# ---- Build pipelines dynamically ----

dynamic = FilterPipeline()
dynamic.add(DropDuplicates())
dynamic.add(WhereFilter("department != 'hr'"))
print(f"Dynamic pipeline length: {len(dynamic)}")
print(f"  {dynamic}")

result2 = dynamic(raw)
print(f"  Result: {result2.count()} rows")

# ---- Callable interface: filters are functions ----

# Filters implement __call__, so you can use them directly
filter_fn = WhereFilter("score > 80")
print(f"\nCallable filter: {filter_fn(raw).count()} rows with score > 80")

# ---- Custom filter: subclass Filter ----


class ScaleScores(Filter):
    """Multiply all scores by a factor."""

    def __init__(self, factor: float) -> None:
        self._factor = factor

    def apply(self, df: DataFrame) -> DataFrame:
        from pyspark.sql.functions import col

        return df.withColumn("score", (col("score") * self._factor).cast("int"))


scaled_pipeline = FilterPipeline(
    [
        DropDuplicates(subset=["id"]),
        ScaleScores(factor=1.1),
        WhereFilter("score > 60"),
    ]
)
result3 = scaled_pipeline(raw)
print(f"\nCustom filter pipeline: {result3.count()} rows after scaling by 1.1x and filtering > 60")
result3.show()

spark.stop()
print("Done!")
