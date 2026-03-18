# Quality Gates

## When to use this

Before writing data to a target table. Quality gates catch nulls, duplicates, out-of-range values, and schema issues before they propagate downstream.

## Built-in expectations

```python
from databricks4py.quality import NotNull, InRange, Unique, RowCount, MatchesRegex, ColumnExists

NotNull("order_id", "customer_id")           # no nulls in these columns
InRange("amount", min_val=0, max_val=1e6)    # value within bounds
Unique("order_id")                            # no duplicate keys
RowCount(min_count=1, max_count=10_000_000)   # sanity check on volume
MatchesRegex("email", r"^[^@]+@[^@]+\..+$")  # pattern match
ColumnExists("id", "name", dtype="string")    # column exists with expected type
```

Each returns an `ExpectationResult` with `passed`, `total_rows`, and `failing_rows`.

## QualityGate

Groups expectations and enforces a policy on failure:

```python
from databricks4py.quality import QualityGate, NotNull, InRange

gate = QualityGate(
    NotNull("id"),
    InRange("score", min_val=0, max_val=100),
    on_fail="raise",  # "raise" | "warn" | "quarantine"
)
```

### Check without enforcement

```python
report = gate.check(df)
print(report.summary())
# [PASS] NotNull('id') (0/1000 failing)
# [FAIL] InRange('score', min_val=0, max_val=100) (23/1000 failing)
# Overall: FAILED
```

### Enforce

```python
# Raises QualityError on failure
clean_df = gate.enforce(df)

# Warns and passes through
gate_warn = QualityGate(NotNull("id"), on_fail="warn")
df = gate_warn.enforce(df)  # logs warning, returns original df
```

## Quarantine handler

Route bad rows somewhere (dead letter table, file, alert) instead of failing the job:

```python
def send_to_quarantine(bad_rows_df):
    bad_rows_df.write.format("delta").mode("append").saveAsTable("warehouse.quarantine.orders")

gate = QualityGate(
    NotNull("order_id"),
    InRange("amount", min_val=0),
    on_fail="quarantine",
    quarantine_handler=send_to_quarantine,
)

# Returns only clean rows; bad rows go to quarantine table
clean = gate.enforce(df)
```

The gate uses `failing_condition()` from each expectation to build a combined filter. Rows matching any failing condition go to the handler; the rest pass through.

Aggregate-only expectations (like `RowCount`) don't produce row-level conditions. If all failing expectations are aggregate-only, quarantine mode logs a warning and returns the full DataFrame.

## Integration with Workflow

`quality_check()` runs the gate and emits a metric:

```python
class MyJob(Workflow):
    def run(self):
        df = self.spark.read.table(self.config.table("source"))

        gate = QualityGate(NotNull("id"), Unique("id"), on_fail="raise")
        df = self.quality_check(df, gate, table_name="source")

        # df is guaranteed clean here
        df.write.format("delta").mode("append").saveAsTable(self.config.table("target"))
```

The emitted metric includes `{"passed": True/False, "checks": N}` in its metadata.

## Custom expectations

Subclass `Expectation` and implement `validate()`:

```python
from databricks4py.quality import Expectation, ExpectationResult

class FreshnessCheck(Expectation):
    """Validates that the most recent record is within a time window."""

    def __init__(self, timestamp_col: str, max_age_hours: int = 24):
        self._col = timestamp_col
        self._max_hours = max_age_hours

    def validate(self, df):
        from pyspark.sql import functions as F

        max_ts = df.agg(F.max(self._col)).collect()[0][0]
        if max_ts is None:
            return ExpectationResult(
                expectation=repr(self), passed=False, total_rows=df.count()
            )
        from datetime import datetime, timedelta
        age = datetime.now() - max_ts
        return ExpectationResult(
            expectation=repr(self),
            passed=age < timedelta(hours=self._max_hours),
            total_rows=df.count(),
        )

    def __repr__(self):
        return f"FreshnessCheck({self._col!r}, max_age_hours={self._max_hours})"
```

Optionally override `failing_condition()` to return a Column expression for row-level quarantine support.

## Complete example

```python
from databricks4py.quality import (
    QualityGate, NotNull, InRange, Unique, RowCount, MatchesRegex
)

gate = QualityGate(
    NotNull("user_id", "email"),
    Unique("user_id"),
    MatchesRegex("email", r"^[^@]+@[^@]+\.\w+$"),
    InRange("age", min_val=0, max_val=150),
    RowCount(min_count=1),
    on_fail="quarantine",
    quarantine_handler=lambda bad: bad.write.format("delta").mode("append")
        .saveAsTable("warehouse.quarantine.users"),
)

clean_users = gate.enforce(raw_users_df)
clean_users.write.format("delta").mode("append").saveAsTable("warehouse.silver.users")
```
