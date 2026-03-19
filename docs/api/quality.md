# API: Quality

## Expectation (ABC)

```python
class Expectation(ABC):
    @abstractmethod
    def validate(self, df: DataFrame) -> ExpectationResult: ...

    def failing_condition(self) -> Column | None:
        """Return a Column that is True for failing rows. None for aggregate checks."""
        return None
```

## ExpectationResult

```python
@dataclass(frozen=True)
class ExpectationResult:
    expectation: str
    passed: bool
    total_rows: int
    failing_rows: int = 0
    sample: list[Row] | None = None
```

## QualityReport

```python
@dataclass(frozen=True)
class QualityReport:
    results: list[ExpectationResult]
    passed: bool

    def summary(self) -> str
```

---

## Built-in Expectations

### NotNull(*columns: str)

Validates no nulls in the specified columns. Supports `failing_condition()`.

### InRange(column, *, min_val=None, max_val=None)

Validates values within bounds. Either or both bounds can be specified. Supports `failing_condition()`.

### Unique(*columns: str)

Validates no duplicate rows for the specified columns. Aggregate-only (no `failing_condition()`).

### RowCount(*, min_count=None, max_count=None)

Validates DataFrame row count within bounds. Aggregate-only.

### MatchesRegex(column: str, pattern: str)

Validates column values match a regex pattern. Supports `failing_condition()`.

### ColumnExists(*columns: str, dtype: str | None = None)

Validates columns exist in the schema. Optionally checks data type. Aggregate-only.

---

## QualityGate

```python
class QualityGate:
    def __init__(
        self,
        *expectations: Expectation,
        on_fail: Literal["raise", "warn", "quarantine"] = "raise",
        quarantine_handler: Callable[[DataFrame], None] | None = None,
    ) -> None
```

`quarantine_handler` is required when `on_fail="quarantine"`.

### check(df) -> QualityReport

Run all expectations and return a report without enforcement.

### enforce(df) -> DataFrame

Run expectations and enforce policy. Returns clean DataFrame.

## QualityError

```python
class QualityError(Exception):
    report: QualityReport
```

Raised by `QualityGate.enforce()` when `on_fail="raise"`.
