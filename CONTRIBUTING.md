# Contributing

Thanks for your interest. Here's how to get started.

## Setup

```bash
git clone https://github.com/kirankbs/databricks4py.git
cd databricks4py
pip install -e ".[dev]"
```

Requires Python 3.10+ and Java 17+ (for PySpark).

## Making changes

1. Fork the repo and create a branch
2. Write code following the conventions below
3. Add tests — untested code won't be merged
4. Run the checks: `ruff check src/ tests/ && ruff format --check src/ tests/`
5. Run tests: `pytest -v -m "no_pyspark" --timeout=30`
6. Open a PR against `main`

## Conventions

- **Style**: Ruff enforces formatting and lint rules (line-length 100, Python 3.10 target)
- **Type hints**: Required on all public functions and methods
- **Docstrings**: On public APIs where behavior is non-obvious. Skip trivial getters
- **Comments**: Only where the *why* is non-obvious from the code
- **Tests**: Use `@pytest.mark.no_pyspark` for tests that don't need Spark, `@pytest.mark.integration` for those that do
- **Exceptions**: Use `AnalysisException` not bare `except Exception` for Spark operations
- **Imports**: Heavy modules (io.delta, filters, streaming) stay out of top-level `__init__.py`

## Test markers

| Marker | What it means | CI job |
|--------|--------------|--------|
| `no_pyspark` | No SparkContext, runs fast | test-unit |
| `unit` | May spin up SparkContext | test-integration |
| `integration` | Real Spark + Delta Lake | test-integration |

## Reporting issues

Open a GitHub issue with:
- Python, PySpark, and delta-spark versions
- Minimal reproduction code
- Actual vs expected behavior
