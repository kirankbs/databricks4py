# databricks4py

Spark, Delta Lake, and Databricks utility library for Python.

A collection of reusable abstractions for building PySpark applications on Databricks and locally.

## Features

- **SparkSession Management** — utilities for obtaining and managing SparkSession instances
- **DeltaTable Abstraction** — structured API for creating, reading, writing, and managing Delta tables
- **Streaming Utilities** — abstract base for structured streaming micro-batch processing
- **Filter Pipeline** — composable DataFrame filter chain
- **Migration Framework** — table structure validation for safe Delta table migrations
- **Secrets Management** — Databricks dbutils secrets integration
- **Catalog Management** — schema-qualified table naming with versioned table support
- **Workflow Base Class** — structured entry point for Databricks job scripts
- **Test Fixtures** — pytest fixtures for SparkSession, mock dbutils, and environment isolation

## Installation

```bash
pip install databricks4py
```

With pydantic support:

```bash
pip install "databricks4py[pydantic]"
```

## Requirements

- Python >= 3.10
- PySpark >= 3.4
- Delta Spark >= 2.4

## Development

```bash
git clone https://github.com/kirankbs/databricks4py.git
cd databricks4py
pip install -e ".[dev]"
pytest -v
```

## License

MIT
