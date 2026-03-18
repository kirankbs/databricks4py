# Installation

## pip

```bash
pip install databricks4py
```

## From source

```bash
git clone https://github.com/kirankbs/databricks4py.git
cd databricks4py
pip install -e .
```

## Dev setup

Includes pytest, ruff, and mypy:

```bash
pip install -e ".[dev]"
```

## Requirements

| Dependency | Version |
|---|---|
| Python | >= 3.10 |
| PySpark | >= 3.4 |
| delta-spark | >= 2.4 |
| Java | 8, 11, or 17 (for local Spark) |

PySpark and delta-spark are hard dependencies -- they install automatically. Java must already be on your `PATH`.

## Optional dependencies

Pydantic integration (if you want typed config validation):

```bash
pip install databricks4py[pydantic]
```

This pulls in `pydantic>=2.0` and `pydantic-settings>=2.0`.

## On Databricks

PySpark and Delta are pre-installed in Databricks runtimes. Install just the library:

```bash
%pip install databricks4py
```

Or add it to your cluster's library configuration.
