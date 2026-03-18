"""Test utilities and fixtures for databricks4py.

Provides pytest fixtures and mock objects for testing Spark and
Databricks applications locally.

Usage in your ``conftest.py``::

    from databricks4py.testing.fixtures import *  # noqa: F401,F403
"""

from databricks4py.testing.assertions import assert_frame_equal, assert_schema_equal
from databricks4py.testing.builders import DataFrameBuilder
from databricks4py.testing.fixtures import (
    clear_env,
    df_builder,
    spark_session,
    spark_session_function,
    temp_delta,
)
from databricks4py.testing.mocks import MockDBUtils, MockDBUtilsModule
from databricks4py.testing.temp_table import TempDeltaTable

__all__ = [
    "DataFrameBuilder",
    "MockDBUtils",
    "MockDBUtilsModule",
    "TempDeltaTable",
    "assert_frame_equal",
    "assert_schema_equal",
    "clear_env",
    "df_builder",
    "spark_session",
    "spark_session_function",
    "temp_delta",
]
