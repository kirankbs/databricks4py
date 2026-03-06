"""Tests for catalog management."""

import pytest

from databricks4py.catalog import CatalogSchema


class TestCatalogSchema:
    @pytest.mark.no_pyspark
    def test_table_access(self) -> None:
        schema = CatalogSchema("sales", tables=["orders", "customers"])
        assert schema.orders == "sales.orders"
        assert schema.customers == "sales.customers"

    @pytest.mark.no_pyspark
    def test_versioned_tables(self) -> None:
        schema = CatalogSchema(
            "analytics",
            tables=["raw_events"],
            versioned_tables={"metrics": "metrics_v3"},
        )
        assert schema.raw_events == "analytics.raw_events"
        assert schema.metrics == "analytics.metrics_v3"

    @pytest.mark.no_pyspark
    def test_missing_table_raises(self) -> None:
        schema = CatalogSchema("sales", tables=["orders"])
        with pytest.raises(AttributeError, match="no table 'missing'"):
            _ = schema.missing

    @pytest.mark.no_pyspark
    def test_schema_name(self) -> None:
        schema = CatalogSchema("myschema", tables=[])
        assert schema.schema_name == "myschema"

    @pytest.mark.no_pyspark
    def test_repr(self) -> None:
        schema = CatalogSchema("s", tables=["b", "a"])
        assert "CatalogSchema('s'" in repr(schema)

    @pytest.mark.no_pyspark
    def test_empty_schema(self) -> None:
        schema = CatalogSchema("empty")
        with pytest.raises(AttributeError):
            _ = schema.anything
