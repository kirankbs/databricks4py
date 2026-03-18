"""Tests for config module."""

from __future__ import annotations

import pytest

from databricks4py.config import Environment, JobConfig, UnityConfig


class TestEnvironment:
    @pytest.mark.no_pyspark
    def test_values(self) -> None:
        assert Environment.DEV.value == "dev"
        assert Environment.STAGING.value == "staging"
        assert Environment.PROD.value == "prod"

    @pytest.mark.no_pyspark
    def test_count(self) -> None:
        assert len(Environment) == 3


class TestJobConfig:
    @pytest.mark.no_pyspark
    def test_table_resolution(self) -> None:
        cfg = JobConfig(tables={"orders": "catalog.schema.orders"})
        assert cfg.table("orders") == "catalog.schema.orders"

    @pytest.mark.no_pyspark
    def test_table_not_found_raises_with_available(self) -> None:
        cfg = JobConfig(tables={"orders": "catalog.schema.orders", "users": "catalog.schema.users"})
        with pytest.raises(KeyError, match="orders"):
            cfg.table("missing")

    @pytest.mark.no_pyspark
    def test_env_defaults_to_dev(self) -> None:
        cfg = JobConfig(tables={})
        assert cfg.env == Environment.DEV

    @pytest.mark.no_pyspark
    def test_env_from_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ENV", "prod")
        cfg = JobConfig(tables={})
        assert cfg.env == Environment.PROD

    @pytest.mark.no_pyspark
    def test_env_from_environment_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ENVIRONMENT", "staging")
        cfg = JobConfig(tables={})
        assert cfg.env == Environment.STAGING

    @pytest.mark.no_pyspark
    def test_env_precedence(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ENV", "prod")
        monkeypatch.setenv("ENVIRONMENT", "staging")
        cfg = JobConfig(tables={})
        assert cfg.env == Environment.PROD

    @pytest.mark.no_pyspark
    def test_spark_configs_default(self) -> None:
        cfg = JobConfig(tables={})
        assert cfg.spark_configs == {}

    @pytest.mark.no_pyspark
    def test_spark_configs_set(self) -> None:
        configs = {"spark.sql.shuffle.partitions": "200"}
        cfg = JobConfig(tables={}, spark_configs=configs)
        assert cfg.spark_configs == configs

    @pytest.mark.no_pyspark
    def test_from_env(self) -> None:
        cfg = JobConfig.from_env(tables={"t": "c.s.t"})
        assert cfg.table("t") == "c.s.t"

    @pytest.mark.no_pyspark
    def test_invalid_env_defaults_to_dev(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ENV", "garbage")
        cfg = JobConfig(tables={})
        assert cfg.env == Environment.DEV

    @pytest.mark.no_pyspark
    def test_secret_no_scope_raises(self) -> None:
        cfg = JobConfig(tables={})
        with pytest.raises(ValueError, match="secret_scope"):
            cfg.secret("some-key")

    @pytest.mark.no_pyspark
    def test_repr(self) -> None:
        cfg = JobConfig(tables={"t": "c.s.t"}, secret_scope="my-scope")
        r = repr(cfg)
        assert "JobConfig" in r
        assert "dev" in r


class TestUnityConfig:
    @pytest.mark.no_pyspark
    def test_table_resolution_dev(self) -> None:
        cfg = UnityConfig(catalog_prefix="analytics", schemas=["bronze", "silver"])
        assert cfg.table("bronze.events") == "analytics_dev.bronze.events"

    @pytest.mark.no_pyspark
    def test_table_resolution_prod(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ENV", "prod")
        cfg = UnityConfig(catalog_prefix="analytics", schemas=["bronze", "silver"])
        assert cfg.table("bronze.events") == "analytics_prod.bronze.events"

    @pytest.mark.no_pyspark
    def test_unknown_schema_raises(self) -> None:
        cfg = UnityConfig(catalog_prefix="analytics", schemas=["bronze", "silver"])
        with pytest.raises(KeyError, match="gold"):
            cfg.table("gold.events")

    @pytest.mark.no_pyspark
    def test_invalid_format_raises(self) -> None:
        cfg = UnityConfig(catalog_prefix="analytics", schemas=["bronze"])
        with pytest.raises(ValueError, match="schema.table"):
            cfg.table("just_a_table")

    @pytest.mark.no_pyspark
    def test_secret_scope(self) -> None:
        cfg = UnityConfig(
            catalog_prefix="analytics", schemas=["bronze"], secret_scope="my-scope"
        )
        assert cfg.secret_scope == "my-scope"

    @pytest.mark.no_pyspark
    def test_storage_root(self) -> None:
        cfg = UnityConfig(
            catalog_prefix="analytics",
            schemas=["bronze"],
            storage_root="abfss://container@storage.dfs.core.windows.net",
        )
        assert cfg.storage_root == "abfss://container@storage.dfs.core.windows.net"

    @pytest.mark.no_pyspark
    def test_repr(self) -> None:
        cfg = UnityConfig(catalog_prefix="analytics", schemas=["bronze", "silver"])
        r = repr(cfg)
        assert "UnityConfig" in r
        assert "analytics_dev" in r
