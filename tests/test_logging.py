"""Tests for logging configuration."""

import logging
import os

import pytest

from databricks4py.logging import configure_logging, get_logger


class TestConfigureLogging:
    @pytest.mark.no_pyspark
    def test_default_level(self) -> None:
        configure_logging()
        assert logging.getLogger().level == logging.INFO

    @pytest.mark.no_pyspark
    def test_custom_level(self) -> None:
        configure_logging(level=logging.DEBUG)
        assert logging.getLogger().level == logging.DEBUG

    @pytest.mark.no_pyspark
    def test_env_var_level(self) -> None:
        os.environ["LOG_LEVEL"] = "WARNING"
        configure_logging()
        assert logging.getLogger().level == logging.WARNING

    @pytest.mark.no_pyspark
    def test_py4j_silenced(self) -> None:
        configure_logging()
        assert logging.getLogger("py4j").level == logging.ERROR


class TestGetLogger:
    @pytest.mark.no_pyspark
    def test_returns_logger(self) -> None:
        logger = get_logger("test.module")
        assert isinstance(logger, logging.Logger)
        assert logger.name == "test.module"
