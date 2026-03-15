"""Unit tests for the configuration module."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest
import yaml

from src.utils.config import AppConfig, SparkConfig, StorageConfig


@pytest.mark.unit
class TestAppConfig:
    def test_default_config_loads(self) -> None:
        config = AppConfig()
        assert config.env == "dev"
        assert config.spark.app_name == "spark-project"

    def test_from_file_loads_yaml(self, tmp_path: Path) -> None:
        config_data = {
            "spark": {"app_name": "my-test-app", "master": "local[2]"},
            "storage": {"data_lake_path": "s3a://test-bucket/lake"},
        }
        config_file = tmp_path / "test.yaml"
        config_file.write_text(yaml.dump(config_data))

        config = AppConfig.from_file(config_file, env="test")
        assert config.spark.app_name == "my-test-app"
        assert config.storage.data_lake_path == "s3a://test-bucket/lake"

    def test_env_var_overrides_yaml(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        config_data = {"spark": {"app_name": "yaml-app"}}
        config_file = tmp_path / "test.yaml"
        config_file.write_text(yaml.dump(config_data))

        monkeypatch.setenv("SPARK_APP_NAME", "env-override-app")
        config = AppConfig.from_file(config_file)
        assert config.spark.app_name == "env-override-app"

    def test_get_zone_path(self) -> None:
        config = AppConfig()
        config.storage.data_lake_path = "s3a://bucket"
        config.storage.silver_zone = "silver"
        path = config.get_zone_path("silver", "orders")
        assert path == "s3a://bucket/silver/orders"

    def test_from_file_missing_file_uses_defaults(self, tmp_path: Path) -> None:
        config = AppConfig.from_file(tmp_path / "nonexistent.yaml")
        assert config.spark.master == "local[*]"

    def test_from_env_defaults_to_dev(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("ENV", raising=False)
        config = AppConfig.from_env()
        assert config.env == "dev"
