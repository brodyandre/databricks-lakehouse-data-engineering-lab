"""Tests for runtime configuration."""

from __future__ import annotations

import os

import pytest

from databricks_lakehouse.config import get_settings


def test_default_settings_use_local_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure local execution is the default."""
    monkeypatch.delenv("LAKEHOUSE_ENV", raising=False)
    monkeypatch.delenv("DATABRICKS_CATALOG", raising=False)

    settings = get_settings()

    assert settings.environment == "local"
    assert settings.catalog == "dev_databricks_lakehouse"
    assert settings.is_local is True
    assert settings.is_databricks is False


def test_databricks_environment_can_be_selected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure Databricks execution can be selected through environment variables."""
    monkeypatch.setenv("LAKEHOUSE_ENV", "databricks")

    settings = get_settings()

    assert settings.environment == "databricks"
    assert settings.is_local is False
    assert settings.is_databricks is True


def test_catalog_and_schemas_can_be_overridden(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure catalog and schema names can be configured externally."""
    monkeypatch.setenv("DATABRICKS_CATALOG", "test_catalog")
    monkeypatch.setenv("DATABRICKS_BRONZE_SCHEMA", "raw")
    monkeypatch.setenv("DATABRICKS_SILVER_SCHEMA", "refined")
    monkeypatch.setenv("DATABRICKS_GOLD_SCHEMA", "analytics")
    monkeypatch.setenv("DATABRICKS_QUALITY_SCHEMA", "quality_checks")

    settings = get_settings()

    assert settings.catalog == "test_catalog"
    assert settings.bronze_schema == "raw"
    assert settings.silver_schema == "refined"
    assert settings.gold_schema == "analytics"
    assert settings.quality_schema == "quality_checks"


def test_invalid_environment_raises_value_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure unsupported execution environments are rejected."""
    monkeypatch.setenv("LAKEHOUSE_ENV", "production")

    with pytest.raises(ValueError, match="LAKEHOUSE_ENV"):
        get_settings()


def test_settings_do_not_require_sensitive_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure runtime settings do not depend on Databricks credentials."""
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)

    settings = get_settings()

    assert settings.catalog
    assert "DATABRICKS_HOST" not in os.environ
    assert "DATABRICKS_TOKEN" not in os.environ
