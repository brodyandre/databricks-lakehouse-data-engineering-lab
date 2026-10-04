"""Runtime settings for local and Databricks execution."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Literal

ExecutionEnvironment = Literal["local", "databricks"]


@dataclass(frozen=True)
class RuntimeSettings:
    """Project runtime configuration."""

    environment: ExecutionEnvironment
    catalog: str
    bronze_schema: str
    silver_schema: str
    gold_schema: str
    quality_schema: str

    @property
    def is_local(self) -> bool:
        """Return whether the project is running in local mode."""
        return self.environment == "local"

    @property
    def is_databricks(self) -> bool:
        """Return whether the project is running in Databricks mode."""
        return self.environment == "databricks"


def get_settings() -> RuntimeSettings:
    """Build runtime settings from environment variables."""
    environment = os.getenv("LAKEHOUSE_ENV", "local").lower()

    if environment not in {"local", "databricks"}:
        msg = f"LAKEHOUSE_ENV must be either 'local' or 'databricks'. Received: {environment!r}"
        raise ValueError(msg)

    return RuntimeSettings(
        environment=environment,
        catalog=os.getenv(
            "DATABRICKS_CATALOG",
            "dev_databricks_lakehouse",
        ),
        bronze_schema=os.getenv(
            "DATABRICKS_BRONZE_SCHEMA",
            "bronze",
        ),
        silver_schema=os.getenv(
            "DATABRICKS_SILVER_SCHEMA",
            "silver",
        ),
        gold_schema=os.getenv(
            "DATABRICKS_GOLD_SCHEMA",
            "gold",
        ),
        quality_schema=os.getenv(
            "DATABRICKS_QUALITY_SCHEMA",
            "quality",
        ),
    )
