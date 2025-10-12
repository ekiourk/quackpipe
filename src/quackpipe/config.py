"""
Defines the typed configuration objects for quackpipe.
"""
import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import yaml
from jsonschema import validate


def validate_config(config_data: dict) -> None:
    """
    Validates the given configuration data against the schema.

    Args:
        config_data: The configuration data to validate.

    Raises:
        ValidationError: If the configuration is invalid.
    """
    schema_path = os.path.join(os.path.dirname(__file__), "config.schema.yml")
    with open(schema_path) as f:
        schema = yaml.safe_load(f)
    validate(instance=config_data, schema=schema)


@dataclass(frozen=True)
class Plugin:
    """A structured definition for a DuckDB plugin that may require special installation."""
    name: str
    repository: str | None = None


class SourceType(Enum):
    """Enumeration of supported source types."""
    POSTGRES = "postgres"
    MYSQL = "mysql"
    S3 = "s3"
    AZURE = "azure"
    DUCKLAKE = "ducklake"
    SQLITE = "sqlite"
    PARQUET = "parquet"
    CSV = "csv"


@dataclass
class SourceConfig:
    """
    A structured configuration object for a single data source.
    """
    name: str
    type: SourceType
    config: dict[str, Any] = field(default_factory=dict)
    secret_name: str | None = None
    before_source_statements: list[str] = field(default_factory=list)
    after_source_statements: list[str] = field(default_factory=list)
