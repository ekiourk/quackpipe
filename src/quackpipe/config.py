"""
Defines the typed configuration objects for quackpipe.
"""
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


@dataclass(frozen=True)
class Plugin:
    """A structured definition for a DuckDB plugin that may require special installation."""
    name: str
    repository: str | None = None


class SourceType(Enum):
    """Enumeration of supported source types."""
    POSTGRES = "postgres"
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
