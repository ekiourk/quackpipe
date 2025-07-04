"""
Defines the typed configuration objects for quackpipe.
"""
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Any, List, Optional

class SourceType(Enum):
    """Enumeration of supported source types."""
    POSTGRES = "postgres"
    S3 = "s3"
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
    config: Dict[str, Any] = field(default_factory=dict)
    secret_name: Optional[str] = None
