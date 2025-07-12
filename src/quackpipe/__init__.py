"""
quackpipe - A configuration-driven ETL helper for DuckDB.

This library provides simple, high-level functions to connect DuckDB
to various data sources based on a YAML configuration file or a
programmatic builder.
"""

# Expose the primary user-facing functions and classes.
from .core import session, with_session
from .builder import QuackpipeBuilder
from .config import SourceConfig, SourceType
from .secrets import configure_secret_provider
from .exceptions import QuackpipeError, ConfigError, SecretError

__all__ = [
    # Core API
    "session",
    "with_session",

    # Builder API
    "QuackpipeBuilder",

    # Configuration Types
    "SourceConfig",
    "SourceType",

    # Secret Management
    "configure_secret_provider",

    # Exceptions
    "QuackpipeError",
    "ConfigError",
    "SecretError",
]
