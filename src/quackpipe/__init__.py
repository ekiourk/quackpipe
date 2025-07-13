"""
quackpipe - A configuration-driven ETL helper for DuckDB.

This library provides simple, high-level functions to connect DuckDB
to various data sources based on a YAML configuration file or a
programmatic builder.
"""

import logging
import os

# Expose the primary user-facing functions and classes.
from .builder import QuackpipeBuilder
from .config import SourceConfig, SourceType
from .core import session, with_session
from .exceptions import ConfigError, QuackpipeError, SecretError
from .secrets import configure_secret_provider

# Set up the library's top-level logger
_default_level = os.getenv('QUACKPIPE_LOG_LEVEL', 'WARNING').upper()
_root_logger = logging.getLogger(__name__)
_root_logger.setLevel(getattr(logging, _default_level, logging.WARNING))
_root_logger.addHandler(logging.NullHandler())


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
