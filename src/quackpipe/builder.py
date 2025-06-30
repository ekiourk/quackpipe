"""
The Builder API for programmatically constructing a quackpipe session.
"""
from typing import List, Dict, Any, Self
from .config import SourceConfig, SourceType
from .core import session as core_session # Avoid circular import

class QuackpipeBuilder:
    """A fluent builder for creating a quackpipe session without a YAML file."""

    def __init__(self):
        self._sources: List[SourceConfig] = []

    def add_source(self, name: str, type: SourceType, config: Dict[str, Any] = None, secret_name: str = None) -> Self:
        """
        Adds a data source to the configuration.

        Args:
            name: The name for the data source (e.g., 'pg_main').
            type: The type of the source, using the SourceType enum.
            config: A dictionary of non-secret parameters.
            secret_name: The logical name of the secret bundle.

        Returns:
            The builder instance for chaining.
        """
        source = SourceConfig(
            name=name,
            type=type,
            config=config or {},
            secret_name=secret_name
        )
        self._sources.append(source)
        return self

    def session(self):
        """
        Builds and enters the session context manager.

        Returns:
            A context manager yielding a configured DuckDB connection.
        """
        if not self._sources:
            raise ValueError("Cannot build a session with no sources defined.")
        # The core session function is designed to accept a list of configs directly
        return core_session(configs=self._sources)
