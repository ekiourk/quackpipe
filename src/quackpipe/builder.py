"""
The Builder API for programmatically constructing a quackpipe session.
"""
from typing import Any, Self

from .config import SourceConfig, SourceType
from .core import session as core_session  # Avoid circular import


class QuackpipeBuilder:
    """A fluent builder for creating a quackpipe session without a YAML file."""

    def __init__(self):
        self._sources: list[SourceConfig] = []

    def add_source(self, name: str, type: SourceType, config: dict[str, Any] = None, secret_name: str = None) -> Self:
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

    def get_configs(self) -> list[SourceConfig]:
        """
        Returns the list of SourceConfig objects that have been added to the builder.
        This is useful for passing to high-level utilities like `move_data`.
        """
        return self._sources

    def session(self, **kwargs):
        """
        Builds and enters the session context manager. Can accept the same arguments
        as the core session function, like `sources=['source_a']`.

        Returns:
            A context manager yielding a configured DuckDB connection.
        """
        if not self._sources:
            raise ValueError("Cannot build a session with no sources defined.")

        # Pass the built configs and any extra arguments (like `sources`)
        # to the core session manager.
        return core_session(configs=self._sources, **kwargs)
