"""
The Builder API for programmatically constructing a quackpipe session.
"""

from __future__ import annotations

from typing import Any, Self

import duckdb

from .config import SourceConfig, SourceParams, SourceType
from .core import session as core_session  # Avoid circular import
from .exceptions import ExecutionError
from .sources import SOURCE_HANDLER_REGISTRY


class QuackpipeBuilder:
    """A fluent builder for creating a quackpipe session without a YAML file."""

    def __init__(self) -> None:
        self._sources: list[SourceConfig] = []

    def add_source(
        self,
        name: str,
        source_type: SourceType | str,
        config: dict[str, Any] | None = None,
        secret_name: str | None = None,
    ) -> Self:
        """
        Adds a data source to the configuration by specifying its components.

        Unknown source types (strings not in the SourceType enum) are allowed
        to support custom DuckDB extensions, but will skip semantic validation.

        Args:
            name: The name for the data source (e.g., 'pg_main').
            source_type: The type of the source, using the SourceType enum or its string value.
            config: A dictionary of non-secret parameters.
            secret_name: The logical name of the secret bundle.

        Returns:
            The builder instance for chaining.
        """
        final_type: SourceType | str
        if isinstance(source_type, str):
            try:
                final_type = SourceType(source_type)
            except ValueError:
                # Keep as string to allow for custom/future source types
                final_type = source_type
        else:
            final_type = source_type

        clean_config = config or {}
        # Perform semantic validation if a handler exists for this type
        if isinstance(final_type, SourceType):
            HandlerClass: Any = SOURCE_HANDLER_REGISTRY.get(final_type)
            if HandlerClass:
                # We don't resolve secrets at 'add_source' time by default,
                # as the environment might not be set yet.
                HandlerClass.validate(clean_config, secret_name, resolve_secrets=False)

        source = SourceConfig(name=name, type=final_type, config=SourceParams(clean_config), secret_name=secret_name)
        self._sources.append(source)
        return self

    def add_source_config(self, source_config: SourceConfig) -> Self:
        """
        Adds a pre-constructed SourceConfig object to the builder.

        Args:
            source_config: An instance of the SourceConfig dataclass.

        Returns:
            The builder instance for chaining.
        """
        if not isinstance(source_config, SourceConfig):
            raise TypeError("Argument must be a SourceConfig instance.")

        self._sources.append(source_config)
        return self

    def chain(self, other_builder: QuackpipeBuilder) -> Self:
        """
        Chains another builder, absorbing all of its sources into this one.

        This is useful for composing configurations from multiple builder instances.

        Args:
            other_builder: Another QuackpipeBuilder instance.

        Returns:
            The current builder instance for further chaining.
        """
        if not isinstance(other_builder, QuackpipeBuilder):
            raise TypeError("Argument must be another QuackpipeBuilder instance.")

        # Extend the current list of sources with the sources from the other builder
        self._sources.extend(other_builder.get_configs())
        return self

    def get_configs(self) -> list[SourceConfig]:
        """
        Returns the list of SourceConfig objects that have been added to the builder.
        This is useful for passing to high-level utilities like `move_data`.
        """
        return self._sources

    def session(self, **kwargs: Any) -> duckdb.DuckDBPyConnection:
        """
        Builds and enters the session context manager. Can accept the same arguments
        as the core session function, like `sources=['source_a']`.

        Returns:
            A configured DuckDB connection.
        """
        if not self._sources:
            raise ExecutionError("Cannot build a session with no sources defined.")

        # Pass the built configs and any extra arguments (like `sources`)
        # to the core session manager.
        return core_session(configs=self.get_configs(), **kwargs)
