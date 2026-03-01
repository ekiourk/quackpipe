"""
Abstract Base Class for all Source Handlers.
"""

from abc import ABC, abstractmethod
from typing import Any


class BaseSourceHandler(ABC):
    """
    Abstract base class for a data source handler.
    It is initialized with a context dictionary containing all its configuration.
    """

    def __init__(self, context: dict[str, Any]):
        """
        Initializes the handler with its specific configuration context.

        Args:
            context: A dictionary containing the combined config and secrets.
        """
        self.context = context
        self.before_source_statements = context.get("before_source_statements", [])
        self.after_source_statements = context.get("after_source_statements", [])

    @property
    @abstractmethod
    def source_type(self) -> str:
        """The name used in the config YAML `type` field, e.g., 'postgres'."""
        pass

    @property
    @abstractmethod
    def required_plugins(self) -> list[str]:
        """A list of DuckDB extensions needed for this source."""
        pass

    @classmethod
    @abstractmethod
    def validate(cls, config: dict[str, Any], secret_name: str | None = None, resolve_secrets: bool = False):
        """
        Validates the source-specific configuration.
        Must be implemented by subclasses.
        """
        pass

    @abstractmethod
    def render_sql(self) -> str:
        """
        Renders the setup SQL using the stored context.

        Returns:
            The final, executable SQL string.
        """
        pass
