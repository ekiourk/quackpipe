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

    @abstractmethod
    def render_sql(self) -> str:
        """
        Renders the setup SQL using the stored context.

        Returns:
            The final, executable SQL string.
        """
        pass
