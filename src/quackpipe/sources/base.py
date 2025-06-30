"""
Abstract Base Class for all Source Handlers.
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any

class BaseSourceHandler(ABC):
    """
    Abstract base class for a data source handler.

    Each handler is responsible for:
    1. Declaring the DuckDB plugins it requires.
    2. Providing a SQL template for its setup.
    3. Rendering the template with configuration and secrets.
    """
    @property
    @abstractmethod
    def source_type(self) -> str:
        """The name used in the config YAML `type` field, e.g., 'postgres'."""
        pass

    @property
    @abstractmethod
    def required_plugins(self) -> List[str]:
        """List of DuckDB extensions needed, e.g., ['postgres', 'httpfs']."""
        pass

    @abstractmethod
    def render_sql(self, context: Dict[str, Any]) -> str:
        """
        Renders the setup SQL by populating the template with config and secrets.
        Subclasses can override this for more complex rendering logic.

        Args:
            context: A dictionary containing the combined config and secrets.

        Returns:
            The final, executable SQL string.
        """
        pass
