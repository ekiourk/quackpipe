"""
This module defines Provider classes that act as adaptors between a standard
Source Handler and the specific requirements of the DuckLakeHandler.
"""
from abc import ABC, abstractmethod
from typing import Any

from quackpipe.sources.base import BaseSourceHandler
from quackpipe.sources.postgres import PostgresHandler
from quackpipe.sources.s3 import S3Handler
from quackpipe.sources.sqlite import SQLiteHandler

# --- Provider Interfaces ---

class CatalogProvider(ABC):
    """An interface for classes that can provide catalog setup for a DuckLake."""
    # This defines the contract for type checkers without causing instantiation errors.
    handler: BaseSourceHandler

    @property
    @abstractmethod
    def required_plugins(self) -> list[str]:
        """A list of DuckDB extensions needed for this provider."""
        pass

    @abstractmethod
    def render_catalog_setup_sql(self, duckdb_secret_name: str) -> str:
        """Renders only the prerequisite SQL needed for this catalog."""
        pass

    @abstractmethod
    def get_ducklake_catalog_reference(self, duckdb_secret_name: str) -> str:
        """Returns the string used to reference this catalog in the ATTACH statement."""
        pass

class StorageProvider(ABC):
    """An interface for classes that can provide storage setup for a DuckLake."""
    handler: BaseSourceHandler

    @property
    @abstractmethod
    def required_plugins(self) -> list[str]:
        """A list of DuckDB extensions needed for this provider."""
        pass

    @abstractmethod
    def render_storage_setup_sql(self, duckdb_secret_name: str) -> str:
        """Renders only the prerequisite SQL needed for this storage backend."""
        pass

# --- Provider Implementations ---

class PostgresCatalogProvider(CatalogProvider):
    """A CatalogProvider that uses a PostgresHandler internally."""

    handler: PostgresHandler

    def __init__(self, context: dict[str, Any]):
        # Composition: Create an instance of the handler to delegate to.
        self.handler = PostgresHandler(context)

    @property
    def required_plugins(self) -> list[str]:
        return self.handler.required_plugins

    def render_catalog_setup_sql(self, duckdb_secret_name: str) -> str:
        # Delegate the call to the handler's internal method.
        return self.handler.render_create_secret_sql(duckdb_secret_name)

    def get_ducklake_catalog_reference(self, duckdb_secret_name: str) -> str:
        return f"postgres:{duckdb_secret_name}"

class SQLiteCatalogProvider(CatalogProvider):
    """A CatalogProvider that uses a SQLiteHandler internally."""
    def __init__(self, context: dict[str, Any]):
        self.handler = SQLiteHandler(context)

    @property
    def required_plugins(self) -> list[str]:
        return self.handler.required_plugins

    def render_catalog_setup_sql(self, duckdb_secret_name: str) -> str:
        # SQLite needs no secret, so it returns an empty string.
        return ""

    def get_ducklake_catalog_reference(self, duckdb_secret_name: str) -> str:
        db_path = self.handler.context.get('path')
        if not db_path:
            raise ValueError("SQLite catalog requires a 'path' in its configuration.")
        return f"sqlite:{db_path}"

class S3StorageProvider(StorageProvider):
    """A StorageProvider that uses an S3Handler internally."""

    handler: S3Handler

    def __init__(self, context: dict[str, Any]):
        self.handler = S3Handler(context)

    @property
    def required_plugins(self) -> list[str]:
        return self.handler.required_plugins

    def render_storage_setup_sql(self, duckdb_secret_name: str) -> str:
        # For S3, the setup is to create a secret if one is named.
        if self.handler.context.get('secret_name'):
            return self.handler.render_create_secret_sql(duckdb_secret_name)
        return ""
