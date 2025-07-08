"""Source Handler for DuckLake, combining a catalog and storage."""
from typing import List, Dict, Any

from quackpipe.sources.base import BaseSourceHandler
from quackpipe.sources.postgres import PostgresHandler
from quackpipe.sources.s3 import S3Handler
from quackpipe.sources.sqlite import SQLiteHandler
from ..exceptions import ConfigError


class DuckLakeHandler(BaseSourceHandler):
    """
    Handler for a DuckLake source. It reuses other handlers to manage
    its catalog and storage components and creates a single DUCKLAKE secret.
    """

    def __init__(self, context: Dict[str, Any]):
        super().__init__(context)
        self.catalog_config = self.context.get('catalog', {})
        self.storage_config = self.context.get('storage', {})

        if not self.catalog_config or not self.storage_config:
            raise ConfigError("DuckLake source requires 'catalog' and 'storage' sections in config.")

        # Instantiate the appropriate sub-handlers
        self.catalog_handler = self._get_catalog_handler()
        self.storage_handler = self._get_storage_handler()

    def _get_catalog_handler(self) -> BaseSourceHandler:
        """Factory function to create the catalog handler instance."""
        catalog_type = self.catalog_config.get('type')
        if catalog_type == 'postgres':
            return PostgresHandler(self.catalog_config)
        elif catalog_type == 'sqlite':
            return SQLiteHandler(self.catalog_config)
        raise ConfigError(f"Unsupported DuckLake catalog type: '{catalog_type}'")

    def _get_storage_handler(self) -> BaseSourceHandler | None:
        """Factory function to create the storage handler instance."""
        storage_type = self.storage_config.get('type')
        if storage_type == 's3':
            return S3Handler(self.storage_config)
        elif storage_type == 'local':
            return None  # Local storage needs no setup SQL
        raise ConfigError(f"Unsupported DuckLake storage type: '{storage_type}'")

    @property
    def source_type(self) -> str:
        return "ducklake"

    @property
    def required_plugins(self) -> List[str]:
        """Dynamically determines required plugins by delegating to sub-handlers."""
        plugins = {"ducklake"}
        plugins.update(self.catalog_handler.required_plugins)
        if self.storage_handler:
            plugins.update(self.storage_handler.required_plugins)
        return list(plugins)

    def render_sql(self) -> str:
        """
        Orchestrates the SQL generation to create all necessary secrets and
        then construct the final ATTACH statement.
        """
        connection_name = self.context['connection_name']
        ducklake_secret_name = f"{connection_name}_secret"
        sql_statements = []

        # --- Part 1: Generate setup for sub-components ---
        catalog_type = self.catalog_config.get('type')

        if catalog_type == 'postgres':
            # First, create the secret for the Postgres catalog itself.
            postgres_secret_name = f"{connection_name}_catalog_secret"
            sql_statements.append(self.catalog_handler._render_create_secret_sql(postgres_secret_name))

            # The METADATA_PARAMETERS will reference this newly created secret.
            metadata_params = f"MAP {{'TYPE': 'postgres', 'SECRET': '{postgres_secret_name}'}}"
            metadata_path = "''"  # Path is not used when METADATA_PARAMETERS is set
        elif catalog_type == 'sqlite':
            metadata_path = f"'{self.catalog_config.get('path')}'"
            metadata_params = "NULL"  # No parameters needed for SQLite
        else:
            raise ConfigError(f"Unsupported DuckLake catalog type: '{catalog_type}'")

        # Create the S3 secret if needed for the storage backend. This is correct.
        # The httpfs extension will find and use this secret automatically.
        if self.storage_handler and isinstance(self.storage_handler, S3Handler):
            storage_secret_name = f"{connection_name}_storage_secret"
            sql_statements.append(self.storage_handler._render_create_secret_sql(storage_secret_name))

        # --- Part 2: Generate the main DUCKLAKE secret ---
        data_path = self.storage_config.get('path')
        if not data_path:
            raise ConfigError(f"DuckLake source '{connection_name}' requires a 'path' for storage.")

        ducklake_secret_parts = [
            f"CREATE OR REPLACE SECRET {ducklake_secret_name} (",
            "    TYPE DUCKLAKE,",
            f"   METADATA_PATH {metadata_path},",
            f"   DATA_PATH '{data_path}'"
        ]

        # Conditionally add METADATA_PARAMETERS only if it's not NULL.
        if metadata_params != "NULL":
            ducklake_secret_parts.append(f",   METADATA_PARAMETERS {metadata_params}")

        ducklake_secret_parts.append(");")
        sql_statements.append("\n".join(ducklake_secret_parts))

        # --- Part 3: Generate the final ATTACH statement ---
        attach_sql = f"ATTACH 'ducklake:{ducklake_secret_name}' AS {connection_name};"
        sql_statements.append(attach_sql)

        return "\n".join(sql_statements)
