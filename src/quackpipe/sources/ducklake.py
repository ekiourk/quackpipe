"""Source Handler for DuckLake, combining a catalog and storage."""
from typing import List, Dict, Any

from quackpipe.secrets import fetch_secret_bundle
from quackpipe.sources.base import BaseSourceHandler


class DuckLakeHandler(BaseSourceHandler):
    """
    Handler for a DuckLake source, which combines a metadata catalog
    (like Postgres or SQLite) with a data storage backend (like S3 or local).
    """

    def __init__(self, context: Dict[str, Any]):
        super().__init__(context)
        self.catalog_config = self.context.get('catalog', {})
        self.storage_config = self.context.get('storage', {})

        if not self.catalog_config or not self.storage_config:
            raise ValueError("DuckLake source requires 'catalog' and 'storage' sections in config.")

    @property
    def source_type(self) -> str:
        return "ducklake"

    @property
    def required_plugins(self) -> List[str]:
        """Dynamically determines required plugins based on sub-configs."""
        plugins = {"ducklake"}

        catalog_type = self.catalog_config.get('type')
        if catalog_type == 'postgres':
            plugins.add('postgres')
        elif catalog_type == 'sqlite':
            plugins.add('sqlite')

        storage_type = self.storage_config.get('type')
        if storage_type == 's3':
            plugins.add('httpfs')

        return list(plugins)

    def render_sql(self) -> str:
        """
        Renders SQL to create secrets (if needed) and attach the DuckLake.
        """
        connection_name = self.context['connection_name']
        sql_statements = []

        # --- Part 1: Handle Catalog ---
        catalog_type = self.catalog_config.get('type')
        catalog_secret_name = None
        if catalog_type == 'postgres':
            catalog_secrets = fetch_secret_bundle(self.catalog_config.get('secret_name'))
            catalog_secret_name = f"{connection_name}_catalog_secret"
            catalog_sql_context = {**self.catalog_config, **catalog_secrets}
            catalog_db_name = catalog_sql_context.get('database')
            catalog_host = catalog_sql_context.get('host')

            sql_statements.append(
                f"CREATE OR REPLACE SECRET {catalog_secret_name} ("
                f"  TYPE POSTGRES, HOST '{catalog_host}',"
                f"  PORT {catalog_sql_context.get('port', 5432)}, DATABASE '{catalog_db_name}',"
                f"  USER '{catalog_sql_context.get('user')}', PASSWORD '{catalog_sql_context.get('password')}'"
                f");"
            )
            catalog_reference = f"postgres:dbname={catalog_db_name} host={catalog_host}"
        elif catalog_type == 'sqlite':
            catalog_path = self.catalog_config.get('path')
            if not catalog_path:
                raise ValueError(f"DuckLake source '{connection_name}' with SQLite catalog requires a 'path'.")
            catalog_reference = f"sqlite:{catalog_path}"
        else:
            raise ValueError(f"Unsupported DuckLake catalog type: '{catalog_type}'")

        # --- Part 2: Handle Storage ---
        storage_type = self.storage_config.get('type')
        storage_attach_options = []
        if storage_type == 's3':
            storage_secrets = fetch_secret_bundle(self.storage_config.get('secret_name'))
            storage_secret_name = f"{connection_name}_storage_secret"
            storage_sql_context = {**self.storage_config, **storage_secrets}
            url_style = f"URL_STYLE '{storage_sql_context.get('url_style')}', " if storage_sql_context.get('url_style') else ""
            sql_statements.append(
                f"CREATE OR REPLACE SECRET {storage_secret_name} ("
                f"  TYPE S3, "
                f"  {url_style}"
                f"  USE_SSL {storage_sql_context.get('use_ssl', 'true')}, "
                f"  KEY_ID '{storage_sql_context.get('access_key_id')}', "
                f"  ENDPOINT '{storage_sql_context.get('endpoint')}',"
                f"  SECRET '{storage_sql_context.get('secret_access_key')}', "
                f"  REGION '{storage_sql_context.get('region')}'"
                f");"
            )
        elif storage_type != 'local':
            raise ValueError(f"Unsupported DuckLake storage type: '{storage_type}'")

        data_path = self.storage_config.get('path')
        if not data_path:
            raise ValueError(f"DuckLake source '{connection_name}' requires a 'path' for storage.")
        if catalog_secret_name:
            storage_attach_options.append(f"DATA_PATH '{data_path}', META_SECRET '{catalog_secret_name}'")
        else:
            storage_attach_options.append(f"DATA_PATH '{data_path}'")

        # --- Part 3: Build Final ATTACH Statement ---
        attach_options_str = ", ".join(storage_attach_options)
        attach_sql = f"ATTACH 'ducklake:{catalog_reference}' AS {connection_name} ({attach_options_str});"
        sql_statements.append(attach_sql)

        return "\n".join(sql_statements)
