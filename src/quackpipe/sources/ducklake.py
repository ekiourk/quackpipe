"""Source Handler for DuckLake, combining a catalog and storage."""
from .base import BaseSourceHandler
from ..secrets import fetch_secret_bundle
from typing import List, Dict, Any


class DuckLakeHandler(BaseSourceHandler):
    """
    Handler for a DuckLake source, which combines a metadata catalog
    (like Postgres) with a data storage backend (like S3).
    """

    @property
    def source_type(self):
        return "ducklake"

    @property
    def required_plugins(self) -> List[str]:
        # Requires plugins for the lake itself, the catalog, and the storage
        return ["ducklake", "postgres", "httpfs"]

    def render_sql(self, context: Dict[str, Any]) -> str:
        """
        Renders SQL to create secrets for both catalog and storage,
        then attaches the DuckLake.
        """
        connection_name = context['connection_name']

        # Extract catalog and storage configs from the main context
        catalog_config = context.get('catalog', {})
        storage_config = context.get('storage', {})

        if not catalog_config or not storage_config:
            raise ValueError("DuckLake source requires 'catalog' and 'storage' sections in config.")

        # 1. Fetch secrets for both components
        catalog_secrets = fetch_secret_bundle(catalog_config.get('secret_name'))
        storage_secrets = fetch_secret_bundle(storage_config.get('secret_name'))

        # 2. Generate CREATE SECRET statements
        # Note: DuckLake requires secrets to be created with specific names.
        catalog_secret_name = f"{connection_name}_catalog_secret"
        storage_secret_name = f"{connection_name}_storage_secret"

        # SQL for catalog secret
        catalog_sql_context = {**catalog_config, **catalog_secrets}
        create_catalog_secret_sql = (
            f"CREATE OR REPLACE SECRET {catalog_secret_name} ("
            f"  TYPE POSTGRES,"
            f"  HOST '{catalog_sql_context.get('host')}',"
            f"  PORT {catalog_sql_context.get('port', 5432)},"
            f"  DATABASE '{catalog_sql_context.get('database')}',"
            f"  USER '{catalog_sql_context.get('user')}',"
            f"  PASSWORD '{catalog_sql_context.get('password')}'"
            f");"
        )

        # SQL for storage secret
        storage_sql_context = {**storage_config, **storage_secrets}
        create_storage_secret_sql = (
            f"CREATE OR REPLACE SECRET {storage_secret_name} ("
            f"  TYPE S3,"
            f"  KEY_ID '{storage_sql_context.get('access_key_id')}',"
            f"  SECRET '{storage_sql_context.get('secret_access_key')}',"
            f"  REGION '{storage_sql_context.get('region')}'"
            f");"
        )

        # 3. Generate the final ATTACH statement
        catalog_type = catalog_config.get('type', 'postgres')
        data_path = storage_config.get('path')

        attach_sql = (
            f"ATTACH 'ducklake:{catalog_type}:{catalog_secret_name}' AS {connection_name} ("
            f"  DATA_PATH '{data_path}',"
            f"  STORAGE_SECRET '{storage_secret_name}'"
            f");"
        )

        # Return all statements combined
        return "\n".join([
            create_catalog_secret_sql,
            create_storage_secret_sql,
            attach_sql
        ])
