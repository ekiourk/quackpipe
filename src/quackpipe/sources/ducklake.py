"""Source Handler for DuckLake, combining a catalog and storage."""
from .base import BaseSourceHandler
from ..secrets import fetch_secret_bundle
from typing import List, Dict, Any

class DuckLakeHandler(BaseSourceHandler):
    """
    Handler for a DuckLake source, which combines a metadata catalog
    (like Postgres) with a data storage backend (like S3).
    """
    def __init__(self, context: Dict[str, Any]):
        super().__init__(context)
        self.catalog_config = self.context.get('catalog', {})
        self.storage_config = self.context.get('storage', {})

        if not self.catalog_config or not self.storage_config:
            raise ValueError("DuckLake source requires 'catalog' and 'storage' sections in config.")

    @property
    def source_type(self):
        return "ducklake"

    @property
    def required_plugins(self) -> List[str]:
        """Dynamically determines required plugins based on sub-configs."""
        plugins = {"ducklake"}  # Base plugin is always required

        # Dynamically add catalog plugin
        catalog_type = self.catalog_config.get('type')
        if catalog_type == 'postgres':
            plugins.add('postgres')
        # Future extension: elif catalog_type == 'mysql': plugins.add('mysql')

        # Dynamically add storage plugin
        storage_type = self.storage_config.get('type')
        if storage_type in ['s3', 'gcs', 'r2']:  # httpfs handles all of these
            plugins.add('httpfs')

        return list(plugins)

    def render_sql(self) -> str:
        """
        Renders SQL to create secrets for both catalog and storage,
        then attaches the DuckLake.
        """
        connection_name = self.context['connection_name']

        catalog_secrets = fetch_secret_bundle(self.catalog_config.get('secret_name'))
        storage_secrets = fetch_secret_bundle(self.storage_config.get('secret_name'))

        catalog_secret_name = f"{connection_name}_catalog_secret"
        storage_secret_name = f"{connection_name}_storage_secret"

        catalog_sql_context = {**self.catalog_config, **catalog_secrets}
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

        storage_sql_context = {**self.storage_config, **storage_secrets}
        create_storage_secret_sql = (
            f"CREATE OR REPLACE SECRET {storage_secret_name} ("
            f"  TYPE S3,"
            f"  KEY_ID '{storage_sql_context.get('access_key_id')}',"
            f"  SECRET '{storage_sql_context.get('secret_access_key')}',"
            f"  REGION '{storage_sql_context.get('region')}'"
            f");"
        )

        catalog_type = self.catalog_config.get('type', 'postgres')
        data_path = self.storage_config.get('path')

        attach_sql = (
            f"ATTACH 'ducklake:{catalog_type}:{catalog_secret_name}' AS {connection_name} ("
            f"  DATA_PATH '{data_path}',"
            f"  STORAGE_SECRET '{storage_secret_name}'"
            f");"
        )

        return "\n".join([
            create_catalog_secret_sql,
            create_storage_secret_sql,
            attach_sql
        ])
