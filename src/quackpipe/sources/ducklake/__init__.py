"""Source Handler for DuckLake, combining a catalog and storage."""

import re
from typing import Any

import duckdb

from quackpipe.validation_utils import get_merged_params, validate_required_fields

from ...exceptions import ConfigError, DuckLakeMigrationError, QuackpipeError, ValidationError
from ..base import BaseSourceHandler
from ..s3 import S3Handler
from .providers import (
    CatalogProvider,
    PostgresCatalogProvider,
    S3StorageProvider,
    SQLiteCatalogProvider,
    StorageProvider,
)

# DuckLake 1.0 (shipped with DuckDB 1.5.2) stopped migrating older catalogs
# implicitly and introduced the AUTOMATIC_MIGRATION attach option. Older
# DuckLake extensions reject the option as unknown.
MIN_DUCKDB_FOR_AUTOMATIC_MIGRATION = (1, 5, 2)

# Substring of the error DuckDB raises when the catalog was written by an older
# DuckLake version than the loaded extension requires.
CATALOG_VERSION_MISMATCH_MARKER = "DuckLake catalog version mismatch"


def installed_duckdb_version() -> tuple[int, int, int]:
    """Returns the installed DuckDB version as a (major, minor, patch) tuple."""
    match = re.match(r"(\d+)\.(\d+)\.(\d+)", duckdb.__version__)
    if not match:
        return (0, 0, 0)
    return (int(match.group(1)), int(match.group(2)), int(match.group(3)))


def supports_automatic_migration() -> bool:
    """Whether the installed DuckDB ships a DuckLake extension that accepts AUTOMATIC_MIGRATION."""
    return installed_duckdb_version() >= MIN_DUCKDB_FOR_AUTOMATIC_MIGRATION


def _automatic_migration_unsupported_message() -> str:
    required = ".".join(str(part) for part in MIN_DUCKDB_FOR_AUTOMATIC_MIGRATION)
    return (
        f"'automatic_migration' requires DuckDB >= {required} (DuckLake 1.0), "
        f"but the installed DuckDB is {duckdb.__version__}. Remove the option to keep "
        "using the existing catalog with this DuckDB version."
    )


class DuckLakeHandler(BaseSourceHandler):
    """
    Handler for a DuckLake source. It uses dedicated Provider classes
    to manage its catalog and storage components.
    """

    def __init__(self, context: dict[str, Any]):
        super().__init__(context)
        self.catalog_config = self.context.get("catalog", {})
        self.storage_config = self.context.get("storage", {})

        # Instantiate the appropriate provider classes
        self.catalog_provider: CatalogProvider = self._get_catalog_provider()
        self.storage_provider: StorageProvider | None = self._get_storage_provider()

    @classmethod
    def validate(cls, config: dict[str, Any], secret_name: str | None = None, resolve_secrets: bool = False) -> None:
        """Validates DuckLake configuration parameters."""
        params = get_merged_params(config, secret_name, resolve_secrets)
        catalog_config = params.get("catalog", {})
        storage_config = params.get("storage", {})

        if not catalog_config:
            raise ValidationError("DuckLake source requires a 'catalog' section.")
        if not storage_config:
            raise ValidationError("DuckLake source requires a 'storage' section.")

        validate_required_fields(storage_config, ["path"], "ducklake storage", secret_name, resolve_secrets)

        catalog_type = catalog_config.get("type")
        if catalog_type not in ["postgres", "sqlite"]:
            raise ValidationError(
                f"Unsupported DuckLake catalog type: '{catalog_type}'. Must be 'postgres' or 'sqlite'."
            )

        if catalog_type == "sqlite":
            validate_required_fields(catalog_config, ["path"], "ducklake sqlite catalog", secret_name, resolve_secrets)

        if catalog_config.get("automatic_migration") and not supports_automatic_migration():
            raise ValidationError(_automatic_migration_unsupported_message())

    def _get_catalog_provider(self) -> CatalogProvider:
        """Factory function to create the catalog provider instance."""
        catalog_type = self.catalog_config.get("type")
        if catalog_type == "postgres":
            return PostgresCatalogProvider(self.catalog_config)
        elif catalog_type == "sqlite":
            return SQLiteCatalogProvider(self.catalog_config)
        raise ConfigError(f"Unsupported DuckLake catalog type: '{catalog_type}'")

    def _get_storage_provider(self) -> StorageProvider | None:
        """Factory function to create the storage provider instance."""
        storage_type = self.storage_config.get("type")
        if storage_type == "s3":
            return S3StorageProvider(self.storage_config)
        elif storage_type == "local":
            return None  # Local storage needs no setup SQL
        raise ConfigError(f"Unsupported DuckLake storage type: '{storage_type}'")

    @property
    def source_type(self) -> str:
        return "ducklake"

    @property
    def required_plugins(self) -> list[str]:
        """Dynamically determines required plugins by delegating to the provider instances."""
        plugins = {"ducklake"}
        plugins.update(self.catalog_provider.required_plugins)
        if self.storage_provider:
            plugins.update(self.storage_provider.required_plugins)
        return list(plugins)

    def render_sql(self) -> str:
        """
        Orchestrates the SQL generation by delegating to the provider classes.
        """
        connection_name = self.context["connection_name"]
        ducklake_secret_name = f"{connection_name}_secret"
        sql_statements = []

        # --- Part 1: Generate prerequisite secrets ---
        catalog_secret_name = f"{connection_name}_catalog_secret"
        sql_statements.append(self.catalog_provider.render_catalog_setup_sql(catalog_secret_name))

        if self.storage_provider and isinstance(self.storage_provider.handler, S3Handler):
            s3_handler = self.storage_provider.handler
            # If a secret_name is provided for S3, create a named secret.
            if s3_handler.context.get("secret_name"):
                storage_secret_name = f"{connection_name}_storage_secret"
                sql_statements.append(s3_handler.render_create_secret_sql(storage_secret_name))
            # Otherwise, generate SET commands for direct configuration (e.g., MinIO).
            else:
                sql_statements.append(s3_handler._render_set_commands_sql())

        # --- Part 2: Generate the main DUCKLAKE secret ---
        data_path = self.storage_config.get("path")

        ducklake_secret_parts = [
            f"CREATE OR REPLACE SECRET {ducklake_secret_name} (",
            "    TYPE DUCKLAKE,",
            f"   DATA_PATH '{data_path}'",
        ]

        if self.catalog_config.get("type") == "postgres":
            metadata_params = f"MAP {{'TYPE': 'postgres', 'SECRET': '{catalog_secret_name}'}}"
            ducklake_secret_parts.append(f",   METADATA_PARAMETERS {metadata_params}")
            ducklake_secret_parts.append(",   METADATA_PATH ''")
        elif self.catalog_config.get("type") == "sqlite":
            metadata_path = self.catalog_config.get("path")
            ducklake_secret_parts.append(f",   METADATA_PATH '{metadata_path}'")

            # Pass encryption key if present for SQLite catalog
            encryption_key = self.catalog_config.get("encryption_key")
            if encryption_key:
                ducklake_secret_parts.append(f",   METADATA_PARAMETERS MAP {{'ENCRYPTION_KEY': '{encryption_key}'}}")
        else:
            # Defensive check
            raise ConfigError(f"Unsupported DuckLake catalog type in render_sql: '{self.catalog_config.get('type')}'")

        ducklake_secret_parts.append(");")
        sql_statements.append("\n".join(ducklake_secret_parts))

        # --- Part 3: Generate the final ATTACH statement ---
        attach_sql = f"ATTACH 'ducklake:{ducklake_secret_name}' AS {connection_name}{self._render_attach_options()};"
        sql_statements.append(attach_sql)

        # Filter out any empty strings from providers that don't produce SQL
        return "\n".join(filter(None, sql_statements))

    def _render_attach_options(self) -> str:
        """
        Renders the optional ATTACH option list. Returns an empty string when no
        option is configured so the emitted SQL is unchanged for existing configs
        and stays valid on DuckDB versions that predate these options.
        """
        options = []
        if self.catalog_config.get("automatic_migration"):
            if not supports_automatic_migration():
                raise ConfigError(_automatic_migration_unsupported_message())
            options.append("AUTOMATIC_MIGRATION")
        return f" ({', '.join(options)})" if options else ""

    def translate_error(self, error: Exception) -> QuackpipeError | None:
        """Turns DuckDB's catalog version mismatch into an actionable DuckLakeMigrationError."""
        if CATALOG_VERSION_MISMATCH_MARKER not in str(error):
            return None
        connection_name = self.context.get("connection_name", "ducklake")
        duckdb_message = str(error).strip().splitlines()[0]
        return DuckLakeMigrationError(
            f"The DuckLake catalog of source '{connection_name}' was created by an older DuckLake version "
            f"and must be migrated before DuckDB {duckdb.__version__} can open it ({duckdb_message}). "
            "Migration is one-way: clients running an older DuckDB will no longer be able to open this lake. "
            "Before migrating: (1) back up the catalog (copy the catalog file, or dump the catalog database); "
            "(2) upgrade every client that uses this lake to the same DuckDB version; "
            f"(3) set 'automatic_migration: true' in the 'catalog' section of source '{connection_name}' "
            "for one run, then remove it."
        )
