"""
High-level utility functions for common ETL operations.
"""

import logging
from typing import cast

import duckdb
import pandas as pd

from .config import SourceConfig, SourceType, get_configs

# Import the session context manager from core and config loader from utils
from .core import session
from .exceptions import AccessDeniedError, ValidationError

logger = logging.getLogger(__name__)


def to_df(con: duckdb.DuckDBPyConnection, query: str) -> pd.DataFrame:
    """Executes a query and returns the result as a pandas DataFrame."""
    return con.execute(query).fetchdf()


def create_table_from_df(con: duckdb.DuckDBPyConnection, df: pd.DataFrame, table_name: str) -> None:  # noqa: ARG001
    """Creates a new table in DuckDB from a pandas DataFrame, replacing if it exists."""
    con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")


def move_data(
    source_query: str,
    destination_name: str,
    table_name: str,
    config_path: str | None = None,
    configs: list[SourceConfig] | None = None,
    env_file: str | None = None,
    mode: str = "replace",
    file_format: str = "parquet",
    primary_key: str | list[str] | None = None,
) -> None:
    """
    A self-contained utility to move data from a source query to a destination.
    This function creates and manages its own quackpipe session.

    Args:
        source_query: The SELECT query to execute for the source data.
        destination_name: The logical name of the destination source from the config.
        table_name: The name of the table or file to create at the destination.
        config_path: Path to the YAML configuration file. Can also be set via the
            `QUACKPIPE_CONFIG_PATH` environment variable.
        configs: A direct list of SourceConfig objects.
        env_file: Path to an env file to use.
        mode: Write mode. 'replace', 'append', or 'merge'.
        file_format: The file format for file-based destinations (e.g., 'parquet', 'csv').
        primary_key: The primary key(s) to use for 'merge' mode. Can be a string
            or a list of strings.
    """
    # Load all configurations using the shared helper function.
    all_configs = get_configs(config_path, configs)

    # Configure secret provider before validation
    from quackpipe.secrets import configure_secret_provider

    configure_secret_provider(env_file=env_file)

    # Perform pre-flight validation
    from quackpipe.sources import SOURCE_HANDLER_REGISTRY

    for cfg in all_configs:
        HandlerClass = SOURCE_HANDLER_REGISTRY.get(cfg.type)
        if HandlerClass:
            HandlerClass.validate(cfg.config, cfg.secret_name, resolve_secrets=True)

    try:
        # Find the destination config to determine its type.
        dest_config = next(c for c in all_configs if c.name == destination_name)
    except StopIteration as e:
        raise ValueError(f"Destination '{destination_name}' not found in the provided configuration.") from e

    # Hoist 'merge' mode validation
    if mode == "merge" and primary_key is None:
        raise ValidationError("Primary key(s) must be provided for 'merge' mode.")

    # Helper function to generate MERGE SQL
    def _generate_merge_sql(target_table: str, source_q: str, pk: str | list[str]) -> str:
        if not pk:
            raise ValidationError("Primary key(s) must be provided for 'merge' mode.")

        pk_list = [pk] if isinstance(pk, str) else pk
        on_clause = " AND ".join([f"target.{k} = source.{k}" for k in pk_list])

        return f"""
            MERGE INTO {target_table} AS target
            USING ({source_q}) AS source
            ON {on_clause}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT BY NAME;
        """

    # This utility creates its own session to perform the work.
    with session(configs=all_configs, env_file=env_file) as con:
        if dest_config.type == SourceType.S3:
            if mode == "merge":
                raise ValidationError("Mode 'merge' is not supported for S3 file destinations.")
            base_path = dest_config.config.get("path", f"s3://{destination_name}/")
            if not base_path.endswith("/"):
                base_path += "/"
            full_path = f"{base_path}{table_name}.{file_format}"
            sql = f"COPY ({source_query}) TO '{full_path}' (FORMAT {file_format.upper()});"
            con.execute(sql)
            logger.info("Data successfully copied to %s", full_path)

        elif dest_config.type == SourceType.DUCKLAKE:
            full_table_name = f"{destination_name}.{table_name}"
            if mode == "replace":
                sql = f"CREATE OR REPLACE TABLE {full_table_name} AS ({source_query});"
            elif mode == "append":
                sql = f"INSERT INTO {full_table_name} ({source_query});"
            elif mode == "merge":
                sql = _generate_merge_sql(full_table_name, source_query, cast(str | list[str], primary_key))
            else:
                raise ValidationError(f"Invalid mode '{mode}'. Use 'replace', 'append' or 'merge'.")
            con.execute(sql)
            logger.info("Data successfully moved to table %s", full_table_name)

        elif dest_config.type in [SourceType.POSTGRES, SourceType.SQLITE]:
            is_read_only = dest_config.config.get("read_only", True)
            if is_read_only:
                raise AccessDeniedError(
                    f"Cannot write to destination '{destination_name}' because it is configured as read-only. "
                    "To enable writing, set 'read_only: false' in your configuration for this source."
                )

            full_table_name = f"{destination_name}.{table_name}"
            if mode == "replace":
                con.execute(f"DROP TABLE IF EXISTS {full_table_name};")
                sql = f"CREATE TABLE {full_table_name} AS ({source_query});"
            elif mode == "append":
                sql = f"INSERT INTO {full_table_name} ({source_query});"
            elif mode == "merge":
                sql = _generate_merge_sql(full_table_name, source_query, cast(str | list[str], primary_key))
            else:
                raise ValidationError(f"Invalid mode '{mode}'. Use 'replace', 'append' or 'merge'.")
            con.execute(sql)
            logger.info("Data successfully moved to table %s", full_table_name)

        else:
            if mode == "replace":
                sql = f"CREATE OR REPLACE TABLE {table_name} AS ({source_query});"
            elif mode == "append":
                sql = f"INSERT INTO {table_name} ({source_query});"
            elif mode == "merge":
                sql = _generate_merge_sql(table_name, source_query, cast(str | list[str], primary_key))
            else:
                raise ValidationError(f"Invalid mode '{mode}'. Use 'replace', 'append' or 'merge'.")
            con.execute(sql)
            logger.info("Data successfully moved to in-memory table '%s'", table_name)
