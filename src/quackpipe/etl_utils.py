"""
High-level utility functions for common ETL operations.
"""
import logging

import duckdb
import pandas as pd

from .config import SourceConfig, SourceType

# Import the session context manager from core and config loader from utils
from .core import session
from .utils import get_configs

logger = logging.getLogger(__name__)


def to_df(con: duckdb.DuckDBPyConnection, query: str) -> pd.DataFrame:
    """Executes a query and returns the result as a pandas DataFrame."""
    return con.execute(query).fetchdf()


def create_table_from_df(con: duckdb.DuckDBPyConnection, df: pd.DataFrame, table_name: str):
    """Creates a new table in DuckDB from a pandas DataFrame, replacing if it exists."""
    con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")


def move_data(
        source_query: str,
        destination_name: str,
        table_name: str,
        config_path: str | None = None,
        configs: list[SourceConfig] | None = None,
        env_file: str | None = None,
        mode: str = 'replace',
        format: str = 'parquet'
):
    """
    A self-contained utility to move data from a source query to a destination.
    This function creates and manages its own quackpipe session.

    Args:
        source_query: The SELECT query to execute for the source data.
        destination_name: The logical name of the destination source from the config.
        table_name: The name of the table or file to create at the destination.
        config_path: Path to the YAML configuration file.
        configs: A direct list of SourceConfig objects.
        env_file: Path to an env file to use.
        mode: Write mode. 'replace' or 'append'.
        format: The file format for file-based destinations (e.g., 'parquet', 'csv').
    """
    # Load all configurations using the shared helper function.
    all_configs = get_configs(config_path, configs)

    try:
        # Find the destination config to determine its type.
        dest_config = next(c for c in all_configs if c.name == destination_name)
    except StopIteration as e:
        raise ValueError(f"Destination '{destination_name}' not found in the provided configuration.") from e

    # This utility creates its own session to perform the work.
    with session(configs=all_configs, env_file=env_file) as con:
        if dest_config.type == SourceType.S3:
            base_path = dest_config.config.get('path', f"s3://{destination_name}/")
            if not base_path.endswith('/'):
                base_path += '/'
            full_path = f"{base_path}{table_name}.{format}"
            sql = f"COPY ({source_query}) TO '{full_path}' (FORMAT {format.upper()});"
            con.execute(sql)
            logger.info("Data successfully copied to %s", full_path)

        elif dest_config.type == SourceType.DUCKLAKE:
            full_table_name = f"{destination_name}.{table_name}"
            if mode == 'replace':
                sql = f"CREATE OR REPLACE TABLE {full_table_name} AS ({source_query});"
            elif mode == 'append':
                sql = f"INSERT INTO {full_table_name} ({source_query});"
            else:
                raise ValueError(f"Invalid mode '{mode}'. Use 'replace' or 'append'.")
            con.execute(sql)
            logger.info("Data successfully moved to table %s", full_table_name)

        elif dest_config.type in [SourceType.POSTGRES, SourceType.SQLITE]:
            is_read_only = dest_config.config.get('read_only', True)
            if is_read_only:
                raise PermissionError(
                    f"Cannot write to destination '{destination_name}' because it is configured as read-only. "
                    "To enable writing, set 'read_only: false' in your configuration for this source."
                )

            full_table_name = f"{destination_name}.{table_name}"
            if mode == 'replace':
                con.execute(f"DROP TABLE IF EXISTS {full_table_name};")
                sql = f"CREATE TABLE {full_table_name} AS ({source_query});"
            elif mode == 'append':
                sql = f"INSERT INTO {full_table_name} ({source_query});"
            else:
                raise ValueError(f"Invalid mode '{mode}'. Use 'replace' or 'append'.")
            con.execute(sql)
            logger.info("Data successfully moved to table %s", full_table_name)

        else:
            if mode == 'replace':
                sql = f"CREATE OR REPLACE TABLE {table_name} AS ({source_query});"
            elif mode == 'append':
                sql = f"INSERT INTO {table_name} ({source_query});"
            else:
                raise ValueError(f"Invalid mode '{mode}'. Use 'replace' or 'append'.")
            con.execute(sql)
            logger.info("Data successfully moved to in-memory table '%s'", table_name)
