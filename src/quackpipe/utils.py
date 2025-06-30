"""
Utility functions for common ETL tasks within quackpipe.
"""
import pandas as pd
import duckdb

class ETLUtils:
    """A collection of static methods for common ETL operations."""

    @staticmethod
    def to_df(con: duckdb.DuckDBPyConnection, query: str) -> pd.DataFrame:
        """
        Executes a query and returns the result as a pandas DataFrame.

        Args:
            con: An active DuckDB connection.
            query: The SQL query to execute.

        Returns:
            A pandas DataFrame with the query results.
        """
        return con.execute(query).fetchdf()

    @staticmethod
    def copy(con: duckdb.DuckDBPyConnection, source_query: str, target_path: str, format: str = 'parquet'):
        """
        Copies the result of a query to a file (e.g., in S3 or locally).

        Args:
            con: An active DuckDB connection.
            source_query: A SELECT query providing the data to copy.
            target_path: The destination file path (e.g., 's3://bucket/file.parquet').
            format: The output format (e.g., 'PARQUET', 'CSV').
        """
        con.execute(f"COPY ({source_query}) TO '{target_path}' (FORMAT {format.upper()})")

    @staticmethod
    def create_table_from_df(con: duckdb.DuckDBPyConnection, df: pd.DataFrame, table_name: str):
        """
        Creates a new table in DuckDB from a pandas DataFrame, replacing if it exists.

        Args:
            con: An active DuckDB connection.
            df: The pandas DataFrame to load.
            table_name: The name of the table to create.
        """
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
