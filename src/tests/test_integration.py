"""
tests/test_integration.py

This file contains end-to-end integration tests for the quackpipe library,
ensuring that different components work together as expected with a real
DuckDB connection.
"""
import pandas as pd

import quackpipe
from quackpipe import SourceConfig, SourceType


def test_ducklake_with_sqlite_and_local_storage(tmp_path):
    """
    An end-to-end test of a DuckLake source using a SQLite catalog and
    local file storage, validating the full quackpipe.session workflow.
    """
    # 1. Arrange: Set up temporary paths for the database and storage
    catalog_dir = tmp_path / "catalog"
    catalog_dir.mkdir()
    catalog_db_path = catalog_dir / "lake_catalog.db"
    storage_dir = tmp_path / "storage"
    storage_dir.mkdir()

    # 2. Create the SourceConfig programmatically
    configs = [
        SourceConfig(
            name="local_lake",
            type=SourceType.DUCKLAKE,
            config={
                "catalog": {"type": "sqlite", "path": str(catalog_db_path)},
                "storage": {"type": "local", "path": str(storage_dir)}
            }
        )
    ]

    # 3. Act & Assert: Use the session manager to perform real DB operations
    with quackpipe.session(configs=configs) as con:
        assert con is not None, "Connection object should not be None"

        # Create a schema in the lake. In local storage, this creates a directory.
        con.execute("CREATE SCHEMA local_lake.test_schema;")

        # Create a pandas DataFrame to insert into the lake
        df_to_insert = pd.DataFrame({'id': [1, 2, 3], 'value': ['a', 'b', 'c']})

        # Register the DataFrame and create a table in the lake from it
        con.register('temp_df', df_to_insert)
        con.execute("CREATE TABLE local_lake.test_schema.my_table AS SELECT * FROM temp_df;")

        # Verify that the catalog contains the table metadata
        tables_in_catalog = con.execute("SELECT table_name FROM information_schema.tables;").fetchall()
        assert ('my_table',) in tables_in_catalog

    # Create again the connection to the duck lake and check if the data are still there
    with quackpipe.session(configs=configs) as con:
        # Query the data back from the lake to verify it was written correctly
        result_df = con.execute("SELECT * FROM local_lake.test_schema.my_table ORDER BY id;").fetchdf()

        # Assert that the data retrieved matches the data inserted
        pd.testing.assert_frame_equal(df_to_insert, result_df)
