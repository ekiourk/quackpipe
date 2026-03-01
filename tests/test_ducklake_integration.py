"""
This file contains end-to-end integration tests for the quackpipe library,
ensuring that different components work together as expected with a real
DuckDB connection.
"""
import pandas as pd

import quackpipe


def assert_ducklake_works(**session_kwargs):
    # Use the session manager to perform real DB operations
    with quackpipe.session(**session_kwargs) as con:
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
    with quackpipe.session(**session_kwargs) as con:
        # Query the data back from the lake to verify it was written correctly
        result_df = con.execute("SELECT * FROM local_lake.test_schema.my_table ORDER BY id;").fetchdf()

        # Assert that the data retrieved matches the data inserted
        pd.testing.assert_frame_equal(df_to_insert, result_df)


def assert_merge_adjacent_files_works(**session_kwargs):
    with quackpipe.session(**session_kwargs) as con:
        con.execute("""
CREATE SCHEMA local_lake.test_schema;

CREATE TABLE local_lake.test_schema.sales_data (
    sale_id INTEGER,
    product_name VARCHAR,
    country VARCHAR,
);

ALTER TABLE local_lake.test_schema.sales_data SET PARTITIONED BY (product_name, country);

INSERT INTO local_lake.test_schema.sales_data VALUES
    (1, 'Laptop', 'UK'),
    (2, 'Mouse', 'GR');

INSERT INTO local_lake.test_schema.sales_data VALUES
    (3, 'Monitor', 'ES'),
    (4, 'Laptop', 'UK');

        -- Verify the data
        SELECT * FROM local_lake.test_schema.sales_data;
        """).df()

        duplicate = con.execute("""
        WITH partition_pivot AS (
            SELECT
                table_id,
                data_file_id,
                -- build a composite partition key across all partition_key_index
                string_agg(
                    partition_key_index || '=' || partition_value,
                    ',' ORDER BY partition_key_index
                ) AS partition_spec
            FROM __ducklake_metadata_local_lake.ducklake_file_partition_value
            GROUP BY table_id, data_file_id
        )
        SELECT
            table_id,
            partition_spec,
            array_agg(data_file_id ORDER BY data_file_id) AS files_in_partition
        FROM partition_pivot
        GROUP BY table_id, partition_spec
        HAVING count(*) > 1
        ORDER BY table_id, partition_spec;
        """).df()

        assert len(list(duplicate['files_in_partition'])) == 1
        # NOTE: This assertion relies on internal data file ID generation order.
        # If DuckLake's internal ID logic changes, these specific IDs {1, 4} may need update.
        assert set(list(duplicate['files_in_partition'])[0]) == {1, 4}

        files_list_before = con.execute("SELECT * FROM ducklake_list_files('local_lake', 'sales_data', schema => 'test_schema');").df()
        assert len(files_list_before) == 4

        # Perform the merge
        con.execute("CALL local_lake.merge_adjacent_files();").df()

        # Verify that the two files in the 'Laptop/UK' partition were merged into one
        files_list_after = con.execute("SELECT * FROM ducklake_list_files('local_lake', 'sales_data', schema => 'test_schema');").df()
        assert len(files_list_after) == 3



def test_ducklake_with_sqlite_and_local_storage(local_ducklake_config):
    """
    An end-to-end test of a DuckLake source using a SQLite catalog and
    local file storage, validating the full quackpipe.session workflow.
    """
    assert_ducklake_works(configs=[local_ducklake_config])
    assert_merge_adjacent_files_works(configs=[local_ducklake_config])


def test_ducklake_with_postgres_and_s3_storage(quackpipe_config_files, postgres_container, minio_container):

    source_config = {
        "catalog": {
            "type": "postgres",
            "secret_name": "LAKE_CATALOG",
            "database": "test",
            "host": postgres_container.get_container_host_ip(),
            "port": str(postgres_container.get_exposed_port(5432)),
            "read_only": False
        },
        "storage": {
            "type": "s3",
            "secret_name": "STORAGE",
            "path": "s3://test-bucket/",
            "endpoint": minio_container.get_config()["endpoint"],
            "use_ssl": False,
            "url_style": "path"
        }
    }

    env_vars = {
        "LAKE_CATALOG_USER": "test",
        "LAKE_CATALOG_PASSWORD": "test",
        "STORAGE_ACCESS_KEY_ID": minio_container.access_key,
        "STORAGE_SECRET_ACCESS_KEY": minio_container.secret_key
    }

    config_file, env_file = quackpipe_config_files(source_config, env_vars, source_name="local_lake", source_type="ducklake")

    assert_ducklake_works(
        config_path=str(config_file),
        env_file=str(env_file),
        sources=["local_lake"],
    )

    assert_merge_adjacent_files_works(
        config_path=str(config_file),
        env_file=str(env_file),
        sources=["local_lake"],
    )


def test_ducklake_with_postgres_and_local_storage(quackpipe_config_files, postgres_container, tmp_path):
    # create a new databse so it doesnt collide with other tests in the module that already have creates a ducklake
    postgres_container.get_wrapped_container().exec_run(
        f'psql -U {postgres_container.username} -c "CREATE DATABASE test_with_local_storage"'
    )

    storage_dir = tmp_path / "storage"
    storage_dir.mkdir()

    source_config = {
        "catalog": {
            "type": "postgres",
            "secret_name": "LAKE_CATALOG",
            "database": "test_with_local_storage",
            "host": postgres_container.get_container_host_ip(),
            "port": str(postgres_container.get_exposed_port(5432)),
            "read_only": False
        },
        "storage": {"type": "local", "path": str(storage_dir)}
    }

    env_vars = {
        "LAKE_CATALOG_USER": postgres_container.username,
        "LAKE_CATALOG_PASSWORD": "test"
    }

    config_file, env_file = quackpipe_config_files(source_config, env_vars, source_name="local_lake", source_type="ducklake")

    assert_ducklake_works(
        config_path=str(config_file),
        env_file=str(env_file),
        sources=["local_lake"],
    )

    assert_merge_adjacent_files_works(
        config_path=str(config_file),
        env_file=str(env_file),
        sources=["local_lake"],
    )


def test_ducklake_with_sqlite_and_s3_storage(quackpipe_config_files, postgres_container, minio_container, tmp_path):
    catalog_dir = tmp_path / "catalog"
    catalog_dir.mkdir()
    catalog_db_path = catalog_dir / "lake_catalog.db"

    source_config = {
        "catalog": {"type": "sqlite", "path": str(catalog_db_path)},
        "storage": {
            "type": "s3",
            "secret_name": "STORAGE",
            "path": "s3://test-bucket/",
            "endpoint": minio_container.get_config()["endpoint"],
            "use_ssl": False,
            "url_style": "path"
        }
    }

    env_vars = {
        "STORAGE_ACCESS_KEY_ID": minio_container.access_key,
        "STORAGE_SECRET_ACCESS_KEY": minio_container.secret_key
    }

    config_file, env_file = quackpipe_config_files(source_config, env_vars, source_name="local_lake", source_type="ducklake")

    assert_ducklake_works(
        config_path=str(config_file),
        env_file=str(env_file),
        sources=["local_lake"],
    )

    assert_merge_adjacent_files_works(
        config_path=str(config_file),
        env_file=str(env_file),
        sources=["local_lake"],
    )
