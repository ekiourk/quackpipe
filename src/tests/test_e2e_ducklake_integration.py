"""
tests/test_e2e_ducklake_integration.py

This file contains true end-to-end integration tests using testcontainers
to spin up real services like PostgreSQL and MinIO in Docker.

NOTE: To run these tests, you must have Docker installed and running.
You will also need to install the required test dependencies.

Installation command:
pip install "quackpipe[test]" "testcontainers-postgres>=3.7.0" "testcontainers-minio>=2.3.1" "httpx>=0.23.0"
"""
import pandas as pd
from testcontainers.minio import MinioContainer
from testcontainers.postgres import PostgresContainer

from quackpipe import QuackpipeBuilder, SourceType
from quackpipe.etl_utils import move_data


# Assume the fixtures you provided are in a conftest.py and available
# (source_postgres_container, catalog_postgres_container, minio_container, test_datasets)

# ==================== END-TO-END INTEGRATION TEST ====================

def test_e2e_postgres_to_ducklake(
        source_postgres_container: PostgresContainer,
        catalog_postgres_container: PostgresContainer,
        minio_container: MinioContainer,
        test_datasets: dict
):
    """
    Tests a full ETL pipeline moving multiple tables from a Postgres source to a DuckLake
    destination (which itself uses Postgres for catalog and MinIO for storage).
    """
    # 1. ARRANGE: Get dynamic connection details from all running containers

    # Source Postgres details
    source_pg_host = source_postgres_container.get_container_host_ip()
    source_pg_port = source_postgres_container.get_exposed_port(5432)
    source_pg_user = source_postgres_container.POSTGRES_USER
    source_pg_pass = source_postgres_container.POSTGRES_PASSWORD
    source_pg_db = source_postgres_container.POSTGRES_DB

    # Catalog Postgres details
    catalog_pg_host = catalog_postgres_container.get_container_host_ip()
    catalog_pg_port = catalog_postgres_container.get_exposed_port(5432)
    catalog_pg_user = catalog_postgres_container.POSTGRES_USER
    catalog_pg_pass = catalog_postgres_container.POSTGRES_PASSWORD
    catalog_pg_db = catalog_postgres_container.POSTGRES_DB

    # MinIO storage details
    minio_endpoint = minio_container.get_config()["endpoint"]
    minio_access_key = minio_container.get_config()["access_key"]
    minio_secret_key = minio_container.get_config()["secret_key"]

    # 2. ARRANGE: Programmatically configure quackpipe using the Builder
    builder = (
        QuackpipeBuilder()
        .add_source(
            name="pg_source",
            type=SourceType.POSTGRES,
            config={
                "read_only": False,  # Source is read-only for this test, but fixture creates it as writeable
                "host": source_pg_host,
                "port": int(source_pg_port),
                "user": source_pg_user,
                "password": source_pg_pass,
                "database": source_pg_db
            }
        )
        .add_source(
            name="my_datalake",
            type=SourceType.DUCKLAKE,
            config={
                "catalog": {
                    "type": "postgres",
                    "host": catalog_pg_host,
                    "port": int(catalog_pg_port),
                    "user": catalog_pg_user,
                    "password": catalog_pg_pass,
                    "database": catalog_pg_db
                },
                "storage": {
                    "type": "s3",
                    "path": "s3://test-lake/",
                    "endpoint": minio_endpoint,
                    "access_key_id": minio_access_key,
                    "secret_access_key": minio_secret_key,
                    "use_ssl": False,
                    "url_style": "path"  # Important for MinIO
                }
            }
        )
    )

    # 3. ACT: Move data from the pre-populated Postgres source to the DuckLake destination

    # Move the 'employees' table
    print("Moving 'employees' table to DuckLake...")
    move_data(
        configs=builder.get_configs(),
        source_query="SELECT * FROM pg_source.company.employees",
        destination_name="my_datalake",
        table_name="employees_archive",
        mode="replace"
    )

    # Move the 'vessels' table
    print("Moving 'vessels' table to DuckLake...")
    move_data(
        configs=builder.get_configs(),
        source_query="SELECT * FROM pg_source.public.vessels",
        destination_name="my_datalake",
        table_name="vessels_archive",
        mode="replace"
    )

    # 4. ASSERT: Verify the data arrived correctly in the DuckLake
    with builder.session(sources=["my_datalake"]) as con:
        # Verify employees table
        print("Verifying 'employees_archive' table...")
        employees_result_df = con.execute("SELECT * FROM my_datalake.employees_archive ORDER BY id;").fetchdf()
        expected_employees_df = test_datasets['employees'].sort_values(by='id').reset_index(drop=True)
        pd.testing.assert_frame_equal(expected_employees_df, employees_result_df)
        print("'employees_archive' table verified successfully.")

        # Verify vessels table
        print("Verifying 'vessels_archive' table...")
        vessels_result_df = con.execute("SELECT * FROM my_datalake.vessels_archive ORDER BY mmsi;").fetchdf()
        expected_vessels_df = test_datasets['vessels'].sort_values(by='mmsi').reset_index(drop=True)
        pd.testing.assert_frame_equal(expected_vessels_df, vessels_result_df)
        print("'vessels_archive' table verified successfully.")

    print("\nIntegration test successful: Data moved from Postgres to DuckLake correctly.")
