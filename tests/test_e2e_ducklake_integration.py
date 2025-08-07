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

from quackpipe import QuackpipeBuilder, SourceConfig
from quackpipe.etl_utils import move_data

# ==================== END-TO-END INTEGRATION TEST ====================

def test_e2e_postgres_to_ducklake(
        quackpipe_with_pg_source: QuackpipeBuilder,
        postgres_s3_ducklake_config: SourceConfig,
        test_datasets: dict
):
    """
    Tests a full ETL pipeline moving multiple tables from a Postgres source to a DuckLake
    destination (which itself uses Postgres for catalog and MinIO for storage).
    """

    # Programmatically configure quackpipe using the Builder
    builder = (
        quackpipe_with_pg_source  # this already contains the pg_source configuration
        .add_source_config(postgres_s3_ducklake_config)
    )

    # Move data from the pre-populated Postgres source to the DuckLake destination
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

    # ASSERT: Verify the data arrived correctly in the DuckLake
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
