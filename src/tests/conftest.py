import io
import os
import tempfile
from unittest.mock import Mock, patch

import pandas as pd
import pytest
import yaml
from sqlalchemy import create_engine, text
from testcontainers.minio import MinioContainer
from testcontainers.postgres import PostgresContainer

from quackpipe import configure_secret_provider
from tests.data_fixtures import (generate_synthetic_ais_data, create_vessel_definitions, create_monthly_data,
                                 create_employee_data, create_ais_summary)


@pytest.fixture(autouse=True)
def reset_secret_provider_fixture():
    """
    This fixture automatically runs before each test in this file. It resets
    the global secret provider, ensuring a clean state and preventing tests
    from interfering with each other's environment variables.
    """
    # This call re-initializes the global provider with the current os.environ
    # at the start of each test function.
    configure_secret_provider(env_file=None)
    yield
    # Optional: reset again after the test for good measure
    configure_secret_provider(env_file=None)


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield tmp_dir


@pytest.fixture
def sample_config_dict():
    """Sample configuration dictionary for testing."""
    return {
        'sources': {
            'pg_main': {
                'type': 'postgres',
                'secret_name': 'pg_prod',
                'port': 5432,
                'read_only': True,
                'tables': ['users', 'orders']
            },
            'datalake': {
                'type': 's3',
                'secret_name': 'aws_datalake',
                'region': 'us-east-1'
            }
        }
    }


@pytest.fixture
def sample_yaml_config(temp_dir, sample_config_dict):
    """Create a temporary YAML config file."""
    config_path = os.path.join(temp_dir, 'test_config.yml')
    with open(config_path, 'w') as f:
        yaml.dump(sample_config_dict, f)
    return config_path


@pytest.fixture
def mock_duckdb_connection():
    """Mock DuckDB connection for testing."""
    mock_con = Mock()
    mock_con.execute = Mock()
    mock_con.install_extension = Mock()
    mock_con.load_extension = Mock()
    mock_con.close = Mock()

    # Mock fetchdf for pandas integration
    mock_result = Mock()
    mock_result.fetchdf.return_value = pd.DataFrame({'id': [1, 2], 'name': ['Alice', 'Bob']})
    mock_con.execute.return_value = mock_result

    return mock_con


# @pytest.fixture
# def mock_duckdb_connection():
#     """Provides a mock DuckDB connection object."""
#     mock_con = MagicMock()
#
#     # Mock the return value for fetchdf() to be a sample DataFrame
#     sample_df = pd.DataFrame({'id': [1, 2], 'name': ['A', 'B']})
#     mock_execute_result = MagicMock()
#     mock_execute_result.fetchdf.return_value = sample_df
#     mock_con.execute.return_value = mock_execute_result
#
#     return mock_con


@pytest.fixture
def env_secrets():
    """Set up environment variables for testing."""
    env_vars = {
        'PG_PROD_HOST': 'localhost',
        'PG_PROD_USER': 'testuser',
        'PG_PROD_PASSWORD': 'testpass',
        'PG_PROD_DATABASE': 'testdb',
        'AWS_DATALAKE_ACCESS_KEY_ID': 'test_key',
        'AWS_DATALAKE_SECRET_ACCESS_KEY': 'test_secret'
    }

    # Set environment variables
    for key, value in env_vars.items():
        os.environ[key] = value

    yield env_vars

    # Clean up
    for key in env_vars:
        os.environ.pop(key, None)


@pytest.fixture
def mock_session(mock_duckdb_connection):
    """A patch fixture for the quackpipe.etl_utils.session context manager."""
    with patch('quackpipe.etl_utils.session') as mock_session_context:
        # Make the context manager yield our mock connection
        mock_session_context.return_value.__enter__.return_value = mock_duckdb_connection
        yield mock_session_context


@pytest.fixture
def mock_get_configs():
    """A patch fixture for the quackpipe.etl_utils.get_configs function."""
    with patch('quackpipe.etl_utils.get_configs') as mock:
        yield mock


# ==================== PYTEST FIXTURES FOR MINIO CONTAINERS ====================

@pytest.fixture(scope="module")
def minio_container_with_data():
    """
    Starts a MinIO container with sample data for testing.
    Creates a bucket with example CSV and Parquet files.
    test-lake/
    ├── data/
    │   ├── employees.csv
    │   ├── employees.parquet
    │   └── monthly_reports.csv
    ├── partitioned/
    │   ├── department=Engineering/employees.csv
    │   ├── department=Marketing/employees.csv
    │   └── department=Sales/employees.csv
    └── external/
        ├── ais_data_synthetic.csv
        ├── ais_data_synthetic.parquet
        └── ais_data_summary.json
    """
    with MinioContainer("minio/minio:RELEASE.2025-06-13T11-33-47Z") as minio:
        client = minio.get_client()

        # Create the test bucket
        bucket_name = "test-lake"
        client.make_bucket(bucket_name)

        # Generate all datasets
        employee_data = create_employee_data()
        monthly_data = create_monthly_data()
        vessels = create_vessel_definitions()

        # Create DataFrames
        df = pd.DataFrame(employee_data)
        monthly_df = pd.DataFrame(monthly_data)

        # Upload employee CSV file
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue().encode('utf-8')

        client.put_object(
            bucket_name=bucket_name,
            object_name="data/employees.csv",
            data=io.BytesIO(csv_data),
            length=len(csv_data),
            content_type="text/csv"
        )

        # Upload employee Parquet file
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_data = parquet_buffer.getvalue()

        client.put_object(
            bucket_name=bucket_name,
            object_name="data/employees.parquet",
            data=io.BytesIO(parquet_data),
            length=len(parquet_data),
            content_type="application/octet-stream"
        )

        # Upload monthly CSV
        monthly_csv_buffer = io.StringIO()
        monthly_df.to_csv(monthly_csv_buffer, index=False)
        monthly_csv_data = monthly_csv_buffer.getvalue().encode('utf-8')

        client.put_object(
            bucket_name=bucket_name,
            object_name="data/monthly_reports.csv",
            data=io.BytesIO(monthly_csv_data),
            length=len(monthly_csv_data),
            content_type="text/csv"
        )

        # Create partitioned data
        for dept in ['Engineering', 'Marketing', 'Sales']:
            dept_data = df[df['department'] == dept].copy()
            if not dept_data.empty:
                dept_csv_buffer = io.StringIO()
                dept_data.to_csv(dept_csv_buffer, index=False)
                dept_csv_data = dept_csv_buffer.getvalue().encode('utf-8')

                client.put_object(
                    bucket_name=bucket_name,
                    object_name=f"partitioned/department={dept}/employees.csv",
                    data=io.BytesIO(dept_csv_data),
                    length=len(dept_csv_data),
                    content_type="text/csv"
                )

        # Generate synthetic AIS data
        print("Creating synthetic AIS data...")
        synthetic_ais_df = generate_synthetic_ais_data(vessels)

        # Upload synthetic CSV
        synthetic_csv_buffer = io.StringIO()
        synthetic_ais_df.to_csv(synthetic_csv_buffer, index=False)
        synthetic_csv_data = synthetic_csv_buffer.getvalue().encode('utf-8')

        client.put_object(
            bucket_name=bucket_name,
            object_name="external/ais_data_synthetic.csv",
            data=io.BytesIO(synthetic_csv_data),
            length=len(synthetic_csv_data),
            content_type="text/csv"
        )

        # Upload synthetic Parquet
        synthetic_parquet_buffer = io.BytesIO()
        synthetic_ais_df.to_parquet(synthetic_parquet_buffer, index=False)
        synthetic_parquet_data = synthetic_parquet_buffer.getvalue()

        client.put_object(
            bucket_name=bucket_name,
            object_name="external/ais_data_synthetic.parquet",
            data=io.BytesIO(synthetic_parquet_data),
            length=len(synthetic_parquet_data),
            content_type="application/octet-stream"
        )

        # Create and upload AIS summary
        ais_summary = create_ais_summary(synthetic_ais_df, vessels)
        summary_json = pd.Series(ais_summary).to_json(indent=2)

        client.put_object(
            bucket_name=bucket_name,
            object_name="external/ais_data_summary.json",
            data=io.BytesIO(summary_json.encode('utf-8')),
            length=len(summary_json.encode('utf-8')),
            content_type="application/json"
        )

        print(f"Successfully created synthetic AIS data with {len(synthetic_ais_df)} records")
        print("AIS data setup complete!")

        yield minio


@pytest.fixture(scope="module")
def minio_with_data_client(minio_container_with_data):
    """Returns a configured MinIO client for testing."""
    return minio_container_with_data.get_client()


@pytest.fixture(scope="module")
def minio_with_data_connection_params(minio_container_with_data):
    """Returns connection parameters for the MinIO container."""
    return {
        'endpoint_url': minio_container_with_data.get_config()["endpoint"],
        'access_key': minio_container_with_data.get_config()["access_key"],
        'secret_key': minio_container_with_data.get_config()["secret_key"],
        'bucket_name': 'test-lake'
    }


# ==================== PYTEST FIXTURES FOR POSTGRES CONTAINERS ====================

@pytest.fixture(scope="module")
def source_postgres_container():
    """
    Starts a PostgreSQL container with sample data for testing.
    Creates tables and populates them with the same synthetic data used in MinIO.
    """
    with PostgresContainer("postgres:15-alpine", user="test", password="test", dbname="test") as postgres:
        # Create connection
        engine = create_engine(postgres.get_connection_url())

        # Generate all datasets (same as MinIO)
        employee_data = create_employee_data()
        monthly_data = create_monthly_data()
        vessels = create_vessel_definitions()

        # Create DataFrames
        employees_df = pd.DataFrame(employee_data)
        monthly_df = pd.DataFrame(monthly_data)
        synthetic_ais_df = generate_synthetic_ais_data(vessels)

        # Create tables and insert data
        with engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA company"))
            # Create and populate employees table
            employees_df.to_sql('employees', conn, schema='company', if_exists='replace', index=False)

            # Create and populate monthly_reports table
            monthly_df.to_sql('monthly_reports', conn, schema='company', if_exists='replace', index=False)

            # Create and populate vessels table (from vessel definitions)
            vessels_df = pd.DataFrame(vessels)
            vessels_df.to_sql('vessels', conn, if_exists='replace', index=False)

            # Create and populate AIS data table
            # Note: Converting BaseDateTime to proper datetime for PostgreSQL
            ais_df_pg = synthetic_ais_df.copy()
            ais_df_pg['BaseDateTime'] = pd.to_datetime(ais_df_pg['BaseDateTime'])

            # Convert column names to lowercase for PostgreSQL
            ais_df_pg.columns = ais_df_pg.columns.str.lower()
            ais_df_pg.to_sql('ais_data', conn, if_exists='replace', index=False)

            # Create some indexes for better query performance
            conn.execute(text("CREATE INDEX idx_employees_department ON company.employees(department)"))
            conn.execute(text("CREATE INDEX idx_ais_mmsi ON ais_data(mmsi)"))
            conn.execute(text("CREATE INDEX idx_ais_datetime ON ais_data(basedatetime)"))
            conn.execute(text("CREATE INDEX idx_vessels_mmsi ON vessels(mmsi)"))

            # Create a view that joins AIS data with vessel information
            conn.execute(text("""
                              CREATE VIEW ais_with_vessel_info AS
                              SELECT a.*,
                                     v.name   as vessel_name_from_vessels,
                                     v.type   as vessel_type_from_vessels,
                                     v.length as vessel_length_from_vessels,
                                     v.width  as vessel_width_from_vessels
                              FROM ais_data a
                                       LEFT JOIN vessels v ON a.mmsi = v.mmsi
                              """))

            conn.commit()

        print(f"PostgreSQL container populated with:")
        print(f"  - {len(employees_df)} employee records")
        print(f"  - {len(monthly_df)} monthly report records")
        print(f"  - {len(vessels_df)} vessel definitions")
        print(f"  - {len(synthetic_ais_df)} AIS data records")
        print("  - Created indexes and views for better query performance")

        yield postgres


@pytest.fixture(scope="module")
def postgres_engine(source_postgres_container):
    """Returns a SQLAlchemy engine for the PostgreSQL container."""
    return create_engine(source_postgres_container.get_connection_url())


@pytest.fixture(scope="module")
def postgres_connection_params(source_postgres_container):
    """Returns connection parameters for the PostgreSQL container."""
    return {
        'host': source_postgres_container.get_container_host_ip(),
        'port': source_postgres_container.get_exposed_port(5432),
        'database': 'test',
        'user': 'test',
        'password': 'test',
        'connection_url': source_postgres_container.get_connection_url()
    }


# Helper fixture to get all test data as DataFrames (useful for tests)
@pytest.fixture(scope="module")
def test_datasets():
    """Returns all test datasets as DataFrames for easy access in tests."""
    employee_data = create_employee_data()
    monthly_data = create_monthly_data()
    vessels = create_vessel_definitions()

    return {
        'employees': pd.DataFrame(employee_data),
        'monthly_reports': pd.DataFrame(monthly_data),
        'vessels': pd.DataFrame(vessels),
        'ais_data': generate_synthetic_ais_data(vessels)
    }


# ==================== PYTEST FIXTURES FOR LAKEHOUSE CONTAINERS ====================

@pytest.fixture(scope="module")
def catalog_postgres_container():
    """Starts a second, separate PostgreSQL container to act as the DuckLake catalog."""
    with PostgresContainer("postgres:15-alpine", user="catalog", password="catalog", dbname="catalog") as postgres:
        yield postgres


@pytest.fixture(scope="module")
def minio_container():
    """Starts a MinIO container to act as S3-compatible storage for the DuckLake."""
    with MinioContainer("minio/minio:RELEASE.2025-06-13T11-33-47Z") as minio:
        # It's good practice to create the bucket ahead of time.
        minio.get_client().make_bucket("test-lake")
        yield minio
