"""
tests/test_etl_utils.py

This file contains pytest tests for the standalone functions in etl_utils.py.
"""
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from quackpipe import QuackpipeBuilder
from quackpipe.config import SourceConfig, SourceType
from quackpipe.etl_utils import create_table_from_df, move_data, to_df

# ==================== FIXTURES ====================

@pytest.fixture
def mock_duckdb_connection():
    """Provides a mock DuckDB connection object."""
    mock_con = MagicMock()

    sample_df = pd.DataFrame({'id': [1, 2], 'name': ['A', 'B']})
    mock_execute_result = MagicMock()
    mock_execute_result.fetchdf.return_value = sample_df
    mock_con.execute.return_value = mock_execute_result

    return mock_con


@pytest.fixture
def mock_session(mock_duckdb_connection):
    """A patch fixture for the quackpipe.etl_utils.session context manager."""
    with patch('quackpipe.etl_utils.session') as mock_session_context:
        mock_session_context.return_value.__enter__.return_value = mock_duckdb_connection
        yield mock_session_context


@pytest.fixture
def mock_get_configs():
    """A patch fixture for the quackpipe.etl_utils.get_configs function."""
    with patch('quackpipe.etl_utils.get_configs') as mock:
        yield mock


# ==================== UNIT TESTS ====================

def test_to_df(mock_duckdb_connection):
    """Test the to_df utility function."""
    query = "SELECT * FROM test_table"
    result = to_df(mock_duckdb_connection, query)

    mock_duckdb_connection.execute.assert_called_once_with(query)
    assert isinstance(result, pd.DataFrame)
    assert not result.empty


def test_create_table_from_df(mock_duckdb_connection):
    """Test the create_table_from_df utility function."""
    df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
    table_name = "new_table"
    create_table_from_df(mock_duckdb_connection, df, table_name)

    expected_sql = f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df"
    mock_duckdb_connection.execute.assert_called_once_with(expected_sql)


@pytest.mark.parametrize("format_type", ["parquet", "csv", "json"])
def test_move_data_to_s3(mock_session, mock_get_configs, mock_duckdb_connection, format_type):
    """Test moving data to an S3 destination with various formats."""
    # Arrange
    source_query = "SELECT * FROM source_table"
    s3_configs = [SourceConfig(name="s3_dest", type=SourceType.S3, config={"path": "s3://bucket/"})]
    mock_get_configs.return_value = s3_configs

    # Act
    move_data(
        source_query=source_query,
        destination_name="s3_dest",
        table_name="output_table",
        configs=s3_configs,
        format=format_type
    )

    # Assert
    expected_sql = f"COPY ({source_query}) TO 's3://bucket/output_table.{format_type}' (FORMAT {format_type.upper()});"
    mock_duckdb_connection.execute.assert_any_call(expected_sql)
    mock_session.assert_called_once_with(configs=s3_configs, env_file=None)


@pytest.mark.parametrize("mode, expected_sql_pattern", [
    ("replace", "CREATE TABLE pg_dest.output_table AS ({});"),
    ("append", "INSERT INTO pg_dest.output_table ({});")
])
def test_move_data_to_writeable_postgres(mock_session, mock_get_configs, mock_duckdb_connection, mode,
                                         expected_sql_pattern):
    """Test moving data to a writeable Postgres destination."""
    # Arrange
    source_query = "SELECT id, name FROM source"
    pg_configs = [SourceConfig(name="pg_dest", type=SourceType.POSTGRES, config={"read_only": False})]
    mock_get_configs.return_value = pg_configs

    # Mock the DROP TABLE call for replace mode
    if mode == 'replace':
        mock_duckdb_connection.execute.side_effect = [None, None]  # First call is DROP, second is CREATE

    # Act
    move_data(
        source_query=source_query,
        destination_name="pg_dest",
        table_name="output_table",
        configs=pg_configs,
        mode=mode
    )

    # Assert
    if mode == 'replace':
        mock_duckdb_connection.execute.assert_any_call("DROP TABLE IF EXISTS pg_dest.output_table;")

    final_sql = expected_sql_pattern.format(source_query)
    mock_duckdb_connection.execute.assert_called_with(final_sql)


def test_move_data_to_readonly_postgres_raises_error(mock_session, mock_get_configs):
    """Test that moving data to a read-only destination raises a PermissionError."""
    # Arrange
    pg_configs = [SourceConfig(name="pg_dest", type=SourceType.POSTGRES, config={"read_only": True})]
    mock_get_configs.return_value = pg_configs

    # Act & Assert
    with pytest.raises(PermissionError,
                       match="Cannot write to destination 'pg_dest' because it is configured as read-only."):
        move_data(
            source_query="SELECT 1",
            destination_name="pg_dest",
            table_name="output_table",
            configs=pg_configs
        )


def test_move_data_destination_not_found_raises_error(mock_get_configs):
    """Test that a ValueError is raised if the destination config is not found."""
    # Arrange
    mock_get_configs.return_value = [SourceConfig(name="some_other_source", type=SourceType.S3)]

    # Act & Assert
    with pytest.raises(ValueError, match="Destination 'non_existent_dest' not found in the provided configuration."):
        move_data(
            source_query="SELECT 1",
            destination_name="non_existent_dest",
            table_name="output",
            configs=[]
        )


# ==================== INTEGRATION TEST ====================

@patch('quackpipe.etl_utils.session')
@patch('quackpipe.etl_utils.get_configs')
def test_full_workflow_with_move_data(mock_get_configs, mock_session, mock_duckdb_connection):
    """
    Test a high-level workflow using the self-contained move_data utility.
    """
    # Arrange: Setup mock configurations for both a source and a destination
    all_configs = [
        SourceConfig(name="pg_main", type=SourceType.POSTGRES),
        SourceConfig(name="s3_lake", type=SourceType.S3, config={"path": "s3://my-lake/"})
    ]
    mock_get_configs.return_value = all_configs

    # The session mock will be used by the move_data function internally
    mock_session.return_value.__enter__.return_value = mock_duckdb_connection

    # Act: Call the high-level utility function directly.
    move_data(
        source_query="SELECT * FROM pg_main.users",
        destination_name="s3_lake",
        table_name="users_backup",
        configs=all_configs,
        format="parquet"
    )

    # Assert
    # 1. Verify that get_configs was called to load the configuration
    mock_get_configs.assert_called_once_with(None, all_configs)

    # 2. Verify that an internal session was created with all the necessary configs
    mock_session.assert_called_once_with(configs=all_configs, env_file=None)

    # 3. Verify the correct COPY command was executed inside the session
    expected_sql = "COPY (SELECT * FROM pg_main.users) TO 's3://my-lake/users_backup.parquet' (FORMAT PARQUET);"
    mock_duckdb_connection.execute.assert_any_call(expected_sql)


@patch('duckdb.connect')
@patch('quackpipe.core.configure_secret_provider')
def test_full_workflow_builder_api(mock_configure_secrets, mock_connect, mock_duckdb_connection):
    """Test complete workflow using Builder API."""
    # Arrange
    mock_connect.return_value = mock_duckdb_connection

    builder = (QuackpipeBuilder()
               .add_source("pg_test", SourceType.POSTGRES,
                           config={"port": 5432, "tables": ["users"]},
                           secret_name="pg_prod"))

    # Act
    with builder.session() as con:
        df = to_df(con, "SELECT * FROM pg_test_users")
        assert isinstance(df, pd.DataFrame)

    # Assert
    mock_duckdb_connection.close.assert_called_once()
    # Verify that the secret provider was configured when the session started
    mock_configure_secrets.assert_called_once()
