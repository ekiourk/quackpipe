import json
import os
import tempfile
from unittest.mock import Mock, patch

import pandas as pd
import pytest
import yaml

from quackpipe import set_secret_providers
from quackpipe.secrets import EnvSecretProvider


@pytest.fixture(autouse=True)
def reset_secret_providers():
    """Reset secret providers after each test."""
    yield
    # Reset to default after each test
    set_secret_providers([EnvSecretProvider()])


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
def json_secrets_dir(temp_dir):
    """Create JSON secret files for testing."""
    secrets_dir = os.path.join(temp_dir, 'secrets')
    os.makedirs(secrets_dir)

    # Create pg_prod.json
    pg_secrets = {
        'host': 'json-localhost',
        'user': 'json-user',
        'password': 'json-pass',
        'database': 'json-db'
    }
    with open(os.path.join(secrets_dir, 'pg_prod.json'), 'w') as f:
        json.dump(pg_secrets, f)

    # Create aws_datalake.json
    aws_secrets = {
        'access_key_id': 'json-key',
        'secret_access_key': 'json-secret'
    }
    with open(os.path.join(secrets_dir, 'aws_datalake.json'), 'w') as f:
        json.dump(aws_secrets, f)

    return secrets_dir


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
