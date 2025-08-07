import logging
import os
import tempfile
from unittest.mock import Mock, patch

import pandas as pd
import pytest
import yaml

from quackpipe import configure_secret_provider
from quackpipe.test_utils.data_generators import (
    create_employee_data,
    create_monthly_data,
    create_vessel_definitions,
    generate_synthetic_ais_data,
)

logger = logging.getLogger(__name__)


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

# ==================== PYTEST FIXTURES FOR POSTGRES CONTAINERS ====================




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

