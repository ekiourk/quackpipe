"""
Comprehensive test suite for quackpipe using pytest.

This test file covers:
- Configuration parsing and validation
- Secret management with multiple providers
- Builder API functionality
- Core session management
- Source handlers
- ETL utilities
- Error handling
"""

import json
import os
import sys
import tempfile
from unittest.mock import Mock, patch

import pandas as pd
import pytest
import yaml

sys.path.insert(0, 'src')

from quackpipe.config import SourceConfig, SourceType
from quackpipe.secrets import (
    EnvSecretProvider, JsonFileSecretProvider, set_secret_providers, fetch_secret_bundle
)
from quackpipe.builder import QuackpipeBuilder
from quackpipe.core import session, _parse_config_from_yaml, _prepare_connection
from quackpipe.sources.postgres import PostgresHandler
from quackpipe.utils import ETLUtils
from quackpipe.exceptions import QuackpipeError, ConfigError, SecretError


# ==================== FIXTURES ====================

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


# ==================== CONFIG TESTS ====================

def test_source_type_enum():
    """Test SourceType enum values."""
    assert SourceType.POSTGRES.value == "postgres"
    assert SourceType.S3.value == "s3"
    assert SourceType.PARQUET.value == "parquet"
    assert SourceType.CSV.value == "csv"


def test_source_config_creation():
    """Test SourceConfig dataclass creation."""
    config = SourceConfig(
        name="test_source",
        type=SourceType.POSTGRES,
        config={"port": 5432},
        secret_name="test_secret"
    )

    assert config.name == "test_source"
    assert config.type == SourceType.POSTGRES
    assert config.config == {"port": 5432}
    assert config.secret_name == "test_secret"


def test_source_config_defaults():
    """Test SourceConfig with default values."""
    config = SourceConfig(name="test", type=SourceType.S3)

    assert config.config == {}
    assert config.secret_name is None


# ==================== SECRET MANAGEMENT TESTS ====================

def test_env_secret_provider(env_secrets):
    """Test EnvSecretProvider functionality."""
    provider = EnvSecretProvider()

    secrets = provider.get_secret('pg_prod')

    assert secrets['host'] == 'localhost'
    assert secrets['user'] == 'testuser'
    assert secrets['password'] == 'testpass'
    assert secrets['database'] == 'testdb'


def test_env_secret_provider_empty():
    """Test EnvSecretProvider with non-existent secret."""
    provider = EnvSecretProvider()

    secrets = provider.get_secret('nonexistent')

    assert secrets == {}


def test_json_file_secret_provider(json_secrets_dir):
    """Test JsonFileSecretProvider functionality."""
    provider = JsonFileSecretProvider(json_secrets_dir)

    secrets = provider.get_secret('pg_prod')

    assert secrets['host'] == 'json-localhost'
    assert secrets['user'] == 'json-user'
    assert secrets['password'] == 'json-pass'
    assert secrets['database'] == 'json-db'


def test_json_file_secret_provider_not_found(json_secrets_dir):
    """Test JsonFileSecretProvider with non-existent file."""
    provider = JsonFileSecretProvider(json_secrets_dir)

    secrets = provider.get_secret('nonexistent')

    assert secrets == {}


def test_set_secret_providers():
    """Test setting custom secret providers."""
    provider1 = EnvSecretProvider()
    provider2 = JsonFileSecretProvider()

    set_secret_providers([provider1, provider2])

    # This would test the global _providers variable
    # We'll verify by testing fetch_secret_bundle behavior


def test_set_secret_providers_invalid():
    """Test setting invalid secret providers."""
    with pytest.raises(TypeError):
        set_secret_providers("not a list")

    with pytest.raises(TypeError):
        set_secret_providers(["not a provider"])


def test_fetch_secret_bundle_success(env_secrets):
    """Test successful secret bundle fetching."""
    # Reset to default provider
    set_secret_providers([EnvSecretProvider()])

    secrets = fetch_secret_bundle('pg_prod')

    assert secrets['host'] == 'localhost'
    assert secrets['user'] == 'testuser'


def test_fetch_secret_bundle_not_found():
    """Test secret bundle not found."""
    set_secret_providers([EnvSecretProvider()])

    with pytest.raises(SecretError, match="Secret bundle 'nonexistent' not found"):
        fetch_secret_bundle('nonexistent')


def test_fetch_secret_bundle_empty_name():
    """Test fetch_secret_bundle with empty name."""
    result = fetch_secret_bundle('')
    assert result == {}


def test_secret_provider_chain(env_secrets, json_secrets_dir):
    """Test multiple secret providers in chain."""
    # Set up providers where JSON has priority
    json_provider = JsonFileSecretProvider(json_secrets_dir)
    env_provider = EnvSecretProvider()

    set_secret_providers([json_provider, env_provider])

    # Should get JSON version (first in chain)
    secrets = fetch_secret_bundle('pg_prod')
    assert secrets['host'] == 'json-localhost'

    # Reset to default
    set_secret_providers([EnvSecretProvider()])


# ==================== BUILDER API TESTS ====================

def test_builder_creation():
    """Test QuackpipeBuilder initialization."""
    builder = QuackpipeBuilder()
    assert builder._sources == []


def test_builder_add_source():
    """Test adding sources to builder."""
    builder = QuackpipeBuilder()

    result = builder.add_source(
        name="test_pg",
        type=SourceType.POSTGRES,
        config={"port": 5432},
        secret_name="pg_secret"
    )

    # Should return self for chaining
    assert result is builder

    # Check source was added
    assert len(builder._sources) == 1
    source = builder._sources[0]
    assert source.name == "test_pg"
    assert source.type == SourceType.POSTGRES
    assert source.config == {"port": 5432}
    assert source.secret_name == "pg_secret"


def test_builder_chaining():
    """Test builder method chaining."""
    builder = QuackpipeBuilder()

    result = (builder
              .add_source("pg1", SourceType.POSTGRES, secret_name="pg_secret")
              .add_source("s3_1", SourceType.S3, secret_name="s3_secret"))

    assert result is builder
    assert len(builder._sources) == 2


def test_builder_session_empty():
    """Test builder session with no sources."""
    builder = QuackpipeBuilder()

    with pytest.raises(ValueError, match="Cannot build a session with no sources"):
        builder.session()


@patch('quackpipe.builder.core_session')
def test_builder_session_success(mock_session):
    """Test successful builder session creation."""
    builder = QuackpipeBuilder()
    builder.add_source("test", SourceType.POSTGRES)

    builder.session()

    mock_session.assert_called_once_with(configs=builder._sources)


# ==================== CORE FUNCTIONALITY TESTS ====================

def test_parse_config_from_yaml(sample_yaml_config):
    """Test parsing YAML configuration."""
    configs = _parse_config_from_yaml(sample_yaml_config)

    assert len(configs) == 2

    pg_config = next(c for c in configs if c.name == 'pg_main')
    assert pg_config.type == SourceType.POSTGRES
    assert pg_config.secret_name == 'pg_prod'
    assert pg_config.config['port'] == 5432
    assert pg_config.config['tables'] == ['users', 'orders']

    s3_config = next(c for c in configs if c.name == 'datalake')
    assert s3_config.type == SourceType.S3
    assert s3_config.secret_name == 'aws_datalake'
    assert s3_config.config['region'] == 'us-east-1'


def test_parse_config_from_yaml_not_found():
    """Test parsing non-existent YAML file."""
    with pytest.raises(ConfigError, match="Configuration file not found"):
        _parse_config_from_yaml("nonexistent.yml")


def test_parse_config_invalid_type(temp_dir):
    """Test parsing YAML with invalid source type."""
    invalid_config = {
        'sources': {
            'bad_source': {
                'type': 'invalid_type',
                'secret_name': 'test'
            }
        }
    }

    config_path = os.path.join(temp_dir, 'invalid.yml')
    with open(config_path, 'w') as f:
        yaml.dump(invalid_config, f)

    with pytest.raises(ConfigError, match="Missing or invalid 'type' for source 'bad_source'."):
        _parse_config_from_yaml(config_path)


@patch('duckdb.connect')
def test_prepare_connection(mock_connect, mock_duckdb_connection, env_secrets):
    """Test connection preparation."""
    mock_connect.return_value = mock_duckdb_connection

    configs = [
        SourceConfig(
            name="test_pg",
            type=SourceType.POSTGRES,
            config={"port": 5432, "tables": ["users"]},
            secret_name="pg_prod"
        )
    ]

    _prepare_connection(mock_duckdb_connection, configs)

    # Verify plugin installation
    mock_duckdb_connection.install_extension.assert_called_with("postgres")
    mock_duckdb_connection.load_extension.assert_called_with("postgres")

    # Verify SQL execution
    mock_duckdb_connection.execute.assert_called()


def test_prepare_connection_empty():
    """Test connection preparation with empty configs."""
    mock_con = Mock()

    _prepare_connection(mock_con, [])

    # Should not call any methods on empty configs
    mock_con.install_extension.assert_not_called()


@patch('duckdb.connect')
@patch('quackpipe.core._prepare_connection')
def test_session_with_config_path(mock_prepare, mock_connect, mock_duckdb_connection, sample_yaml_config):
    """Test session creation with config path."""
    mock_connect.return_value = mock_duckdb_connection

    with session(config_path=sample_yaml_config) as con:
        assert con is mock_duckdb_connection

    mock_prepare.assert_called_once()
    mock_duckdb_connection.close.assert_called_once()


@patch('duckdb.connect')
def test_session_with_configs(mock_connect, mock_duckdb_connection, monkeypatch):
    """Test session creation with direct configs."""
    mock_connect.return_value = mock_duckdb_connection

    # Mock the environment variables that the EnvSecretProvider will be looking for.
    # The prefix comes from the secret_name "my_test_secret" -> "MY_TEST_SECRET_"
    monkeypatch.setenv("MY_TEST_SECRET_DATABASE", "test_db")
    monkeypatch.setenv("MY_TEST_SECRET_USER", "test_user")
    monkeypatch.setenv("MY_TEST_SECRET_PASSWORD", "test_password")
    monkeypatch.setenv("MY_TEST_SECRET_HOST", "localhost")
    monkeypatch.setenv("MY_TEST_SECRET_PORT", "5432")

    configs = [SourceConfig(name="test", type=SourceType.POSTGRES, secret_name="my_test_secret")]

    with session(configs=configs) as con:
        assert con is mock_duckdb_connection

        # Note: The mock handler might generate slightly different SQL with newlines.
        # This is just an example of a more detailed assertion.
        last_call_args = mock_duckdb_connection.execute.call_args.args
        # A simple check:
        assert "CREATE OR REPLACE SECRET test_secret" in last_call_args[0]
        assert "TYPE POSTGRES" in last_call_args[0]
        assert "HOST 'localhost'" in last_call_args[0]
        assert "PORT '5432'" in last_call_args[0]
        assert "DATABASE 'test_db'" in last_call_args[0]
        assert "USER 'test_user'" in last_call_args[0]
        assert "PASSWORD 'test_password'" in last_call_args[0]
        assert "ATTACH 'dbname=test_db' AS test (TYPE POSTGRES, SECRET 'test_secret', READ_ONLY)" in last_call_args[0]


def test_session_no_config():
    """Test session creation without config."""
    with pytest.raises(ConfigError, match="Must provide either 'config_path' or 'configs'"):
        with session():
            pass


@patch('duckdb.connect')
@patch('quackpipe.core._prepare_connection')
def test_session_with_sources_filter(mock_prepare, mock_connect, mock_duckdb_connection):
    """Test session with sources filter."""
    mock_connect.return_value = mock_duckdb_connection

    configs = [
        SourceConfig(name="pg1", type=SourceType.POSTGRES),
        SourceConfig(name="pg2", type=SourceType.POSTGRES),
        SourceConfig(name="s3_1", type=SourceType.S3)
    ]

    with session(configs=configs, sources=["pg1", "s3_1"]) as con:
        pass

    # Should only prepare filtered configs
    call_args = mock_prepare.call_args[0]
    prepared_configs = call_args[1]
    assert len(prepared_configs) == 2
    assert {c.name for c in prepared_configs} == {"pg1", "s3_1"}


# ==================== ETL UTILS TESTS ====================

def test_etl_utils_to_df(mock_duckdb_connection):
    """Test ETLUtils.to_df method."""
    query = "SELECT * FROM test_table"

    result = ETLUtils.to_df(mock_duckdb_connection, query)

    mock_duckdb_connection.execute.assert_called_once_with(query)
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert list(result.columns) == ['id', 'name']


@pytest.mark.parametrize("format_type", ["parquet", "csv", "json"])
def test_etl_utils_copy(mock_duckdb_connection, format_type):
    """Test ETLUtils.copy method with different formats."""
    source_query = "SELECT * FROM source_table"
    target_path = f"s3://bucket/data.{format_type}"

    ETLUtils.copy(mock_duckdb_connection, source_query, target_path, format_type)

    expected_sql = f"COPY ({source_query}) TO '{target_path}' (FORMAT {format_type.upper()})"
    mock_duckdb_connection.execute.assert_called_once_with(expected_sql)


def test_etl_utils_copy_default_format(mock_duckdb_connection):
    """Test ETLUtils.copy with default parquet format."""
    source_query = "SELECT * FROM source_table"
    target_path = "output/data.parquet"

    ETLUtils.copy(mock_duckdb_connection, source_query, target_path)

    expected_sql = f"COPY ({source_query}) TO '{target_path}' (FORMAT PARQUET)"
    mock_duckdb_connection.execute.assert_called_once_with(expected_sql)


def test_etl_utils_create_table_from_df(mock_duckdb_connection):
    """Test ETLUtils.create_table_from_df method."""
    df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
    table_name = "new_table"

    ETLUtils.create_table_from_df(mock_duckdb_connection, df, table_name)

    expected_sql = f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df"
    mock_duckdb_connection.execute.assert_called_once_with(expected_sql)


# ==================== INTEGRATION TESTS ====================

@patch('duckdb.connect')
def test_full_workflow_yaml_config(mock_connect, mock_duckdb_connection, sample_yaml_config, env_secrets):
    """Test complete workflow using YAML configuration."""
    mock_connect.return_value = mock_duckdb_connection

    with session(config_path=sample_yaml_config) as con:
        # Test ETL operations
        df = ETLUtils.to_df(con, "SELECT * FROM pg_main_users LIMIT 5")
        assert len(df) == 2

        ETLUtils.copy(con, "SELECT * FROM pg_main_users", "output/users.parquet")

    # Verify connection setup
    assert mock_duckdb_connection.install_extension.call_count >= 1
    assert mock_duckdb_connection.execute.call_count >= 1


@patch('duckdb.connect')
def test_full_workflow_builder_api(mock_connect, mock_duckdb_connection, env_secrets):
    """Test complete workflow using Builder API."""
    mock_connect.return_value = mock_duckdb_connection

    builder = (QuackpipeBuilder()
               .add_source("pg_test", SourceType.POSTGRES,
                           config={"port": 5432, "tables": ["users"]},
                           secret_name="pg_prod"))

    with builder.session() as con:
        df = ETLUtils.to_df(con, "SELECT * FROM pg_test_users")
        assert isinstance(df, pd.DataFrame)

    mock_duckdb_connection.close.assert_called_once()


# ==================== ERROR HANDLING TESTS ====================

def test_config_error_inheritance():
    """Test ConfigError exception inheritance."""
    error = ConfigError("Test config error")

    assert isinstance(error, QuackpipeError)
    assert isinstance(error, Exception)
    assert str(error) == "Test config error"


def test_secret_error_inheritance():
    """Test SecretError exception inheritance."""
    error = SecretError("Test secret error")

    assert isinstance(error, QuackpipeError)
    assert isinstance(error, Exception)
    assert str(error) == "Test secret error"


# ==================== PARAMETRIZED TESTS ====================

@pytest.mark.parametrize("source_type,expected_plugins", [
    (SourceType.POSTGRES, ["postgres"]),
    (SourceType.S3, ["httpfs"]),
])
def test_handler_plugins(source_type, expected_plugins):
    """Test that handlers return correct required plugins."""
    from quackpipe.core import SOURCE_HANDLER_REGISTRY

    handler = SOURCE_HANDLER_REGISTRY.get(source_type)
    if handler:
        assert handler.required_plugins == expected_plugins


@pytest.mark.parametrize("config_data,expected_count", [
    ({'sources': {}}, 0),
    ({'sources': {'pg1': {'type': 'postgres'}}}, 1),
    ({'sources': {'pg1': {'type': 'postgres'}, 's3_1': {'type': 's3'}}}, 2),
])
def test_config_parsing_counts(temp_dir, config_data, expected_count):
    """Test configuration parsing with different source counts."""
    config_path = os.path.join(temp_dir, 'test.yml')
    with open(config_path, 'w') as f:
        yaml.dump(config_data, f)

    configs = _parse_config_from_yaml(config_path)
    assert len(configs) == expected_count


# ==================== CONFTEST ADDITIONAL FIXTURES ====================

@pytest.fixture(autouse=True)
def reset_secret_providers():
    """Reset secret providers after each test."""
    yield
    # Reset to default after each test
    set_secret_providers([EnvSecretProvider()])


# ==================== PERFORMANCE/EDGE CASE TESTS ====================

def test_large_config_handling(temp_dir):
    """Test handling of configuration with many sources."""
    large_config = {'sources': {}}

    # Create 50 sources
    for i in range(50):
        large_config['sources'][f'source_{i}'] = {
            'type': 'postgres',
            'secret_name': f'secret_{i}'
        }

    config_path = os.path.join(temp_dir, 'large.yml')
    with open(config_path, 'w') as f:
        yaml.dump(large_config, f)

    configs = _parse_config_from_yaml(config_path)
    assert len(configs) == 50


def test_empty_secret_bundle_handling():
    """Test handling of empty secret bundles."""
    # Test with None
    result = fetch_secret_bundle('')
    assert result == {}


def test_builder_with_none_config():
    """Test builder with None config parameter."""
    builder = QuackpipeBuilder()
    builder.add_source("test", SourceType.POSTGRES, config=None)

    assert builder._sources[0].config == {}


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
