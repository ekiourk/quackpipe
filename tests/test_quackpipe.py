"""
Comprehensive test suite for quackpipe using pytest.

This test file covers:
- Configuration parsing and validation
- Builder API functionality
- Core session management
- Error handling
"""

import os
import sys
from unittest.mock import Mock, patch

import pytest
import yaml

from quackpipe.builder import QuackpipeBuilder
from quackpipe.config import SourceConfig, SourceType
from quackpipe.core import _prepare_connection, session
from quackpipe.exceptions import ConfigError, QuackpipeError, SecretError
from quackpipe.secrets import fetch_secret_bundle
from quackpipe.utils import parse_config_from_yaml

sys.path.insert(0, 'src')


# ==================== CONFIG TESTS ====================

def test_source_type_enum():
    """Test SourceType enum values."""
    assert SourceType.POSTGRES.value == "postgres"
    assert SourceType.S3.value == "s3"
    assert SourceType.DUCKLAKE.value == "ducklake"
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


def test_builder_add_source_config():
    """Test adding sources to builder."""
    builder = QuackpipeBuilder()

    source_config = SourceConfig(
        name="test_pg",
        type=SourceType.POSTGRES,
        config={"port": 5432},
        secret_name="pg_secret"
    )

    result = builder.add_source_config(source_config)

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
              .add_source_config(SourceConfig("s3_1", SourceType.S3, secret_name="s3_secret")))

    assert result is builder
    assert len(builder._sources) == 2

    # chaining an empty builder should not fail but have no effect
    builder.chain(QuackpipeBuilder())
    assert len(builder._sources) == 2

    # chaining another builder should merge them
    new_builder = QuackpipeBuilder().add_source_config(SourceConfig("s3_2", SourceType.S3, secret_name="s3_2_secret"))
    builder.chain(new_builder)
    assert len(builder._sources) == 3


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
    configs = parse_config_from_yaml(sample_yaml_config)

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
        parse_config_from_yaml("nonexistent.yml")


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
        parse_config_from_yaml(config_path)


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
    with pytest.raises(ConfigError, match="Must provide either a 'config_path' or a 'configs'"):
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

    with session(configs=configs, sources=["pg1", "s3_1"]) as _:
        pass

    # Should only prepare filtered configs
    call_args = mock_prepare.call_args[0]
    prepared_configs = call_args[1]
    assert len(prepared_configs) == 2
    assert {c.name for c in prepared_configs} == {"pg1", "s3_1"}


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

    configs = parse_config_from_yaml(config_path)
    assert len(configs) == expected_count


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

    configs = parse_config_from_yaml(config_path)
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
