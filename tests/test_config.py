"""
Tests for the core quackpipe functions.
"""
import textwrap

import pytest

from quackpipe import get_source_params
from quackpipe.exceptions import ParsingError, ValidationError


def test_config(tmp_path):
    """
    Tests that the config function correctly merges config and secrets.
    """
    config_yml = textwrap.dedent("""
        sources:
          test_source:
            type: "postgres"
            secret_name: "TEST_SOURCE"
            host: "localhost"
    """)
    env_file = textwrap.dedent("""
        TEST_SOURCE_USER=test_user
        TEST_SOURCE_PASSWORD=test_password
    """)
    config_path = tmp_path / "config.yml"
    config_path.write_text(config_yml)
    env_path = tmp_path / ".env"
    env_path.write_text(env_file)

    merged_config = get_source_params("test_source", config_path=str(config_path), env_file=str(env_path))

    assert merged_config.HOST == merged_config.host == merged_config['host'] == merged_config['HOST'] == merged_config['Host']

    assert merged_config == {
        "host": "localhost",
        "user": "test_user",
        "password": "test_password",
    }


def test_config_source_not_found(tmp_path):
    """
    Tests that the get_source_params raises a ValidationError when the source is not found.
    """
    config_yml = textwrap.dedent("""
        sources:
          test_source:
            type: "postgres"
            secret_name: "TEST_SOURCE"
            host: "localhost"
            database: "db"
    """)
    config_path = tmp_path / "config.yml"
    config_path.write_text(config_yml)

    with pytest.raises(ValidationError, match="not found in configuration"):
        get_source_params("not_found", config_path=str(config_path))


def test_parsing_error_malformed_yaml(tmp_path):
    """Test that a ParsingError is raised for malformed YAML."""
    config_path = tmp_path / "bad.yml"
    config_path.write_text("invalid: [yaml: block")  # Malformed

    with pytest.raises(ParsingError, match="Error parsing YAML file"):
        get_source_params("any", config_path=str(config_path))


def test_parsing_error_missing_file():
    """Test that a ParsingError is raised for a missing config file."""
    with pytest.raises(ParsingError, match="Configuration file not found"):
        get_source_params("any", config_path="nonexistent.yml")
