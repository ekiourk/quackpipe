"""
Tests for the core quackpipe functions.
"""
import textwrap

import pytest

from quackpipe import ConfigError, get_source_params


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
    Tests that the config function raises a ValueError when the source is not found.
    """
    config_yml = textwrap.dedent("""
        sources:
          test_source:
            type: "postgres"
            secret_name: "TEST_SOURCE"
    """)
    config_path = tmp_path / "config.yml"
    config_path.write_text(config_yml)

    with pytest.raises(ConfigError):
        get_source_params("not_found", config_path=str(config_path))
