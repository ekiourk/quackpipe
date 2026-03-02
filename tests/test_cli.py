"""
tests/test_cli.py

This file contains pytest tests for the CLI functions in cli.py.
"""

import io
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from quackpipe import configure_secret_provider
from quackpipe.cli import main
from quackpipe.commands.generate_sqlmesh_config import _replace_secrets_with_placeholders
from quackpipe.config import SourceConfig, SourceParams, SourceType

# ==================== UNIT TESTS FOR HELPER FUNCTIONS ====================


@pytest.mark.parametrize(
    "test_id, raw_sql, configs, secrets_bundle, expected_sql",
    [
        (
            "simple_postgres_replacement",
            "ATTACH 'dbname=testdb' (SECRET 'my_password');",
            [SourceConfig(name="pg", type=SourceType.POSTGRES, secret_name="pg_creds")],
            {"PG_CREDS_PASSWORD": "my_password"},
            "ATTACH 'dbname=testdb' (SECRET '${PG_CREDS_PASSWORD}');",
        ),
        (
            "substring_value_protection",
            "CREATE SECRET (KEY_ID 'admin', SECRET 'minioadmin');",
            [SourceConfig(name="s3", type=SourceType.S3, secret_name="minio")],
            {"MINIO_KEY_ID": "admin", "MINIO_SECRET": "minioadmin"},
            "CREATE SECRET (KEY_ID '${MINIO_KEY_ID}', SECRET '${MINIO_SECRET}');",
        ),
        (
            "no_secret_name_no_change",
            "ATTACH 'my.db' (TYPE SQLITE);",
            [SourceConfig(name="sqlite", type=SourceType.SQLITE, secret_name=None)],
            {},  # No secrets to fetch
            "ATTACH 'my.db' (TYPE SQLITE);",
        ),
        (
            "numeric_value_replacement",
            "CREATE SECRET (TYPE POSTGRES, PORT 5432);",
            [SourceConfig(name="pg", type=SourceType.POSTGRES, secret_name="pg_port_test")],
            {"PG_PORT_TEST_PORT": 5432},
            "CREATE SECRET (TYPE POSTGRES, PORT ${PG_PORT_TEST_PORT});",
        ),
        (
            "multiple_configs_and_secrets",
            "ATTACH 'pg' (SECRET 'pg_pass'); CREATE SECRET (TYPE S3, KEY_ID 's3_key');",
            [
                SourceConfig(name="pg", type=SourceType.POSTGRES, secret_name="db1"),
                SourceConfig(name="s3", type=SourceType.S3, secret_name="store1"),
            ],
            # Mock returns different bundles based on the secret_name
            {"db1": {"DB1_PASSWORD": "pg_pass"}, "store1": {"STORE1_KEY_ID": "s3_key"}},
            "ATTACH 'pg' (SECRET '${DB1_PASSWORD}'); CREATE SECRET (TYPE S3, KEY_ID '${STORE1_KEY_ID}');",
        ),
    ],
)
def test_replace_secrets_with_placeholders(monkeypatch, test_id, raw_sql, configs, secrets_bundle, expected_sql):
    """
    Tests the _replace_secrets_with_placeholders function with various scenarios.
    """
    # Arrange: Mock the fetch_raw_secret_bundle function to return our test data.
    # The lambda allows the mock to return different values based on the secret_name.
    with patch("quackpipe.commands.generate_sqlmesh_config.fetch_raw_secret_bundle") as mock_fetch:
        # If the bundle key is the secret name, use it, otherwise it's a single bundle
        mock_fetch.side_effect = lambda name: secrets_bundle.get(name, secrets_bundle)

        # Act
        result_sql = _replace_secrets_with_placeholders(raw_sql, configs)

        # Assert
        assert result_sql == expected_sql


# ==================== INTEGRATION TEST FOR CLI COMMAND ====================


@patch("quackpipe.commands.generate_sqlmesh_config.get_configs")
@patch("quackpipe.commands.generate_sqlmesh_config.yaml.dump")
@patch("pathlib.Path.open")
def test_generate_sqlmesh_config_command(mock_path_open, mock_yaml_dump, mock_get_configs, monkeypatch):
    """
    Tests the end-to-end flow of the generate_sqlmesh_config function.
    """
    # Arrange
    # 1. Mock the input quackpipe config
    mock_configs = [
        SourceConfig(
            name="my_source",
            type=SourceType.POSTGRES,
            secret_name="prod_db",
            config=SourceParams({"host": "localhost"}),  # Non-secret config
        )
    ]
    mock_get_configs.return_value = mock_configs

    # 2. Mock the environment variables that will be fetched
    monkeypatch.setenv("PROD_DB_USER", "test_user")
    monkeypatch.setenv("PROD_DB_PASSWORD", "test_pass")

    # has set the environment variables. This ensures the provider reads the
    # correct state for this specific test run.
    configure_secret_provider(env_file=None)

    # 3. Mock the handler to return a predictable SQL template
    mock_handler_instance = MagicMock()
    # The raw SQL contains the real secrets that need to be replaced
    mock_handler_instance.render_sql.return_value = "ATTACH (USER 'test_user', PASSWORD 'test_pass', HOST 'localhost');"

    with patch.dict(
        "quackpipe.commands.generate_sqlmesh_config.SOURCE_HANDLER_REGISTRY",
        {SourceType.POSTGRES: MagicMock(return_value=mock_handler_instance)},
    ):
        # 4. Mock the command-line arguments
        args = MagicMock()
        args.config = "config.yml"
        args.output = "sqlmesh_config.yml"
        args.gateway_name = "test_gateway"
        args.state_db = "state.db"
        args.verbose = 1  # Enable INFO logging

        # Act
        from quackpipe.commands.generate_sqlmesh_config import handler

        handler(args)

    # Assert
    # Verify that the final dictionary passed to yaml.dump is correct
    mock_yaml_dump.assert_called_once()
    call_args, _ = mock_yaml_dump.call_args
    generated_dict = call_args[0]

    # Check the structure and placeholder replacement
    assert generated_dict["default_gateway"] == "test_gateway"
    init_sql = generated_dict["gateways"]["test_gateway"]["connection"]["init"]

    # Verify that the secrets were replaced with placeholders
    assert "USER '${PROD_DB_USER}'" in init_sql
    assert "PASSWORD '${PROD_DB_PASSWORD}'" in init_sql
    # Verify that non-secret values remain untouched
    assert "HOST 'localhost'" in init_sql


# ==================== TESTS FOR VALIDATE COMMAND ====================


@patch("sys.stdout", new_callable=io.StringIO)
def test_validate_command_valid_config(mock_stdout, tmpdir):
    """Test the validate command with a valid config file."""
    config_data = {"sources": {"my_source": {"type": "sqlite", "path": "test.db"}}}
    config_path = Path(tmpdir) / "config.yml"
    with config_path.open("w") as f:
        yaml.dump(config_data, f)

    with patch.object(sys, "argv", ["quackpipe", "validate", "-v", "--config", str(config_path)]):
        main()

    output = mock_stdout.getvalue()
    assert f"Attempting to validate configuration from: ['{config_path}']" in output
    assert f"✅ Configuration from '['{config_path}']' is valid." in output


@patch("sys.stdout", new_callable=io.StringIO)
def test_validate_command_invalid_config(mock_stdout, tmpdir):
    """Test the validate command with an invalid config file."""
    config_data = {"sources": {"my_source": {"type": "sqlite"}}}  # Missing path
    config_path = Path(tmpdir) / "config.yml"
    with config_path.open("w") as f:
        yaml.dump(config_data, f)

    with (
        pytest.raises(SystemExit) as e,
        patch.object(sys, "argv", ["quackpipe", "validate", "-v", "--config", str(config_path)]),
    ):
        main()
    assert e.value.code == 1

    output = mock_stdout.getvalue()
    assert f"Attempting to validate configuration from: ['{config_path}']" in output


@patch("sys.stdout", new_callable=io.StringIO)
def test_validate_command_no_file(mock_stdout):
    """Test the validate command with a non-existent config file."""
    config_path = "non_existent_config.yml"
    with (
        pytest.raises(SystemExit) as e,
        patch.object(sys, "argv", ["quackpipe", "validate", "-v", "--config", config_path]),
    ):
        main()
    assert e.value.code == 1

    output = mock_stdout.getvalue()
    assert f"Attempting to validate configuration from: ['{config_path}']" in output


@patch("sys.stdout", new_callable=io.StringIO)
def test_validate_command_multiple_valid_configs(mock_stdout, tmpdir):
    """Test the validate command with multiple valid config files."""
    base_data = {"sources": {"my_source": {"type": "sqlite", "path": "base.db"}}}
    dev_data = {"sources": {"my_source": {"path": "dev.db"}}}

    f1 = Path(tmpdir) / "base.yml"
    f2 = Path(tmpdir) / "dev.yml"

    with f1.open("w") as f:
        yaml.dump(base_data, f)
    with f2.open("w") as f:
        yaml.dump(dev_data, f)

    with patch.object(sys, "argv", ["quackpipe", "validate", "-v", "--config", str(f1), str(f2)]):
        main()

    output = mock_stdout.getvalue()
    assert f"Attempting to validate configuration from: ['{f1}', '{f2}']" in output
    assert f"✅ Configuration from '['{f1}', '{f2}']' is valid." in output


def test_preview_config_command(capfd, tmpdir):
    """Test the preview-config command with multiple config files."""
    base_data = {"sources": {"my_source": {"type": "sqlite", "path": "base.db"}}}
    dev_data = {"sources": {"my_source": {"path": "dev.db"}}}

    f1 = Path(tmpdir) / "base.yml"
    f2 = Path(tmpdir) / "dev.yml"

    with f1.open("w") as f:
        yaml.dump(base_data, f)
    with f2.open("w") as f:
        yaml.dump(dev_data, f)

    with patch.object(sys, "argv", ["quackpipe", "preview-config", "--config", str(f1), str(f2)]):
        main()

    out, _ = capfd.readouterr()
    # The output is directly printed now.
    parsed_output = yaml.safe_load(out)

    assert parsed_output["sources"]["my_source"]["type"] == "sqlite"
    assert parsed_output["sources"]["my_source"]["path"] == "dev.db"


@patch("sys.stdout", new_callable=io.StringIO)
def test_version_flag(mock_stdout):
    """Test the --version flag."""
    with pytest.raises(SystemExit) as e, patch.object(sys, "argv", ["quackpipe", "--version"]):
        main()

    assert e.value.code == 0
    output = mock_stdout.getvalue()
    # The version output format is "%(prog)s {__version__}"
    # By default, prog is the script name (or sys.argv[0]), here mocked as 'quackpipe'
    assert "quackpipe" in output
    assert "." in output  # Basic check for version number structure


def test_main_module_entry_point():
    """Test that python -m quackpipe works via __main__.py."""
    import subprocess

    result = subprocess.run([sys.executable, "-m", "quackpipe", "--help"], capture_output=True, text=True)
    assert result.returncode == 0
    assert "quackpipe" in result.stdout
    assert "--version" in result.stdout
