"""
tests/test_cli.py

This file contains pytest tests for the CLI functions in cli.py.
"""
from unittest.mock import MagicMock, patch

import pytest

from quackpipe import configure_secret_provider
from quackpipe.commands.generate_sqlmesh_config import _replace_secrets_with_placeholders
from quackpipe.config import SourceConfig, SourceType

# ==================== UNIT TESTS FOR HELPER FUNCTIONS ====================

@pytest.mark.parametrize(
    "test_id, raw_sql, configs, secrets_bundle, expected_sql",
    [
        (
                "simple_postgres_replacement",
                "ATTACH 'dbname=testdb' (SECRET 'my_password');",
                [SourceConfig(name="pg", type=SourceType.POSTGRES, secret_name="pg_creds")],
                {"PG_CREDS_PASSWORD": "my_password"},
                "ATTACH 'dbname=testdb' (SECRET '${PG_CREDS_PASSWORD}');"
        ),
        (
                "substring_value_protection",
                "CREATE SECRET (KEY_ID 'admin', SECRET 'minioadmin');",
                [SourceConfig(name="s3", type=SourceType.S3, secret_name="minio")],
                {"MINIO_KEY_ID": "admin", "MINIO_SECRET": "minioadmin"},
                "CREATE SECRET (KEY_ID '${MINIO_KEY_ID}', SECRET '${MINIO_SECRET}');"
        ),
        (
                "no_secret_name_no_change",
                "ATTACH 'my.db' (TYPE SQLITE);",
                [SourceConfig(name="sqlite", type=SourceType.SQLITE, secret_name=None)],
                {},  # No secrets to fetch
                "ATTACH 'my.db' (TYPE SQLITE);"
        ),
        (
                "numeric_value_replacement",
                "CREATE SECRET (TYPE POSTGRES, PORT 5432);",
                [SourceConfig(name="pg", type=SourceType.POSTGRES, secret_name="pg_port_test")],
                {"PG_PORT_TEST_PORT": 5432},
                "CREATE SECRET (TYPE POSTGRES, PORT ${PG_PORT_TEST_PORT});"
        ),
        (
                "multiple_configs_and_secrets",
                "ATTACH 'pg' (SECRET 'pg_pass'); CREATE SECRET (TYPE S3, KEY_ID 's3_key');",
                [
                    SourceConfig(name="pg", type=SourceType.POSTGRES, secret_name="db1"),
                    SourceConfig(name="s3", type=SourceType.S3, secret_name="store1")
                ],
                # Mock returns different bundles based on the secret_name
                {"db1": {"DB1_PASSWORD": "pg_pass"}, "store1": {"STORE1_KEY_ID": "s3_key"}},
                "ATTACH 'pg' (SECRET '${DB1_PASSWORD}'); CREATE SECRET (TYPE S3, KEY_ID '${STORE1_KEY_ID}');"
        ),
    ]
)
def test_replace_secrets_with_placeholders(monkeypatch, test_id, raw_sql, configs, secrets_bundle, expected_sql):
    """
    Tests the _replace_secrets_with_placeholders function with various scenarios.
    """
    # Arrange: Mock the fetch_raw_secret_bundle function to return our test data.
    # The lambda allows the mock to return different values based on the secret_name.
    with patch('quackpipe.commands.generate_sqlmesh_config.fetch_raw_secret_bundle') as mock_fetch:
        # If the bundle key is the secret name, use it, otherwise it's a single bundle
        mock_fetch.side_effect = lambda name: secrets_bundle.get(name, secrets_bundle)

        # Act
        result_sql = _replace_secrets_with_placeholders(raw_sql, configs)

        # Assert
        assert result_sql == expected_sql


# ==================== INTEGRATION TEST FOR CLI COMMAND ====================

@patch('quackpipe.commands.generate_sqlmesh_config.get_configs')
@patch('quackpipe.commands.generate_sqlmesh_config.yaml.dump')
@patch('builtins.open')
def test_generate_sqlmesh_config_command(mock_open, mock_yaml_dump, mock_get_configs, monkeypatch):
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
            config={"host": "localhost"}  # Non-secret config
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

    with patch.dict('quackpipe.commands.generate_sqlmesh_config.SOURCE_HANDLER_REGISTRY',
                    {SourceType.POSTGRES: MagicMock(return_value=mock_handler_instance)}):
        # 4. Mock the command-line arguments
        args = MagicMock()
        args.config = "config.yml"
        args.output = "sqlmesh_config.yml"
        args.gateway_name = "test_gateway"
        args.state_db = "state.db"

        # Act
        from quackpipe.commands.generate_sqlmesh_config import handler
        handler(args)

    # Assert
    # Verify that the final dictionary passed to yaml.dump is correct
    mock_yaml_dump.assert_called_once()
    call_args, _ = mock_yaml_dump.call_args
    generated_dict = call_args[0]

    # Check the structure and placeholder replacement
    assert generated_dict['default_gateway'] == "test_gateway"
    init_sql = generated_dict['gateways']['test_gateway']['connection']['init']

    # Verify that the secrets were replaced with placeholders
    assert "USER '${PROD_DB_USER}'" in init_sql
    assert "PASSWORD '${PROD_DB_PASSWORD}'" in init_sql
    # Verify that non-secret values remain untouched
    assert "HOST 'localhost'" in init_sql
