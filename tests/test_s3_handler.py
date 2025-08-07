"""
tests/test_s3_handler.py

This file contains pytest tests for the S3Handler class in quackpipe.
The tests are written as standalone functions, leveraging pytest features
like parametrize and fixtures for setup.
"""
import pytest

from quackpipe import configure_secret_provider
from quackpipe.sources.s3 import S3Handler


def test_s3_handler_properties():
    """Verify that the handler correctly reports its static properties."""
    # Arrange
    handler = S3Handler({
        "connection_name": "aws_explicit",
        "secret_name": "aws_creds",  # The logical name for the secret bundle
        "region": "us-east-1"
    })

    # Assert
    assert handler.required_plugins == ["httpfs"]
    assert handler.source_type == "s3"


@pytest.mark.parametrize(
    "test_id, context, secret_bundle, expected_sql_parts",
    [
        (
                "full_aws_config",
                {
                    "connection_name": "aws_explicit",
                    "secret_name": "aws_creds",  # The logical name for the secret bundle
                    "region": "us-east-1"
                },
                {
                    # These keys will be part of the env var name
                    "access_key_id": "AWS_KEY",
                    "secret_access_key": "AWS_SECRET",
                    "session_token": "AWS_TOKEN"
                },
                [
                    "CREATE OR REPLACE SECRET aws_explicit_secret (",
                    "TYPE S3",
                    "KEY_ID 'AWS_KEY'",
                    "SECRET 'AWS_SECRET'",
                    "REGION 'us-east-1'",
                    "SESSION_TOKEN 'AWS_TOKEN'",
                    ");"
                ]
        ),
        (
                "minio_config_with_bool",
                {
                    "connection_name": "minio_test",
                    "secret_name": "minio_creds",
                    "endpoint": "localhost:9000",
                    "use_ssl": False,
                    "url_style": "path"
                },
                {
                    "access_key_id": "MINIO_KEY",
                    "secret_access_key": "MINIO_SECRET"
                },
                [
                    "CREATE OR REPLACE SECRET minio_test_secret (",
                    "KEY_ID 'MINIO_KEY'",
                    "SECRET 'MINIO_SECRET'",
                    "ENDPOINT 'localhost:9000'",
                    "URL_STYLE 'path'",
                    "USE_SSL False",
                    ");"
                ]
        ),
    ]
)
def test_render_sql_with_secret_creation(monkeypatch, test_id, context, secret_bundle, expected_sql_parts):
    """
    Tests that render_sql correctly generates a CREATE SECRET statement when a
    secret_name is provided, using environment variables for the credentials.
    """
    # Arrange
    handler = S3Handler(context)
    secret_name = context["secret_name"]

    # Use monkeypatch to set environment variables based on the secret bundle.
    # This tests the actual EnvSecretProvider logic.
    for key, value in secret_bundle.items():
        env_var_name = f"{secret_name.upper()}_{key.upper()}"
        monkeypatch.setenv(env_var_name, value)

    # has set the environment variables. This ensures the provider reads the
    # correct state for this specific test run.
    configure_secret_provider(env_file=None)

    # Act
    generated_sql = handler.render_sql()

    # Assert
    # Normalize whitespace for robust comparison
    normalized_sql = " ".join(generated_sql.split())
    for part in expected_sql_parts:
        assert part in normalized_sql


@pytest.mark.parametrize(
    "test_id, context, expected_sql",
    [
        (
                "iam_role_with_region_and_endpoint",
                {
                    "connection_name": "iam_test",
                    "region": "us-west-2",
                    "endpoint": "s3.us-west-2.amazonaws.com",
                    "use_ssl": True
                    # No 'secret_name' provided
                },
                "SET s3_region = 'us-west-2';\nSET s3_endpoint = 's3.us-west-2.amazonaws.com';\nSET s3_use_ssl = True;"
        ),
        (
                "region_only",
                {
                    "connection_name": "iam_test_minimal",
                    "region": "eu-central-1"
                },
                "SET s3_region = 'eu-central-1';"
        ),
        (
                "no_relevant_keys",
                {
                    "connection_name": "iam_test_empty",
                    "some_other_key": "value"
                },
                ""  # Expect an empty string if no relevant keys are present
        ),
    ]
)
def test_render_sql_with_set_commands(test_id, context, expected_sql):
    """
    Tests that render_sql correctly generates SET commands when no secret_name
    is provided, for scenarios like IAM-based authentication.
    """

    generated_sql = S3Handler(context).render_sql()
    assert generated_sql.strip() == expected_sql.strip()


def test_integration_with_minio_e2e(minio_container_with_data, quackpipe_with_minio):
    bucket_name = 'test-bucket'  # this is the name of the bucket that is set on TEST_BUCKET_NAME
    with quackpipe_with_minio.session() as con:
        results = con.execute(
            f"FROM read_parquet('s3://{bucket_name}/data/employees.parquet')"
        ).fetchall()
        assert len(results) == 5

        results = con.execute(
            f"FROM read_parquet('s3://{bucket_name}/data/employees.parquet') WHERE department='Engineering'"
        ).fetchall()
        assert len(results) == 2
        assert results[0][1] == "Alice"
        assert results[1][1] == "Diana"
