"""
tests/test_ducklake_handler.py

This file contains pytest tests for the DuckLakeHandler class in quackpipe.
"""
import pytest
from quackpipe.sources.ducklake import DuckLakeHandler

def test_ducklake_handler_properties():
    """Verify that the handler correctly reports its static properties."""
    # Arrange
    handler = DuckLakeHandler()

    # Assert
    assert handler.required_plugins == ["ducklake", "postgres", "httpfs"]
    assert handler.source_type == "ducklake"

def test_render_sql_with_valid_config(monkeypatch):
    """
    Tests that render_sql correctly generates all required SQL statements
    for a valid DuckLake configuration.
    """
    # Arrange
    handler = DuckLakeHandler()
    context = {
        "connection_name": "my_lake",
        "catalog": {
            "type": "postgres",
            "secret_name": "pg_creds_for_lake"
        },
        "storage": {
            "type": "s3",
            "secret_name": "s3_creds_for_lake",
            "path": "s3://my-bucket/data/"
        }
    }

    # Use monkeypatch to set environment variables for BOTH secret bundles.
    # Postgres catalog secrets
    monkeypatch.setenv("PG_CREDS_FOR_LAKE_DATABASE", "lake_catalog_db")
    monkeypatch.setenv("PG_CREDS_FOR_LAKE_USER", "lake_user")
    monkeypatch.setenv("PG_CREDS_FOR_LAKE_PASSWORD", "lake_pass")
    monkeypatch.setenv("PG_CREDS_FOR_LAKE_HOST", "db.example.com")
    monkeypatch.setenv("PG_CREDS_FOR_LAKE_PORT", "5432")

    # S3 storage secrets
    monkeypatch.setenv("S3_CREDS_FOR_LAKE_ACCESS_KEY_ID", "LAKE_AWS_KEY")
    monkeypatch.setenv("S3_CREDS_FOR_LAKE_SECRET_ACCESS_KEY", "LAKE_AWS_SECRET")
    monkeypatch.setenv("S3_CREDS_FOR_LAKE_REGION", "eu-west-1")

    # Expected SQL parts to verify
    expected_sql_parts = [
        # Catalog Secret
        "CREATE OR REPLACE SECRET my_lake_catalog_secret (",
        "TYPE POSTGRES",
        "HOST 'db.example.com'",
        "DATABASE 'lake_catalog_db'",
        # Storage Secret
        "CREATE OR REPLACE SECRET my_lake_storage_secret (",
        "TYPE S3",
        "KEY_ID 'LAKE_AWS_KEY'",
        "SECRET 'LAKE_AWS_SECRET'",
        "REGION 'eu-west-1'",
        # Attach Statement
        "ATTACH 'ducklake:postgres:my_lake_catalog_secret' AS my_lake",
        "DATA_PATH 's3://my-bucket/data/'",
        "STORAGE_SECRET 'my_lake_storage_secret'"
    ]

    # Act
    generated_sql = handler.render_sql(context)

    # Assert
    normalized_sql = " ".join(generated_sql.split())
    for part in expected_sql_parts:
        normalized_part = " ".join(part.split())
        assert normalized_part in normalized_sql

@pytest.mark.parametrize(
    "test_id, invalid_context",
    [
        ("missing_catalog", {"connection_name": "test", "storage": {}}),
        ("missing_storage", {"connection_name": "test", "catalog": {}}),
        ("empty_context", {"connection_name": "test"}),
    ]
)
def test_render_sql_raises_error_for_invalid_config(test_id, invalid_context):
    """
    Tests that render_sql raises a ValueError if the 'catalog' or 'storage'
    sections are missing from the configuration context.
    """
    # Arrange
    handler = DuckLakeHandler()

    # Act & Assert
    with pytest.raises(ValueError, match="DuckLake source requires 'catalog' and 'storage' sections in config."):
        handler.render_sql(invalid_context)

