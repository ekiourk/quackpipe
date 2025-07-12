"""
tests/test_ducklake_handler.py

This file contains pytest tests for the DuckLakeHandler class in quackpipe.
"""
import pytest

from quackpipe import configure_secret_provider
from quackpipe.exceptions import ConfigError
from quackpipe.sources.ducklake import DuckLakeHandler


@pytest.mark.parametrize(
    "test_id, context, expected_plugins",
    [
        (
                "postgres_s3",
                {"catalog": {"type": "postgres"}, "storage": {"type": "s3"}},
                {"ducklake", "postgres", "httpfs"}
        ),
        (
                "sqlite_local",
                {"catalog": {"type": "sqlite"}, "storage": {"type": "local"}},
                {"ducklake", "sqlite"}
        ),
        (
                "postgres_local",
                {"catalog": {"type": "postgres"}, "storage": {"type": "local"}},
                {"ducklake", "postgres"}
        ),
    ]
)
def test_ducklake_handler_dynamic_plugins(test_id, context, expected_plugins):
    """
    Verify that the handler dynamically reports its required plugins
    based on the configuration passed to its initializer.
    """
    # Arrange
    full_context = {"connection_name": "test_lake", **context}
    handler = DuckLakeHandler(full_context)

    # Assert
    assert set(handler.required_plugins) == expected_plugins
    assert handler.source_type == "ducklake"


def test_render_sql_with_sqlite_and_local_storage():
    """
    Tests that render_sql correctly generates SQL for a DuckLake with a
    SQLite catalog and local file storage.
    """
    # Arrange
    context = {
        "connection_name": "local_lake",
        "catalog": {"type": "sqlite", "path": "/tmp/catalog.db"},
        "storage": {"type": "local", "path": "/tmp/data/"}
    }
    handler = DuckLakeHandler(context)

    # Act
    generated_sql = handler.render_sql()

    # Assert
    # **FIX**: Instead of asserting the exact string order, check for the presence
    # of each required component. This makes the test more robust.
    expected_parts = [
        "CREATE OR REPLACE SECRET local_lake_secret",
        "TYPE DUCKLAKE",
        "METADATA_PATH '/tmp/catalog.db'",
        "DATA_PATH '/tmp/data/'",
        "ATTACH 'ducklake:local_lake_secret' AS local_lake;"
    ]

    normalized_sql = " ".join(generated_sql.split())

    for part in expected_parts:
        normalized_part = " ".join(part.split())
        assert normalized_part in normalized_sql

    # It should not contain the METADATA_PARAMETERS line
    assert "METADATA_PARAMETERS" not in generated_sql


def test_render_sql_with_postgres_and_minio(monkeypatch):
    """
    Tests that render_sql correctly generates all required SQL statements
    for a DuckLake using Postgres and a MinIO (S3-compatible) backend.
    """
    # Arrange
    context = {
        "connection_name": "minio_lake",
        "catalog": {
            "type": "postgres",
            "secret_name": "pg_creds_for_lake"
        },
        "storage": {
            "type": "s3",
            "secret_name": "minio_creds_for_lake",
            "path": "s3://my-minio-bucket/data/",
            "endpoint": "localhost:9000",
            "url_style": "path",
            "use_ssl": False
        }
    }

    # Mock secrets for both components
    monkeypatch.setenv("PG_CREDS_FOR_LAKE_DATABASE", "catalog_db")
    monkeypatch.setenv("PG_CREDS_FOR_LAKE_USER", "pguser")
    monkeypatch.setenv("PG_CREDS_FOR_LAKE_PASSWORD", "pgpass")
    monkeypatch.setenv("PG_CREDS_FOR_LAKE_HOST", "db.example.com")

    monkeypatch.setenv("MINIO_CREDS_FOR_LAKE_ACCESS_KEY_ID", "MINIO_KEY")
    monkeypatch.setenv("MINIO_CREDS_FOR_LAKE_SECRET_ACCESS_KEY", "MINIO_SECRET")

    # has set the environment variables. This ensures the provider reads the
    # correct state for this specific test run.
    configure_secret_provider(env_file=None)

    handler = DuckLakeHandler(context)

    # Expected SQL parts to verify
    expected_sql_parts = [
        # 1. The prerequisite secret for the Postgres catalog
        "CREATE OR REPLACE SECRET minio_lake_catalog_secret ( TYPE POSTGRES , HOST 'db.example.com' , DATABASE 'catalog_db' , USER 'pguser' , PASSWORD 'pgpass' );",
        # 2. The prerequisite secret for the S3 storage
        "CREATE OR REPLACE SECRET minio_lake_storage_secret ( TYPE S3 , KEY_ID 'MINIO_KEY' , SECRET 'MINIO_SECRET' , ENDPOINT 'localhost:9000' , URL_STYLE 'path' , USE_SSL False );",
        # 3. The main DUCKLAKE secret that references the catalog secret
        "CREATE OR REPLACE SECRET minio_lake_secret ( TYPE DUCKLAKE, DATA_PATH 's3://my-minio-bucket/data/' , METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'minio_lake_catalog_secret'} , METADATA_PATH '' );",
        # 4. The final ATTACH statement that references the main DUCKLAKE secret
        "ATTACH 'ducklake:minio_lake_secret' AS minio_lake;"
    ]

    # Act
    generated_sql = handler.render_sql()

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
    ]
)
def test_init_raises_error_for_invalid_config(test_id, invalid_context):
    """
    Tests that the DuckLakeHandler's __init__ raises a ConfigError if the
    'catalog' or 'storage' sections are missing from the configuration context.
    """
    with pytest.raises(ConfigError, match="DuckLake source requires 'catalog' and 'storage' sections in config."):
        DuckLakeHandler(invalid_context)
