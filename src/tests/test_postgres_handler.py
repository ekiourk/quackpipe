"""
tests/test_postgres_handler.py

This file contains pytest tests for the PostgresHandler class in quackpipe.
The tests are written as standalone functions, leveraging pytest features
like parametrize and fixtures for setup.
"""
import pytest
from quackpipe.sources.postgres import PostgresHandler

def test_postgres_handler_properties():
    """Verify that the handler correctly reports its static properties."""
    # Arrange
    handler = PostgresHandler()

    # Assert
    assert handler.required_plugins == ["postgres"]
    assert handler.source_type == "postgres"

@pytest.mark.parametrize(
    "test_id, context, expected_sql_parts, unexpected_sql_parts",
    [
        (
            "basic_config_is_readonly",
            {
                "connection_name": "pg_test",
                "secret_name": "pg_creds",
                "port": 5433
                # read_only defaults to True
            },
            [
                "CREATE OR REPLACE SECRET pg_test_secret",
                "ATTACH 'dbname=testdb' AS pg_test (TYPE POSTGRES, SECRET 'pg_test_secret', READ_ONLY);"
            ],
            [] # No unexpected parts
        ),
        (
            "read_write_config",
            {
                "connection_name": "pg_rw",
                "secret_name": "pg_creds",
                "port": 5432,
                "read_only": False # Explicitly set to read-write
            },
            [
                "CREATE OR REPLACE SECRET pg_rw_secret",
                "ATTACH 'dbname=testdb' AS pg_rw (TYPE POSTGRES, SECRET 'pg_rw_secret');"
            ],
            ["READ_ONLY"] # Should NOT contain the READ_ONLY flag
        ),
        (
            "with_table_views",
            {
                "connection_name": "pg_views",
                "secret_name": "pg_creds",
                "port": 5432,
                "tables": ["users", "products"]
            },
            [
                "CREATE OR REPLACE SECRET pg_views_secret",
                "ATTACH 'dbname=testdb' AS pg_views",
                "READ_ONLY", # Default is read-only
                "CREATE OR REPLACE VIEW pg_views_users AS SELECT * FROM pg_views.users;",
                "CREATE OR REPLACE VIEW pg_views_products AS SELECT * FROM pg_views.products;"
            ],
            []
        ),
    ]
)
def test_postgres_render_sql(monkeypatch, test_id, context, expected_sql_parts, unexpected_sql_parts):
    """
    Tests that the PostgresHandler's render_sql method correctly generates
    a CREATE SECRET statement followed by an ATTACH statement.
    """
    # Arrange
    handler = PostgresHandler()
    secret_name = context["secret_name"]

    # Use monkeypatch to set the environment variables for the secret bundle.
    monkeypatch.setenv(f"{secret_name.upper()}_DATABASE", "testdb")
    monkeypatch.setenv(f"{secret_name.upper()}_USER", "pguser")
    monkeypatch.setenv(f"{secret_name.upper()}_PASSWORD", "pgpass")
    monkeypatch.setenv(f"{secret_name.upper()}_HOST", "localhost")

    # Act
    generated_sql = handler.render_sql(context)

    # Assert
    # Normalize whitespace for robust comparison
    normalized_sql = " ".join(generated_sql.split())

    for part in expected_sql_parts:
        normalized_part = " ".join(part.split())
        assert normalized_part in normalized_sql

    for part in unexpected_sql_parts:
        assert part not in normalized_sql


def test_postgres_handler_render_sql():
    """Test PostgresHandler SQL rendering."""
    handler = PostgresHandler()

    context = {
        'database': 'testdb',
        'user': 'testuser',
        'password': 'testpass',
        'host': 'localhost',
        'port': 5432,
        'connection_name': 'pg_main',
        'read_only': True,
        'tables': ['users', 'orders']
    }

    sql = handler.render_sql(context)

    assert "ATTACH" in sql
    assert "pg_main" in sql
    assert "POSTGRES" in sql
    assert "READ_ONLY" in sql
    assert "CREATE OR REPLACE VIEW pg_main_users" in sql
    assert "CREATE OR REPLACE VIEW pg_main_orders" in sql


def test_postgres_handler_no_tables():
    """Test PostgresHandler without tables."""
    handler = PostgresHandler()

    context = {
        'database': 'testdb',
        'user': 'testuser',
        'password': 'testpass',
        'host': 'localhost',
        'port': 5432,
        'connection_name': 'pg_main',
        'read_only': False
    }

    sql = handler.render_sql(context)

    assert "ATTACH" in sql
    assert "READ_ONLY" not in sql
    assert "CREATE OR REPLACE VIEW" not in sql
