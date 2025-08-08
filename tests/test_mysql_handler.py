"""
tests/test_mysql_handler.py

This file contains pytest tests for the MySQLHandler class in quackpipe.
The tests are written as standalone functions, leveraging pytest features
like parametrize and fixtures for setup.
"""
import os

import pytest

from quackpipe import configure_secret_provider
from quackpipe.sources.mysql import MySQLHandler


@pytest.fixture(scope="function")
def mysql_env_vars(monkeypatch) -> dict[str, str]:
    secret_name = 'mysql_creds'
    # Use monkeypatch to set the environment variables for the secret bundle.
    monkeypatch.setenv(f"{secret_name.upper()}_DATABASE", "testdb")
    monkeypatch.setenv(f"{secret_name.upper()}_USER", "mysqluser")
    monkeypatch.setenv(f"{secret_name.upper()}_PASSWORD", "mysqlpass")
    monkeypatch.setenv(f"{secret_name.upper()}_HOST", "localhost")
    # has set the environment variables. This ensures the provider reads the
    # correct state for this specific test run.
    configure_secret_provider(env_file=None)
    return dict(os.environ)


def test_mysql_handler_properties(mysql_env_vars):
    """Verify that the handler correctly reports its static properties."""
    # Arrange
    handler = MySQLHandler({
        "connection_name": "mysql_test",
        "secret_name": "mysql_creds",
        "port": 3306
        # read_only defaults to True
    })

    # Assert
    assert handler.required_plugins == ["mysql"]
    assert handler.source_type == "mysql"


@pytest.mark.parametrize(
    "test_id, context, expected_sql_parts, unexpected_sql_parts",
    [
        (
            "basic_config_is_readonly",
            {
                "connection_name": "mysql_test",
                "secret_name": "mysql_creds",
                "port": 3306
                # read_only defaults to True
            },
            [
                "CREATE OR REPLACE SECRET mysql_test_secret",
                "ATTACH '' AS mysql_test (TYPE MYSQL, SECRET mysql_test_secret, READ_ONLY);"
            ],
            []  # No unexpected parts
        ),
        (
            "read_write_config",
            {
                "connection_name": "mysql_rw",
                "secret_name": "mysql_creds",
                "port": 3306,
                "read_only": False  # Explicitly set to read-write
            },
            [
                "CREATE OR REPLACE SECRET mysql_rw_secret",
                "ATTACH '' AS mysql_rw (TYPE MYSQL, SECRET mysql_rw_secret);"
            ],
            ["READ_ONLY"]  # Should NOT contain the READ_ONLY flag
        ),
        (
            "with_table_views",
            {
                "connection_name": "mysql_views",
                "secret_name": "mysql_creds",
                "port": 3306,
                "tables": ["users", "products"]
            },
            [
                "CREATE OR REPLACE SECRET mysql_views_secret",
                "ATTACH '' AS mysql_views",
                "READ_ONLY",  # Default is read-only
                "CREATE OR REPLACE VIEW mysql_views_users AS SELECT * FROM mysql_views.users;",
                "CREATE OR REPLACE VIEW mysql_views_products AS SELECT * FROM mysql_views.products;"
            ],
            []
        ),
    ]
)
def test_mysql_render_sql(mysql_env_vars, test_id, context, expected_sql_parts, unexpected_sql_parts):
    """
    Tests that the MySQLHandler's render_sql method correctly generates
    a CREATE SECRET statement followed by an ATTACH statement.
    """
    handler = MySQLHandler(context)

    # Act
    generated_sql = handler.render_sql()

    # Assert
    # Normalize whitespace for robust comparison
    normalized_sql = " ".join(generated_sql.split())

    for part in expected_sql_parts:
        normalized_part = " ".join(part.split())
        assert normalized_part in normalized_sql

    for part in unexpected_sql_parts:
        assert part not in normalized_sql


def test_mysql_handler_render_sql():
    """Test MySQLHandler SQL rendering."""

    context = {
        'database': 'testdb',
        'user': 'testuser',
        'password': 'testpass',
        'host': 'localhost',
        'port': 3306,
        'connection_name': 'mysql_main',
        'read_only': True,
        'tables': ['users', 'orders']
    }
    handler = MySQLHandler(context)

    sql = handler.render_sql()

    assert "ATTACH" in sql
    assert "mysql_main" in sql
    assert "MYSQL" in sql
    assert "READ_ONLY" in sql
    assert "CREATE OR REPLACE VIEW mysql_main_users" in sql
    assert "CREATE OR REPLACE VIEW mysql_main_orders" in sql


def test_mysql_handler_no_tables():
    """Test MySQLHandler without tables."""

    context = {
        'database': 'testdb',
        'user': 'testuser',
        'password': 'testpass',
        'host': 'localhost',
        'port': 3306,
        'connection_name': 'mysql_main',
        'read_only': False
    }

    sql = MySQLHandler(context).render_sql()

    assert "ATTACH" in sql
    assert "READ_ONLY" not in sql
    assert "CREATE OR REPLACE VIEW" not in sql


def test_integration_with_mysql_e2e(quackpipe_with_mysql_source):
    with quackpipe_with_mysql_source.session() as con:
        results = con.execute(
            "FROM mysql_source.company_employees"
        ).fetchall()
        assert len(results) == 5

        # check the view
        results = con.execute(
            "FROM mysql_source_company_employees"
        ).fetchall()
        assert len(results) == 5

        results = con.execute(
            "FROM mysql_source.company_employees WHERE department='Engineering'"
        ).fetchall()
        assert len(results) == 2
        assert results[0][1] == "Alice"
        assert results[1][1] == "Diana"

        results = con.execute('FROM mysql_source.vessels').fetchall()
        assert len(results) == 5

        # check the view
        results = con.execute("FROM mysql_source_vessels").fetchall()
        assert len(results) == 5
