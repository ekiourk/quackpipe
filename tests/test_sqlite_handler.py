"""
tests/test_sqlite_handler.py

This file contains pytest tests for the SQLiteHandler class in quackpipe.
"""
import pytest

from quackpipe import QuackpipeBuilder, configure_secret_provider
from quackpipe.exceptions import ValidationError
from quackpipe.sources.sqlite import SQLiteHandler


def test_sqlite_handler_properties():
    """Verify that the handler correctly reports its static properties."""
    # Arrange: Context is needed for initialization, but can be empty for this test.
    handler = SQLiteHandler(context={})

    # Assert
    assert handler.required_plugins == ["sqlite"]
    assert handler.source_type == "sqlite"


@pytest.mark.parametrize(
    "test_id, context, expected_sql, unexpected_sql_parts",
    [
        (
                "read_only_default",
                {
                    "connection_name": "analytics_db",
                    "path": "/data/analytics.db"
                    # read_only defaults to True
                },
                "ATTACH '/data/analytics.db' AS analytics_db (TYPE SQLITE, READ_ONLY);",
                []  # No unexpected parts
        ),
        (
                "read_write_explicit",
                {
                    "connection_name": "main_db",
                    "path": "main.sqlite",
                    "read_only": False  # Explicitly set to read-write
                },
                "ATTACH 'main.sqlite' AS main_db (TYPE SQLITE);",
                ["READ_ONLY"]  # Should NOT contain the READ_ONLY flag
        ),
        (
                "read_only_explicit",
                {
                    "connection_name": "archive",
                    "path": "archive.db",
                    "read_only": True
                },
                "ATTACH 'archive.db' AS archive (TYPE SQLITE, READ_ONLY);",
                []
        ),
        (
                "with_encryption",
                {
                    "connection_name": "secure_db",
                    "path": "secure.db",
                    "encryption_key": "secret_key_123"
                },
                "ATTACH 'secure.db' AS secure_db (TYPE SQLITE, READ_ONLY, ENCRYPTION_KEY 'secret_key_123');",
                []
        ),
    ]
)
def test_sqlite_render_sql(test_id, context, expected_sql, unexpected_sql_parts):
    """
    Tests that the SQLiteHandler's render_sql method correctly generates
    the ATTACH statement for various configurations.
    """
    # Arrange
    handler = SQLiteHandler(context)

    # Act
    generated_sql = handler.render_sql()

    # Assert
    # Normalize whitespace for robust comparison
    normalized_sql = " ".join(generated_sql.split())
    normalized_expected = " ".join(expected_sql.split())

    assert normalized_sql == normalized_expected

    for part in unexpected_sql_parts:
        assert part not in normalized_sql


def test_sqlite_render_sql_with_secrets(monkeypatch):
    """
    Tests that the SQLiteHandler correctly fetches and merges secrets.
    """
    # Arrange
    secret_name = "my_sqlite_secret"
    monkeypatch.setenv("MY_SQLITE_SECRET_ENCRYPTION_KEY", "secret_from_env")
    configure_secret_provider(env_file=None)

    context = {
        "connection_name": "secure_sqlite",
        "path": "secure.db",
        "secret_name": secret_name
    }
    handler = SQLiteHandler(context)

    # Act
    generated_sql = handler.render_sql()

    # Assert
    assert "ENCRYPTION_KEY 'secret_from_env'" in generated_sql


def test_sqlite_render_sql_raises_error_if_path_is_missing():
    """
    Tests that a ValidationError is raised if the 'path' key is
    missing from the configuration.
    """
    builder = QuackpipeBuilder()
    with pytest.raises(ValidationError, match="Sqlite source requires 'path' in its configuration."):
        builder.add_source(name="bad_config", type="sqlite", config={})
