"""
tests/test_sqlite_handler.py

This file contains pytest tests for the SQLiteHandler class in quackpipe.
"""
import pytest

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


def test_sqlite_render_sql_raises_error_if_path_is_missing():
    """
    Tests that render_sql raises a ValueError if the 'path' key is
    missing from the configuration context.
    """
    # Arrange
    context = {"connection_name": "bad_config"}  # Missing 'path'
    handler = SQLiteHandler(context)

    # Act & Assert
    with pytest.raises(ValueError, match="SQLite source 'bad_config' requires a 'path' in its configuration."):
        handler.render_sql()
