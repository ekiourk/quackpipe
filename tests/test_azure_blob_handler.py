"""
tests/test_azure_blob_handler.py

This file contains pytest tests for the AzureBlobHandler class in quackpipe.
"""
import pytest
from testcontainers.azurite import AzuriteContainer

from quackpipe import QuackpipeBuilder
from quackpipe.secrets import configure_secret_provider
from quackpipe.sources.azure_blob import AzureBlobHandler


def test_azure_handler_properties():
    """Verify that the handler correctly reports its static properties."""
    handler = AzureBlobHandler(context={})
    assert set(handler.required_plugins) == {"azure", "httpfs"}
    assert handler.source_type == "azure"


def test_render_sql_with_connection_string(monkeypatch):
    """
    Tests the handler when using a connection string for authentication.
    """
    # Arrange
    context = {
        "connection_name": "azure_cs",
        "secret_name": "azure_prod_cs",
        "provider": "connection_string"
    }
    monkeypatch.setenv("AZURE_PROD_CS_CONNECTION_STRING", "DefaultEndpointsProtocol=https...")

    configure_secret_provider()

    handler = AzureBlobHandler(context)

    # Act
    generated_sql = handler.render_sql()

    # Assert
    expected_parts = [
        "CREATE OR REPLACE SECRET azure_cs_secret",
        "TYPE AZURE",
        "CONNECTION_STRING 'DefaultEndpointsProtocol=https...'"
    ]
    normalized_sql = " ".join(generated_sql.split())
    for part in expected_parts:
        assert part in normalized_sql


def test_render_sql_with_service_principal(monkeypatch):
    """
    Tests the handler when using a service principal for authentication.
    """
    # Arrange
    context = {
        "connection_name": "azure_sp",
        "secret_name": "azure_prod_sp",
        "provider": "service_principal",
        "account": "myazurestorage"  # Can be in config or secret
    }
    monkeypatch.setenv("AZURE_PROD_SP_TENANT_ID", "tenant-id-123")
    monkeypatch.setenv("AZURE_PROD_SP_CLIENT_ID", "client-id-456")
    monkeypatch.setenv("AZURE_PROD_SP_CLIENT_SECRET", "client-secret-789")

    configure_secret_provider()

    handler = AzureBlobHandler(context)

    # Act
    generated_sql = handler.render_sql()

    # Assert
    expected_parts = [
        "CREATE OR REPLACE SECRET azure_sp_secret",
        "TYPE AZURE",
        "PROVIDER 'service_principal'",
        "ACCOUNT 'myazurestorage'",
        "TENANT_ID 'tenant-id-123'",
        "CLIENT_ID 'client-id-456'",
        "CLIENT_SECRET 'client-secret-789'"
    ]
    normalized_sql = " ".join(generated_sql.split())
    for part in expected_parts:
        assert part in normalized_sql


def test_render_sql_with_managed_identity():
    """
    Tests the handler when using a managed identity (credential_chain).
    """
    # Arrange
    context = {
        "connection_name": "azure_mi",
        "provider": "managed_identity",
        "account": "myazurestorage"
    }
    handler = AzureBlobHandler(context)

    # Act
    generated_sql = handler.render_sql()

    # Assert
    expected_parts = [
        "CREATE OR REPLACE SECRET azure_mi_secret",
        "TYPE AZURE",
        "PROVIDER 'credential_chain'",
        "ACCOUNT 'myazurestorage'"
    ]
    normalized_sql = " ".join(generated_sql.split())
    for part in expected_parts:
        assert part in normalized_sql


def test_render_sql_raises_error_for_invalid_provider():
    """
    Tests that a ValueError is raised for an unsupported provider type.
    """
    context = {"connection_name": "azure_bad", "provider": "invalid_method"}
    handler = AzureBlobHandler(context)
    with pytest.raises(ValueError, match="Unsupported Azure provider type: 'invalid_method'"):
        handler.render_sql()


# ==================== END-TO-END INTEGRATION TEST ====================

def test_e2e_read_from_azure(azurite_container_with_data: AzuriteContainer, quackpipe_with_azurite: QuackpipeBuilder):
    """
    Tests reading pre-existing Parquet data from an Azurite container,
    validating the AzureBlobHandler.
    """

    # Open a session and run queries against the data
    with quackpipe_with_azurite.session() as con:
        blob_container_name = "test-container"  # this is the same value set on TEST_BLOB_CONTAINER_NAME in fixtures

        # Test reading all records
        results = con.execute(f"FROM read_parquet('azure://{blob_container_name}/employees.parquet');").fetchall()
        assert len(results) == 5

        # Test filtering records
        filtered_results = con.execute(
            f"FROM read_parquet('azure://{blob_container_name}/employees.parquet') WHERE department='Engineering';"
        ).fetchall()

        assert len(filtered_results) == 2
        # Convert results to a set of names for order-agnostic comparison
        names = {row[1] for row in filtered_results}
        assert names == {"Alice", "Diana"}

    print("\nIntegration test successful: Data read from Azure Blob Storage correctly.")
