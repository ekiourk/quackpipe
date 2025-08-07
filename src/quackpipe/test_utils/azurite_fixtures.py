import io
from collections.abc import Generator

import pandas as pd
import pytest
from azure.storage.blob import BlobServiceClient, ContainerClient
from testcontainers.azurite import AzuriteContainer

from quackpipe import QuackpipeBuilder, SourceType
from quackpipe.test_utils.data_generators import create_employee_data

TEST_BLOB_CONTAINER_NAME = "test-container"


@pytest.fixture(scope="module")
def azurite_container() -> Generator[AzuriteContainer, None, None]:
    """Starts an Azurite docker container."""
    with AzuriteContainer("mcr.microsoft.com/azure-storage/azurite:latest") as azurite:
        yield azurite


@pytest.fixture(scope="module")
def quackpipe_with_azurite(azurite_container) -> QuackpipeBuilder:
    builder = QuackpipeBuilder().add_source(
            name="my_azure_storage",
            type=SourceType.AZURE,
            config={
                "provider": "connection_string",
                "connection_string": azurite_container.get_connection_string(),
            }
        )
    return builder


@pytest.fixture(scope="module")
def azurite_test_container_client(azurite_container) -> ContainerClient:
    """Gets an Azurite container and creates and returns a blob container client."""
    blob_service_client = BlobServiceClient.from_connection_string(azurite_container.get_connection_string())

    blob_service_client.create_container(TEST_BLOB_CONTAINER_NAME)

    # Get a client for the specific container to upload blobs
    return blob_service_client.get_container_client(container=TEST_BLOB_CONTAINER_NAME)


@pytest.fixture(scope="module")
def azurite_container_with_data(azurite_container, azurite_test_container_client) -> AzuriteContainer:
    """
    Starts an Azurite container and populates it with sample data.
    Returns the Docker container object
    """
    # connection string from the container.

    # Generate and upload sample employee data as a Parquet file
    df = pd.DataFrame(create_employee_data())

    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_data = parquet_buffer.getvalue()

    azurite_test_container_client.upload_blob(
        name="employees.parquet",
        data=io.BytesIO(parquet_data),
        length=len(parquet_data),
        overwrite=True
    )
    print(f"Uploaded employees.parquet to Azurite container '{azurite_test_container_client.container_name}'.")

    return azurite_container
