from collections.abc import Generator
from typing import Any

import pytest
from testcontainers.postgres import PostgresContainer

from quackpipe import SourceConfig, SourceType


@pytest.fixture(scope="module")
def catalog_postgres_container() -> Generator[PostgresContainer, Any, None]:
    """Starts a second, separate PostgreSQL container to act as the DuckLake catalog."""
    with PostgresContainer("postgres:15-alpine", username="catalog", password="catalog", dbname="catalog") as postgres:
        yield postgres


@pytest.fixture(scope="function")
def postgres_s3_ducklake_config(catalog_postgres_container, minio_container) -> SourceConfig:
    return SourceConfig(
        name="my_datalake",
        type=SourceType.DUCKLAKE,
        config={
            "catalog": {
                "type": "postgres",
                "host": catalog_postgres_container.get_container_host_ip(),
                "port": int(catalog_postgres_container.get_exposed_port(5432)),
                "user": catalog_postgres_container.username,
                "password": catalog_postgres_container.password,
                "database": catalog_postgres_container.dbname
            },
            "storage": {
                "type": "s3",
                "path": "s3://test-bucket/",  # this is the name of the bucket that is set on TEST_BUCKET_NAME
                "endpoint": minio_container.get_config()["endpoint"],
                "access_key_id": minio_container.access_key,
                "secret_access_key": minio_container.secret_key,
                "use_ssl": False,
                "url_style": "path"  # Important for MinIO
            }
        }
    )


@pytest.fixture(scope="function")
def local_ducklake_config(tmp_path) -> SourceConfig:
    # Set up temporary paths for the database and storage
    catalog_dir = tmp_path / "catalog"
    catalog_dir.mkdir()
    catalog_db_path = catalog_dir / "lake_catalog.db"
    storage_dir = tmp_path / "storage"
    storage_dir.mkdir()

    # Create the SourceConfig programmatically and return it
    return SourceConfig(
        name="local_lake",
        type=SourceType.DUCKLAKE,
        config={
            "catalog": {"type": "sqlite", "path": str(catalog_db_path)},
            "storage": {"type": "local", "path": str(storage_dir)}
        }
    )
