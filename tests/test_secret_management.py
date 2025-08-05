from quackpipe.secrets import configure_secret_provider, fetch_secret_bundle


def test_fetch_secret_bundle_from_os_environ(monkeypatch):
    """
    Tests the default behavior: fetching secrets from pre-existing
    environment variables without loading a .env file.
    """
    # Arrange: Use monkeypatch to set system environment variables
    monkeypatch.setenv("PROD_DB_HOST", "db.example.com")
    monkeypatch.setenv("PROD_DB_USER", "prod_user")
    # This variable should not be picked up
    monkeypatch.setenv("STAGING_DB_USER", "staging_user")

    # Ensure the provider is in its default state
    configure_secret_provider(env_file=None)

    # Act
    secrets = fetch_secret_bundle("prod_db")

    # Assert
    assert secrets == {
        'host': 'db.example.com',
        'user': 'prod_user'
    }


def test_configure_and_fetch_from_env_file(tmp_path):
    """
    Tests loading secrets from a specified .env file.
    """
    # Arrange: Create a temporary .env file
    env_content = (
        "MINIO_STORAGE_ACCESS_KEY_ID=minio_key\n"
        "MINIO_STORAGE_SECRET_ACCESS_KEY=minio_secret\n"
    )
    p = tmp_path / "test.env"
    p.write_text(env_content)

    # Act: Configure the provider to use our temporary file
    configure_secret_provider(env_file=str(p))
    secrets = fetch_secret_bundle("minio_storage")

    # Assert
    assert secrets == {
        'access_key_id': 'minio_key',
        'secret_access_key': 'minio_secret'
    }


def test_env_file_overrides_os_environ(monkeypatch, tmp_path):
    """
    Tests that variables loaded from a .env file take precedence over
    existing system environment variables.
    """
    # Arrange: Set a variable in the system environment
    monkeypatch.setenv("PG_CATALOG_HOST", "system.host.com")
    monkeypatch.setenv("PG_CATALOG_USER", "system_user")

    # Create a .env file with an overriding value and a new value
    env_content = (
        "PG_CATALOG_HOST=file.host.com\n"
        "PG_CATALOG_PASSWORD=file_password\n"
    )
    p = tmp_path / "test.env"
    p.write_text(env_content)

    # Act: Configure the provider to use the file
    configure_secret_provider(env_file=str(p))
    secrets = fetch_secret_bundle("pg_catalog")

    # Assert
    assert secrets == {
        'host': 'file.host.com',  # Value from file should win
        'user': 'system_user',  # Value from system should persist
        'password': 'file_password'  # New value from file should be present
    }


def test_fetch_secret_bundle_no_match():
    """
    Tests that an empty dictionary is returned when no environment variables
    match the given secret name prefix.
    """
    # Arrange: Ensure the provider is in its default state
    configure_secret_provider(env_file=None)

    # Act
    secrets = fetch_secret_bundle("non_existent_secret")

    # Assert
    assert secrets == {}


def test_configure_with_nonexistent_env_file(caplog):
    """
    Tests that a warning is printed to stdout if the specified .env file
    is not found, and the system falls back to os.environ.
    """
    # Arrange
    non_existent_path = "/path/that/does/not/exist/.env"

    # Act
    configure_secret_provider(env_file=non_existent_path)

    # Assert: Check that the warning was logged
    assert f"Warning: env_file '{non_existent_path}' not found." in caplog.text
