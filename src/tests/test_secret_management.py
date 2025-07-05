import sys

import pytest

sys.path.insert(0, 'src')

from quackpipe.secrets import EnvSecretProvider, JsonFileSecretProvider, set_secret_providers, fetch_secret_bundle
from quackpipe.exceptions import SecretError


def test_env_secret_provider(env_secrets):
    """Test EnvSecretProvider functionality."""
    provider = EnvSecretProvider()

    secrets = provider.get_secret('pg_prod')

    assert secrets['host'] == 'localhost'
    assert secrets['user'] == 'testuser'
    assert secrets['password'] == 'testpass'
    assert secrets['database'] == 'testdb'


def test_env_secret_provider_empty():
    """Test EnvSecretProvider with non-existent secret."""
    provider = EnvSecretProvider()

    secrets = provider.get_secret('nonexistent')

    assert secrets == {}


def test_json_file_secret_provider(json_secrets_dir):
    """Test JsonFileSecretProvider functionality."""
    provider = JsonFileSecretProvider(json_secrets_dir)

    secrets = provider.get_secret('pg_prod')

    assert secrets['host'] == 'json-localhost'
    assert secrets['user'] == 'json-user'
    assert secrets['password'] == 'json-pass'
    assert secrets['database'] == 'json-db'


def test_json_file_secret_provider_not_found(json_secrets_dir):
    """Test JsonFileSecretProvider with non-existent file."""
    provider = JsonFileSecretProvider(json_secrets_dir)

    secrets = provider.get_secret('nonexistent')

    assert secrets == {}


def test_set_secret_providers():
    """Test setting custom secret providers."""
    provider1 = EnvSecretProvider()
    provider2 = JsonFileSecretProvider()

    set_secret_providers([provider1, provider2])

    # This would test the global _providers variable
    # We'll verify by testing fetch_secret_bundle behavior


def test_set_secret_providers_invalid():
    """Test setting invalid secret providers."""
    with pytest.raises(TypeError):
        set_secret_providers("not a list")

    with pytest.raises(TypeError):
        set_secret_providers(["not a provider"])


def test_fetch_secret_bundle_success(env_secrets):
    """Test successful secret bundle fetching."""
    # Reset to default provider
    set_secret_providers([EnvSecretProvider()])

    secrets = fetch_secret_bundle('pg_prod')

    assert secrets['host'] == 'localhost'
    assert secrets['user'] == 'testuser'


def test_fetch_secret_bundle_not_found():
    """Test secret bundle not found."""
    set_secret_providers([EnvSecretProvider()])

    with pytest.raises(SecretError, match="Secret bundle 'nonexistent' not found"):
        fetch_secret_bundle('nonexistent')


def test_fetch_secret_bundle_empty_name():
    """Test fetch_secret_bundle with empty name."""
    result = fetch_secret_bundle('')
    assert result == {}


def test_secret_provider_chain(env_secrets, json_secrets_dir):
    """Test multiple secret providers in chain."""
    # Set up providers where JSON has priority
    json_provider = JsonFileSecretProvider(json_secrets_dir)
    env_provider = EnvSecretProvider()

    set_secret_providers([json_provider, env_provider])

    # Should get JSON version (first in chain)
    secrets = fetch_secret_bundle('pg_prod')
    assert secrets['host'] == 'json-localhost'

    # Reset to default
    set_secret_providers([EnvSecretProvider()])